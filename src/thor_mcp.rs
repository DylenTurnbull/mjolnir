//! MCP bridge exposed to the ACP host running Thor.

use std::io::{BufRead, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    HttpHeader, McpServer, McpServerHttp, PermissionOptionKind, SessionUpdate, StopReason,
    ToolCallUpdate, ToolKind, Usage,
};
use anyhow::{Context, Result, anyhow, bail};
use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json as AxumJson, Router};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::acp::{self, AcpRuntimeConfig};
use crate::config::{self, Config, SelectedAgent};
use crate::event::{PermissionDecision, UiCommand, UiEvent, content_block_text};
use crate::thor;

#[derive(Debug, Deserialize)]
struct RpcRequest {
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct RpcResponse<'a> {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError<'a>>,
}

#[derive(Debug, Serialize)]
struct RpcError<'a> {
    code: i64,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunAgentArgs {
    source_id: String,
    prompt: String,
    cwd: Option<PathBuf>,
    #[serde(default)]
    permission_mode: BridgePermissionMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BridgePermissionMode {
    #[default]
    Reject,
    AcceptEdits,
    Bypass,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentSummary {
    source_id: String,
    command: String,
    args: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DelegatedRunResult {
    source_id: String,
    text: String,
    stop_reason: String,
    usage: Option<Usage>,
    permissions: Vec<String>,
    error: Option<String>,
}

#[derive(Clone)]
struct HttpState {
    config_path: PathBuf,
    token: String,
}

pub struct ThorMcpHttpServer {
    mcp_server: McpServer,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl ThorMcpHttpServer {
    pub fn mcp_servers(&self) -> Vec<McpServer> {
        vec![self.mcp_server.clone()]
    }
}

impl Drop for ThorMcpHttpServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.task.abort();
    }
}

pub fn start_http(config_path: PathBuf) -> Result<ThorMcpHttpServer> {
    let token = random_token()?;
    let listener = TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
        .context("bind Thor MCP HTTP server")?;
    listener
        .set_nonblocking(true)
        .context("set Thor MCP HTTP listener nonblocking")?;
    let addr = listener.local_addr().context("read Thor MCP HTTP addr")?;
    let listener =
        tokio::net::TcpListener::from_std(listener).context("create Tokio MCP listener")?;
    let url = format!("http://{addr}/mcp");
    let mcp_server = http_mcp_server(url, token.clone());
    let state = Arc::new(HttpState { config_path, token });
    let app = Router::new()
        .route("/mcp", post(handle_http_request))
        .with_state(state);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let shutdown = async move {
            let _ = shutdown_rx.await;
        };
        if let Err(error) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::debug!("Thor MCP HTTP server exited: {error:#}");
        }
    });
    Ok(ThorMcpHttpServer {
        mcp_server,
        shutdown_tx: Some(shutdown_tx),
        task,
    })
}

fn http_mcp_server(url: String, token: String) -> McpServer {
    McpServer::Http(
        McpServerHttp::new(thor::THOR_MCP_SERVER_NAME, url).headers(vec![HttpHeader::new(
            "Authorization",
            format!("Bearer {token}"),
        )]),
    )
}

fn random_token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow!("generate Thor MCP HTTP token: {error}"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

async fn handle_http_request(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
    AxumJson(request): AxumJson<RpcRequest>,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(id) = request.id.clone() else {
        return StatusCode::ACCEPTED.into_response();
    };
    let response = match handle_request_with_config(request, Some(state.config_path.clone())).await
    {
        Ok(result) => RpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        },
        Err(error) => RpcResponse {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(RpcError {
                code: -32000,
                message: "thor MCP bridge error",
                data: Some(Value::String(error.to_string())),
            }),
        },
    };
    (StatusCode::OK, AxumJson(response)).into_response()
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let Some(header) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    header == format!("Bearer {token}")
}

pub async fn run_stdio() -> Result<()> {
    let stdin = std::io::stdin();
    let mut reader = std::io::BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();

    while let Some(message) = read_message(&mut reader)? {
        let request: RpcRequest = serde_json::from_slice(&message).context("parse MCP request")?;
        let Some(id) = request.id.clone() else {
            continue;
        };
        let response = match handle_request_with_config(request, None).await {
            Ok(result) => RpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => RpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(RpcError {
                    code: -32000,
                    message: "thor MCP bridge error",
                    data: Some(Value::String(error.to_string())),
                }),
            },
        };
        write_message(&mut writer, &serde_json::to_vec(&response)?)?;
    }
    Ok(())
}

async fn handle_request_with_config(
    request: RpcRequest,
    config_path: Option<PathBuf>,
) -> Result<Value> {
    match request.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": {} },
            "serverInfo": {
                "name": thor::THOR_MCP_SERVER_NAME,
                "version": env!("CARGO_PKG_VERSION"),
            },
        })),
        "tools/list" => Ok(json!({ "tools": tool_definitions() })),
        "tools/call" => {
            let params: ToolCallParams = serde_json::from_value(request.params)?;
            call_tool(params, config_path).await
        }
        method => bail!("unsupported MCP method {method}"),
    }
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "thor_list_acp_agents",
            "description": "List ACP agents mj can launch as Thor workers.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_run_acp_agent",
            "description": "Run a prompt on a configured ACP worker and return its transcript, usage, and permission summary.",
            "inputSchema": {
                "type": "object",
                "required": ["sourceId", "prompt"],
                "properties": {
                    "sourceId": { "type": "string" },
                    "prompt": { "type": "string" },
                    "cwd": { "type": "string" },
                    "permissionMode": {
                        "type": "string",
                        "enum": ["reject", "accept_edits", "bypass"]
                    }
                }
            }
        }),
    ]
}

async fn call_tool(params: ToolCallParams, config_path: Option<PathBuf>) -> Result<Value> {
    match params.name.as_str() {
        "thor_list_acp_agents" => {
            let config = load_config(config_path.as_ref())?;
            let agents = thor::worker_catalog(&config)
                .into_iter()
                .map(|agent| AgentSummary {
                    source_id: agent.source_id,
                    command: agent.program.to_string_lossy().into_owned(),
                    args: agent.args,
                })
                .collect::<Vec<_>>();
            Ok(tool_text_result(&serde_json::to_string_pretty(&agents)?))
        }
        "thor_run_acp_agent" => {
            let args: RunAgentArgs = serde_json::from_value(params.arguments)?;
            let result = run_agent(args, config_path.as_ref()).await?;
            Ok(tool_text_result(&serde_json::to_string_pretty(&result)?))
        }
        name => bail!("unknown Thor MCP tool {name}"),
    }
}

fn tool_text_result(text: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": false
    })
}

fn load_config(config_path: Option<&PathBuf>) -> Result<Config> {
    let path = config_path.cloned().unwrap_or_else(|| {
        std::env::var_os("MJ_THOR_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(config::default_config_path)
    });
    Config::load(&path).with_context(|| format!("load {}", path.display()))
}

async fn run_agent(
    args: RunAgentArgs,
    config_path: Option<&PathBuf>,
) -> Result<DelegatedRunResult> {
    if args.prompt.trim().is_empty() {
        bail!("empty delegated prompt");
    }
    let config = load_config(config_path)?;
    let agent = thor::worker_catalog(&config)
        .into_iter()
        .find(|agent| agent.source_id == args.source_id)
        .ok_or_else(|| anyhow!("unknown ACP agent {}", args.source_id))?;
    run_agent_prompt(agent, args).await
}

async fn run_agent_prompt(agent: SelectedAgent, args: RunAgentArgs) -> Result<DelegatedRunResult> {
    let cwd = match args.cwd {
        Some(cwd) => cwd,
        None => std::env::current_dir().context("current dir")?,
    };
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let source_id = agent.source_id.clone();
    let runtime_cfg = AcpRuntimeConfig {
        command: agent.program,
        args: agent.args,
        cwd,
        additional_directories: Vec::new(),
        mcp_servers: Vec::new(),
        resume_session: None,
        env: agent.env,
        agent_stderr: None,
        fs_max_text_bytes: acp::DEFAULT_FS_TEXT_BYTES,
    };
    let runtime = tokio::spawn(acp::run(runtime_cfg, event_tx, cmd_rx));

    let mut final_text = String::new();
    let mut collecting_turn_output = false;
    let mut prompt_sent = false;
    let mut stop_reason = None;
    let mut usage = None;
    let mut error = None;
    let mut permissions = Vec::new();

    while let Some(event) = event_rx.recv().await {
        match event {
            UiEvent::SessionStarted { .. } if !prompt_sent => {
                prompt_sent = true;
                cmd_tx
                    .send(UiCommand::SendPrompt {
                        text: args.prompt.clone(),
                        images: Vec::new(),
                    })
                    .context("send delegated prompt")?;
            }
            UiEvent::SessionUpdate(SessionUpdate::UserMessageChunk(_))
            | UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(_))
                if prompt_sent =>
            {
                collecting_turn_output = true;
            }
            UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(chunk))
                if collecting_turn_output =>
            {
                final_text.push_str(&content_block_text(&chunk.content));
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCall(tool_call)) => {
                permissions.push(format!("tool: {}", tool_call.title));
                if prompt_sent {
                    collecting_turn_output = true;
                }
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCallUpdate(update)) => {
                if let Some(title) = update.fields.title {
                    permissions.push(format!("tool update: {title}"));
                }
                if prompt_sent {
                    collecting_turn_output = true;
                }
            }
            UiEvent::PermissionRequest(prompt) => {
                let decision =
                    permission_decision(args.permission_mode, &prompt.tool_call, &prompt.options);
                permissions.push(format!(
                    "permission {}: {}",
                    prompt.tool_call.tool_call_id,
                    if decision.is_some() {
                        "selected"
                    } else {
                        "cancelled"
                    }
                ));
                let _ = prompt.responder.send(match decision {
                    Some(option_id) => PermissionDecision::Selected(option_id),
                    None => PermissionDecision::Cancelled,
                });
            }
            UiEvent::PromptDone {
                stop_reason: reason,
                usage: prompt_usage,
            } => {
                stop_reason = Some(reason);
                usage = prompt_usage;
                break;
            }
            UiEvent::PromptFailed { message }
            | UiEvent::SessionForkFailed { message }
            | UiEvent::Fatal(message) => {
                error = Some(message);
                break;
            }
            UiEvent::Connected { .. }
            | UiEvent::SessionStarted { .. }
            | UiEvent::TerminalOutput(_)
            | UiEvent::SessionConfigOptions { .. }
            | UiEvent::CancelPendingPermissions
            | UiEvent::RemotePermissionDecision { .. }
            | UiEvent::Warning(_)
            | UiEvent::Info(_) => {}
            UiEvent::SessionUpdate(_) => {}
        }
    }

    let _ = cmd_tx.send(UiCommand::Shutdown);
    let _ = tokio::time::timeout(Duration::from_secs(2), runtime).await;
    let reason = stop_reason.unwrap_or(StopReason::Cancelled);
    Ok(DelegatedRunResult {
        source_id,
        text: final_text,
        stop_reason: stop_reason_label(reason).to_string(),
        usage,
        permissions,
        error,
    })
}

fn permission_decision(
    mode: BridgePermissionMode,
    tool_call: &ToolCallUpdate,
    options: &[agent_client_protocol::schema::v1::PermissionOption],
) -> Option<String> {
    let allow = match mode {
        BridgePermissionMode::Reject => false,
        BridgePermissionMode::AcceptEdits => matches!(
            tool_call.fields.kind,
            Some(ToolKind::Edit | ToolKind::Delete | ToolKind::Move)
        ),
        BridgePermissionMode::Bypass => true,
    };
    if !allow {
        return None;
    }
    options
        .iter()
        .find(|option| option.kind == PermissionOptionKind::AllowAlways)
        .or_else(|| {
            options
                .iter()
                .find(|option| option.kind == PermissionOptionKind::AllowOnce)
        })
        .map(|option| option.option_id.to_string())
}

fn stop_reason_label(reason: StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn => "end_turn",
        StopReason::MaxTokens => "max_tokens",
        StopReason::MaxTurnRequests => "max_turn_requests",
        StopReason::Refusal => "refusal",
        StopReason::Cancelled => "cancelled",
        _ => "other",
    }
}

fn read_message(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some(value) = trimmed.strip_prefix("Content-Length:") {
            content_length = Some(value.trim().parse::<usize>()?);
        }
    }
    let Some(len) = content_length else {
        bail!("missing MCP Content-Length header");
    };
    let mut body = vec![0; len];
    reader.read_exact(&mut body)?;
    Ok(Some(body))
}

fn write_message(writer: &mut impl Write, body: &[u8]) -> Result<()> {
    write!(writer, "Content-Length: {}\r\n\r\n", body.len())?;
    writer.write_all(body)?;
    writer.flush()?;
    Ok(())
}

#[allow(dead_code)]
fn _assert_runtime_sendable() {
    fn assert_send<T: Send>() {}
    assert_send::<Arc<AtomicBool>>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_mcp_server_uses_local_url_and_bearer_header() {
        let server = http_mcp_server(
            "http://127.0.0.1:49152/mcp".to_string(),
            "secret-token".to_string(),
        );
        let McpServer::Http(http) = server else {
            panic!("expected HTTP MCP server");
        };
        assert_eq!(http.name, thor::THOR_MCP_SERVER_NAME);
        assert_eq!(http.url, "http://127.0.0.1:49152/mcp");
        assert_eq!(http.headers[0].name, "Authorization");
        assert_eq!(http.headers[0].value, "Bearer secret-token");
    }
}
