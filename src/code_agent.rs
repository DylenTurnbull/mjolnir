//! One-shot nested ACP agent orchestration exposed to the primary agent as MCP.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    HttpHeader, McpServer, McpServerHttp, SessionUpdate, StopReason,
};
use anyhow::{Context, Result, anyhow, bail};
use axum::extract::{Request, State};
use axum::http::{StatusCode, header::AUTHORIZATION};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::ToolCallContext, wrapper::Parameters},
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    tool, tool_router,
    transport::{
        StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_server::session::local::LocalSessionManager,
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::acp::{self, AcpRuntimeConfig, RuntimeAccessMode};
use crate::event::{CodeAgentEvent, CodeAgentOutcome, UiCommand, UiEvent, content_block_text};

pub const LABEL: &str = "codex";
pub const MCP_SERVER_NAME: &str = "mj-code-agent";
pub const PRIMARY_SESSION_DIRECTIVE: &str = "<mj-code-agent-policy>\nYou are the primary coordinator, not the implementation agent. This policy applies to every subsequent user request in this ACP session. If a user asks to create, modify, debug, refactor, test, or otherwise implement code, you MUST call the code_agent MCP tool from the mj-code-agent server before using any other tool. Do not inspect files, edit files, or run implementation commands yourself, even when the task is trivial. Pass the complete user request and all relevant context as standalone instructions, wait for the delegated agent to finish, then answer the user from its result. If a request requires no code changes, answer it yourself without calling code_agent. Do not call any tool now. Acknowledge this policy with exactly MJ_CODE_AGENT_POLICY_READY.\n</mj-code-agent-policy>";
const MCP_PATH: &str = "/mcp";

#[derive(Debug, Clone)]
pub struct Config {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub agent_stderr: Option<PathBuf>,
}

impl Config {
    pub fn codex(agent_stderr: Option<PathBuf>, env: HashMap<String, String>) -> Self {
        Self {
            command: PathBuf::from("npx"),
            args: vec![
                "-y".to_string(),
                "@agentclientprotocol/codex-acp".to_string(),
            ],
            env,
            agent_stderr,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunContext {
    pub cwd: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub fs_max_text_bytes: u64,
    pub access_mode: RuntimeAccessMode,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CodeAgentArgs {
    /// Complete, standalone coding task for the delegated agent.
    pub instructions: String,
}

#[derive(Clone)]
struct McpHandler {
    config: Config,
    context: RunContext,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    controller: Controller,
    tools_listed: watch::Sender<bool>,
    tool_router: ToolRouter<Self>,
}

#[tool_router(router = tool_router)]
impl McpHandler {
    fn new(
        config: Config,
        context: RunContext,
        ui_tx: mpsc::UnboundedSender<UiEvent>,
        controller: Controller,
        tools_listed: watch::Sender<bool>,
    ) -> Self {
        Self {
            config,
            context,
            ui_tx,
            controller,
            tools_listed,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "code_agent",
        description = "MANDATORY CODING ROUTER. For EVERY user request that asks to create, modify, debug, refactor, test, or otherwise implement code, call this tool BEFORE using any other tool or doing the work yourself. This rule applies even when the task is trivial and even when you could complete it directly. You are the coordinator: do not read, edit, write, or run commands for an implementation task yourself. Pass complete standalone instructions including the desired outcome, relevant constraints, and verification requirements. Wait for the delegated agent's final message, then use that result to finish your response. Do not call this tool only for explanation-only or informational requests that require no code changes."
    )]
    async fn code_agent(
        &self,
        Parameters(args): Parameters<CodeAgentArgs>,
    ) -> std::result::Result<CallToolResult, McpError> {
        if args.instructions.trim().is_empty() {
            return Err(McpError::invalid_params(
                "instructions must not be empty",
                None,
            ));
        }
        if !self.controller.begin().await {
            return Ok(CallToolResult::error(vec![Content::text(
                "a code agent run is already active",
            )]));
        }

        let result = run_boxed(
            self.config.clone(),
            self.context.clone(),
            args.instructions,
            self.ui_tx.clone(),
            self.controller.clone(),
        )
        .await;
        Ok(match result {
            Ok(message) => CallToolResult::success(vec![Content::text(message)]),
            Err(error) => CallToolResult::error(vec![Content::text(error.to_string())]),
        })
    }
}

impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("mj-code-agent", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "MANDATORY DELEGATION POLICY: You are the primary coordinator. Whenever the user asks to create, modify, debug, refactor, test, or otherwise implement code, you must call code_agent before any other tool. Never perform implementation work with your own file, edit, or terminal tools, even for a trivial task. Give code_agent complete standalone instructions, wait for it to finish, and then report its result. Use your own tools only for requests that require no code changes.",
            )
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListToolsResult, McpError>> + Send + '_ {
        let _ = self.tools_listed.send(true);
        std::future::ready(Ok(ListToolsResult::with_all_items(
            self.tool_router.list_all(),
        )))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<CallToolResult, McpError>> + Send + '_ {
        self.tool_router
            .call(ToolCallContext::new(self, request, context))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned()
    }
}

/// In-process, loopback-only MCP endpoint advertised to the primary ACP agent.
/// Dropping it cancels the listener and every open MCP session.
pub struct HttpServer {
    advertised: McpServer,
    tools_listed: watch::Receiver<bool>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl HttpServer {
    pub async fn start(
        config: Config,
        context: RunContext,
        ui_tx: mpsc::UnboundedSender<UiEvent>,
        controller: Controller,
    ) -> Result<Self> {
        let mut token_bytes = [0_u8; 32];
        getrandom::fill(&mut token_bytes)
            .map_err(|error| anyhow!("generate code-agent MCP bearer token: {error}"))?;
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_bytes);
        let authorization = format!("Bearer {token}");

        let (tools_listed_tx, tools_listed) = watch::channel(false);
        let handler = McpHandler::new(config, context, ui_tx, controller, tools_listed_tx);
        let cancellation = CancellationToken::new();
        let mut server_config = StreamableHttpServerConfig::default();
        server_config.cancellation_token = cancellation.clone();
        let service = StreamableHttpService::new(
            move || Ok(handler.clone()),
            Arc::new(LocalSessionManager::default()),
            server_config,
        );
        let protected = axum::Router::new().nest_service(MCP_PATH, service).layer(
            axum::middleware::from_fn_with_state(authorization.clone(), require_bearer),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind code-agent MCP listener")?;
        let addr = listener
            .local_addr()
            .context("read code-agent MCP listener address")?;
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, protected)
                .with_graceful_shutdown(task_cancellation.cancelled_owned())
                .await
            {
                tracing::warn!("code-agent MCP listener stopped: {error}");
            }
        });
        let advertised = McpServer::Http(
            McpServerHttp::new(MCP_SERVER_NAME, format!("http://{addr}{MCP_PATH}"))
                .headers(vec![HttpHeader::new("Authorization", authorization)]),
        );
        Ok(Self {
            advertised,
            tools_listed,
            cancellation,
            task,
        })
    }

    pub fn advertised(&self) -> &McpServer {
        &self.advertised
    }

    pub async fn wait_until_tools_listed(&self, timeout: Duration) -> Result<()> {
        let mut tools_listed = self.tools_listed.clone();
        if *tools_listed.borrow() {
            return Ok(());
        }
        tokio::time::timeout(timeout, tools_listed.changed())
            .await
            .map_err(|_| anyhow!("primary agent timed out loading the code_agent MCP tool"))?
            .map_err(|_| anyhow!("code-agent MCP server closed before tools/list"))?;
        Ok(())
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

async fn require_bearer(
    State(expected): State<String>,
    request: Request,
    next: Next,
) -> std::result::Result<Response, (StatusCode, &'static str)> {
    let authorized = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.as_bytes() == expected.as_bytes());
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err((StatusCode::UNAUTHORIZED, "unauthorized"))
    }
}

#[derive(Debug, Default)]
enum ActiveRun {
    #[default]
    Idle,
    Starting {
        cancel_requested: bool,
        shutdown_requested: bool,
    },
    Running {
        commands: mpsc::UnboundedSender<UiCommand>,
    },
}

/// Coordinates the one allowed nested run and routes primary UI cancellation
/// without giving the nested runtime ownership of the primary command stream.
#[derive(Debug, Clone, Default)]
pub struct Controller {
    state: Arc<Mutex<ActiveRun>>,
}

impl Controller {
    pub async fn begin(&self) -> bool {
        let mut state = self.state.lock().await;
        if !matches!(*state, ActiveRun::Idle) {
            return false;
        }
        *state = ActiveRun::Starting {
            cancel_requested: false,
            shutdown_requested: false,
        };
        true
    }

    async fn attach(&self, commands: mpsc::UnboundedSender<UiCommand>) {
        let mut state = self.state.lock().await;
        let (cancel_requested, shutdown_requested) = match *state {
            ActiveRun::Starting {
                cancel_requested,
                shutdown_requested,
            } => (cancel_requested, shutdown_requested),
            _ => (false, false),
        };
        *state = ActiveRun::Running {
            commands: commands.clone(),
        };
        if shutdown_requested {
            let _ = commands.send(UiCommand::Shutdown);
        } else if cancel_requested {
            let _ = commands.send(UiCommand::CancelPrompt);
        }
    }

    pub async fn cancel(&self) -> bool {
        let mut state = self.state.lock().await;
        match &mut *state {
            ActiveRun::Idle => false,
            ActiveRun::Starting {
                cancel_requested, ..
            } => {
                *cancel_requested = true;
                true
            }
            ActiveRun::Running { commands } => {
                let _ = commands.send(UiCommand::CancelPrompt);
                true
            }
        }
    }

    pub async fn shutdown(&self) -> bool {
        let mut state = self.state.lock().await;
        match &mut *state {
            ActiveRun::Idle => false,
            ActiveRun::Starting {
                shutdown_requested, ..
            } => {
                *shutdown_requested = true;
                true
            }
            ActiveRun::Running { commands } => {
                let _ = commands.send(UiCommand::Shutdown);
                true
            }
        }
    }

    pub async fn finish(&self) {
        *self.state.lock().await = ActiveRun::Idle;
    }
}

struct AgentMessageCollector {
    last: String,
    message_open: bool,
}

impl AgentMessageCollector {
    fn new() -> Self {
        Self {
            last: String::new(),
            message_open: false,
        }
    }

    fn observe(&mut self, update: &SessionUpdate) {
        match update {
            SessionUpdate::AgentMessageChunk(chunk) => {
                if !self.message_open {
                    self.last.clear();
                    self.message_open = true;
                }
                self.last.push_str(&content_block_text(&chunk.content));
            }
            SessionUpdate::UserMessageChunk(_)
            | SessionUpdate::AgentThoughtChunk(_)
            | SessionUpdate::ToolCall(_)
            | SessionUpdate::Plan(_) => self.message_open = false,
            _ => {}
        }
    }

    fn finish(self) -> Result<String> {
        if self.last.trim().is_empty() {
            bail!("code agent completed without a final message");
        }
        Ok(self.last)
    }
}

pub fn run_boxed(
    config: Config,
    context: RunContext,
    instructions: String,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    controller: Controller,
) -> futures::future::BoxFuture<'static, Result<String>> {
    Box::pin(run(config, context, instructions, ui_tx, controller))
}

async fn run(
    config: Config,
    context: RunContext,
    instructions: String,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    controller: Controller,
) -> Result<String> {
    let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Started {
        label: LABEL.to_string(),
        instructions: instructions.clone(),
    }));

    let (nested_event_tx, mut nested_event_rx) = mpsc::unbounded_channel();
    let (nested_cmd_tx, nested_cmd_rx) = mpsc::unbounded_channel();
    controller.attach(nested_cmd_tx.clone()).await;

    let runtime_config = AcpRuntimeConfig {
        command: config.command,
        args: config.args,
        cwd: context.cwd,
        additional_directories: context.additional_directories,
        mcp_servers: Vec::new(),
        resume_session: None,
        env: config.env,
        agent_stderr: config.agent_stderr,
        fs_max_text_bytes: context.fs_max_text_bytes,
        access_mode: context.access_mode,
        agent_source_id: None,
        config_path: None,
        saved_session_config: HashMap::new(),
        code_agent: None,
    };
    let mut runtime = tokio::spawn(acp::run(runtime_config, nested_event_tx, nested_cmd_rx));

    let mut prompt_sent = false;
    let mut collector = AgentMessageCollector::new();
    let result = loop {
        tokio::select! {
            joined = &mut runtime => {
                break match joined {
                    Ok(Ok(())) => Err(anyhow!("code agent runtime closed before completing")),
                    Ok(Err(error)) => Err(error).context("code agent runtime"),
                    Err(error) => Err(anyhow!("code agent task failed: {error}")),
                };
            }
            event = nested_event_rx.recv() => {
                let Some(event) = event else {
                    break Err(anyhow!("code agent event stream closed before completing"));
                };
                match event {
                    UiEvent::Connected { .. } => {}
                    UiEvent::SessionStarted { .. } if !prompt_sent => {
                        prompt_sent = true;
                        if nested_cmd_tx
                            .send(UiCommand::SendPrompt {
                                text: instructions.clone(),
                                images: Vec::new(),
                            })
                            .is_err()
                        {
                            break Err(anyhow!("send instructions to code agent"));
                        }
                    }
                    UiEvent::SessionStarted { .. } | UiEvent::SessionConfigOptions { .. } => {}
                    UiEvent::SessionUpdate(update) => {
                        collector.observe(&update);
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::SessionUpdate(update)));
                    }
                    UiEvent::TerminalOutput(snapshot) => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::TerminalOutput(snapshot)));
                    }
                    UiEvent::PermissionRequest(prompt) => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::PermissionRequest(prompt)));
                    }
                    UiEvent::ElicitationRequest(prompt) => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::ElicitationRequest(prompt)));
                    }
                    UiEvent::CancelPendingPermissions => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::CancelPendingPermissions));
                    }
                    UiEvent::Info(message) | UiEvent::Warning(message) => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Status(message)));
                    }
                    UiEvent::PromptDone { stop_reason, .. } => {
                        break if matches!(stop_reason, StopReason::Cancelled) {
                            Err(anyhow!("code agent cancelled"))
                        } else {
                            collector.finish()
                        };
                    }
                    UiEvent::PromptFailed { message }
                    | UiEvent::SessionForkFailed { message }
                    | UiEvent::Fatal(message) => break Err(anyhow!(message)),
                    UiEvent::ClaudeUsage(_)
                    | UiEvent::RemotePermissionDecision { .. }
                    | UiEvent::ActorActivity(_) => {}
                    UiEvent::CodeAgent(_) => {
                        break Err(anyhow!("nested code agent attempted recursive delegation"));
                    }
                }
            }
        }
    };

    let _ = nested_cmd_tx.send(UiCommand::Shutdown);
    if !runtime.is_finished()
        && tokio::time::timeout(Duration::from_secs(2), &mut runtime)
            .await
            .is_err()
    {
        runtime.abort();
    }
    controller.finish().await;

    let outcome = match &result {
        Ok(_) => CodeAgentOutcome::Completed,
        Err(error) if error.to_string().contains("cancelled") => CodeAgentOutcome::Cancelled,
        Err(error) => CodeAgentOutcome::Failed(error.to_string()),
    };
    let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Finished { outcome }));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{ContentBlock, ContentChunk, TextContent};

    fn test_handler() -> McpHandler {
        let (ui_tx, _ui_rx) = mpsc::unbounded_channel();
        let (tools_listed, _tools_listed_rx) = watch::channel(false);
        McpHandler::new(
            Config::codex(None, HashMap::new()),
            RunContext {
                cwd: PathBuf::from("/tmp/workspace"),
                additional_directories: Vec::new(),
                fs_max_text_bytes: 1024,
                access_mode: RuntimeAccessMode::Full,
            },
            ui_tx,
            Controller::default(),
            tools_listed,
        )
    }

    #[test]
    fn collector_returns_last_agent_message() {
        let mut collector = AgentMessageCollector::new();
        collector.observe(&SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("first")),
        )));
        collector.observe(&SessionUpdate::ToolCall(
            agent_client_protocol::schema::v1::ToolCall::new("tool", "work"),
        ));
        collector.observe(&SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("final")),
        )));
        assert_eq!(collector.finish().expect("message"), "final");
    }

    #[test]
    fn collector_rejects_message_less_completion() {
        let collector = AgentMessageCollector::new();
        assert!(collector.finish().is_err());
    }

    #[tokio::test]
    async fn controller_rejects_concurrent_runs_and_resets() {
        let controller = Controller::default();
        assert!(controller.begin().await);
        assert!(!controller.begin().await);
        assert!(controller.cancel().await);
        controller.finish().await;
        assert!(controller.begin().await);
    }

    #[tokio::test]
    async fn shutdown_requested_while_starting_reaches_nested_runtime() {
        let controller = Controller::default();
        assert!(controller.begin().await);
        assert!(controller.shutdown().await);
        let (commands, mut receiver) = mpsc::unbounded_channel();
        controller.attach(commands).await;
        assert!(matches!(receiver.recv().await, Some(UiCommand::Shutdown)));
    }

    #[test]
    fn tool_arguments_are_strict() {
        let parsed: CodeAgentArgs =
            serde_json::from_str(r#"{"instructions":"fix it"}"#).expect("valid arguments");
        assert_eq!(parsed.instructions, "fix it");
        assert!(
            serde_json::from_str::<CodeAgentArgs>(r#"{"instructions":"fix it","unexpected":true}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<CodeAgentArgs>("{}").is_err());
    }

    #[test]
    fn tool_is_model_visible_and_directs_coding_delegation() {
        let tools = test_handler().tool_router.list_all();
        assert_eq!(tools.len(), 1);
        let tool = serde_json::to_value(&tools[0]).expect("serialize tool");
        assert_eq!(tool["name"], "code_agent");
        let description = tool["description"].as_str().expect("description");
        assert!(description.contains("MANDATORY CODING ROUTER"));
        assert!(description.contains("BEFORE using any other tool"));
        assert_eq!(
            tool["inputSchema"]["required"],
            serde_json::json!(["instructions"])
        );
    }
}
