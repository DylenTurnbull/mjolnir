//! MCP bridge exposed to the ACP host running Thor.

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    EnvVariable, McpServer, McpServerStdio, PermissionOptionKind, SessionUpdate, StopReason,
    ToolCall, ToolCallStatus, ToolCallUpdate, ToolKind, Usage, UsageUpdate,
};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::acp::{self, AcpRuntimeConfig};
use crate::config::{self, Config, SelectedAgent};
use crate::event::{PermissionDecision, UiCommand, UiEvent, content_block_text};
use crate::thor;
use crate::thor_catalog::{self, CatalogRequest};
use crate::thor_probe::{self, AgentValidation, QuotaSnapshot};

const DEFAULT_WORKER_TIMEOUT: Duration = Duration::from_secs(900);
const MAX_WORKER_TIMEOUT_SECONDS: u64 = 7_200;

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

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListAgentsArgs {
    #[serde(default)]
    refresh_quota: bool,
    #[serde(default)]
    validate: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunAgentArgs {
    #[serde(default)]
    job_id: Option<String>,
    source_id: String,
    prompt: String,
    cwd: Option<PathBuf>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    permission_mode: BridgePermissionMode,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunAgentBatchArgs {
    jobs: Vec<RunAgentJob>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunAgentJob {
    id: Option<String>,
    source_id: String,
    prompt: String,
    cwd: Option<PathBuf>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    permission_mode: BridgePermissionMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BridgePermissionMode {
    #[default]
    Reject,
    AcceptEdits,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentSummary {
    source_id: String,
    command: String,
    args: Vec<String>,
    quota: Vec<QuotaSnapshot>,
    validation: Option<AgentValidation>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DelegatedRunResult {
    job_id: Option<String>,
    source_id: String,
    final_text: String,
    text: String,
    stop_reason: String,
    usage: Option<Usage>,
    context_usage: Option<ContextUsageSummary>,
    quota: Vec<QuotaSnapshot>,
    tool_calls: Vec<ToolSummary>,
    progress: Vec<ProgressEvent>,
    permissions: Vec<String>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchRunResult {
    jobs: Vec<DelegatedRunResult>,
    aggregate_usage: UsageAggregate,
    progress: Vec<BatchProgressEvent>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageAggregate {
    jobs: usize,
    jobs_with_usage: usize,
    total_tokens: u64,
    input_tokens: u64,
    output_tokens: u64,
    thought_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ContextUsageSummary {
    used: u64,
    size: u64,
    cost: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolSummary {
    id: String,
    title: String,
    kind: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProgressEvent {
    sequence: usize,
    kind: String,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchProgressEvent {
    sequence: usize,
    job_id: Option<String>,
    source_id: String,
    kind: String,
    detail: String,
}

pub fn mcp_servers(config_path: PathBuf) -> Result<Vec<McpServer>> {
    Ok(vec![stdio_mcp_server(
        std::env::current_exe().context("resolve current mj executable")?,
        config_path,
    )])
}

fn stdio_mcp_server(command: PathBuf, config_path: PathBuf) -> McpServer {
    McpServer::Stdio(
        McpServerStdio::new(thor::THOR_MCP_SERVER_NAME, command)
            .args(vec!["thor-mcp".to_string()])
            .env(vec![EnvVariable::new(
                "MJ_THOR_CONFIG",
                config_path.to_string_lossy().into_owned(),
            )]),
    )
}

pub async fn run_stdio() -> Result<()> {
    let stdin = std::io::stdin();
    let mut reader = std::io::BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();

    while let Some(message) = read_message(&mut reader)? {
        let request: RpcRequest = match serde_json::from_slice(&message) {
            Ok(request) => request,
            Err(error) => {
                let response = RpcResponse {
                    jsonrpc: "2.0",
                    id: Value::Null,
                    result: None,
                    error: Some(RpcError {
                        code: -32700,
                        message: "parse error",
                        data: Some(Value::String(error.to_string())),
                    }),
                };
                write_message(&mut writer, &serde_json::to_vec(&response)?)?;
                continue;
            }
        };
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
            "description": "List ACP agents mj can launch as Thor workers, including cached direct provider quota signals when available.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "refreshQuota": {
                        "type": "boolean",
                        "description": "When true, actively refresh quota through direct Claude Code /usage and Codex appserver account/rateLimits/read queries before returning workers."
                    },
                    "validate": {
                        "type": "boolean",
                        "description": "When true, launch each worker and verify it completes ACP initialize plus session startup before returning workers."
                    }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_refresh_quota",
            "description": "Actively refresh quota/capacity hints for configured workers through direct provider queries: Claude Code /usage and Codex appserver account/rateLimits/read.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_validate_acp_agents",
            "description": "Launch configured ACP workers and report which ones initialize and open a session successfully.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_run_acp_agent",
            "description": "Run a prompt on one configured ACP worker and return final text, structured progress, tool calls, usage, and permission summary.",
            "inputSchema": {
                "type": "object",
                "required": ["sourceId", "prompt"],
                "properties": {
                    "sourceId": { "type": "string" },
                    "prompt": { "type": "string" },
                    "cwd": { "type": "string" },
                    "timeoutSeconds": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 7200,
                        "description": "Overall deadline for the delegated worker run. Defaults to 900 seconds."
                    },
                    "permissionMode": {
                        "type": "string",
                        "enum": ["reject", "accept_edits"]
                    }
                }
            }
        }),
        json!({
            "name": "thor_run_acp_agents",
            "description": "Run multiple configured ACP worker prompts concurrently and return per-worker results plus aggregate usage/progress.",
            "inputSchema": {
                "type": "object",
                "required": ["jobs"],
                "properties": {
                    "jobs": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 8,
                        "items": {
                            "type": "object",
                            "required": ["sourceId", "prompt"],
                            "properties": {
                                "id": { "type": "string" },
                                "sourceId": { "type": "string" },
                                "prompt": { "type": "string" },
                                "cwd": { "type": "string" },
                                "timeoutSeconds": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 7200,
                                    "description": "Overall deadline for this delegated worker run. Defaults to 900 seconds."
                                },
                                "permissionMode": {
                                    "type": "string",
                                    "enum": ["reject", "accept_edits"]
                                }
                            }
                        }
                    }
                }
            }
        }),
        json!({
            "name": "thor_get_model_catalog",
            "description": "Return Thor's cached model strength/pricing catalog, refreshing LM Arena/OpenRouter metadata when requested.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "refresh": { "type": "boolean" },
                    "maxAgeSeconds": { "type": "integer", "minimum": 0 }
                }
            }
        }),
    ]
}

async fn call_tool(params: ToolCallParams, config_path: Option<PathBuf>) -> Result<Value> {
    match params.name.as_str() {
        "thor_list_acp_agents" => {
            let config = load_config(config_path.as_ref())?;
            let workers = thor::worker_catalog(&config);
            let args: ListAgentsArgs = if params.arguments.is_null() {
                ListAgentsArgs::default()
            } else {
                serde_json::from_value(params.arguments)?
            };
            let mut quota = thor_probe::load_quota_snapshots().unwrap_or_default();
            if args.refresh_quota {
                let refreshed = thor_probe::refresh_configured_quota_snapshots(&workers).await;
                if !refreshed.is_empty() {
                    quota = refreshed;
                }
            }
            let validations = if args.validate {
                let cwd = std::env::current_dir().context("current dir")?;
                thor_probe::validate_agents(&workers, cwd).await
            } else {
                Vec::new()
            };
            let agents = workers
                .into_iter()
                .map(|agent| AgentSummary {
                    quota: quota
                        .iter()
                        .filter(|snapshot| snapshot.source_id == agent.source_id)
                        .cloned()
                        .collect(),
                    validation: validations
                        .iter()
                        .find(|validation| validation.source_id == agent.source_id)
                        .cloned(),
                    source_id: agent.source_id,
                    command: agent.program.to_string_lossy().into_owned(),
                    args: agent.args,
                })
                .collect::<Vec<_>>();
            Ok(tool_text_result(&serde_json::to_string_pretty(&agents)?))
        }
        "thor_refresh_quota" => {
            let config = load_config(config_path.as_ref())?;
            let snapshots =
                thor_probe::refresh_configured_quota_snapshots(&thor::worker_catalog(&config))
                    .await;
            Ok(tool_text_result(&serde_json::to_string_pretty(&snapshots)?))
        }
        "thor_validate_acp_agents" => {
            let config = load_config(config_path.as_ref())?;
            let cwd = std::env::current_dir().context("current dir")?;
            let validations =
                thor_probe::validate_agents(&thor::worker_catalog(&config), cwd).await;
            Ok(tool_text_result(&serde_json::to_string_pretty(
                &validations,
            )?))
        }
        "thor_run_acp_agent" => {
            let args: RunAgentArgs = serde_json::from_value(params.arguments)?;
            let result = run_agent(args, config_path.as_ref()).await?;
            Ok(tool_text_result(&serde_json::to_string_pretty(&result)?))
        }
        "thor_run_acp_agents" => {
            let args: RunAgentBatchArgs = serde_json::from_value(params.arguments)?;
            let result = run_agent_batch(args, config_path.as_ref()).await?;
            Ok(tool_text_result(&serde_json::to_string_pretty(&result)?))
        }
        "thor_get_model_catalog" => {
            let config = load_config(config_path.as_ref())?;
            let request: CatalogRequest = if params.arguments.is_null() {
                CatalogRequest::default()
            } else {
                serde_json::from_value(params.arguments)?
            };
            let catalog = thor_catalog::load_or_refresh_catalog(&config.thor, request).await?;
            Ok(tool_text_result(&serde_json::to_string_pretty(&catalog)?))
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

async fn run_agent_batch(
    args: RunAgentBatchArgs,
    config_path: Option<&PathBuf>,
) -> Result<BatchRunResult> {
    if args.jobs.is_empty() {
        bail!("empty delegated job list");
    }
    if args.jobs.len() > 8 {
        bail!("too many delegated jobs: max 8");
    }

    let config_path = config_path.cloned();
    let futures = args.jobs.into_iter().map(|job| {
        let config_path = config_path.clone();
        async move {
            let job_id = job.id.clone();
            let source_id = job.source_id.clone();
            let result = run_agent(job.into(), config_path.as_ref()).await;
            (job_id, source_id, result)
        }
    });
    let completed = futures::future::join_all(futures).await;

    let mut progress = Vec::new();
    let mut jobs = Vec::new();
    for (idx, (job_id, source_id, result)) in completed.into_iter().enumerate() {
        progress.push(BatchProgressEvent {
            sequence: idx + 1,
            job_id: job_id.clone(),
            source_id: source_id.clone(),
            kind: "worker_finished".to_string(),
            detail: match &result {
                Ok(result) => format!("{} stopped with {}", result.source_id, result.stop_reason),
                Err(error) => format!("{source_id} failed: {error}"),
            },
        });
        match result {
            Ok(result) => jobs.push(result),
            Err(error) => jobs.push(DelegatedRunResult {
                job_id,
                source_id,
                final_text: String::new(),
                text: String::new(),
                stop_reason: "error".to_string(),
                usage: None,
                context_usage: None,
                quota: Vec::new(),
                tool_calls: Vec::new(),
                progress: vec![ProgressEvent {
                    sequence: 1,
                    kind: "error".to_string(),
                    detail: error.to_string(),
                }],
                permissions: Vec::new(),
                error: Some(error.to_string()),
            }),
        }
    }

    Ok(BatchRunResult {
        aggregate_usage: aggregate_usage(&jobs),
        jobs,
        progress,
    })
}

async fn run_agent_prompt(agent: SelectedAgent, args: RunAgentArgs) -> Result<DelegatedRunResult> {
    let cwd = delegated_cwd(args.cwd.as_ref())?;
    let worker_timeout = worker_timeout(args.timeout_seconds);
    let deadline = tokio::time::Instant::now() + worker_timeout;
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
    let mut stop_reason_text = None;
    let mut usage = None;
    let mut context_usage = None;
    let quota = Vec::<QuotaSnapshot>::new();
    let mut error = None;
    let mut permissions = Vec::new();
    let mut tool_calls = Vec::<ToolSummary>::new();
    let mut progress = Vec::<ProgressEvent>::new();

    loop {
        let event = match tokio::time::timeout_at(deadline, event_rx.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                if error.is_none() && stop_reason.is_none() {
                    stop_reason_text = Some("worker_closed".to_string());
                    push_progress(
                        &mut progress,
                        "worker_closed",
                        "worker event channel closed",
                    );
                }
                break;
            }
            Err(_) => {
                let message = format!(
                    "delegated worker timed out after {}s",
                    worker_timeout.as_secs()
                );
                stop_reason_text = Some("timeout".to_string());
                push_progress(&mut progress, "timeout", message.clone());
                error = Some(message);
                break;
            }
        };
        match event {
            UiEvent::SessionStarted { .. } if !prompt_sent => {
                prompt_sent = true;
                push_progress(&mut progress, "session_started", "worker session ready");
                cmd_tx
                    .send(UiCommand::SendPrompt {
                        text: args.prompt.clone(),
                        images: Vec::new(),
                    })
                    .context("send delegated prompt")?;
                push_progress(&mut progress, "prompt_sent", "delegated prompt sent");
            }
            UiEvent::SessionUpdate(SessionUpdate::UserMessageChunk(_)) if prompt_sent => {
                collecting_turn_output = true;
            }
            UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(chunk)) if prompt_sent => {
                collecting_turn_output = true;
                push_progress(
                    &mut progress,
                    "agent_thought",
                    preview(&content_block_text(&chunk.content), 160),
                );
            }
            UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(chunk))
                if collecting_turn_output =>
            {
                let text = content_block_text(&chunk.content);
                final_text.push_str(&text);
                push_progress(&mut progress, "agent_message", preview(&text, 160));
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCall(tool_call)) => {
                push_progress(
                    &mut progress,
                    "tool_call",
                    format!("{} ({})", tool_call.title, tool_kind_label(tool_call.kind)),
                );
                upsert_tool_call_summary(&mut tool_calls, &tool_call);
                if prompt_sent {
                    collecting_turn_output = true;
                }
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCallUpdate(update)) => {
                upsert_tool_update_summary(&mut tool_calls, &update);
                if let Some(title) = update.fields.title {
                    push_progress(&mut progress, "tool_update", title);
                }
                if prompt_sent {
                    collecting_turn_output = true;
                }
            }
            UiEvent::SessionUpdate(SessionUpdate::UsageUpdate(update)) => {
                context_usage = Some(context_usage_summary(update));
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
                push_progress(
                    &mut progress,
                    "permission",
                    format!(
                        "{} {}",
                        prompt.tool_call.tool_call_id,
                        if decision.is_some() {
                            "accepted"
                        } else {
                            "rejected"
                        }
                    ),
                );
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
                push_progress(
                    &mut progress,
                    "prompt_done",
                    stop_reason_label(reason).to_string(),
                );
                break;
            }
            UiEvent::PromptFailed { message }
            | UiEvent::SessionForkFailed { message }
            | UiEvent::Fatal(message) => {
                push_progress(&mut progress, "error", message.clone());
                error = Some(message);
                break;
            }
            UiEvent::Connected { .. }
            | UiEvent::SessionStarted { .. }
            | UiEvent::TerminalOutput(_)
            | UiEvent::SessionConfigOptions
            | UiEvent::CancelPendingPermissions
            | UiEvent::RemotePermissionDecision { .. }
            | UiEvent::Warning(_)
            | UiEvent::Info(_) => {}
            UiEvent::SessionUpdate(_) => {}
        }
    }

    let _ = cmd_tx.send(UiCommand::Shutdown);
    let _ = tokio::time::timeout(Duration::from_secs(2), runtime).await;
    let reason = stop_reason_text.unwrap_or_else(|| {
        stop_reason
            .map(stop_reason_label)
            .unwrap_or("worker_closed")
            .to_string()
    });
    let text = final_text.clone();
    Ok(DelegatedRunResult {
        job_id: args.job_id,
        source_id,
        final_text,
        text,
        stop_reason: reason,
        usage,
        context_usage,
        quota,
        tool_calls,
        progress,
        permissions,
        error,
    })
}

impl From<RunAgentJob> for RunAgentArgs {
    fn from(job: RunAgentJob) -> Self {
        Self {
            job_id: job.id,
            source_id: job.source_id,
            prompt: job.prompt,
            cwd: job.cwd,
            timeout_seconds: job.timeout_seconds,
            permission_mode: job.permission_mode,
        }
    }
}

fn delegated_cwd(requested: Option<&PathBuf>) -> Result<PathBuf> {
    let workspace = std::env::current_dir()
        .context("current dir")?
        .canonicalize()
        .context("canonicalize current dir")?;
    let requested = match requested {
        Some(path) if path.is_absolute() => path.clone(),
        Some(path) => workspace.join(path),
        None => workspace.clone(),
    };
    let cwd = requested
        .canonicalize()
        .with_context(|| format!("canonicalize delegated cwd {}", requested.display()))?;
    if !cwd.starts_with(&workspace) {
        bail!(
            "delegated cwd {} is outside workspace {}",
            cwd.display(),
            workspace.display()
        );
    }
    Ok(cwd)
}

fn worker_timeout(timeout_seconds: Option<u64>) -> Duration {
    timeout_seconds
        .map(|seconds| seconds.clamp(1, MAX_WORKER_TIMEOUT_SECONDS))
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_WORKER_TIMEOUT)
}

fn push_progress(
    progress: &mut Vec<ProgressEvent>,
    kind: impl Into<String>,
    detail: impl Into<String>,
) {
    progress.push(ProgressEvent {
        sequence: progress.len() + 1,
        kind: kind.into(),
        detail: detail.into(),
    });
}

fn context_usage_summary(update: UsageUpdate) -> ContextUsageSummary {
    ContextUsageSummary {
        used: update.used,
        size: update.size,
        cost: update
            .cost
            .map(|cost| format!("{:.4} {}", cost.amount, cost.currency)),
    }
}

fn aggregate_usage(jobs: &[DelegatedRunResult]) -> UsageAggregate {
    let mut aggregate = UsageAggregate {
        jobs: jobs.len(),
        ..UsageAggregate::default()
    };
    for usage in jobs.iter().filter_map(|job| job.usage.as_ref()) {
        aggregate.jobs_with_usage += 1;
        aggregate.total_tokens = aggregate.total_tokens.saturating_add(usage.total_tokens);
        aggregate.input_tokens = aggregate.input_tokens.saturating_add(usage.input_tokens);
        aggregate.output_tokens = aggregate.output_tokens.saturating_add(usage.output_tokens);
        aggregate.thought_tokens = aggregate
            .thought_tokens
            .saturating_add(usage.thought_tokens.unwrap_or_default());
    }
    aggregate
}

fn upsert_tool_call_summary(tool_calls: &mut Vec<ToolSummary>, tool_call: &ToolCall) {
    let id = tool_call.tool_call_id.to_string();
    if let Some(existing) = tool_calls.iter_mut().find(|tool| tool.id == id) {
        existing.title = tool_call.title.clone();
        existing.kind = Some(tool_kind_label(tool_call.kind).to_string());
        existing.status = Some(tool_status_label(tool_call.status).to_string());
        return;
    }
    tool_calls.push(ToolSummary {
        id,
        title: tool_call.title.clone(),
        kind: Some(tool_kind_label(tool_call.kind).to_string()),
        status: Some(tool_status_label(tool_call.status).to_string()),
    });
}

fn upsert_tool_update_summary(tool_calls: &mut Vec<ToolSummary>, update: &ToolCallUpdate) {
    let id = update.tool_call_id.to_string();
    if let Some(existing) = tool_calls.iter_mut().find(|tool| tool.id == id) {
        if let Some(title) = update.fields.title.as_ref() {
            existing.title = title.clone();
        }
        if let Some(kind) = update.fields.kind {
            existing.kind = Some(tool_kind_label(kind).to_string());
        }
        if let Some(status) = update.fields.status {
            existing.status = Some(tool_status_label(status).to_string());
        }
        return;
    }
    tool_calls.push(ToolSummary {
        id,
        title: update
            .fields
            .title
            .clone()
            .unwrap_or_else(|| "tool call".to_string()),
        kind: update
            .fields
            .kind
            .map(|kind| tool_kind_label(kind).to_string()),
        status: update
            .fields
            .status
            .map(|status| tool_status_label(status).to_string()),
    });
}

fn preview(text: &str, max_chars: usize) -> String {
    let mut preview = text.replace('\n', " ");
    if preview.chars().count() > max_chars {
        preview = preview.chars().take(max_chars).collect::<String>();
        preview.push_str("...");
    }
    preview
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

fn tool_kind_label(kind: ToolKind) -> &'static str {
    match kind {
        ToolKind::Read => "read",
        ToolKind::Edit => "edit",
        ToolKind::Delete => "delete",
        ToolKind::Move => "move",
        ToolKind::Search => "search",
        ToolKind::Execute => "execute",
        ToolKind::Think => "think",
        ToolKind::Fetch => "fetch",
        ToolKind::SwitchMode => "switch_mode",
        ToolKind::Other => "other",
        _ => "other",
    }
}

fn tool_status_label(status: ToolCallStatus) -> &'static str {
    match status {
        ToolCallStatus::Pending => "pending",
        ToolCallStatus::InProgress => "in_progress",
        ToolCallStatus::Completed => "completed",
        ToolCallStatus::Failed => "failed",
        _ => "other",
    }
}

fn read_message(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>> {
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            return Ok(None);
        }
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        return Ok(Some(line));
    }
}

fn write_message(writer: &mut impl Write, body: &[u8]) -> Result<()> {
    writer.write_all(body)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdio_mcp_server_uses_current_binary_entrypoint_shape() {
        let server = stdio_mcp_server(PathBuf::from("/tmp/mj"), PathBuf::from("/tmp/config.toml"));
        let McpServer::Stdio(stdio) = server else {
            panic!("expected stdio MCP server");
        };
        assert_eq!(stdio.name, thor::THOR_MCP_SERVER_NAME);
        assert_eq!(stdio.command, PathBuf::from("/tmp/mj"));
        assert_eq!(stdio.args, vec!["thor-mcp"]);
        assert_eq!(stdio.env[0].name, "MJ_THOR_CONFIG");
        assert_eq!(stdio.env[0].value, "/tmp/config.toml");
    }

    #[test]
    fn tool_definitions_include_catalog_and_batch_runner() {
        let tools = tool_definitions();
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();

        assert!(names.iter().any(|name| name == "thor_get_model_catalog"));
        assert!(names.iter().any(|name| name == "thor_validate_acp_agents"));
        assert!(names.iter().any(|name| name == "thor_refresh_quota"));
        assert!(names.iter().any(|name| name == "thor_run_acp_agents"));
        let list_tool = tools
            .iter()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some("thor_list_acp_agents"))
            .expect("list tool");
        assert!(
            list_tool
                .pointer("/inputSchema/properties/validate")
                .is_some()
        );
        let run_tool = tools
            .iter()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some("thor_run_acp_agent"))
            .expect("run tool");
        assert_eq!(
            run_tool.pointer("/inputSchema/properties/permissionMode/enum"),
            Some(&json!(["reject", "accept_edits"]))
        );
        assert!(
            run_tool
                .pointer("/inputSchema/properties/timeoutSeconds")
                .is_some()
        );
    }

    #[test]
    fn stdio_messages_are_newline_delimited_json() {
        let mut reader = std::io::Cursor::new(
            br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}
"#,
        );

        assert_eq!(
            read_message(&mut reader).expect("read message"),
            Some(br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_vec())
        );

        let mut writer = Vec::new();
        write_message(&mut writer, br#"{"jsonrpc":"2.0","id":1,"result":{}}"#)
            .expect("write message");

        assert_eq!(
            String::from_utf8(writer).expect("utf8"),
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"
        );
    }

    #[test]
    fn run_agent_args_reject_bypass_permission_mode() {
        let parsed = serde_json::from_value::<RunAgentArgs>(json!({
            "sourceId": "codex",
            "prompt": "work",
            "permissionMode": "bypass"
        }));

        assert!(parsed.is_err());
    }

    #[test]
    fn delegated_cwd_must_stay_inside_workspace() {
        let outside = PathBuf::from("..");

        assert!(delegated_cwd(Some(&outside)).is_err());
    }

    #[test]
    fn worker_timeout_defaults_and_clamps() {
        assert_eq!(worker_timeout(None), DEFAULT_WORKER_TIMEOUT);
        assert_eq!(worker_timeout(Some(0)), Duration::from_secs(1));
        assert_eq!(
            worker_timeout(Some(MAX_WORKER_TIMEOUT_SECONDS + 1)),
            Duration::from_secs(MAX_WORKER_TIMEOUT_SECONDS)
        );
    }

    #[test]
    fn aggregate_usage_sums_completed_worker_usage() {
        let jobs = vec![
            DelegatedRunResult {
                job_id: Some("a".to_string()),
                source_id: "claude".to_string(),
                final_text: String::new(),
                text: String::new(),
                stop_reason: "end_turn".to_string(),
                usage: Some(Usage::new(10, 4, 6)),
                context_usage: None,
                quota: Vec::new(),
                tool_calls: Vec::new(),
                progress: Vec::new(),
                permissions: Vec::new(),
                error: None,
            },
            DelegatedRunResult {
                job_id: Some("b".to_string()),
                source_id: "codex".to_string(),
                final_text: String::new(),
                text: String::new(),
                stop_reason: "error".to_string(),
                usage: None,
                context_usage: None,
                quota: Vec::new(),
                tool_calls: Vec::new(),
                progress: Vec::new(),
                permissions: Vec::new(),
                error: Some("failed".to_string()),
            },
        ];

        let aggregate = aggregate_usage(&jobs);
        assert_eq!(aggregate.jobs, 2);
        assert_eq!(aggregate.jobs_with_usage, 1);
        assert_eq!(aggregate.total_tokens, 10);
        assert_eq!(aggregate.input_tokens, 4);
        assert_eq!(aggregate.output_tokens, 6);
    }
}
