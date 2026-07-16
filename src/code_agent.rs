//! One-shot nested ACP agent orchestration exposed to the primary agent as MCP.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    HttpHeader, McpServer, McpServerHttp, SessionUpdate, StopReason, UsageUpdate,
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
use crate::council::ResolvedRole;
use crate::council_usage::{Purpose, Record, Role};
use crate::event::{
    CodeAgentEvent, CodeAgentOutcome, InternalMessage, InternalMessageKind, UiCommand, UiEvent,
    content_block_text,
};
use crate::loki;
use crate::workspace_snapshot::{WorkspaceDelta, WorkspaceSnapshot};

pub const LABEL: &str = "Eitri";
pub const MCP_SERVER_NAME: &str = "mj-code-agent";
pub const PRIMARY_SESSION_DIRECTIVE: &str = r#"<mj-code-agent-policy>
You are Thor, the primary coordinator and owner of the user's outcome. You are responsible for understanding the request, doing necessary research and context gathering, forming the plan, coordinating implementation, reviewing and verifying the result, and delivering the final answer. You are not a thin handoff between the user and Eitri. This policy applies to every subsequent user request in this ACP session.

Eitri is available through optional MCP tools. explore_agent is a single read-only scout for bounded, multi-step codebase research at any point in ongoing work. explore_agents is the only way to request concurrent scouting: use it only for two or more independent, complete standalone prompts. It atomically admits and launches every requested scout together or rejects the batch for insufficient capacity; it never queues or serializes overflow work. Do not claim scouts are parallel merely because you made separate explore_agent calls—those calls may be sequential. A concurrency claim is justified only after explore_agents reports that it launched the batch concurrently. Direct tools are usually faster for a known path, known symbol, exact definition, work confined to roughly two or three known files, or a trivial single-step lookup; use your judgment. Because every Eitri call starts with fresh context, every exploration prompt must state the current task state and work already completed, the specific question, known context, scope, stopping condition, and expected report.

Treat code_agent as delegation to a strong coding engineer with fresh context. Give Eitri one forgeable unit at a time: a substantial, self-contained implementation slice that can be completed in one focused pass and returned as one coherent, reviewable diff. A good handoff has one clear outcome, enough context and decisions to begin immediately, explicit constraints and acceptance checks, and leaves the workspace in a coherent, testable state. Delegate when implementing the change is clearly more work than writing the handoff and reviewing the result. Do not delegate trivial local edits, investigation better handled with direct tools or explore_agent, unresolved architectural questions, or an entire open-ended project. Split large work into sequential, independently verifiable units. You may personally make small, local code changes when describing and delegating them would take more effort than simply doing them; use judgment rather than delegating mechanically. Pass code_agent complete standalone instructions with the task, plan, relevant findings, current workspace state, and acceptance criteria. Its result includes the bounded full workspace diff attributable to that invocation. After Eitri returns, independently review its result and diff, inspect or verify the work as needed, and delegate a substantial corrective follow-up if implementation changes remain. If a request requires no code changes and no open-ended exploration, handle it yourself.

Every Eitri call starts a brand-new ACP process and session. Eitri has no conversation context and no memory of the user's request or any earlier Eitri call, including an immediately preceding call. Apply this policy while handling the user's request above; do not acknowledge or summarize the policy.
</mj-code-agent-policy>"#;

const CODE_PREAMBLE: &str = "You are Eitri, the implementation agent. This is a fresh ACP process and session. You have no memory of the user conversation or of any earlier Eitri call, including an immediately preceding call. Treat the standalone instructions below and the current workspace as your only task context.\n\n";
const EXPLORE_PREAMBLE: &str = r#"You are Eitri, a fast read-only codebase scout. This is a fresh ACP process and session with no memory of the user conversation or any earlier Eitri call. Your delegation may occur at any point in Thor's ongoing work, so treat the supplied current state and completed work as authoritative context rather than assuming the task is just beginning. Return compressed context that Thor can use directly.

READ-ONLY EXPLORATION: Never create, modify, delete, move, or copy files. Never install dependencies, change configuration, create commits, or run commands that modify system or workspace state. Do not create a report file. Do not run builds, tests, formatters, linters, package managers, or git status; inspect their definitions or source instead when relevant.

Work efficiently:
- Locate relevant code with file-pattern and regex/text searches, then read only the smallest sections needed to answer the request. Never read a large file in full.
- Follow only imports, callers, tests, types, and configuration necessary to establish the requested behavior.
- If a search is empty, try one materially different pattern, name, or path before concluding the target is absent.
- Parallelize only independent, targeted searches or reads when supported.
- Stop as soon as the requested question and stopping condition are satisfied. Do not inventory adjacent systems.

Return one concise final report with:
- a direct answer or summary;
- the minimal relevant absolute file paths, symbols, and line references, each with why it matters;
- the necessary control flow or relationships between those pieces;
- only material uncertainties or unanswered questions.

Do not narrate your search chronology, paste large search results, include nonessential code snippets, or propose implementation work unless the request asks for it.

"#;
const MCP_PATH: &str = "/mcp";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EitriPurpose {
    Code,
    Explore,
}

impl EitriPurpose {
    fn marks_implementation_delegation(self) -> bool {
        matches!(self, Self::Code)
    }

    fn internal_message_kind(self) -> InternalMessageKind {
        match self {
            Self::Code => InternalMessageKind::Delegation,
            Self::Explore => InternalMessageKind::Exploration,
        }
    }

    fn access_mode(self, configured: RuntimeAccessMode) -> RuntimeAccessMode {
        match self {
            Self::Code => configured,
            Self::Explore => RuntimeAccessMode::ReadOnly,
        }
    }

    fn standalone_prompt(self, task: &str) -> String {
        match self {
            Self::Code => format!("{CODE_PREAMBLE}{task}"),
            Self::Explore => format!("{EXPLORE_PREAMBLE}Search request:\n{task}"),
        }
    }

    fn loki_context(self, task: &str) -> String {
        match self {
            Self::Code => {
                format!("Eitri received this standalone implementation delegation:\n{task}")
            }
            Self::Explore => {
                format!("Eitri received this standalone read-only exploration request:\n{task}")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub display_label: String,
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub agent_stderr: Option<PathBuf>,
    pub role_config: Option<acp::RuntimeRoleConfig>,
    pub loki: Option<loki::Handle>,
    pub implementation_handoff_counter: Option<Arc<AtomicUsize>>,
    pub active_implementation_workers: ActiveCodeWorkers,
    pub max_parallel_explores: usize,
}

impl Config {
    pub fn council(
        role: ResolvedRole,
        agent_stderr: Option<PathBuf>,
        loki: Option<loki::Handle>,
    ) -> Self {
        Self {
            display_label: format!("Eitri · {}", role.model.model),
            command: role.launch.command,
            args: role.launch.args,
            env: role.launch.env,
            agent_stderr,
            role_config: Some(acp::RuntimeRoleConfig {
                label: LABEL.to_string(),
                model_id: role.model.model,
                model_value: role.model_value,
                adapter_source_id: role.launch.source_id,
                council_session: None,
            }),
            loki,
            implementation_handoff_counter: None,
            active_implementation_workers: ActiveCodeWorkers::default(),
            max_parallel_explores: 6,
        }
    }

    pub fn with_implementation_handoff_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.implementation_handoff_counter = Some(counter);
        self
    }

    pub fn with_active_implementation_workers(mut self, workers: ActiveCodeWorkers) -> Self {
        self.active_implementation_workers = workers;
        self
    }

    pub fn with_max_parallel_explores(mut self, max: usize) -> Self {
        self.max_parallel_explores = max.min(16);
        self
    }
}

/// Observable lifetime of implementation workers. The count reaches zero
/// only after the supervisor has reaped its ACP process tree and released its
/// controller lease.
#[derive(Clone, Debug)]
pub struct ActiveCodeWorkers {
    updates: watch::Sender<usize>,
}

impl Default for ActiveCodeWorkers {
    fn default() -> Self {
        let (updates, _) = watch::channel(0);
        Self { updates }
    }
}

impl ActiveCodeWorkers {
    pub fn subscribe(&self) -> watch::Receiver<usize> {
        self.updates.subscribe()
    }

    pub(crate) fn set(&self, count: usize) {
        self.updates.send_replace(count);
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
    /// Optional absolute implementation directory for this delegation.
    pub cwd: Option<PathBuf>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExploreAgentArgs {
    /// Complete, standalone read-only research request for the delegated agent.
    pub prompt: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExploreAgentsArgs {
    /// Ordered, complete standalone read-only research requests to launch concurrently.
    pub prompts: Vec<String>,
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
        description = "IMPLEMENTATION DELEGATE (EITRI). Treat this as delegation to a strong coding engineer with fresh context. Give Eitri one forgeable unit: a substantial, self-contained implementation slice that can be completed in one focused pass and returned as one coherent, reviewable diff. A good handoff has one clear outcome, enough context and decisions to begin immediately, explicit constraints and acceptance checks, and leaves the workspace coherent and testable. For an explicitly authorized implementation worktree, pass its absolute cwd argument; do not infer a worktree from instructions. Delegate when implementation is clearly more work than writing the handoff and reviewing the result. Do NOT delegate trivial local edits, investigation better handled directly or with explore_agent, unresolved architectural questions, or an entire open-ended project; split large work into sequential, independently verifiable units. Thor owns research, planning, coordination, review, verification, and the final response, and should make small local changes directly when delegation would cost more effort. Every call starts a fresh ACP process/session with zero conversation or prior-call memory. Pass complete standalone instructions with the task, plan, relevant findings, current workspace state, and acceptance criteria. The result includes the bounded full workspace diff attributable to this invocation. Review Eitri's result and diff independently and call it again for substantial corrections."
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
        let context = resolve_code_context(&self.context, args.cwd.as_deref()).await?;
        let Some(run_id) = self.controller.begin(RunKind::Code).await else {
            return Ok(CallToolResult::error(vec![Content::text(
                "an Eitri implementation run is already active",
            )]));
        };

        let result = await_supervisor(
            self.config.clone(),
            context,
            args.instructions,
            EitriPurpose::Code,
            self.ui_tx.clone(),
            self.controller.clone(),
            run_id,
        )
        .await;
        Ok(match result.outcome {
            Ok(message) => CallToolResult::success(vec![Content::text(with_workspace_diff(
                &message,
                result.workspace_delta.as_ref(),
            ))]),
            Err(error) => CallToolResult::error(vec![Content::text(with_workspace_diff(
                &error.to_string(),
                result.workspace_delta.as_ref(),
            ))]),
        })
    }

    #[tool(
        name = "explore_agent",
        description = "OPTIONAL READ-ONLY EXPLORATION DELEGATE (EITRI). Use this at any point in ongoing work to offload bounded, multi-step codebase research, especially when affected locations are unknown or the question requires multiple search rounds. It is not a required phase before implementation. Direct tools are usually faster for a known path, known symbol, exact definition, work confined to roughly 2-3 known files, or a trivial lookup. Use your judgment. Every call starts with fresh context, so the complete standalone prompt must state the current task state and work already completed, the specific question, known context, scope, stopping condition, and expected report. Returns one concise research report."
    )]
    async fn explore_agent(
        &self,
        Parameters(args): Parameters<ExploreAgentArgs>,
    ) -> std::result::Result<CallToolResult, McpError> {
        let prompt = args.prompt.trim();
        if prompt.is_empty() {
            return Err(McpError::invalid_params("prompt must not be empty", None));
        }
        let Some(run_id) = self.controller.begin(RunKind::Explore).await else {
            return Ok(CallToolResult::error(vec![Content::text(
                "the Eitri exploration pool is full or disabled",
            )]));
        };

        let result = await_supervisor(
            self.config.clone(),
            self.context.clone(),
            prompt.to_string(),
            EitriPurpose::Explore,
            self.ui_tx.clone(),
            self.controller.clone(),
            run_id,
        )
        .await;
        Ok(match result.outcome {
            Ok(message) => CallToolResult::success(vec![Content::text(message)]),
            Err(error) => CallToolResult::error(vec![Content::text(error.to_string())]),
        })
    }

    #[tool(
        name = "explore_agents",
        description = "CONCURRENT READ-ONLY EXPLORATION FAN-OUT (EITRI). Use only for two or more independent research questions, each supplied as a complete standalone prompt with current task state, specific question, known context, scope, stopping condition, and expected report. This tool atomically admits the entire ordered batch and launches all scouts concurrently, or rejects it when capacity is unavailable; it never queues or runs overflow prompts sequentially. Its ordered result identifies each scout as completed or failed and retains successful reports when siblings fail. For one scout, use explore_agent."
    )]
    async fn explore_agents(
        &self,
        Parameters(args): Parameters<ExploreAgentsArgs>,
    ) -> std::result::Result<CallToolResult, McpError> {
        validate_explore_batch(&args.prompts)?;
        let results = match run_explore_fanout(&self.controller, args.prompts, |prompt, run_id| {
            await_supervisor(
                self.config.clone(),
                self.context.clone(),
                prompt,
                EitriPurpose::Explore,
                self.ui_tx.clone(),
                self.controller.clone(),
                run_id,
            )
        })
        .await
        {
            Ok(results) => results,
            Err(rejection) => {
                return Ok(CallToolResult::error(vec![Content::text(
                    rejection.message(),
                )]));
            }
        };
        let completed = results
            .iter()
            .filter(|result| result.outcome.is_ok())
            .count();
        let failed = results.len() - completed;
        tracing::info!(
            event = "eitri_explore_fanout_completed",
            launched = results.len(),
            completed,
            failed,
            "Eitri exploration fan-out completed"
        );
        Ok(explore_fanout_tool_result(&results))
    }
}

fn validate_explore_batch(prompts: &[String]) -> std::result::Result<(), McpError> {
    if prompts.len() < 2 {
        return Err(McpError::invalid_params(
            "prompts must contain at least two items; use explore_agent for one prompt",
            None,
        ));
    }
    if prompts.iter().any(|prompt| prompt.trim().is_empty()) {
        return Err(McpError::invalid_params(
            "every exploration prompt must not be empty",
            None,
        ));
    }
    Ok(())
}

/// Narrows an explicit implementation delegation to its requested worktree.
/// The outer runtime has already authorized `cwd` and `additional_directories`;
/// a delegation cannot use those roots to gain access to an arbitrary sibling.
async fn resolve_code_context(
    outer: &RunContext,
    delegated_cwd: Option<&Path>,
) -> std::result::Result<RunContext, McpError> {
    let Some(delegated_cwd) = delegated_cwd else {
        return Ok(outer.clone());
    };
    if !delegated_cwd.is_absolute() {
        return Err(McpError::invalid_params(
            "delegated cwd must be an absolute path",
            None,
        ));
    }
    let delegated_cwd = tokio::fs::canonicalize(delegated_cwd)
        .await
        .map_err(|error| {
            McpError::invalid_params(
                format!("delegated cwd must be an existing, accessible directory: {error}"),
                None,
            )
        })?;
    if !tokio::fs::metadata(&delegated_cwd)
        .await
        .map_err(|error| {
            McpError::invalid_params(
                format!("delegated cwd must be an existing, accessible directory: {error}"),
                None,
            )
        })?
        .is_dir()
    {
        return Err(McpError::invalid_params(
            "delegated cwd must be an existing directory",
            None,
        ));
    }

    let mut authorized_roots = Vec::with_capacity(1 + outer.additional_directories.len());
    authorized_roots.push(outer.cwd.clone());
    authorized_roots.extend(outer.additional_directories.iter().cloned());
    let mut contains_delegated_cwd = false;
    for root in authorized_roots {
        let root = tokio::fs::canonicalize(&root).await.map_err(|error| {
            McpError::invalid_params(
                format!("configured workspace root is inaccessible: {error}"),
                None,
            )
        })?;
        if delegated_cwd.starts_with(root) {
            contains_delegated_cwd = true;
            break;
        }
    }
    if !contains_delegated_cwd {
        return Err(McpError::invalid_params(
            format!(
                "delegated cwd {} is outside the authorized workspace roots; code_agent may only delegate within the current workspace root or configured additional workspace roots. Configure the target as an additional workspace root before delegating",
                delegated_cwd.display()
            ),
            None,
        ));
    }

    Ok(RunContext {
        cwd: delegated_cwd,
        additional_directories: Vec::new(),
        fs_max_text_bytes: outer.fs_max_text_bytes,
        access_mode: outer.access_mode,
    })
}

/// Returns the Git roots whose changes belong to one implementation delegation.
/// An explicit `code_agent` cwd has already been narrowed by
/// `resolve_code_context`, so this deliberately cannot reach outer siblings.
fn implementation_workspace_roots(context: &RunContext) -> Vec<PathBuf> {
    let mut roots = Vec::with_capacity(1 + context.additional_directories.len());
    roots.push(context.cwd.clone());
    roots.extend(context.additional_directories.iter().cloned());
    roots
}

async fn capture_implementation_snapshot(context: &RunContext) -> WorkspaceSnapshot {
    WorkspaceSnapshot::capture(&implementation_workspace_roots(context)).await
}

impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("mj-code-agent", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "EITRI DELEGATION POLICY: explore_agent is one optional read-only scout. For two or more independent scouts that must actually overlap, use explore_agents with complete standalone prompts: it atomically launches the whole batch concurrently or rejects it without queueing. Do not claim separate explore_agent calls ran in parallel. Give code_agent one forgeable implementation unit at a time. Thor chooses and sequences tools, retains planning, coordination, review, verification, and the final answer, and must give each fresh Eitri call complete standalone context including the current task state and work already completed.",
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
        controller
            .configure(
                config.max_parallel_explores,
                config.active_implementation_workers.clone(),
            )
            .await;
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
            .map_err(|_| anyhow!("primary agent timed out loading the Eitri MCP tools"))?
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunKind {
    Code,
    Explore,
}

#[derive(Debug)]
enum ActiveRun {
    Starting {
        kind: RunKind,
        cancel_requested: bool,
        shutdown_requested: bool,
        termination: RunTermination,
    },
    Running {
        kind: RunKind,
        commands: mpsc::UnboundedSender<UiCommand>,
        termination: RunTermination,
    },
}

#[derive(Debug)]
struct ControllerState {
    next_id: u64,
    max_parallel_explores: usize,
    runs: HashMap<u64, ActiveRun>,
    active_implementation_workers: ActiveCodeWorkers,
    active_runs: watch::Sender<usize>,
}

impl Default for ControllerState {
    fn default() -> Self {
        let (active_runs, _) = watch::channel(0);
        Self {
            next_id: 1,
            max_parallel_explores: 6,
            runs: HashMap::new(),
            active_implementation_workers: ActiveCodeWorkers::default(),
            active_runs,
        }
    }
}

/// Coordinates one implementation run and a bounded pool of read-only scouts.
#[derive(Debug, Clone, Default)]
pub struct Controller {
    state: Arc<Mutex<ControllerState>>,
}

impl Controller {
    async fn configure(
        &self,
        max_parallel_explores: usize,
        active_implementation_workers: ActiveCodeWorkers,
    ) {
        let mut state = self.state.lock().await;
        state.max_parallel_explores = max_parallel_explores.min(16);
        state.active_implementation_workers = active_implementation_workers;
    }

    async fn begin(&self, kind: RunKind) -> Option<u64> {
        let mut state = self.state.lock().await;
        let allowed = match kind {
            RunKind::Code => !state.runs.values().any(|run| run.kind() == RunKind::Code),
            RunKind::Explore => {
                let active = state
                    .runs
                    .values()
                    .filter(|run| run.kind() == RunKind::Explore)
                    .count();
                active < state.max_parallel_explores
            }
        };
        if !allowed {
            return None;
        }
        let id = state.insert_starting(kind);
        if matches!(kind, RunKind::Code) {
            state.active_implementation_workers.set(1);
        }
        let active = state.runs.len();
        state.active_runs.send_replace(active);
        Some(id)
    }

    /// Atomically reserves all requested read-only slots. Unlike repeated
    /// `begin(Explore)` calls, this never leaves a partially admitted batch.
    async fn begin_explores(
        &self,
        requested: usize,
    ) -> std::result::Result<Vec<u64>, ExploreAdmission> {
        let mut state = self.state.lock().await;
        let active = state
            .runs
            .values()
            .filter(|run| run.kind() == RunKind::Explore)
            .count();
        let available = state.max_parallel_explores.saturating_sub(active);
        if requested > state.max_parallel_explores || requested > available {
            return Err(ExploreAdmission {
                requested,
                available,
                maximum: state.max_parallel_explores,
            });
        }
        let ids = (0..requested)
            .map(|_| state.insert_starting(RunKind::Explore))
            .collect();
        state.active_runs.send_replace(state.runs.len());
        Ok(ids)
    }

    async fn attach(&self, id: u64, commands: mpsc::UnboundedSender<UiCommand>) {
        let mut state = self.state.lock().await;
        let Some(run) = state.runs.remove(&id) else {
            let _ = commands.send(UiCommand::Shutdown);
            return;
        };
        let ActiveRun::Starting {
            kind,
            cancel_requested,
            shutdown_requested,
            termination,
        } = run
        else {
            return;
        };
        state.runs.insert(
            id,
            ActiveRun::Running {
                kind,
                commands: commands.clone(),
                termination,
            },
        );
        if shutdown_requested {
            let _ = commands.send(UiCommand::Shutdown);
        } else if cancel_requested {
            let _ = commands.send(UiCommand::CancelPrompt);
        }
    }

    pub async fn cancel(&self) -> bool {
        let mut state = self.state.lock().await;
        let mut active = false;
        for run in state.runs.values_mut() {
            active = true;
            match run {
                ActiveRun::Starting {
                    cancel_requested,
                    termination,
                    ..
                } => {
                    *cancel_requested = true;
                    termination.request(TerminationCause::UserCancelled);
                }
                ActiveRun::Running {
                    commands,
                    termination,
                    ..
                } => {
                    let _ = commands.send(UiCommand::CancelPrompt);
                    termination.request(TerminationCause::UserCancelled);
                }
            }
        }
        active
    }

    pub async fn shutdown(&self) -> bool {
        let mut state = self.state.lock().await;
        let mut active = false;
        for run in state.runs.values_mut() {
            active = true;
            match run {
                ActiveRun::Starting {
                    shutdown_requested,
                    termination,
                    ..
                } => {
                    *shutdown_requested = true;
                    termination.request(TerminationCause::RuntimeShutdown);
                }
                ActiveRun::Running {
                    commands,
                    termination,
                    ..
                } => {
                    let _ = commands.send(UiCommand::Shutdown);
                    termination.request(TerminationCause::RuntimeShutdown);
                }
            }
        }
        active
    }

    pub async fn shutdown_and_wait(&self) -> bool {
        let mut active_runs = self.state.lock().await.active_runs.subscribe();
        let active = self.shutdown().await;
        while *active_runs.borrow_and_update() > 0 {
            if active_runs.changed().await.is_err() {
                break;
            }
        }
        active
    }

    async fn termination(&self, id: u64) -> Option<RunTermination> {
        self.state
            .lock()
            .await
            .runs
            .get(&id)
            .map(ActiveRun::termination)
    }

    async fn finish(&self, id: u64) {
        let mut state = self.state.lock().await;
        if matches!(state.runs.remove(&id), Some(run) if run.kind() == RunKind::Code) {
            state.active_implementation_workers.set(0);
        }
        let active = state.runs.len();
        state.active_runs.send_replace(active);
    }

    #[cfg(test)]
    async fn active_explore_count(&self) -> usize {
        self.state
            .lock()
            .await
            .runs
            .values()
            .filter(|run| run.kind() == RunKind::Explore)
            .count()
    }
}

impl ControllerState {
    fn insert_starting(&mut self, kind: RunKind) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.runs.insert(
            id,
            ActiveRun::Starting {
                kind,
                cancel_requested: false,
                shutdown_requested: false,
                termination: RunTermination::default(),
            },
        );
        id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExploreAdmission {
    requested: usize,
    available: usize,
    maximum: usize,
}

impl ExploreAdmission {
    fn message(self) -> String {
        format!(
            "Eitri exploration fan-out was not launched: requested {} concurrent scouts, but only {} of {} exploration slots are available; no scouts were queued or started",
            self.requested, self.available, self.maximum
        )
    }
}

impl ActiveRun {
    fn kind(&self) -> RunKind {
        match self {
            Self::Starting { kind, .. } | Self::Running { kind, .. } => *kind,
        }
    }

    fn termination(&self) -> RunTermination {
        match self {
            Self::Starting { termination, .. } | Self::Running { termination, .. } => {
                termination.clone()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum TerminationCause {
    None = 0,
    UserCancelled = 1,
    RuntimeShutdown = 2,
    RequestDisconnected = 3,
    RunCompleted = 4,
}

impl TerminationCause {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::UserCancelled,
            2 => Self::RuntimeShutdown,
            3 => Self::RequestDisconnected,
            4 => Self::RunCompleted,
            _ => Self::None,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::None => "unspecified",
            Self::UserCancelled => "user cancellation",
            Self::RuntimeShutdown => "runtime shutdown",
            Self::RequestDisconnected => "MCP request timeout or disconnect",
            Self::RunCompleted => "normal completion",
        }
    }
}

#[derive(Clone, Debug)]
struct RunTermination {
    token: CancellationToken,
    cause: Arc<AtomicU8>,
}

impl Default for RunTermination {
    fn default() -> Self {
        Self {
            token: CancellationToken::new(),
            cause: Arc::new(AtomicU8::new(TerminationCause::None as u8)),
        }
    }
}

impl RunTermination {
    fn request(&self, cause: TerminationCause) {
        let _ = self.cause.compare_exchange(
            TerminationCause::None as u8,
            cause as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.token.cancel();
    }

    fn cause(&self) -> TerminationCause {
        TerminationCause::from_u8(self.cause.load(Ordering::Acquire))
    }

    async fn cancelled(&self) {
        self.token.cancelled().await;
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

    fn finish(&self) -> Result<String> {
        if self.last.trim().is_empty() {
            bail!("Eitri completed without a final message");
        }
        Ok(self.last.clone())
    }
}

fn exploration_activity(update: &SessionUpdate) -> Option<String> {
    match update {
        SessionUpdate::ToolCall(call) => Some(call.title.clone()),
        SessionUpdate::ToolCallUpdate(update) => update.fields.title.clone().or_else(|| {
            update
                .fields
                .status
                .map(|status| format!("tool {status:?}"))
        }),
        SessionUpdate::Plan(_) => Some("planning exploration".to_string()),
        _ => None,
    }
}

struct EitriRunResult {
    outcome: Result<String>,
    workspace_delta: Option<WorkspaceDelta>,
}

#[derive(Clone)]
struct RunLease {
    controller: Controller,
    run_id: u64,
    termination: RunTermination,
}

fn run_boxed(
    config: Config,
    context: RunContext,
    task: String,
    purpose: EitriPurpose,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    lease: RunLease,
) -> futures::future::BoxFuture<'static, EitriRunResult> {
    Box::pin(run(config, context, task, purpose, ui_tx, lease))
}

/// Keep the per-run supervisor independent from the HTTP request future.
/// rmcp drops that future when a client times out or disconnects; dropping a
/// JoinHandle detaches the supervisor, while this guard tells it to terminate
/// and reap before it can release the controller lease.
async fn await_supervisor(
    config: Config,
    context: RunContext,
    task: String,
    purpose: EitriPurpose,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    controller: Controller,
    run_id: u64,
) -> EitriRunResult {
    let termination = self_termination(&controller, run_id).await;
    let supervisor_controller = controller.clone();
    let lease = RunLease {
        controller: controller.clone(),
        run_id,
        termination: termination.clone(),
    };
    let mut supervisor = tokio::spawn(async move {
        let worker = tokio::spawn(run_boxed(config, context, task, purpose, ui_tx, lease));
        let result = match worker.await {
            Ok(result) => result,
            Err(error) => EitriRunResult {
                outcome: Err(anyhow!("Eitri worker task failed: {error}")),
                workspace_delta: None,
            },
        };
        supervisor_controller.finish(run_id).await;
        tracing::info!(
            event = "eitri_slot_released",
            run_id,
            purpose = ?purpose,
            "Eitri controller slot released after reap"
        );
        result
    });
    let mut request_guard = RequestDropGuard::new(termination, run_id, purpose);
    let result = match (&mut supervisor).await {
        Ok(result) => result,
        Err(error) => EitriRunResult {
            outcome: Err(anyhow!("Eitri supervisor failed: {error}")),
            workspace_delta: None,
        },
    };
    request_guard.disarm();
    result
}

/// Poll every supervisor before awaiting the aggregate, so a batch cannot
/// accidentally turn into one launch followed by the next.
async fn await_explore_fanout<F>(supervisors: Vec<F>) -> Vec<EitriRunResult>
where
    F: Future<Output = EitriRunResult>,
{
    futures::future::join_all(supervisors).await
}

/// Atomically admit an ordered batch, construct every scout future, and drive
/// them together. Keeping those steps inseparable prevents a fan-out from
/// regressing into sequential admission or launch.
async fn run_explore_fanout<F, Fut>(
    controller: &Controller,
    prompts: Vec<String>,
    mut supervise: F,
) -> std::result::Result<Vec<EitriRunResult>, ExploreAdmission>
where
    F: FnMut(String, u64) -> Fut,
    Fut: Future<Output = EitriRunResult>,
{
    let run_ids = controller.begin_explores(prompts.len()).await?;
    tracing::info!(
        event = "eitri_explore_fanout_admitted",
        requested = prompts.len(),
        reserved = run_ids.len(),
        "Eitri exploration fan-out atomically admitted; preparing supervisor futures"
    );
    let supervisors = prompts
        .into_iter()
        .zip(run_ids)
        .map(|(prompt, run_id)| supervise(prompt, run_id))
        .collect();
    Ok(await_explore_fanout(supervisors).await)
}

fn format_explore_fanout(results: &[EitriRunResult]) -> String {
    let completed = results
        .iter()
        .filter(|result| result.outcome.is_ok())
        .count();
    let failed = results.len() - completed;
    let summary = match (completed, failed) {
        (_, 0) => format!(
            "launched {} Eitri explorations concurrently; all completed",
            results.len()
        ),
        (0, _) => format!(
            "launched {} Eitri explorations concurrently; all failed",
            results.len()
        ),
        _ => format!(
            "launched {} Eitri explorations concurrently; {} completed and {} failed",
            results.len(),
            completed,
            failed
        ),
    };
    let mut report = summary;
    for (index, result) in results.iter().enumerate() {
        match &result.outcome {
            Ok(content) => report.push_str(&format!("\n\n[{index}] completed\n{content}")),
            Err(error) => report.push_str(&format!("\n\n[{index}] failed\n{error}")),
        }
    }
    report
}

fn explore_fanout_tool_result(results: &[EitriRunResult]) -> CallToolResult {
    let report = format_explore_fanout(results);
    if results.iter().any(|result| result.outcome.is_ok()) {
        CallToolResult::success(vec![Content::text(report)])
    } else {
        CallToolResult::error(vec![Content::text(report)])
    }
}

async fn self_termination(controller: &Controller, run_id: u64) -> RunTermination {
    controller
        .termination(run_id)
        .await
        .expect("controller retains the run lease until supervisor finalization")
}

struct RequestDropGuard {
    termination: RunTermination,
    run_id: u64,
    purpose: EitriPurpose,
    armed: bool,
}

impl RequestDropGuard {
    fn new(termination: RunTermination, run_id: u64, purpose: EitriPurpose) -> Self {
        Self {
            termination,
            run_id,
            purpose,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RequestDropGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        tracing::warn!(
            event = "eitri_request_disconnected",
            run_id = self.run_id,
            purpose = ?self.purpose,
            "Eitri MCP request timed out or disconnected; terminating worker"
        );
        self.termination
            .request(TerminationCause::RequestDisconnected);
    }
}

async fn run(
    config: Config,
    context: RunContext,
    task: String,
    purpose: EitriPurpose,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    lease: RunLease,
) -> EitriRunResult {
    let RunLease {
        controller,
        run_id,
        termination,
    } = lease;
    let log_role = config.role_config.clone();
    tracing::info!(
        event = "eitri_worker_started",
        run_id,
        purpose = ?purpose,
        "Eitri supervised worker started"
    );
    if purpose.marks_implementation_delegation()
        && let Some(counter) = config.implementation_handoff_counter.as_ref()
    {
        counter.fetch_add(1, Ordering::AcqRel);
    }
    let standalone_prompt = purpose.standalone_prompt(&task);
    if let Some(role) = log_role.as_ref()
        && let Some(council_session) = role.council_session.as_deref()
    {
        tracing::info!(
            event = "delegation_started",
            council_session,
            god = "Eitri",
            source = "Thor",
            model = %role.model_id,
            adapter = %role.adapter_source_id,
            purpose = ?purpose,
            run_id,
            task = %task,
            "Thor delegated work to Eitri"
        );
    }
    let display_label = config.display_label.clone();
    let _ = ui_tx.send(UiEvent::InternalMessage(InternalMessage {
        source: "Thor".to_string(),
        target: LABEL.to_string(),
        kind: purpose.internal_message_kind(),
        text: task.clone(),
    }));
    let start_event = match purpose {
        EitriPurpose::Code => CodeAgentEvent::Started {
            label: display_label,
        },
        EitriPurpose::Explore => CodeAgentEvent::ExplorationStarted {
            run_id,
            label: display_label,
        },
    };
    let _ = ui_tx.send(UiEvent::CodeAgent(start_event));

    let invocation_snapshot = if purpose.marks_implementation_delegation() {
        Some(capture_implementation_snapshot(&context).await)
    } else {
        None
    };

    let (nested_event_tx, mut nested_event_rx) = mpsc::unbounded_channel();
    let (nested_cmd_tx, nested_cmd_rx) = mpsc::unbounded_channel();
    controller.attach(run_id, nested_cmd_tx.clone()).await;

    // Share the Council's one persistent Loki session with implementation
    // runs. Read-only scouts stay outside Loki's continuous Thor/Eitri-code view.
    let loki = purpose
        .marks_implementation_delegation()
        .then(|| config.loki.clone())
        .flatten();
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
        access_mode: purpose.access_mode(context.access_mode),
        agent_source_id: None,
        config_path: None,
        saved_session_config: HashMap::new(),
        role_config: config.role_config,
        code_agent: None,
        termination: Some(termination.token.clone()),
    };
    let mut runtime = tokio::spawn(acp::run(runtime_config, nested_event_tx, nested_cmd_rx));

    let mut prompt_sent = false;
    let mut collector = AgentMessageCollector::new();
    let epoch = loki.as_ref().map_or(0, loki::Handle::current_epoch);
    let eitri_invocation = if epoch > 0
        && let Some(reviewer) = loki.as_ref()
    {
        Some(reviewer.begin_eitri(epoch, purpose.loki_context(&task)))
    } else {
        None
    };
    let mut tracker = loki::BoundaryTracker::default();
    let mut latest_usage_update: Option<UsageUpdate> = None;
    let mut joined_runtime_result = None;
    let result = loop {
        tokio::select! {
            biased;
            () = termination.cancelled() => {
                let cause = termination.cause();
                break Err(match cause {
                    TerminationCause::UserCancelled => anyhow!("Eitri cancelled"),
                    TerminationCause::RuntimeShutdown => anyhow!("Eitri shutdown requested"),
                    TerminationCause::RequestDisconnected => {
                        anyhow!("Eitri MCP request timed out or disconnected")
                    }
                    TerminationCause::RunCompleted | TerminationCause::None => {
                        anyhow!("Eitri termination requested")
                    }
                });
            }
            joined = &mut runtime => {
                let (runtime_result, run_result) = match joined {
                    Ok(Ok(())) => (
                        Ok(()),
                        Err(anyhow!("Eitri runtime closed before completing")),
                    ),
                    Ok(Err(error)) => {
                        let message = format!("{error:#}");
                        (Err(error), Err(anyhow!("Eitri runtime: {message}")))
                    }
                    Err(error) => {
                        let message = format!("Eitri task failed: {error}");
                        (Err(anyhow!(message.clone())), Err(anyhow!(message)))
                    }
                };
                joined_runtime_result = Some(runtime_result);
                break run_result;
            }
            event = nested_event_rx.recv() => {
                let Some(event) = event else {
                    break Err(anyhow!("Eitri event stream closed before completing"));
                };
                if let Some(boundary) = (epoch > 0).then(|| tracker.observe(&event)).flatten()
                    && let Some(reviewer) = loki.as_ref()
                {
                    reviewer.observe(epoch, loki::Target::Eitri, eitri_invocation, boundary);
                }
                match event {
                    UiEvent::Connected { .. } => {}
                    UiEvent::SessionStarted { .. } if !prompt_sent => {
                        prompt_sent = true;
                        if nested_cmd_tx
                            .send(UiCommand::SendPrompt {
                                text: standalone_prompt.clone(),
                                images: Vec::new(),
                            })
                            .is_err()
                        {
                            break Err(anyhow!("send prompt to Eitri"));
                        }
                    }
                    UiEvent::SessionStarted { .. }
                    | UiEvent::SessionConfigOptions { .. }
                    | UiEvent::CouncilUpdate { .. }
                    | UiEvent::WorkspaceDiff(_) => {}
                    UiEvent::SessionUpdate(update) => {
                        if let SessionUpdate::UsageUpdate(value) = &update {
                            latest_usage_update = Some(value.clone());
                        }
                        collector.observe(&update);
                        match purpose {
                            EitriPurpose::Code => {
                                let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::SessionUpdate(update)));
                            }
                            EitriPurpose::Explore => {
                                if let Some(activity) = exploration_activity(&update) {
                                    let _ = ui_tx.send(UiEvent::CodeAgent(
                                        CodeAgentEvent::ExplorationProgress { run_id, activity },
                                    ));
                                }
                            }
                        }
                    }
                    UiEvent::TerminalOutput(snapshot) => {
                        if matches!(purpose, EitriPurpose::Code) {
                            let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::TerminalOutput(snapshot)));
                        }
                    }
                    UiEvent::PermissionRequest(prompt) => {
                        if matches!(purpose, EitriPurpose::Explore) {
                            let decision = crate::ragnarok::permission_decision_for_access(
                                RuntimeAccessMode::ReadOnly,
                                &prompt,
                            );
                            let _ = prompt.responder.send(decision);
                        } else {
                            let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::PermissionRequest(prompt)));
                        }
                    }
                    UiEvent::ElicitationRequest(prompt) => {
                        if matches!(purpose, EitriPurpose::Explore) {
                            let _ = prompt.responder.send(crate::event::ElicitationOutcome::Decline);
                        } else {
                            let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::ElicitationRequest(prompt)));
                        }
                    }
                    UiEvent::CancelPendingPermissions => {
                        if matches!(purpose, EitriPurpose::Code) {
                            let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::CancelPendingPermissions));
                        }
                    }
                    UiEvent::Info(message) | UiEvent::Warning(message) => {
                        let event = match purpose {
                            EitriPurpose::Code => CodeAgentEvent::Status(message),
                            EitriPurpose::Explore => CodeAgentEvent::ExplorationProgress {
                                run_id,
                                activity: message,
                            },
                        };
                        let _ = ui_tx.send(UiEvent::CodeAgent(event));
                    }
                    UiEvent::PromptDone { stop_reason, usage } => {
                        let _ = ui_tx.send(UiEvent::CouncilUsage(Record {
                            role: Role::Eitri,
                            purpose: Some(match purpose {
                                EitriPurpose::Code => Purpose::Code,
                                EitriPurpose::Explore => Purpose::Explore,
                            }),
                            usage,
                            update: latest_usage_update.take(),
                        }));
                        if matches!(stop_reason, StopReason::Cancelled) {
                            break Err(anyhow!("Eitri cancelled"));
                        }
                        break collector.finish();
                    }
                    UiEvent::PromptFailed { message }
                    | UiEvent::SessionForkFailed { message }
                    | UiEvent::Fatal(message) => {
                        break Err(anyhow!(message));
                    }
                    UiEvent::ClaudeUsage(_)
                    | UiEvent::CodexUsage(_)
                    | UiEvent::CouncilUsage(_)
                    | UiEvent::RemotePermissionDecision { .. }
                    | UiEvent::LokiActivity(_)
                    | UiEvent::InternalMessage(_) => {}
                    UiEvent::CodeAgent(_) => {
                        break Err(anyhow!("Eitri attempted recursive delegation"));
                    }
                }
            }
        }
    };

    // Eitri's completion is a natural turn boundary: hand whatever advice
    // Loki has queued so far back to Thor inside the tool result. Reviews
    // still in flight deliver at a later boundary; nothing waits on Loki.
    let mut result = result;
    if result.is_ok()
        && let Some(reviewer) = loki.as_ref()
    {
        let deferred = reviewer.take_deferred();
        if !deferred.is_empty()
            && let Ok(message) = result.as_mut()
        {
            message.push_str("\n\n<loki_advice target=\"thor\" mode=\"asynchronous; may be superseded by later work\">\n");
            message.push_str(&loki::format_deferred(&deferred, epoch));
            message.push_str("\n</loki_advice>");
        }
    }

    // Never abort `acp::run`: its tail owns process-tree termination and
    // reaping. Cancelling this token drives that tail even when the MCP request
    // disappeared, and the supervisor retains the slot until the join returns.
    termination.request(TerminationCause::RunCompleted);
    let cause = termination.cause();
    tracing::info!(
        event = "eitri_termination_requested",
        run_id,
        purpose = ?purpose,
        reason = cause.description(),
        "terminating Eitri worker process tree"
    );
    let runtime_result = match joined_runtime_result {
        Some(result) => result,
        None => match runtime.await {
            Ok(result) => result,
            Err(error) => Err(anyhow!("Eitri runtime task failed: {error}")),
        },
    };
    if let Err(error) = runtime_result {
        tracing::error!(event = "eitri_teardown_failure", run_id, purpose = ?purpose, error = %error, "Eitri runtime failed while terminating or reaping");
        result = Err(error.context("Eitri teardown"));
    } else {
        tracing::info!(event = "eitri_reaped", run_id, purpose = ?purpose, "Eitri worker process tree reaped");
    }
    let workspace_delta = match invocation_snapshot {
        Some(snapshot) => Some(snapshot.delta().await),
        None => None,
    };

    let outcome = match &result {
        Ok(_) => CodeAgentOutcome::Completed,
        Err(error) if error.to_string().contains("cancelled") => CodeAgentOutcome::Cancelled,
        Err(error) => CodeAgentOutcome::Failed(error.to_string()),
    };
    let finish_event = match purpose {
        EitriPurpose::Code => CodeAgentEvent::Finished { outcome },
        EitriPurpose::Explore => CodeAgentEvent::ExplorationFinished { run_id, outcome },
    };
    let _ = ui_tx.send(UiEvent::CodeAgent(finish_event));
    if let Some(role) = log_role.as_ref()
        && let Some(council_session) = role.council_session.as_deref()
    {
        tracing::info!(
            event = "delegation_finished",
            council_session,
            god = "Eitri",
            target = "Thor",
            model = %role.model_id,
            adapter = %role.adapter_source_id,
            purpose = ?purpose,
            run_id,
            outcome = if result.is_ok() { "completed" } else { "failed" },
            workspace_changed = workspace_delta.as_ref().is_some_and(WorkspaceDelta::changed),
            error = ?result.as_ref().err().map(|error| format!("{error:#}")),
            "Eitri delegation finished"
        );
    }
    EitriRunResult {
        outcome: result,
        workspace_delta,
    }
}

fn with_workspace_diff(message: &str, delta: Option<&WorkspaceDelta>) -> String {
    let Some(delta) = delta else {
        return format!(
            "{message}\n\n<workspace_diff scope=\"eitri-invocation\" authored_by=\"Eitri\">\n[workspace delta unavailable because the supervisor failed]\n</workspace_diff>"
        );
    };
    let diff = delta.review_patch().unwrap_or_else(|| delta.receipt());
    let mut result = format!(
        "{message}\n\n<workspace_diff scope=\"eitri-invocation\" authored_by=\"Eitri\">\n{diff}\n</workspace_diff>"
    );
    if delta.changed() {
        result.push_str("\n\nYou should review Eitri's work now.");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{ContentBlock, ContentChunk, TextContent};

    fn init_repo(root: &Path) {
        for args in [
            ["init", "-q"].as_slice(),
            ["config", "user.email", "mjolnir@example.test"].as_slice(),
            ["config", "user.name", "Mjolnir Tests"].as_slice(),
            ["commit", "--allow-empty", "-qm", "baseline"].as_slice(),
        ] {
            let output = std::process::Command::new("git")
                .current_dir(root)
                .args(args)
                .output()
                .expect("run git");
            assert!(
                output.status.success(),
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
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
    async fn controller_allows_code_and_bounded_explores_concurrently() {
        let controller = Controller::default();
        controller.configure(2, ActiveCodeWorkers::default()).await;
        let code = controller.begin(RunKind::Code).await.expect("code");
        assert!(controller.begin(RunKind::Code).await.is_none());
        let first = controller.begin(RunKind::Explore).await.expect("explore 1");
        let second = controller.begin(RunKind::Explore).await.expect("explore 2");
        assert!(controller.begin(RunKind::Explore).await.is_none());
        assert!(controller.cancel().await);
        controller.finish(first).await;
        assert!(controller.begin(RunKind::Explore).await.is_some());
        controller.finish(second).await;
        controller.finish(code).await;
        assert!(controller.begin(RunKind::Code).await.is_some());
    }

    #[tokio::test]
    async fn shutdown_requested_while_starting_reaches_nested_runtime() {
        let controller = Controller::default();
        let run_id = controller.begin(RunKind::Explore).await.expect("explore");
        assert!(controller.shutdown().await);
        let (commands, mut receiver) = mpsc::unbounded_channel();
        controller.attach(run_id, commands).await;
        assert!(matches!(receiver.recv().await, Some(UiCommand::Shutdown)));
    }

    #[tokio::test]
    async fn request_timeout_holds_code_slot_until_supervisor_finishes_teardown() {
        let controller = Controller::default();
        let workers = ActiveCodeWorkers::default();
        let worker_count = workers.subscribe();
        controller.configure(1, workers).await;
        let run_id = controller.begin(RunKind::Code).await.expect("code run");
        let termination = controller
            .termination(run_id)
            .await
            .expect("termination signal");
        let supervisor_termination = termination.clone();
        let supervisor_controller = controller.clone();
        let (teardown_started_tx, teardown_started_rx) = tokio::sync::oneshot::channel();
        let (release_teardown_tx, release_teardown_rx) = tokio::sync::oneshot::channel();
        let supervisor = tokio::spawn(async move {
            supervisor_termination.cancelled().await;
            let _ = teardown_started_tx.send(());
            let _ = release_teardown_rx.await;
            supervisor_controller.finish(run_id).await;
        });

        {
            let _request = RequestDropGuard::new(termination.clone(), run_id, EitriPurpose::Code);
        }
        teardown_started_rx.await.expect("teardown started");

        assert_eq!(termination.cause(), TerminationCause::RequestDisconnected);
        assert_eq!(*worker_count.borrow(), 1);
        assert!(
            controller.begin(RunKind::Code).await.is_none(),
            "a replacement run must wait for reap"
        );

        release_teardown_tx.send(()).expect("release teardown");
        supervisor.await.expect("supervisor");
        assert_eq!(*worker_count.borrow(), 0);
        let replacement = controller
            .begin(RunKind::Code)
            .await
            .expect("slot released after reap");
        controller.finish(replacement).await;
    }

    #[tokio::test]
    async fn controller_records_user_cancel_and_runtime_shutdown_causes() {
        let controller = Controller::default();
        let cancelled = controller
            .begin(RunKind::Code)
            .await
            .expect("cancelled run");
        let cancelled_signal = controller
            .termination(cancelled)
            .await
            .expect("cancel signal");
        assert!(controller.cancel().await);
        assert_eq!(cancelled_signal.cause(), TerminationCause::UserCancelled);
        controller.finish(cancelled).await;

        let shutdown = controller.begin(RunKind::Code).await.expect("shutdown run");
        let shutdown_signal = controller
            .termination(shutdown)
            .await
            .expect("shutdown signal");
        assert!(controller.shutdown().await);
        assert_eq!(shutdown_signal.cause(), TerminationCause::RuntimeShutdown);
        controller.finish(shutdown).await;
    }

    #[tokio::test]
    async fn outer_runtime_shutdown_waits_for_supervisor_slot_release() {
        let controller = Controller::default();
        let run_id = controller.begin(RunKind::Code).await.expect("code run");
        let termination = controller
            .termination(run_id)
            .await
            .expect("termination signal");
        let shutdown_controller = controller.clone();
        let mut shutdown =
            tokio::spawn(async move { shutdown_controller.shutdown_and_wait().await });

        termination.cancelled().await;
        assert_eq!(termination.cause(), TerminationCause::RuntimeShutdown);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
                .await
                .is_err(),
            "outer runtime returned before the worker supervisor"
        );

        controller.finish(run_id).await;
        assert!(shutdown.await.expect("shutdown task"));
    }

    #[test]
    fn tool_arguments_are_strict() {
        let parsed_without_cwd: CodeAgentArgs =
            serde_json::from_str(r#"{"instructions":"fix it"}"#).expect("valid arguments");
        assert_eq!(parsed_without_cwd.instructions, "fix it");
        assert_eq!(parsed_without_cwd.cwd, None);

        let parsed: CodeAgentArgs =
            serde_json::from_str(r#"{"instructions":"fix it","cwd":"/tmp/worktree"}"#)
                .expect("valid arguments");
        assert_eq!(parsed.instructions, "fix it");
        assert_eq!(parsed.cwd, Some(PathBuf::from("/tmp/worktree")));
        assert!(
            serde_json::from_str::<CodeAgentArgs>(r#"{"instructions":"fix it","unexpected":true}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<CodeAgentArgs>("{}").is_err());

        let parsed: ExploreAgentArgs =
            serde_json::from_str(r#"{"prompt":"trace it"}"#).expect("valid explore arguments");
        assert_eq!(parsed.prompt, "trace it");
        assert!(
            serde_json::from_str::<ExploreAgentArgs>(
                r#"{"prompt":"trace it","thoroughness":"quick","instructions":"wrong field"}"#
            )
            .is_err()
        );
        assert!(serde_json::from_str::<ExploreAgentArgs>("{}").is_err());

        let batch: ExploreAgentsArgs =
            serde_json::from_str(r#"{"prompts":["first","second"]}"#).expect("batch args");
        assert_eq!(batch.prompts, ["first", "second"]);
        assert!(
            serde_json::from_str::<ExploreAgentsArgs>(
                r#"{"prompts":["first","second"],"unexpected":true}"#
            )
            .is_err()
        );
    }

    #[test]
    fn fanout_tool_is_registered_on_the_mcp_router() {
        assert!(McpHandler::tool_router().get("explore_agents").is_some());
    }

    #[tokio::test]
    async fn fanout_reserves_all_explore_slots_atomically_and_respects_capacity() {
        let controller = Controller::default();
        controller.configure(2, ActiveCodeWorkers::default()).await;

        let ids = controller
            .begin_explores(2)
            .await
            .expect("two slots admitted");
        assert_eq!(ids.len(), 2);
        assert!(controller.begin(RunKind::Explore).await.is_none());
        assert_eq!(
            controller
                .begin_explores(2)
                .await
                .expect_err("pool is full"),
            ExploreAdmission {
                requested: 2,
                available: 0,
                maximum: 2,
            }
        );
        controller.finish(ids[0]).await;
        assert!(controller.begin_explores(2).await.is_err());
        controller.finish(ids[1]).await;
        assert!(controller.begin_explores(3).await.is_err());

        let capped = Controller::default();
        capped.configure(99, ActiveCodeWorkers::default()).await;
        assert_eq!(
            capped
                .begin_explores(17)
                .await
                .expect_err("hard cap rejects oversized fan-out")
                .maximum,
            16
        );
    }

    #[tokio::test]
    async fn shared_fanout_orchestration_admits_and_drives_scouts_concurrently() {
        let controller = Controller::default();
        controller.configure(2, ActiveCodeWorkers::default()).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let results = tokio::time::timeout(
            Duration::from_millis(100),
            run_explore_fanout(
                &controller,
                vec!["first".to_string(), "second".to_string()],
                |prompt, run_id| {
                    let controller = controller.clone();
                    let barrier = barrier.clone();
                    let active = active.clone();
                    let max_active = max_active.clone();
                    async move {
                        let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(now_active, Ordering::SeqCst);
                        assert_eq!(
                            controller.active_explore_count().await,
                            2,
                            "atomic admission must reserve both Explore slots before either scout can finish"
                        );
                        barrier.wait().await;
                        controller.finish(run_id).await;
                        EitriRunResult {
                            outcome: Ok(format!("report {prompt}")),
                            workspace_delta: None,
                        }
                    }
                },
            ),
        )
        .await
        .expect("both admitted scout futures were polled")
        .expect("batch admitted");
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 2);
        assert_eq!(controller.active_explore_count().await, 0);
    }

    #[tokio::test]
    async fn rejected_shared_fanout_does_not_construct_scouts_or_leak_slots() {
        let controller = Controller::default();
        controller.configure(2, ActiveCodeWorkers::default()).await;
        let occupied = controller
            .begin(RunKind::Explore)
            .await
            .expect("occupied slot");
        let runner_calls = Arc::new(AtomicUsize::new(0));
        let calls = runner_calls.clone();

        let result = run_explore_fanout(
            &controller,
            vec!["first".to_string(), "second".to_string()],
            move |_, _| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    EitriRunResult {
                        outcome: Ok("unexpected".to_string()),
                        workspace_delta: None,
                    }
                }
            },
        )
        .await;
        let Err(rejection) = result else {
            panic!("one free slot cannot admit a two-scout batch");
        };
        assert_eq!(rejection.available, 1);
        assert_eq!(runner_calls.load(Ordering::SeqCst), 0);
        assert_eq!(controller.active_explore_count().await, 1);
        controller.finish(occupied).await;
        assert_eq!(controller.active_explore_count().await, 0);
    }

    #[tokio::test]
    async fn fanout_aggregation_is_input_ordered_and_retains_partial_failures() {
        let supervisors: Vec<futures::future::BoxFuture<'static, EitriRunResult>> = vec![
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                EitriRunResult {
                    outcome: Ok("first report".to_string()),
                    workspace_delta: None,
                }
            }),
            Box::pin(async {
                EitriRunResult {
                    outcome: Err(anyhow!("second failed")),
                    workspace_delta: None,
                }
            }),
        ];
        let report = format_explore_fanout(&await_explore_fanout(supervisors).await);
        assert!(
            report.starts_with(
                "launched 2 Eitri explorations concurrently; 1 completed and 1 failed"
            )
        );
        assert!(report.find("[0] completed").unwrap() < report.find("[1] failed").unwrap());
        assert!(report.contains("first report"));
        assert!(report.contains("second failed"));

        let all_failed = [
            EitriRunResult {
                outcome: Err(anyhow!("one")),
                workspace_delta: None,
            },
            EitriRunResult {
                outcome: Err(anyhow!("two")),
                workspace_delta: None,
            },
        ];
        assert!(format_explore_fanout(&all_failed).contains("all failed"));
        assert_eq!(explore_fanout_tool_result(&all_failed).is_error, Some(true));
        assert_eq!(
            explore_fanout_tool_result(&[
                EitriRunResult {
                    outcome: Ok("report".to_string()),
                    workspace_delta: None,
                },
                EitriRunResult {
                    outcome: Err(anyhow!("sibling failed")),
                    workspace_delta: None,
                },
            ])
            .is_error,
            Some(false)
        );
        assert!(
            ExploreAdmission {
                requested: 2,
                available: 1,
                maximum: 2
            }
            .message()
            .contains("was not launched")
        );
    }

    #[test]
    fn fanout_rejects_empty_and_single_prompts() {
        assert!(validate_explore_batch(&[]).is_err());
        assert!(validate_explore_batch(&["one".to_string()]).is_err());
        assert!(validate_explore_batch(&["one".to_string(), " ".to_string()]).is_err());
        assert!(validate_explore_batch(&["one".to_string(), "two".to_string()]).is_ok());
    }

    #[tokio::test]
    async fn explicit_delegated_cwd_becomes_the_only_nested_workspace_root() {
        let primary = tempfile::tempdir().expect("primary workspace");
        let delegated = tempfile::tempdir().expect("delegated worktree");
        let context = RunContext {
            cwd: std::fs::canonicalize(primary.path()).expect("canonical primary"),
            additional_directories: vec![
                std::fs::canonicalize(delegated.path()).expect("canonical delegated worktree"),
            ],
            fs_max_text_bytes: 1,
            access_mode: RuntimeAccessMode::Full,
        };

        let resolved = resolve_code_context(&context, Some(delegated.path()))
            .await
            .expect("authorized delegated worktree");

        assert_eq!(
            resolved.cwd,
            std::fs::canonicalize(delegated.path()).expect("canonical delegated worktree")
        );
        assert!(resolved.additional_directories.is_empty());
    }

    #[tokio::test]
    async fn external_delegated_worktree_snapshot_reports_external_changes_only() {
        let workspace = tempfile::tempdir().expect("workspace parent");
        let primary = workspace.path().join("primary");
        let external = workspace.path().join("external");
        std::fs::create_dir_all(&primary).expect("primary directory");
        std::fs::create_dir_all(&external).expect("external directory");
        init_repo(&primary);
        init_repo(&external);
        let primary = std::fs::canonicalize(&primary).expect("canonical primary");
        let external = std::fs::canonicalize(&external).expect("canonical external");
        let outer = RunContext {
            cwd: primary.clone(),
            additional_directories: vec![external.clone()],
            fs_max_text_bytes: 1,
            access_mode: RuntimeAccessMode::Full,
        };

        let delegated = resolve_code_context(&outer, Some(&external))
            .await
            .expect("authorized external worktree");
        assert_eq!(
            implementation_workspace_roots(&delegated),
            vec![external.clone()]
        );
        let snapshot = capture_implementation_snapshot(&delegated).await;

        std::fs::write(external.join("eitri-external.txt"), "changed by Eitri\n")
            .expect("external change");

        let delta = snapshot.delta().await;
        assert!(delta.changed());
        assert!(
            delta
                .receipt()
                .contains(&format!("Repository: {}", external.display()))
        );
        assert!(
            !delta
                .receipt()
                .contains(&format!("Repository: {}", primary.display()))
        );
        assert!(delta.receipt().contains("eitri-external.txt"));
        let patch = delta.review_patch().expect("external review patch");
        assert!(patch.contains(&format!("Repository: {}", external.display())));
        assert!(patch.contains("eitri-external.txt"));
    }

    #[tokio::test]
    async fn external_delegated_worktree_snapshot_reports_no_change_without_mutation() {
        let workspace = tempfile::tempdir().expect("workspace parent");
        let primary = workspace.path().join("primary");
        let external = workspace.path().join("external");
        std::fs::create_dir_all(&primary).expect("primary directory");
        std::fs::create_dir_all(&external).expect("external directory");
        init_repo(&primary);
        init_repo(&external);
        let outer = RunContext {
            cwd: std::fs::canonicalize(&primary).expect("canonical primary"),
            additional_directories: vec![
                std::fs::canonicalize(&external).expect("canonical external worktree"),
            ],
            fs_max_text_bytes: 1,
            access_mode: RuntimeAccessMode::Full,
        };
        let delegated = resolve_code_context(&outer, Some(&external))
            .await
            .expect("authorized external worktree");

        let delta = capture_implementation_snapshot(&delegated)
            .await
            .delta()
            .await;
        assert!(!delta.changed());
        assert_eq!(delta.receipt(), "No workspace changes.");
        assert!(delta.review_patch().is_none());
    }

    #[tokio::test]
    async fn delegation_without_cwd_keeps_primary_and_additional_snapshot_roots() {
        let workspace = tempfile::tempdir().expect("workspace parent");
        let primary = workspace.path().join("primary");
        let additional = workspace.path().join("additional");
        std::fs::create_dir_all(&primary).expect("primary directory");
        std::fs::create_dir_all(&additional).expect("additional directory");
        init_repo(&primary);
        init_repo(&additional);
        let context = RunContext {
            cwd: std::fs::canonicalize(&primary).expect("canonical primary"),
            additional_directories: vec![
                std::fs::canonicalize(&additional).expect("canonical additional workspace"),
            ],
            fs_max_text_bytes: 1,
            access_mode: RuntimeAccessMode::Full,
        };

        let resolved = resolve_code_context(&context, None)
            .await
            .expect("ordinary delegation context");
        assert_eq!(
            implementation_workspace_roots(&resolved),
            vec![
                context.cwd.clone(),
                context.additional_directories[0].clone(),
            ]
        );
        let snapshot = capture_implementation_snapshot(&resolved).await;
        std::fs::write(
            context.additional_directories[0].join("additional-change.txt"),
            "changed\n",
        )
        .expect("additional change");
        let delta = snapshot.delta().await;
        assert!(delta.changed());
        assert!(delta.receipt().contains("additional-change.txt"));
    }

    #[tokio::test]
    async fn explicit_delegated_cwd_rejects_undelegated_sibling_with_workspace_boundary() {
        let workspace = tempfile::tempdir().expect("workspace parent");
        let primary = workspace.path().join("primary");
        let sibling = workspace.path().join("sibling");
        tokio::fs::create_dir_all(&primary).await.expect("primary");
        tokio::fs::create_dir_all(&sibling).await.expect("sibling");
        let context = RunContext {
            cwd: std::fs::canonicalize(&primary).expect("canonical primary"),
            additional_directories: Vec::new(),
            fs_max_text_bytes: 1,
            access_mode: RuntimeAccessMode::Full,
        };

        let error = resolve_code_context(&context, Some(&sibling))
            .await
            .expect_err("sibling is not an authorized workspace root");
        let diagnostic = format!("{error:?}");
        assert!(
            diagnostic.contains("authorized workspace roots"),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("additional workspace root"),
            "{diagnostic}"
        );
    }

    #[test]
    fn explore_forces_read_only_without_marking_implementation_delegation() {
        assert_eq!(
            EitriPurpose::Explore.access_mode(RuntimeAccessMode::Full),
            RuntimeAccessMode::ReadOnly
        );
        assert_eq!(
            EitriPurpose::Code.access_mode(RuntimeAccessMode::Full),
            RuntimeAccessMode::Full
        );
        assert!(!EitriPurpose::Explore.marks_implementation_delegation());
        assert!(EitriPurpose::Code.marks_implementation_delegation());
    }
}
