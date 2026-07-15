//! One-shot nested ACP agent orchestration exposed to the primary agent as MCP.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

Eitri is available through two optional MCP tools. explore_agent is a read-only scout that can offload bounded, multi-step codebase research at any point in ongoing work, especially when affected locations are unknown, the question crosses multiple areas, or tracing architecture or execution flow requires several search rounds. It is not a required phase or gate before implementation. Direct tools are usually faster for a known path, known symbol, exact definition, work confined to roughly two or three known files, or a trivial single-step lookup; use your judgment. Because every Eitri call starts with fresh context, an explore_agent prompt must be a complete standalone brief that states the current task state and work already completed, the specific question, known context, scope, stopping condition, and expected report. Select the lowest adequate explicit thoroughness: quick, medium, or very_thorough.

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

Thoroughness levels:
- quick: targeted lookups and key files only.
- medium: follow the central call path and critical imports, then inspect the nearest relevant tests or types.
- very thorough: trace relevant dependencies, variants, tests, and configuration across multiple locations.

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
    Explore(ExploreThoroughness),
}

impl EitriPurpose {
    fn marks_implementation_delegation(self) -> bool {
        matches!(self, Self::Code)
    }

    fn internal_message_kind(self) -> InternalMessageKind {
        match self {
            Self::Code => InternalMessageKind::Delegation,
            Self::Explore(_) => InternalMessageKind::Exploration,
        }
    }

    fn access_mode(self, configured: RuntimeAccessMode) -> RuntimeAccessMode {
        match self {
            Self::Code => configured,
            Self::Explore(_) => RuntimeAccessMode::ReadOnly,
        }
    }

    fn standalone_prompt(self, task: &str) -> String {
        match self {
            Self::Code => format!("{CODE_PREAMBLE}{task}"),
            Self::Explore(thoroughness) => {
                format!(
                    "{EXPLORE_PREAMBLE}Thoroughness level: {}.\n\nSearch request:\n{task}",
                    thoroughness.prompt_label()
                )
            }
        }
    }

    fn loki_context(self, task: &str) -> String {
        match self {
            Self::Code => {
                format!("Eitri received this standalone implementation delegation:\n{task}")
            }
            Self::Explore(_) => {
                format!("Eitri received this standalone read-only exploration request:\n{task}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExploreThoroughness {
    Quick,
    Medium,
    VeryThorough,
}

impl ExploreThoroughness {
    fn prompt_label(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Medium => "medium",
            Self::VeryThorough => "very thorough",
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
            max_parallel_explores: 6,
        }
    }

    pub fn with_implementation_handoff_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.implementation_handoff_counter = Some(counter);
        self
    }

    pub fn with_max_parallel_explores(mut self, max: usize) -> Self {
        self.max_parallel_explores = max.min(16);
        self
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExploreAgentArgs {
    /// Complete, standalone read-only research request for the delegated agent.
    pub prompt: String,
    /// Lowest adequate search depth: quick, medium, or very_thorough.
    pub thoroughness: ExploreThoroughness,
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
        description = "IMPLEMENTATION DELEGATE (EITRI). Treat this as delegation to a strong coding engineer with fresh context. Give Eitri one forgeable unit: a substantial, self-contained implementation slice that can be completed in one focused pass and returned as one coherent, reviewable diff. A good handoff has one clear outcome, enough context and decisions to begin immediately, explicit constraints and acceptance checks, and leaves the workspace coherent and testable. Delegate when implementation is clearly more work than writing the handoff and reviewing the result. Do NOT delegate trivial local edits, investigation better handled directly or with explore_agent, unresolved architectural questions, or an entire open-ended project; split large work into sequential, independently verifiable units. Thor owns research, planning, coordination, review, verification, and the final response, and should make small local changes directly when delegation would cost more effort. Every call starts a fresh ACP process/session with zero conversation or prior-call memory. Pass complete standalone instructions with the task, plan, relevant findings, current workspace state, and acceptance criteria. The result includes the bounded full workspace diff attributable to this invocation. Review Eitri's result and diff independently and call it again for substantial corrections."
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
        let Some(run_id) = self.controller.begin(RunKind::Code).await else {
            return Ok(CallToolResult::error(vec![Content::text(
                "an Eitri implementation run is already active",
            )]));
        };

        let result = run_boxed(
            self.config.clone(),
            self.context.clone(),
            args.instructions,
            EitriPurpose::Code,
            self.ui_tx.clone(),
            self.controller.clone(),
            run_id,
        )
        .await;
        let workspace_delta = result
            .workspace_delta
            .as_ref()
            .expect("code_agent always captures a workspace delta");
        Ok(match result.outcome {
            Ok(message) => CallToolResult::success(vec![Content::text(with_workspace_diff(
                &message,
                workspace_delta,
            ))]),
            Err(error) => CallToolResult::error(vec![Content::text(with_workspace_diff(
                &error.to_string(),
                workspace_delta,
            ))]),
        })
    }

    #[tool(
        name = "explore_agent",
        description = "OPTIONAL READ-ONLY EXPLORATION DELEGATE (EITRI). Use this at any point in ongoing work to offload bounded, multi-step codebase research, especially when affected locations are unknown or the question requires multiple search rounds. It is not a required phase before implementation. Direct tools are usually faster for a known path, known symbol, exact definition, work confined to roughly 2-3 known files, or a trivial lookup. Use your judgment. Every call starts with fresh context, so the complete standalone prompt must state the current task state and work already completed, the specific question, known context, scope, stopping condition, and expected report. Set the explicit thoroughness field to the lowest adequate depth. Returns one concise research report."
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

        let result = run_boxed(
            self.config.clone(),
            self.context.clone(),
            prompt.to_string(),
            EitriPurpose::Explore(args.thoroughness),
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
}

impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("mj-code-agent", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "EITRI DELEGATION POLICY: explore_agent is an optional read-only scout for bounded, multi-step research at any point in ongoing work, especially when locations are unknown or the question needs multiple search rounds; it is not a required phase before implementation. Direct tools are usually faster for known paths, known symbols, roughly 2-3 known files, and trivial lookups. Set explore_agent's explicit thoroughness field to the lowest adequate depth. Give code_agent one forgeable implementation unit at a time. Thor chooses and sequences tools, retains planning, coordination, review, verification, and the final answer, and must give each fresh Eitri call complete standalone context including the current task state and work already completed.",
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
        controller.configure(config.max_parallel_explores).await;
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
    },
    Running {
        kind: RunKind,
        commands: mpsc::UnboundedSender<UiCommand>,
    },
}

#[derive(Debug)]
struct ControllerState {
    next_id: u64,
    max_parallel_explores: usize,
    runs: HashMap<u64, ActiveRun>,
}

impl Default for ControllerState {
    fn default() -> Self {
        Self {
            next_id: 1,
            max_parallel_explores: 6,
            runs: HashMap::new(),
        }
    }
}

/// Coordinates one implementation run and a bounded pool of read-only scouts.
#[derive(Debug, Clone, Default)]
pub struct Controller {
    state: Arc<Mutex<ControllerState>>,
}

impl Controller {
    async fn configure(&self, max_parallel_explores: usize) {
        self.state.lock().await.max_parallel_explores = max_parallel_explores.min(16);
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
        let id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);
        state.runs.insert(
            id,
            ActiveRun::Starting {
                kind,
                cancel_requested: false,
                shutdown_requested: false,
            },
        );
        Some(id)
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
        } = run
        else {
            return;
        };
        state.runs.insert(
            id,
            ActiveRun::Running {
                kind,
                commands: commands.clone(),
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
                    cancel_requested, ..
                } => *cancel_requested = true,
                ActiveRun::Running { commands, .. } => {
                    let _ = commands.send(UiCommand::CancelPrompt);
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
                    shutdown_requested, ..
                } => *shutdown_requested = true,
                ActiveRun::Running { commands, .. } => {
                    let _ = commands.send(UiCommand::Shutdown);
                }
            }
        }
        active
    }

    async fn finish(&self, id: u64) {
        self.state.lock().await.runs.remove(&id);
    }
}

impl ActiveRun {
    fn kind(&self) -> RunKind {
        match self {
            Self::Starting { kind, .. } | Self::Running { kind, .. } => *kind,
        }
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

fn run_boxed(
    config: Config,
    context: RunContext,
    task: String,
    purpose: EitriPurpose,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    controller: Controller,
    run_id: u64,
) -> futures::future::BoxFuture<'static, EitriRunResult> {
    Box::pin(run(
        config, context, task, purpose, ui_tx, controller, run_id,
    ))
}

async fn run(
    config: Config,
    context: RunContext,
    task: String,
    purpose: EitriPurpose,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    controller: Controller,
    run_id: u64,
) -> EitriRunResult {
    let log_role = config.role_config.clone();
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
        EitriPurpose::Explore(_) => CodeAgentEvent::ExplorationStarted {
            run_id,
            label: display_label,
        },
    };
    let _ = ui_tx.send(UiEvent::CodeAgent(start_event));

    let invocation_snapshot = if purpose.marks_implementation_delegation() {
        let mut workspace_roots = Vec::with_capacity(1 + context.additional_directories.len());
        workspace_roots.push(context.cwd.clone());
        workspace_roots.extend(context.additional_directories.iter().cloned());
        Some(WorkspaceSnapshot::capture(&workspace_roots).await)
    } else {
        None
    };

    let (nested_event_tx, mut nested_event_rx) = mpsc::unbounded_channel();
    let (nested_cmd_tx, nested_cmd_rx) = mpsc::unbounded_channel();
    controller.attach(run_id, nested_cmd_tx.clone()).await;

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
    let mut decisions = loki.as_ref().map(loki::Handle::subscribe);
    let mut tracker = loki::BoundaryTracker::default();
    let mut intervention = loki::DeferredIntervention::default();
    let mut latest_usage_update: Option<UsageUpdate> = None;
    let result = loop {
        tokio::select! {
            joined = &mut runtime => {
                break match joined {
                    Ok(Ok(())) => Err(anyhow!("Eitri runtime closed before completing")),
                    Ok(Err(error)) => Err(error).context("Eitri runtime"),
                    Err(error) => Err(anyhow!("Eitri task failed: {error}")),
                };
            }
            event = nested_event_rx.recv() => {
                let Some(event) = event else {
                    break Err(anyhow!("Eitri event stream closed before completing"));
                };
                let boundary = (epoch > 0).then(|| tracker.observe(&event)).flatten();
                let boundary_observed = boundary.is_some();
                let target_completed = matches!(
                    &event,
                    UiEvent::PromptDone { .. }
                        | UiEvent::PromptFailed { .. }
                        | UiEvent::SessionForkFailed { .. }
                        | UiEvent::Fatal(_)
                );
                if target_completed && let Some(reviewer) = loki.as_ref() {
                    reviewer.target_completed(epoch, loki::Target::Eitri, eitri_invocation);
                }
                let interrupting = boundary_observed
                    && !target_completed
                    && intervention.interrupt_at_boundary();
                if interrupting {
                    let _ = nested_cmd_tx.send(UiCommand::CancelPrompt);
                }
                if let Some(boundary) = boundary
                    && !interrupting
                    && !(target_completed && intervention.is_pending())
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
                    UiEvent::SessionStarted { .. } | UiEvent::SessionConfigOptions { .. } => {}
                    UiEvent::SessionUpdate(update) => {
                        if let SessionUpdate::UsageUpdate(value) = &update {
                            latest_usage_update = Some(value.clone());
                        }
                        collector.observe(&update);
                        match purpose {
                            EitriPurpose::Code => {
                                let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::SessionUpdate(update)));
                            }
                            EitriPurpose::Explore(_) => {
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
                        if matches!(purpose, EitriPurpose::Explore(_)) {
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
                        if matches!(purpose, EitriPurpose::Explore(_)) {
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
                            EitriPurpose::Explore(_) => CodeAgentEvent::ExplorationProgress {
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
                                EitriPurpose::Explore(_) => Purpose::Explore,
                            }),
                            usage,
                            update: latest_usage_update.take(),
                        }));
                        if matches!(stop_reason, StopReason::Cancelled) {
                            if intervention.is_pending()
                                && !intervention.cancellation_was_requested()
                            {
                                // A user cancellation wins if Loki had not yet
                                // reached the deferred interruption boundary.
                                intervention.clear();
                                break Err(anyhow!("Eitri cancelled"));
                            }
                            if let Some(critique) = intervention.take() {
                                if let Some(reviewer) = loki.as_ref() {
                                    reviewer.target_resumed(epoch, loki::Target::Eitri, eitri_invocation);
                                }
                                collector = AgentMessageCollector::new();
                                tracker.reset_attempt();
                                let continuation = continuation_prompt(purpose, &critique);
                                emit_continuation(&ui_tx, &critique);
                                if nested_cmd_tx.send(UiCommand::SendPrompt { text: continuation, images: Vec::new() }).is_err() {
                                    break Err(anyhow!("re-prompt Eitri after Loki intervention"));
                                }
                                continue;
                            }
                            break Err(anyhow!("Eitri cancelled"));
                        }
                        if let Some(critique) = intervention.take() {
                            if let Some(reviewer) = loki.as_ref() {
                                reviewer.target_resumed(epoch, loki::Target::Eitri, eitri_invocation);
                            }
                            collector = AgentMessageCollector::new();
                            tracker.reset_attempt();
                            let continuation = continuation_prompt(purpose, &critique);
                            emit_continuation(&ui_tx, &critique);
                            if nested_cmd_tx.send(UiCommand::SendPrompt { text: continuation, images: Vec::new() }).is_err() {
                                break Err(anyhow!("re-prompt Eitri after Loki intervention"));
                            }
                            continue;
                        }
                        break collector.finish();
                    }
                    UiEvent::PromptFailed { message }
                    | UiEvent::SessionForkFailed { message }
                    | UiEvent::Fatal(message) => {
                        if let Some(critique) = intervention.take() {
                            if let Some(reviewer) = loki.as_ref() {
                                reviewer.target_resumed(epoch, loki::Target::Eitri, eitri_invocation);
                            }
                            collector = AgentMessageCollector::new();
                            tracker.reset_attempt();
                            let continuation = continuation_prompt(purpose, &critique);
                            emit_continuation(&ui_tx, &critique);
                            if nested_cmd_tx.send(UiCommand::SendPrompt { text: continuation, images: Vec::new() }).is_err() {
                                break Err(anyhow!("re-prompt Eitri after Loki intervention"));
                            }
                            continue;
                        }
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
            decision = async {
                match decisions.as_mut() {
                    Some(rx) => rx.recv().await.ok(),
                    None => std::future::pending().await,
                }
            } => {
                let Some(decision) = decision else { continue; };
                if decision.epoch != epoch || decision.target != loki::Target::Eitri {
                    continue;
                }
                {
                    let critique = decision.critique;
                        if let Some(role) = log_role.as_ref()
                            && let Some(council_session) = role.council_session.as_deref()
                        {
                            tracing::info!(
                                event = "advice_received",
                                council_session,
                                god = "Eitri",
                                source = "Loki",
                                model = %role.model_id,
                                adapter = %role.adapter_source_id,
                                advice = %critique,
                                "Eitri received Loki advice"
                            );
                        }
                        intervention.push(decision.id, critique);
                }
            }
        }
    };

    if let (Some(reviewer), Some(invocation)) = (loki.as_ref(), eitri_invocation) {
        reviewer.end_eitri(epoch, invocation);
    }
    let mut result = result;
    if result.is_ok()
        && let Some(reviewer) = loki.as_ref()
    {
        let deferred = reviewer.take_deferred(epoch);
        if !deferred.is_empty()
            && let Ok(message) = result.as_mut()
        {
            message.push_str("\n\n<loki_advice target=\"thor\">\n");
            message.push_str(&loki::format_deferred(&deferred));
            message.push_str("\n</loki_advice>");
        }
    }

    let _ = nested_cmd_tx.send(UiCommand::Shutdown);
    if !runtime.is_finished()
        && tokio::time::timeout(Duration::from_secs(2), &mut runtime)
            .await
            .is_err()
    {
        runtime.abort();
        let _ = runtime.await;
    }
    controller.finish(run_id).await;
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
        EitriPurpose::Explore(_) => CodeAgentEvent::ExplorationFinished { run_id, outcome },
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

fn with_workspace_diff(message: &str, delta: &WorkspaceDelta) -> String {
    let diff = delta.review_patch().unwrap_or_else(|| delta.receipt());
    let mut result = format!(
        "{message}\n\n<workspace_diff scope=\"eitri-invocation\" authored_by=\"Eitri\">\n{diff}\n</workspace_diff>"
    );
    if delta.changed() {
        result.push_str("\n\nYou should review Eitri's work now.");
    }
    result
}

fn continuation_prompt(purpose: EitriPurpose, critique: &str) -> String {
    let activity = match purpose {
        EitriPurpose::Code => "implementation",
        EitriPurpose::Explore(_) => "read-only exploration",
    };
    format!(
        "<advisory guidance=\"weigh, don't blindly obey\">\n{critique}\n</advisory>\n\nContinue the interrupted {activity} turn. Address the material advice, then finish the existing task. Please continue from where you left off."
    )
}

fn emit_continuation(ui_tx: &mpsc::UnboundedSender<UiEvent>, text: &str) {
    let _ = ui_tx.send(UiEvent::InternalMessage(InternalMessage {
        source: "Loki".to_string(),
        target: LABEL.to_string(),
        kind: InternalMessageKind::Continuation,
        text: text.to_string(),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{ContentBlock, ContentChunk, TextContent};

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
        controller.configure(2).await;
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

        let parsed: ExploreAgentArgs =
            serde_json::from_str(r#"{"prompt":"trace it","thoroughness":"very_thorough"}"#)
                .expect("valid explore arguments");
        assert_eq!(parsed.prompt, "trace it");
        assert_eq!(parsed.thoroughness, ExploreThoroughness::VeryThorough);
        assert!(
            serde_json::from_str::<ExploreAgentArgs>(
                r#"{"prompt":"trace it","thoroughness":"quick","instructions":"wrong field"}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ExploreAgentArgs>(
                r#"{"prompt":"trace it","thoroughness":"extreme"}"#
            )
            .is_err()
        );
        assert!(serde_json::from_str::<ExploreAgentArgs>(r#"{"prompt":"trace it"}"#).is_err());
        assert!(serde_json::from_str::<ExploreAgentArgs>("{}").is_err());
    }

    #[test]
    fn explore_uses_explicit_thoroughness_and_forces_read_only_without_marking_delegation() {
        assert_eq!(
            EitriPurpose::Explore(ExploreThoroughness::Quick).access_mode(RuntimeAccessMode::Full),
            RuntimeAccessMode::ReadOnly
        );
        assert_eq!(
            EitriPurpose::Code.access_mode(RuntimeAccessMode::Full),
            RuntimeAccessMode::Full
        );
        assert!(
            !EitriPurpose::Explore(ExploreThoroughness::Medium).marks_implementation_delegation()
        );
        assert!(EitriPurpose::Code.marks_implementation_delegation());
        assert!(
            EitriPurpose::Explore(ExploreThoroughness::VeryThorough)
                .standalone_prompt("don't do a quick pass")
                .contains("Thoroughness level: very thorough")
        );
    }

    #[test]
    fn loki_continuation_prompt_keeps_extra_resume_instruction_hidden() {
        let critique = "Would it be better to verify the fallback first?";
        let prompt = continuation_prompt(EitriPurpose::Code, critique);
        assert!(prompt.contains(critique));
        assert!(prompt.contains("Please continue from where you left off."));

        let (tx, mut rx) = mpsc::unbounded_channel();
        emit_continuation(&tx, critique);
        let event = rx.try_recv().expect("continuation event");
        let UiEvent::InternalMessage(message) = event else {
            panic!("expected internal message");
        };
        assert_eq!(message.text, critique);
        assert!(
            !message
                .text
                .contains("Please continue from where you left off.")
        );
    }
}
