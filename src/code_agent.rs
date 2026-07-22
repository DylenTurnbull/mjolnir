//! One-shot nested ACP agent orchestration exposed to the primary agent as MCP.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
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
use tokio::sync::{Mutex, mpsc, oneshot, watch};
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
const SERVER_DELEGATION_GUIDANCE: &str = "EITRI DELEGATION POLICY: explore_agent is one optional read-only scout. For two or more independent scouts that must actually overlap, use explore_agents with complete standalone prompts: it atomically launches the whole batch concurrently or rejects it without queueing. code_agent runs one implementation slice (about four minutes) and either returns Eitri's final result or reports that Eitri is PAUSED with its partial workspace diff and a run_id; a paused Eitri is idle, not running, and its partial edits are already in the workspace. When paused, call code_agent_continue with that run_id to resume, or code_agent_cancel to stop and keep the partial edits; do not start a new code_agent delegation while a run_id is outstanding. Thor chooses and sequences tools, retains planning, coordination, review, verification, and the final answer.";
pub const PRIMARY_SESSION_DIRECTIVE: &str = r#"<mj-code-agent-policy>
You are Thor, the primary coordinator and owner of the user's outcome. You are responsible for understanding the request, doing necessary research and context gathering, forming the plan, coordinating implementation, reviewing and verifying the result, and delivering the final answer. You are not a thin handoff between the user and Eitri. This policy applies to every subsequent user request in this ACP session.

Loki is Mjolnir's one persistent read-only observer of your work and implementation Eitri's work. Never create, summon, or substitute another Loki process or session. Loki does not observe Explore. Use pull_advice at good semantic stopping points: do not pull on two consecutive semantic steps, and never let more than eight semantic steps pass without pulling. Automatic Loki receipts already drain the queues they name, so do not immediately pull again after a receipt.

Eitri is available through optional MCP tools. explore_agent is a single read-only scout for bounded, multi-step codebase research at any point in ongoing work. explore_agents is the only way to request concurrent scouting: use it only for two or more independent, complete standalone prompts. It atomically admits and launches every requested scout together or rejects the batch for insufficient capacity; it never queues or serializes overflow work. Do not claim scouts are parallel merely because you made separate explore_agent calls—those calls may be sequential. A concurrency claim is justified only after explore_agents reports that it launched the batch concurrently. Direct tools are usually faster for a known path, known symbol, exact definition, work confined to roughly two or three known files, or a trivial single-step lookup; use your judgment. Because every Eitri call starts with fresh context, every exploration prompt must state the current task state and work already completed, the specific question, known context, scope, stopping condition, and expected report.

Treat code_agent as delegation to a strong coding engineer with fresh context. Give Eitri one forgeable unit at a time: a substantial, self-contained implementation slice that can be completed in one focused pass and returned as one coherent, reviewable diff. A good handoff has one clear outcome, enough context and decisions to begin immediately, explicit constraints and acceptance checks, and leaves the workspace in a coherent, testable state. Delegate when implementing the change is clearly more work than writing the handoff and reviewing the result. Do not delegate trivial local edits, investigation better handled with direct tools or explore_agent, unresolved architectural questions, or an entire open-ended project. Split large work into sequential, independently verifiable units. You may personally make small, local code changes when describing and delegating them would take more effort than simply doing them; use judgment rather than delegating mechanically. Pass code_agent complete standalone instructions with the task, plan, relevant findings, current workspace state, and acceptance criteria. Its result includes the bounded full workspace diff attributable to that invocation. After Eitri returns, independently review its result and diff, inspect or verify the work as needed, and delegate a substantial corrective follow-up if implementation changes remain. If a request requires no code changes and no open-ended exploration, handle it yourself.

A code_agent call that reports Eitri as PAUSED is healthy: the slice budget elapsed before Eitri finished, so its in-flight turn was cancelled and it is now idle, holding its partial workspace diff, with no LLM turn in flight and no further file mutation happening. Call code_agent_continue with the returned run_id to resume it, or code_agent_cancel to stop it and keep its partial edits as-is. Do not start a new code_agent delegation while a run_id from a paused run is outstanding; continue or cancel it first. It is safe to inspect the partial diff a paused run has left in the workspace, but do not edit that workspace directly until the run is resolved.

Every Eitri call starts a brand-new ACP process and session. Eitri has no conversation context and no memory of the user's request or any earlier Eitri call, including an immediately preceding call. Apply this policy throughout this ACP session while handling each current user request; do not acknowledge or summarize the policy.
</mj-code-agent-policy>"#;

const CODE_PREAMBLE: &str = "You are Eitri, the implementation agent. This is a fresh ACP process and session. You have no memory of the user conversation or of any earlier Eitri call, including an immediately preceding call. Treat the standalone instructions below and the current workspace as your only task context. Loki is Mjolnir's one persistent read-only observer shared with Thor; never create, summon, or substitute another Loki process or session. Use pull_advice at good semantic stopping points, never on two consecutive semantic steps and at least once every eight semantic steps. Always pull after failed validation before retrying, and before finalizing when at least one semantic step has elapsed since the last pull or automatic receipt. Automatic Loki receipts already drain the named queues.\n\n";
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
/// Duration of one Eitri implementation slice. Chosen to stay safely under
/// the primary MCP client's request deadline (Anvil: being raised to 300s;
/// Codex: 300s already), leaving headroom for turn cancellation to settle
/// and the workspace diff to be computed before the HTTP response is due.
/// If Eitri has not finished its turn when a slice expires, `code_agent`
/// cancels the in-flight ACP turn, snapshots the workspace, and pauses the
/// worker instead of leaving it running unattended: see `code_agent` and
/// `code_agent_continue`.
const CODE_AGENT_SLICE: Duration = Duration::from_secs(250);

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

#[derive(Clone)]
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
    headless_permission_mode: Option<crate::config::CouncilPermissionMode>,
    role_pool: Option<crate::quota::RolePool>,
    warm: Arc<WarmPool>,
}

#[derive(Default)]
struct WarmPool {
    slots: StdMutex<WarmSlots>,
}

#[derive(Default)]
struct WarmSlots {
    code: Option<WarmRuntime>,
    explore: Option<WarmRuntime>,
}

struct WarmRuntime {
    context: RunContext,
    role_key: String,
    events: mpsc::UnboundedReceiver<UiEvent>,
    commands: mpsc::UnboundedSender<UiCommand>,
    task: JoinHandle<Result<()>>,
    cancel: CancellationToken,
    _pull_server: Option<loki::PullServer>,
}

impl Drop for WarmPool {
    fn drop(&mut self) {
        let slots = self.slots.get_mut().expect("Eitri warm pool poisoned");
        for runtime in [slots.code.as_ref(), slots.explore.as_ref()]
            .into_iter()
            .flatten()
        {
            runtime.cancel.cancel();
            let _ = runtime.commands.send(UiCommand::Shutdown);
        }
    }
}

impl Config {
    pub fn council(
        role_pool: crate::quota::RolePool,
        agent_stderr: Option<PathBuf>,
        loki: Option<loki::Handle>,
    ) -> Self {
        let role = role_pool.current();
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
                permission: None,
                council_session: None,
            }),
            loki,
            implementation_handoff_counter: None,
            active_implementation_workers: ActiveCodeWorkers::default(),
            max_parallel_explores: 6,
            headless_permission_mode: None,
            role_pool: Some(role_pool),
            warm: Arc::default(),
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

    pub fn with_headless_permission_mode(
        mut self,
        mode: crate::config::CouncilPermissionMode,
    ) -> Self {
        self.headless_permission_mode = Some(mode);
        self
    }

    pub fn with_prewarm(self, context: RunContext) -> Self {
        self.ensure_warm(EitriPurpose::Code, context.clone());
        self.ensure_warm(EitriPurpose::Explore, context);
        self
    }

    fn ensure_warm(&self, purpose: EitriPurpose, context: RunContext) {
        let mut slots = self.warm.slots.lock().expect("Eitri warm pool poisoned");
        let slot = match purpose {
            EitriPurpose::Code => &mut slots.code,
            EitriPurpose::Explore => &mut slots.explore,
        };
        let role_key = self.role_key();
        if slot
            .as_ref()
            .is_some_and(|runtime| runtime.context != context || runtime.role_key != role_key)
        {
            let stale = slot.take().expect("checked warm slot disappeared");
            stale.cancel.cancel();
            let _ = stale.commands.send(UiCommand::Shutdown);
        }
        if slot.is_none() {
            *slot = Some(spawn_eitri_runtime(self, context, purpose, None));
        }
    }

    fn take_warm(&self, purpose: EitriPurpose, context: &RunContext) -> Option<WarmRuntime> {
        let mut slots = self.warm.slots.lock().expect("Eitri warm pool poisoned");
        let slot = match purpose {
            EitriPurpose::Code => &mut slots.code,
            EitriPurpose::Explore => &mut slots.explore,
        };
        if slot
            .as_ref()
            .is_some_and(|runtime| runtime.task.is_finished())
        {
            let failed = slot.take().expect("finished Eitri warm slot disappeared");
            failed.cancel.cancel();
            let _ = failed.commands.send(UiCommand::Shutdown);
        }
        let role_key = self.role_key();
        if slot
            .as_ref()
            .is_some_and(|runtime| runtime.context == *context && runtime.role_key == role_key)
        {
            slot.take()
        } else {
            None
        }
    }

    fn role_key(&self) -> String {
        self.role_config
            .as_ref()
            .map(|role| {
                format!(
                    "{}\0{}\0{:?}",
                    role.adapter_source_id, role.model_id, self.headless_permission_mode
                )
            })
            .unwrap_or_else(|| self.display_label.clone())
    }

    fn apply_role(&mut self, role: ResolvedRole) {
        self.display_label = format!("Eitri · {}", role.model.model);
        self.command = role.launch.command;
        self.args = role.launch.args;
        self.env = role.launch.env;
        let council_session = self
            .role_config
            .as_ref()
            .and_then(|config| config.council_session.clone());
        self.role_config = Some(acp::RuntimeRoleConfig {
            label: LABEL.to_string(),
            model_id: role.model.model,
            model_value: role.model_value,
            adapter_source_id: role.launch.source_id,
            permission: None,
            council_session,
        });
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
pub struct CodeAgentContinueArgs {
    /// Paused implementation run ID returned by code_agent or code_agent_continue.
    pub run_id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CodeAgentCancelArgs {
    /// Paused implementation run ID returned by code_agent or code_agent_continue.
    pub run_id: u64,
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
    code_runs: CodeRunRegistry,
    handoff_retry: Arc<Mutex<Option<HandoffRetry>>>,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HandoffRetry {
    epoch: u64,
    instructions: String,
    cwd: Option<PathBuf>,
}

#[tool_router(router = tool_router)]
impl McpHandler {
    fn new(
        config: Config,
        context: RunContext,
        ui_tx: mpsc::UnboundedSender<UiEvent>,
        controller: Controller,
    ) -> Self {
        Self {
            config,
            context,
            ui_tx,
            controller,
            code_runs: CodeRunRegistry::default(),
            handoff_retry: Arc::new(Mutex::new(None)),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "code_agent",
        description = "IMPLEMENTATION DELEGATE (EITRI). Treat this as delegation to a strong coding engineer with fresh context. Give Eitri one forgeable unit: a substantial, self-contained implementation slice that can be completed in one focused pass and returned as one coherent, reviewable diff. A good handoff has one clear outcome, enough context and decisions to begin immediately, explicit constraints and acceptance checks, and leaves the workspace coherent and testable. For an explicitly authorized implementation worktree, pass its absolute cwd argument; do not infer a worktree from instructions. Delegate when implementation is clearly more work than writing the handoff and reviewing the result. Do NOT delegate trivial local edits, investigation better handled directly or with explore_agent, unresolved architectural questions, or an entire open-ended project; split large work into sequential, independently verifiable units. Thor owns research, planning, coordination, review, verification, and the final response, and should make small local changes directly when delegation would cost more effort. Every call starts a fresh ACP process/session with zero conversation or prior-call memory. Pass complete standalone instructions with the task, plan, relevant findings, current workspace state, and acceptance criteria. This call runs Eitri for one implementation slice (about four minutes) and returns one of two shapes. FINAL: Eitri finished; the result includes the bounded full workspace diff attributable to this invocation. Review Eitri's result and diff independently and call code_agent again for substantial corrections. PAUSED: the slice budget elapsed before Eitri finished, so its in-flight turn was cancelled and it is now idle, not running, holding a run_id and its partial workspace diff so far; no further file mutation is happening. You MUST call code_agent_continue with that run_id to resume it, or code_agent_cancel to stop it and keep its partial edits, before starting any new code_agent delegation."
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
        if let Some(reviewer) = self.config.loki.as_ref() {
            let retry = HandoffRetry {
                epoch: reviewer.current_epoch(),
                instructions: args.instructions.clone(),
                cwd: args.cwd.clone(),
            };
            let bypass = {
                let mut pending = self.handoff_retry.lock().await;
                if pending.as_ref() == Some(&retry) {
                    pending.take();
                    true
                } else {
                    *pending = None;
                    false
                }
            };
            if !bypass {
                let outcome = reviewer.pull(loki::Consumer::Thor).await;
                if !outcome.is_empty() {
                    *self.handoff_retry.lock().await = Some(retry);
                    let receipt = loki::format_pull_outcome(
                        &outcome,
                        reviewer.current_epoch(),
                        loki::Consumer::Thor,
                    );
                    let mut result = CallToolResult::success(vec![Content::text(format!(
                        "Loki left advice before this implementation handoff. Apply it if relevant; otherwise retry the same delegation. Eitri was not started.\n\n{receipt}"
                    ))]);
                    result.structured_content = Some(serde_json::json!({
                        "delegationStarted": false,
                        "reason": "loki_advice",
                        "adviceCount": outcome.advice.len(),
                        "dropped": outcome.dropped,
                    }));
                    return Ok(result);
                }
            }
        }
        let Some((run_id, termination)) =
            self.controller.begin_with_termination(RunKind::Code).await
        else {
            let message = match self.controller.active_code_run_id().await {
                Some(outstanding) => outstanding_code_run_message(outstanding),
                None => "an Eitri implementation run is already active".to_string(),
            };
            return Ok(CallToolResult::error(vec![Content::text(message)]));
        };
        if let Some(reviewer) = self.config.loki.as_ref() {
            reviewer.begin_eitri_handoff();
        }

        let (control_tx, respond_rx) = launch_code_worker(
            self.controller.clone(),
            self.config.clone(),
            context,
            args.instructions,
            self.ui_tx.clone(),
            run_id,
            termination,
        );
        Ok(self.resolve_slice(run_id, control_tx, respond_rx).await)
    }

    #[tool(
        name = "code_agent_continue",
        description = "RESUME A PAUSED EITRI IMPLEMENTATION. Use only with the run_id of an Eitri run that code_agent or code_agent_continue reported as PAUSED. Sends Eitri a continuation prompt on its retained ACP session, so its prior progress and conversation context are preserved, then runs one more implementation slice (about four minutes). Returns the same two shapes as code_agent: FINAL (Eitri finished; includes the bounded full workspace diff) or PAUSED again (another run_id to continue or cancel). Calling this with an unknown or already-resolved run_id fails."
    )]
    async fn code_agent_continue(
        &self,
        Parameters(args): Parameters<CodeAgentContinueArgs>,
    ) -> std::result::Result<CallToolResult, McpError> {
        let Some(control_tx) = self.code_runs.take(args.run_id) else {
            return Err(McpError::invalid_params(
                unresolved_run_id_message(args.run_id),
                None,
            ));
        };
        let (respond, respond_rx) = oneshot::channel();
        let prompt =
            "Continue the implementation task. Your previous progress is preserved in the workspace."
                .to_string();
        if control_tx
            .send(WorkerRequest::Continue { prompt, respond })
            .is_err()
        {
            return Ok(CallToolResult::error(vec![Content::text(
                worker_unavailable_message(args.run_id),
            )]));
        }
        Ok(self
            .resolve_slice(args.run_id, control_tx, respond_rx)
            .await)
    }

    #[tool(
        name = "code_agent_cancel",
        description = "STOP A PAUSED EITRI IMPLEMENTATION. Use only with the run_id of an Eitri run that code_agent or code_agent_continue reported as PAUSED. Terminates the paused worker and its ACP session; it does NOT revert any changes Eitri already made, so its partial edits remain in the workspace exactly as it left them. Returns the final workspace diff attributable to the whole invocation so Thor can review or finish the work directly. Calling this with an unknown or already-resolved run_id fails."
    )]
    async fn code_agent_cancel(
        &self,
        Parameters(args): Parameters<CodeAgentCancelArgs>,
    ) -> std::result::Result<CallToolResult, McpError> {
        let Some(control_tx) = self.code_runs.take(args.run_id) else {
            return Err(McpError::invalid_params(
                unresolved_run_id_message(args.run_id),
                None,
            ));
        };
        let (respond, respond_rx) = oneshot::channel();
        if control_tx.send(WorkerRequest::Cancel { respond }).is_err() {
            return Ok(CallToolResult::error(vec![Content::text(
                worker_unavailable_message(args.run_id),
            )]));
        }
        Ok(match respond_rx.await {
            Ok(result) => cancelled_tool_result(&result),
            Err(_) => CallToolResult::error(vec![Content::text(format!(
                "Eitri code run {} was cancelled, but its worker ended before confirming teardown. Any partial edits remain in the workspace exactly as Eitri left them.",
                args.run_id
            ))]),
        })
    }

    /// Deliver one slice outcome to Thor and keep the code-run registry in
    /// sync: a run is registered only while it is genuinely paused and idle,
    /// so its presence in the registry is exactly the single-slot rule's
    /// "outstanding, unresolved" state.
    async fn resolve_slice(
        &self,
        run_id: u64,
        control_tx: mpsc::UnboundedSender<WorkerRequest>,
        respond_rx: oneshot::Receiver<SliceOutcome>,
    ) -> CallToolResult {
        match respond_rx.await {
            Ok(SliceOutcome::Complete(result)) => {
                self.code_runs.take(run_id);
                complete_tool_result(&result)
            }
            Ok(SliceOutcome::Paused {
                workspace_delta,
                elapsed,
            }) => {
                self.code_runs.insert(run_id, control_tx);
                paused_tool_result(run_id, workspace_delta.as_ref(), elapsed)
            }
            Err(_) => {
                self.code_runs.take(run_id);
                CallToolResult::error(vec![Content::text(worker_unavailable_message(run_id))])
            }
        }
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

fn spawn_eitri_runtime(
    config: &Config,
    context: RunContext,
    purpose: EitriPurpose,
    termination: Option<CancellationToken>,
) -> WarmRuntime {
    let (event_tx, events) = mpsc::unbounded_channel();
    let (commands, command_rx) = mpsc::unbounded_channel();
    let loki = purpose
        .marks_implementation_delegation()
        .then(|| config.loki.clone())
        .flatten();
    let pull_server = match loki.as_ref() {
        Some(reviewer) => match loki::PullServer::start(reviewer.clone(), loki::Consumer::Eitri) {
            Ok(server) => Some(server),
            Err(error) => {
                tracing::warn!("could not expose Loki pull tool to Eitri: {error:#}");
                None
            }
        },
        None => None,
    };
    let cancel = termination.unwrap_or_default();
    let mut env = config.env.clone();
    let mut role_config = config.role_config.clone();
    if purpose.marks_implementation_delegation()
        && let Some(mode) = config.headless_permission_mode
        && let Some(role) = role_config.as_mut()
        && let Some(kind) = crate::council::AdapterKind::from_source_id(&role.adapter_source_id)
    {
        role.permission = crate::council::configure_permissions(kind, mode, &mut env);
    }
    let runtime_config = AcpRuntimeConfig {
        command: config.command.clone(),
        args: config.args.clone(),
        cwd: context.cwd.clone(),
        additional_directories: context.additional_directories.clone(),
        mcp_servers: pull_server
            .as_ref()
            .map(|server| vec![server.advertised().clone()])
            .unwrap_or_default(),
        resume_session: None,
        env,
        agent_stderr: config.agent_stderr.clone(),
        fs_max_text_bytes: context.fs_max_text_bytes,
        access_mode: purpose.access_mode(context.access_mode),
        agent_source_id: None,
        config_path: None,
        saved_session_config: HashMap::new(),
        role_config,
        code_agent: None,
        side_prompt_policy: false,
        termination: Some(cancel.clone()),
    };
    let task = tokio::spawn(acp::run(runtime_config, event_tx, command_rx));
    WarmRuntime {
        context,
        role_key: config.role_key(),
        events,
        commands,
        task,
        cancel,
        _pull_server: pull_server,
    }
}

impl McpHandler {
    fn server_info() -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "mj-code-agent",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(format!(
                "{SERVER_DELEGATION_GUIDANCE}\n\n{PRIMARY_SESSION_DIRECTIVE}"
            ))
    }
}

impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        Self::server_info()
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListToolsResult, McpError>> + Send + '_ {
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

        let handler = McpHandler::new(config, context, ui_tx, controller);
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
            cancellation,
            task,
        })
    }

    pub fn advertised(&self) -> &McpServer {
        &self.advertised
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
        self.begin_with_termination(kind)
            .await
            .map(|(run_id, _)| run_id)
    }

    /// Atomically admit a run and return its termination handle so callers
    /// can install a disconnect guard without an await gap after admission.
    async fn begin_with_termination(&self, kind: RunKind) -> Option<(u64, RunTermination)> {
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
        let termination = state
            .runs
            .get(&id)
            .expect("newly admitted run is retained by the controller")
            .termination();
        if matches!(kind, RunKind::Code) {
            state.active_implementation_workers.set(1);
        }
        let active = state.runs.len();
        state.active_runs.send_replace(active);
        Some((id, termination))
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

    /// The run_id currently occupying the single Code slot, whether it is
    /// actively running a slice or paused awaiting Thor's decision. Used to
    /// name the outstanding run in the single-slot rejection message.
    async fn active_code_run_id(&self) -> Option<u64> {
        self.state
            .lock()
            .await
            .runs
            .iter()
            .find(|(_, run)| run.kind() == RunKind::Code)
            .map(|(id, _)| *id)
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

/// Sent by `code_agent_continue`/`code_agent_cancel` to a paused Code run's
/// persistent worker task. The worker only reads this channel while it is
/// genuinely paused: idle, with no LLM turn in flight and no file mutation.
enum WorkerRequest {
    /// Resume with a continuation prompt on the retained ACP session and run
    /// one more implementation slice.
    Continue {
        prompt: String,
        respond: oneshot::Sender<SliceOutcome>,
    },
    /// Terminate the paused worker and its ACP session. Does not revert any
    /// workspace edits Eitri already made.
    Cancel {
        respond: oneshot::Sender<EitriRunResult>,
    },
}

/// What one Eitri implementation slice produced.
enum SliceOutcome {
    /// Eitri finished the whole delegation; the worker has already been torn
    /// down and the controller's Code slot released.
    Complete(EitriRunResult),
    /// The slice budget expired mid-turn. Eitri's in-flight turn was
    /// cancelled and has settled; the worker process and its ACP session are
    /// still alive, idle, and continue to hold the single Code slot.
    Paused {
        workspace_delta: Option<WorkspaceDelta>,
        elapsed: Duration,
    },
}

/// Handles threaded into a Code worker's `run()` invocation so it can report
/// each slice's outcome and accept `code_agent_continue`/`code_agent_cancel`
/// requests without tearing down the nested ACP session between slices.
struct CodeSlicing {
    control_rx: mpsc::UnboundedReceiver<WorkerRequest>,
    respond: oneshot::Sender<SliceOutcome>,
}

/// Routes `code_agent_continue`/`code_agent_cancel` to a paused run's worker.
/// A run_id is present here exactly while it is paused and unresolved: it is
/// inserted only when a slice reports `SliceOutcome::Paused` and removed the
/// moment a continue/cancel request is dispatched (re-inserted if the next
/// slice pauses again). This is also the state the single-slot rejection
/// message names.
#[derive(Clone, Default)]
struct CodeRunRegistry {
    runs: Arc<StdMutex<HashMap<u64, mpsc::UnboundedSender<WorkerRequest>>>>,
}

impl CodeRunRegistry {
    fn insert(&self, run_id: u64, control: mpsc::UnboundedSender<WorkerRequest>) {
        self.lock_runs().insert(run_id, control);
    }

    /// Atomically removes and returns the control sender for a paused run, so
    /// at most one in-flight continue/cancel request can act on it at a time.
    fn take(&self, run_id: u64) -> Option<mpsc::UnboundedSender<WorkerRequest>> {
        self.lock_runs().remove(&run_id)
    }

    fn lock_runs(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<u64, mpsc::UnboundedSender<WorkerRequest>>> {
        self.runs.lock().expect("code run registry lock poisoned")
    }
}

fn outstanding_code_run_message(run_id: u64) -> String {
    format!(
        "Eitri implementation run {run_id} is already outstanding. If it is paused, call code_agent_continue or code_agent_cancel with run_id {run_id} before starting a new code_agent delegation. If it is still actively running its current slice, wait for that call to return before starting a new one."
    )
}

fn unresolved_run_id_message(run_id: u64) -> String {
    format!(
        "run_id {run_id} is not a paused Eitri implementation run; it may be unknown, still actively running its current slice, or already resolved by an earlier continue or cancel call"
    )
}

fn worker_unavailable_message(run_id: u64) -> String {
    format!(
        "Eitri code run {run_id} is no longer available; its worker ended unexpectedly. Any partial edits it made remain in the workspace; start a new code_agent delegation if needed."
    )
}

fn complete_tool_result(result: &EitriRunResult) -> CallToolResult {
    match result.outcome.as_ref() {
        Ok(message) => CallToolResult::success(vec![Content::text(with_workspace_diff(
            message,
            result.workspace_delta.as_ref(),
        ))]),
        Err(error) => CallToolResult::error(vec![Content::text(with_workspace_diff(
            &error.to_string(),
            result.workspace_delta.as_ref(),
        ))]),
    }
}

fn cancelled_tool_result(result: &EitriRunResult) -> CallToolResult {
    let message = "Eitri was cancelled before finishing. It did not revert any changes: partial edits remain in the workspace exactly as Eitri left them.";
    CallToolResult::success(vec![Content::text(with_workspace_diff(
        message,
        result.workspace_delta.as_ref(),
    ))])
}

fn paused_tool_result(
    run_id: u64,
    delta: Option<&WorkspaceDelta>,
    elapsed: Duration,
) -> CallToolResult {
    let message = format!(
        "Eitri implementation run {run_id} is PAUSED, not running. Its slice budget ({}s) elapsed after {}s before it finished, so its in-flight turn was cancelled and it is now idle: no LLM turn is in flight and no further file mutation is happening. Its partial edits so far are already in the workspace below. Thor MUST call code_agent_continue with run_id {run_id} to resume it, or code_agent_cancel with run_id {run_id} to stop it and keep its partial edits, before starting any new code_agent delegation. Do not edit this workspace directly while the run remains paused.",
        CODE_AGENT_SLICE.as_secs(),
        elapsed.as_secs(),
    );
    CallToolResult::success(vec![Content::text(with_workspace_diff(&message, delta))])
}

/// Spawn the persistent Code worker and return the handles Thor's tool calls
/// use to drive it: `control_tx` for later continue/cancel requests, and the
/// receiver for this first slice's outcome.
fn launch_code_worker(
    controller: Controller,
    config: Config,
    context: RunContext,
    task: String,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    run_id: u64,
    termination: RunTermination,
) -> (
    mpsc::UnboundedSender<WorkerRequest>,
    oneshot::Receiver<SliceOutcome>,
) {
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let (respond, respond_rx) = oneshot::channel();
    let lease = RunLease {
        controller: controller.clone(),
        run_id,
        termination,
    };
    let worker = run_boxed(
        config,
        context,
        task,
        EitriPurpose::Code,
        ui_tx,
        lease,
        Some(CodeSlicing {
            control_rx,
            respond,
        }),
    );
    launch_code_worker_task(controller, run_id, worker);
    (control_tx, respond_rx)
}

/// Own the worker independently of MCP request/session futures. Every slice
/// outcome is already delivered from inside `worker` via oneshot channels;
/// this task only releases the controller's Code slot once the worker has
/// truly finished (completed or cancelled by Thor), never merely paused.
fn launch_code_worker_task<F>(controller: Controller, run_id: u64, worker: F)
where
    F: Future<Output = Option<EitriRunResult>> + Send + 'static,
{
    tokio::spawn(async move {
        let worker = tokio::spawn(worker);
        if let Err(error) = worker.await {
            tracing::error!(
                event = "eitri_worker_task_failed",
                run_id,
                error = %error,
                "Eitri Code worker task ended unexpectedly"
            );
        }
        controller.finish(run_id).await;
        tracing::info!(event = "eitri_slot_released", run_id, purpose = ?EitriPurpose::Code, "Eitri controller slot released after reap");
    });
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
    slicing: Option<CodeSlicing>,
) -> futures::future::BoxFuture<'static, Option<EitriRunResult>> {
    Box::pin(run(config, context, task, purpose, ui_tx, lease, slicing))
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
        let worker = tokio::spawn(run_boxed(
            config, context, task, purpose, ui_tx, lease, None,
        ));
        let result = match worker.await {
            Ok(Some(result)) => result,
            Ok(None) => EitriRunResult {
                outcome: Err(anyhow!(
                    "Eitri exploration worker delivered no result (unexpected for an unsliced run)"
                )),
                workspace_delta: None,
            },
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

/// Maps a `RunTermination` cause to the error `run()` reports for it. Shared
/// between the in-turn and paused-idle termination branches.
fn termination_error(cause: TerminationCause) -> anyhow::Error {
    match cause {
        TerminationCause::UserCancelled => anyhow!("Eitri cancelled"),
        TerminationCause::RuntimeShutdown => anyhow!("Eitri shutdown requested"),
        TerminationCause::RequestDisconnected => {
            anyhow!("Eitri MCP request timed out or disconnected")
        }
        TerminationCause::RunCompleted | TerminationCause::None => {
            anyhow!("Eitri termination requested")
        }
    }
}

/// Maps the nested ACP runtime's join outcome to (a) the raw result recorded
/// for teardown-failure logging and (b) the run-level error it implies.
/// Shared between the in-turn and paused-idle runtime-join branches.
fn map_runtime_join(
    joined: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> (Result<()>, Result<String>) {
    match joined {
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
    }
}

/// Delivers a terminal `EitriRunResult` through whichever channel is live for
/// this invocation: a Thor-initiated cancel, an in-flight slice response, or
/// (for Explore, which is never sliced) the function's own return value.
fn deliver_result(
    result: EitriRunResult,
    cancel_respond: Option<oneshot::Sender<EitriRunResult>>,
    pending_respond: Option<oneshot::Sender<SliceOutcome>>,
) -> Option<EitriRunResult> {
    if let Some(cancel_respond) = cancel_respond {
        let _ = cancel_respond.send(result);
        None
    } else if let Some(respond) = pending_respond {
        let _ = respond.send(SliceOutcome::Complete(result));
        None
    } else {
        Some(result)
    }
}

/// Runs one Eitri invocation end to end. For `EitriPurpose::Explore` this is
/// always exactly one unsliced turn, matching earlier behavior, and its
/// result is returned directly (`Some`). For `EitriPurpose::Code`,
/// `slicing` carries the channels `code_agent`/`code_agent_continue` use to
/// drive the run one slice at a time: each slice either finishes the whole
/// delegation (tearing the nested session down and delivering the result via
/// its own oneshot) or, if `CODE_AGENT_SLICE` elapses first, cancels the
/// in-flight turn, snapshots the workspace, and pauses — keeping the nested
/// ACP process and session alive and idle until Thor calls
/// `code_agent_continue` (send another prompt, run another slice) or
/// `code_agent_cancel` (tear down, keep the workspace as-is). Every Code
/// result is delivered through a channel, so this always returns `None` for
/// Code; the function's own return type stays `Option<EitriRunResult>` only
/// so the two purposes can share this implementation.
async fn run(
    mut config: Config,
    context: RunContext,
    task: String,
    purpose: EitriPurpose,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    lease: RunLease,
    slicing: Option<CodeSlicing>,
) -> Option<EitriRunResult> {
    let RunLease {
        controller,
        run_id,
        termination,
    } = lease;
    let is_sliced = slicing.is_some();
    let (mut control_rx, mut pending_respond) = match slicing {
        Some(CodeSlicing {
            control_rx,
            respond,
        }) => (Some(control_rx), Some(respond)),
        None => (None, None),
    };
    let mut cancel_respond: Option<oneshot::Sender<EitriRunResult>> = None;
    let mut quota_role = None;
    if let Some(pool) = config.role_pool.clone() {
        match pool.select_for_work().await {
            Ok(selection) => {
                quota_role = Some(selection.role.clone());
                config.apply_role(selection.role);
            }
            Err(message) => {
                return deliver_result(
                    EitriRunResult {
                        outcome: Err(anyhow!(
                            "{message}. The delegation was not started; Thor should decide how to proceed."
                        )),
                        workspace_delta: None,
                    },
                    cancel_respond,
                    pending_respond,
                );
            }
        }
    }
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

    let warm = config.take_warm(purpose, &context);
    let WarmRuntime {
        events: mut nested_event_rx,
        commands: nested_cmd_tx,
        task: mut runtime,
        cancel: runtime_cancel,
        _pull_server: _eitri_pull_server,
        ..
    } = warm.unwrap_or_else(|| {
        spawn_eitri_runtime(
            &config,
            context.clone(),
            purpose,
            Some(termination.token.clone()),
        )
    });
    config.ensure_warm(purpose, context.clone());
    controller.attach(run_id, nested_cmd_tx.clone()).await;

    // Share the Council's one persistent Loki session with implementation
    // runs. Read-only scouts stay outside Loki's continuous Thor/Eitri-code view.
    let loki = purpose
        .marks_implementation_delegation()
        .then(|| config.loki.clone())
        .flatten();
    let mut awaiting_session_start = true;
    let mut prompt_to_send = Some(standalone_prompt.clone());
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
    let mut session_id = None;
    let mut joined_runtime_result = None;
    let result: Result<String> = 'slices: loop {
        let mut collector = AgentMessageCollector::new();
        let mut paused_by_slice = false;
        let slice_started_at = std::time::Instant::now();
        let slice_sleep = tokio::time::sleep(CODE_AGENT_SLICE);
        tokio::pin!(slice_sleep);

        loop {
            tokio::select! {
                biased;
                () = termination.cancelled() => {
                    break 'slices Err(termination_error(termination.cause()));
                }
                () = &mut slice_sleep, if is_sliced && !paused_by_slice => {
                    paused_by_slice = true;
                    let _ = nested_cmd_tx.send(UiCommand::CancelPrompt);
                    let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Status(
                        "implementation slice budget reached; pausing the in-flight turn".to_string(),
                    )));
                }
                joined = &mut runtime => {
                    let (runtime_result, run_result) = map_runtime_join(joined);
                    joined_runtime_result = Some(runtime_result);
                    break 'slices run_result;
                }
                event = nested_event_rx.recv() => {
                    let Some(event) = event else {
                        break 'slices Err(anyhow!("Eitri event stream closed before completing"));
                    };
                    if let Some(boundary) = (epoch > 0).then(|| tracker.observe(&event)).flatten()
                        && let Some(reviewer) = loki.as_ref()
                    {
                        reviewer.observe(epoch, loki::Target::Eitri, eitri_invocation, boundary);
                    }
                    match event {
                        UiEvent::Side(_) | UiEvent::SideStartFailed { .. } => {}
                        UiEvent::Connected { .. } => {}
                        UiEvent::ContextCompacted => {}
                        UiEvent::SessionStarted { session_id: started, .. } if awaiting_session_start => {
                            session_id = Some(started);
                            awaiting_session_start = false;
                            if let Some(prompt) = prompt_to_send.take()
                                && nested_cmd_tx
                                    .send(UiCommand::SendPrompt {
                                        text: prompt,
                                        images: Vec::new(),
                                    })
                                    .is_err()
                            {
                                break 'slices Err(anyhow!("send prompt to Eitri"));
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
                                session_id: session_id.clone(),
                            }));
                            if matches!(stop_reason, StopReason::Cancelled) {
                                if paused_by_slice && termination.cause() == TerminationCause::None {
                                    // Our own slice-deadline cancellation settled;
                                    // exit the turn loop only and pause below.
                                    break;
                                }
                                break 'slices Err(anyhow!("Eitri cancelled"));
                            }
                            break 'slices collector.finish();
                        }
                        UiEvent::PromptFailed { message }
                        | UiEvent::SessionForkFailed { message }
                        | UiEvent::Fatal(message) => {
                            break 'slices Err(anyhow!(message));
                        }
                        UiEvent::ClaudeUsage(_)
                        | UiEvent::CodexUsage(_)
                        | UiEvent::CouncilUsage(_)
                        | UiEvent::CouncilRoleChanged { .. }
                        | UiEvent::RemotePermissionDecision { .. }
                        | UiEvent::LokiActivity(_)
                        | UiEvent::InternalMessage(_) => {}
                        UiEvent::CodeAgent(_) => {
                            break 'slices Err(anyhow!("Eitri attempted recursive delegation"));
                        }
                    }
                }
            }
        }

        // Reached only when this slice's in-flight turn was cancelled by our
        // own slice deadline and has settled. `paused_by_slice` can only be
        // set when `is_sliced` is true, so the slicing handles below are
        // populated.
        let elapsed = slice_started_at.elapsed();
        let delta = invocation_snapshot
            .as_ref()
            .expect("a Code slice implies an invocation snapshot")
            .delta()
            .await;
        let diff_bytes = delta.review_patch().map(str::len).unwrap_or(0);
        tracing::info!(
            event = "eitri_paused",
            run_id,
            elapsed_secs = elapsed.as_secs(),
            diff_bytes,
            "Eitri implementation run paused after its slice budget elapsed"
        );
        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Status(format!(
            "paused after {}s; call code_agent_continue or code_agent_cancel",
            elapsed.as_secs()
        ))));
        if let Some(respond) = pending_respond.take() {
            let _ = respond.send(SliceOutcome::Paused {
                workspace_delta: Some(delta),
                elapsed,
            });
        }

        // No LLM turn is in flight and no file mutation can happen while
        // paused, so there is no dead-man lease here: block until Thor
        // decides, or until a real shutdown/cancel or process death
        // interrupts the wait (session shutdown reaping still applies).
        let control = control_rx
            .as_mut()
            .expect("a Code slice implies a control channel");
        tokio::select! {
            biased;
            () = termination.cancelled() => {
                break 'slices Err(termination_error(termination.cause()));
            }
            joined = &mut runtime => {
                let (runtime_result, run_result) = map_runtime_join(joined);
                joined_runtime_result = Some(runtime_result);
                break 'slices run_result;
            }
            request = control.recv() => {
                match request {
                    Some(WorkerRequest::Continue { prompt, respond }) => {
                        pending_respond = Some(respond);
                        tracing::info!(
                            event = "eitri_resumed",
                            run_id,
                            "Thor resumed a paused Eitri implementation run"
                        );
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Status(
                            "resumed by Thor".to_string(),
                        )));
                        if nested_cmd_tx
                            .send(UiCommand::SendPrompt { text: prompt, images: Vec::new() })
                            .is_err()
                        {
                            break 'slices Err(anyhow!("send continuation prompt to Eitri"));
                        }
                        // Fall through: the outer loop restarts and runs the
                        // next slice on the same, still-open ACP session.
                    }
                    Some(WorkerRequest::Cancel { respond }) => {
                        tracing::info!(
                            event = "eitri_cancelled_by_thor",
                            run_id,
                            "Thor cancelled a paused Eitri implementation run"
                        );
                        cancel_respond = Some(respond);
                        break 'slices Err(anyhow!(
                            "Eitri run cancelled by Thor request; partial edits remain in the workspace"
                        ));
                    }
                    None => {
                        break 'slices Err(anyhow!(
                            "Eitri paused run's control channel closed before Thor resolved it"
                        ));
                    }
                }
            }
        }
    };

    // Eitri's completion is a Thor auto-pull boundary. The shared pull path
    // may wait once for the active relevant Loki generation, but never chases
    // work that starts later.
    let mut result = result;
    if result.is_ok()
        && let Some(reviewer) = loki.as_ref()
    {
        let outcome = reviewer.pull(loki::Consumer::Thor).await;
        if let Ok(message) = result.as_mut() {
            message.push_str("\n\n<loki_advice_receipt target=\"thor\" mode=\"asynchronous; may be superseded by later work\">\n");
            message.push_str(&loki::format_pull_outcome(
                &outcome,
                epoch,
                loki::Consumer::Thor,
            ));
            message.push_str("\n</loki_advice_receipt>");
        }
    }

    // Never abort `acp::run`: its tail owns process-tree termination and
    // reaping. Cancelling this token drives that tail even when the MCP request
    // disappeared, and the supervisor retains the slot until the join returns.
    termination.request(TerminationCause::RunCompleted);
    runtime_cancel.cancel();
    let _ = nested_cmd_tx.send(UiCommand::Shutdown);
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

    if result
        .as_ref()
        .is_err_and(|error| !error.to_string().contains("cancelled"))
        && let (Some(pool), Some(role)) = (config.role_pool.as_ref(), quota_role.as_ref())
    {
        pool.observe_failure(role).await;
    }

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
    deliver_result(
        EitriRunResult {
            outcome: result,
            workspace_delta,
        },
        cancel_respond,
        pending_respond,
    )
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
        assert!(
            McpHandler::tool_router()
                .get("code_agent_continue")
                .is_some()
        );
        assert!(McpHandler::tool_router().get("code_agent_cancel").is_some());
        assert!(McpHandler::tool_router().get("code_agent_wait").is_none());
    }

    #[test]
    fn server_info_instructions_include_primary_policy_and_delegation_guidance() {
        let info = McpHandler::server_info();
        let instructions = info.instructions.as_deref().expect("server instructions");

        assert!(instructions.contains(SERVER_DELEGATION_GUIDANCE));
        assert!(instructions.contains(PRIMARY_SESSION_DIRECTIVE));
        assert!(!instructions.contains("request above"));
    }

    #[test]
    fn code_continue_and_cancel_arguments_are_strict() {
        let args: CodeAgentContinueArgs =
            serde_json::from_str(r#"{"run_id":42}"#).expect("valid continue args");
        assert_eq!(args.run_id, 42);
        assert!(
            serde_json::from_str::<CodeAgentContinueArgs>(r#"{"run_id":42,"extra":true}"#).is_err()
        );
        assert!(serde_json::from_str::<CodeAgentContinueArgs>(r#"{}"#).is_err());

        let args: CodeAgentCancelArgs =
            serde_json::from_str(r#"{"run_id":7}"#).expect("valid cancel args");
        assert_eq!(args.run_id, 7);
        assert!(
            serde_json::from_str::<CodeAgentCancelArgs>(r#"{"run_id":7,"extra":true}"#).is_err()
        );
        assert!(serde_json::from_str::<CodeAgentCancelArgs>(r#"{}"#).is_err());
    }

    fn test_result(message: &str) -> EitriRunResult {
        EitriRunResult {
            outcome: Ok(message.to_string()),
            workspace_delta: None,
        }
    }

    fn tool_result_text(result: &CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|content| content.as_text())
            .map(|text| text.text.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn paused_tool_result_names_run_id_carries_diff_and_instructions() {
        let delta = WorkspaceDelta::changed_for_test("diff --git a/x b/x\n+wip\n".to_string());
        let rendered = paused_tool_result(42, Some(&delta), Duration::from_secs(137));
        assert_eq!(rendered.is_error, Some(false));
        let text = tool_result_text(&rendered);
        assert!(text.contains("run 42 is PAUSED"));
        assert!(text.contains("code_agent_continue"));
        assert!(text.contains("code_agent_cancel"));
        assert!(text.contains("run_id 42"));
        assert!(text.contains("137s"));
        assert!(text.contains("+wip"));
    }

    #[test]
    fn cancelled_tool_result_states_edits_are_kept() {
        let delta = WorkspaceDelta::changed_for_test("diff --git a/x b/x\n+partial\n".to_string());
        let result = EitriRunResult {
            outcome: Err(anyhow!("Eitri run cancelled by Thor request")),
            workspace_delta: Some(delta),
        };
        let rendered = cancelled_tool_result(&result);
        assert_eq!(rendered.is_error, Some(false));
        let text = tool_result_text(&rendered);
        assert!(text.contains("did not revert"));
        assert!(text.contains("+partial"));
    }

    #[tokio::test]
    async fn single_slot_rejection_names_the_outstanding_run_id() {
        let controller = Controller::default();
        let (run_id, _termination) = controller
            .begin_with_termination(RunKind::Code)
            .await
            .expect("first code run admitted");

        assert!(
            controller
                .begin_with_termination(RunKind::Code)
                .await
                .is_none(),
            "a second Code run must be rejected while one is outstanding"
        );
        assert_eq!(controller.active_code_run_id().await, Some(run_id));
        let message = outstanding_code_run_message(run_id);
        assert!(message.contains(&run_id.to_string()));
        assert!(message.contains("code_agent_continue"));
        assert!(message.contains("code_agent_cancel"));

        controller.finish(run_id).await;
        assert_eq!(controller.active_code_run_id().await, None);
        assert!(controller.begin(RunKind::Code).await.is_some());
    }

    /// Drives a hand-written worker through the exact `WorkerRequest`/
    /// `SliceOutcome` protocol `run()` uses for a Code delegation, without a
    /// real ACP process. Exercises the pause -> continue -> final-result
    /// path and confirms the controller's single Code slot stays held for
    /// the whole paused interval and is released only at true completion.
    #[tokio::test]
    async fn paused_run_can_be_continued_to_a_final_result() {
        let controller = Controller::default();
        controller.configure(1, ActiveCodeWorkers::default()).await;
        let (run_id, _termination) = controller
            .begin_with_termination(RunKind::Code)
            .await
            .expect("code run admitted");

        let (control_tx, mut control_rx) = mpsc::unbounded_channel::<WorkerRequest>();
        let (first_respond, first_rx) = oneshot::channel();

        launch_code_worker_task(controller.clone(), run_id, async move {
            let _ = first_respond.send(SliceOutcome::Paused {
                workspace_delta: None,
                elapsed: Duration::from_secs(3),
            });
            match control_rx.recv().await {
                Some(WorkerRequest::Continue { prompt, respond }) => {
                    assert!(prompt.contains("Continue"));
                    let _ = respond.send(SliceOutcome::Complete(test_result(
                        "finished after resuming",
                    )));
                }
                _ => panic!("expected a continue request"),
            }
            None
        });

        let paused = first_rx.await.expect("first slice outcome");
        assert!(matches!(paused, SliceOutcome::Paused { .. }));
        assert!(
            controller.begin(RunKind::Code).await.is_none(),
            "the Code slot stays held while the run is paused"
        );
        assert_eq!(controller.active_code_run_id().await, Some(run_id));

        let (respond, respond_rx) = oneshot::channel();
        control_tx
            .send(WorkerRequest::Continue {
                prompt: "Continue the implementation task.".to_string(),
                respond,
            })
            .expect("send continue");
        let SliceOutcome::Complete(result) = respond_rx.await.expect("second slice outcome") else {
            panic!("expected the resumed slice to report a final result");
        };
        assert_eq!(result.outcome.expect("success"), "finished after resuming");

        for _ in 0..100 {
            if controller.begin(RunKind::Code).await.is_some() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("controller slot was never released after the worker finished");
    }

    /// Same protocol as above, but Thor cancels instead of continuing.
    /// Confirms the worker's own final diff (never reverted) is exactly
    /// what comes back, and that the slot is released.
    #[tokio::test]
    async fn paused_run_can_be_cancelled_and_keeps_partial_edits() {
        let controller = Controller::default();
        controller.configure(1, ActiveCodeWorkers::default()).await;
        let (run_id, _termination) = controller
            .begin_with_termination(RunKind::Code)
            .await
            .expect("code run admitted");

        let (control_tx, mut control_rx) = mpsc::unbounded_channel::<WorkerRequest>();
        let (first_respond, first_rx) = oneshot::channel();
        let partial_patch = "diff --git a/x b/x\n+partial edit\n".to_string();

        launch_code_worker_task(controller.clone(), run_id, {
            let partial_patch = partial_patch.clone();
            async move {
                let _ = first_respond.send(SliceOutcome::Paused {
                    workspace_delta: Some(WorkspaceDelta::changed_for_test(partial_patch.clone())),
                    elapsed: Duration::from_secs(9),
                });
                match control_rx.recv().await {
                    Some(WorkerRequest::Cancel { respond }) => {
                        // A real worker tears its process down here but never
                        // reverts files it already wrote; it hands back the
                        // same diff it was already holding.
                        let _ = respond.send(EitriRunResult {
                            outcome: Err(anyhow!(
                                "Eitri run cancelled by Thor request; partial edits remain in the workspace"
                            )),
                            workspace_delta: Some(WorkspaceDelta::changed_for_test(partial_patch)),
                        });
                    }
                    _ => panic!("expected a cancel request"),
                }
                None
            }
        });

        let paused = first_rx.await.expect("first slice outcome");
        let SliceOutcome::Paused {
            workspace_delta, ..
        } = paused
        else {
            panic!("expected a paused outcome");
        };
        assert!(
            workspace_delta
                .expect("diff present while paused")
                .changed()
        );

        let (respond, respond_rx) = oneshot::channel();
        control_tx
            .send(WorkerRequest::Cancel { respond })
            .expect("send cancel");
        let result = respond_rx.await.expect("cancel result");
        assert!(
            result.outcome.is_err(),
            "cancel is not a successful Eitri message"
        );
        let delta = result
            .workspace_delta
            .clone()
            .expect("diff retained after cancel");
        assert!(delta.changed());
        assert_eq!(delta.review_patch(), Some(partial_patch.as_str()));

        let rendered = cancelled_tool_result(&result);
        assert_eq!(rendered.is_error, Some(false));
        let text = tool_result_text(&rendered);
        assert!(text.contains("did not revert"));
        assert!(text.contains("partial edit"));

        for _ in 0..100 {
            if controller.begin(RunKind::Code).await.is_some() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("controller slot was never released after cancellation");
    }

    #[tokio::test]
    async fn code_run_registry_insert_and_take_round_trip() {
        let registry = CodeRunRegistry::default();
        assert!(registry.take(1).is_none());
        let (control_tx, _control_rx) = mpsc::unbounded_channel::<WorkerRequest>();
        registry.insert(1, control_tx);
        assert!(registry.take(1).is_some());
        assert!(
            registry.take(1).is_none(),
            "take is a one-shot claim, like an outstanding paused run being resolved"
        );
    }

    #[tokio::test]
    async fn code_admission_provides_termination_before_any_follow_up_await() {
        let controller = Controller::default();
        let (run_id, termination) = controller
            .begin_with_termination(RunKind::Code)
            .await
            .expect("code run");

        {
            let _request = RequestDropGuard::new(termination.clone(), run_id, EitriPurpose::Code);
        }
        assert_eq!(termination.cause(), TerminationCause::RequestDisconnected);
        assert!(controller.begin(RunKind::Code).await.is_none());

        controller.finish(run_id).await;
        assert!(controller.begin(RunKind::Code).await.is_some());
    }

    #[tokio::test]
    async fn dropping_pending_code_admission_cannot_orphan_a_controller_slot() {
        let controller = Controller::default();
        let state_lock = controller.state.lock().await;
        let pending = tokio::spawn({
            let controller = controller.clone();
            async move { controller.begin_with_termination(RunKind::Code).await }
        });
        tokio::task::yield_now().await;
        pending.abort();
        assert!(pending.await.is_err());
        drop(state_lock);

        assert!(controller.begin(RunKind::Code).await.is_some());
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
        assert!(
            !EitriPurpose::Explore
                .standalone_prompt("find it")
                .contains("Loki")
        );
        assert!(
            EitriPurpose::Code
                .standalone_prompt("fix it")
                .contains("pull_advice")
        );
        let prompt = EitriPurpose::Code.standalone_prompt("fix it");
        assert!(prompt.contains("after failed validation before retrying"));
        assert!(prompt.contains("before finalizing"));
    }

    #[tokio::test]
    async fn warm_pool_claims_only_an_exact_purpose_and_context_match() {
        let config = Config {
            display_label: "Eitri".into(),
            command: PathBuf::from("unused"),
            args: Vec::new(),
            env: HashMap::new(),
            agent_stderr: None,
            role_config: None,
            loki: None,
            implementation_handoff_counter: None,
            active_implementation_workers: ActiveCodeWorkers::default(),
            max_parallel_explores: 1,
            headless_permission_mode: Some(crate::config::CouncilPermissionMode::Auto),
            role_pool: None,
            warm: Arc::default(),
        };
        let context = RunContext {
            cwd: PathBuf::from("/workspace"),
            additional_directories: Vec::new(),
            fs_max_text_bytes: 42,
            access_mode: RuntimeAccessMode::Full,
        };
        let (commands, _command_rx) = mpsc::unbounded_channel();
        let (_event_tx, events) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(std::future::pending());
        config.warm.slots.lock().unwrap().code = Some(WarmRuntime {
            context: context.clone(),
            role_key: config.role_key(),
            events,
            commands,
            task,
            cancel: cancel.clone(),
            _pull_server: None,
        });

        let mut mismatch = context.clone();
        mismatch.cwd = PathBuf::from("/other");
        assert!(config.take_warm(EitriPurpose::Code, &mismatch).is_none());
        assert!(config.take_warm(EitriPurpose::Explore, &context).is_none());
        let runtime = config
            .take_warm(EitriPurpose::Code, &context)
            .expect("matching warm runtime");
        runtime.cancel.cancel();
        runtime.task.abort();
    }

    #[tokio::test]
    async fn warm_pool_discards_a_runtime_that_failed_during_startup() {
        let config = Config {
            display_label: "Eitri".into(),
            command: PathBuf::from("unused"),
            args: Vec::new(),
            env: HashMap::new(),
            agent_stderr: None,
            role_config: None,
            loki: None,
            implementation_handoff_counter: None,
            active_implementation_workers: ActiveCodeWorkers::default(),
            max_parallel_explores: 1,
            headless_permission_mode: Some(crate::config::CouncilPermissionMode::Auto),
            role_pool: None,
            warm: Arc::default(),
        };
        let context = RunContext {
            cwd: PathBuf::from("/workspace"),
            additional_directories: Vec::new(),
            fs_max_text_bytes: 42,
            access_mode: RuntimeAccessMode::Full,
        };
        let (commands, mut command_rx) = mpsc::unbounded_channel();
        let (_event_tx, events) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(async { Ok(()) });
        tokio::task::yield_now().await;
        assert!(task.is_finished());
        config.warm.slots.lock().unwrap().code = Some(WarmRuntime {
            context: context.clone(),
            role_key: config.role_key(),
            events,
            commands,
            task,
            cancel: cancel.clone(),
            _pull_server: None,
        });

        assert!(config.take_warm(EitriPurpose::Code, &context).is_none());
        assert!(cancel.is_cancelled());
        assert!(matches!(command_rx.try_recv(), Ok(UiCommand::Shutdown)));
        assert!(config.warm.slots.lock().unwrap().code.is_none());
    }
}
