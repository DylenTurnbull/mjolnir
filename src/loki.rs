//! Persistent read-only council advisor. Thor and Eitri stream hidden transcript
//! deltas into one long-lived Loki ACP session; Loki reviews them asynchronously
//! at his own pace. Advice accumulates in a queue and is pulled by the
//! orchestrators at natural turn boundaries — Loki never interrupts a running
//! target and nothing ever waits on him.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use agent_client_protocol::schema::v1::{HttpHeader, McpServer, McpServerHttp};
use anyhow::{Context, Result, anyhow};
use axum::extract::{Request as HttpRequest, State};
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

use crate::acp::{RuntimeAccessMode, RuntimeRoleConfig};
use crate::council::ResolvedRole;
use crate::event::{LokiActivity, LokiIdentity, UiEvent};
use crate::ragnarok::{AgentHandle, Launch, TurnEvent};

const REVIEW_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MAX_CONTEXT_BYTES: usize = 96 * 1024;
const MAX_DELTA_ITEM_BYTES: usize = 16 * 1024;
const MCP_PATH: &str = "/mcp";
const MCP_SERVER_NAME: &str = "mj-loki-advisor";

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AdviseArgs {
    /// Agent whose reviewed work this advice concerns.
    target: Target,
    /// One concrete, material, actionable suggestion for the watched agent.
    note: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewedSpan {
    pub target: Target,
    pub invocation: Option<u64>,
    pub first_step: u64,
    pub last_step: u64,
    pub activities: Vec<String>,
}

impl ReviewedSpan {
    fn marker(&self) -> String {
        let actor = match self.invocation {
            Some(invocation) => format!("{} #{invocation}", self.target.label()),
            None => self.target.label().to_string(),
        };
        let steps = if self.first_step == self.last_step {
            format!("step {}", self.first_step)
        } else {
            format!("steps {}-{}", self.first_step, self.last_step)
        };
        format!("{actor} · {steps}: {}", self.activities.join(", "))
    }
}

#[derive(Debug, Clone)]
pub struct Advice {
    pub epoch: u64,
    pub target: Target,
    pub note: String,
    pub span: ReviewedSpan,
}

impl Advice {
    fn deferred_text(&self, current_epoch: u64) -> String {
        let provenance = if self.epoch == current_epoch {
            format!("[reviewed {}]", self.span.marker())
        } else {
            format!("[reviewed in an earlier turn: {}]", self.span.marker())
        };
        format!("{provenance}\n{}", self.note)
    }
}

#[derive(Debug, Default)]
struct AdviceState {
    deferred: VecDeque<Advice>,
    seen: HashSet<(u64, String)>,
}

type SharedAdviceState = Arc<std::sync::Mutex<AdviceState>>;

#[derive(Debug, Clone)]
struct ActiveAdvice {
    id: u64,
    epoch: u64,
    spans: Vec<ReviewedSpan>,
    accepted: bool,
    accepted_advice: Option<Advice>,
}

#[derive(Debug, Clone, Default)]
struct AdviceSlot {
    active: Arc<Mutex<Option<ActiveAdvice>>>,
    state: SharedAdviceState,
    /// Incremented for every queued note so idle orchestrators can wake up
    /// and interject advice that became ready after their turn completed.
    posted: Option<watch::Sender<u64>>,
}

impl AdviceSlot {
    async fn begin(&self, id: u64, epoch: u64, spans: Vec<ReviewedSpan>) {
        *self.active.lock().await = Some(ActiveAdvice {
            id,
            epoch,
            spans,
            accepted: false,
            accepted_advice: None,
        });
    }

    async fn accept(&self, args: AdviseArgs) -> std::result::Result<Advice, &'static str> {
        let mut active = self.active.lock().await;
        let Some(active) = active.as_mut() else {
            return Err("no advisor update is active");
        };
        if active.accepted {
            return Err("only one advice note is allowed per advisor update");
        }
        let Some(span) = active
            .spans
            .iter()
            .find(|span| span.target == args.target)
            .cloned()
        else {
            return Err("target was not present in this advisor update");
        };
        let advice = Advice {
            epoch: active.epoch,
            target: args.target,
            note: args.note,
            span,
        };
        let mut state = self.state.lock().expect("Loki advice state poisoned");
        let dedupe = (advice.epoch, advice.note.to_ascii_lowercase());
        if !state.seen.insert(dedupe) {
            return Err("duplicate advice was ignored");
        }
        active.accepted = true;
        active.accepted_advice = Some(advice.clone());
        tracing::info!(
            event = "advice_routed",
            review_target = advice.target.label(),
            reviewed_span = %advice.span.marker(),
            delivery_route = "queued_for_boundary",
            "Loki advice routed"
        );
        state.deferred.push_back(advice.clone());
        drop(state);
        if let Some(posted) = self.posted.as_ref() {
            posted.send_modify(|count| *count += 1);
        }
        Ok(advice)
    }

    async fn finish(&self, id: u64) -> Option<Advice> {
        let mut active = self.active.lock().await;
        if active.as_ref().is_some_and(|active| active.id == id) {
            return active.take().and_then(|active| active.accepted_advice);
        }
        None
    }
}

#[derive(Clone)]
struct McpHandler {
    advice: AdviceSlot,
    tools_listed: watch::Sender<bool>,
    tool_router: ToolRouter<Self>,
}

#[tool_router(router = tool_router)]
impl McpHandler {
    fn new(advice: AdviceSlot, tools_listed: watch::Sender<bool>) -> Self {
        Self {
            advice,
            tools_listed,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "advise",
        description = "Queue at most one material advisory note about Thor or Eitri. Advice is asynchronous: it is delivered to Thor at the next natural turn boundary and never interrupts running work, so later work may have superseded it by delivery time. Reserve it for material correctness, safety, scope, or strategy problems; otherwise do not call this tool."
    )]
    async fn advise(
        &self,
        Parameters(args): Parameters<AdviseArgs>,
    ) -> std::result::Result<CallToolResult, McpError> {
        let note = args.note.trim();
        if note.is_empty() {
            return Err(McpError::invalid_params("note must not be empty", None));
        }
        if is_content_free(note) {
            return Ok(CallToolResult::error(vec![Content::text(
                "Content-free advice was ignored.",
            )]));
        }
        let args = AdviseArgs {
            target: args.target,
            note: note.to_string(),
        };
        match self.advice.accept(args).await {
            Ok(_) => Ok(CallToolResult::success(vec![Content::text(
                "Advice accepted.",
            )])),
            Err(message) => Ok(CallToolResult::error(vec![Content::text(message)])),
        }
    }
}

impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "mj-loki-advisor",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "You are a read-only advisor. The advise tool is your only channel back to Thor or Eitri. Stay silent unless a concrete material suggestion is necessary.",
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

struct HttpServer {
    advertised: McpServer,
    tools_listed: watch::Receiver<bool>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl HttpServer {
    async fn start(advice: AdviceSlot) -> Result<Self> {
        let mut token_bytes = [0_u8; 32];
        getrandom::fill(&mut token_bytes)
            .map_err(|error| anyhow!("generate Loki advisor MCP bearer token: {error}"))?;
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_bytes);
        let authorization = format!("Bearer {token}");
        let (tools_listed_tx, tools_listed) = watch::channel(false);
        let handler = McpHandler::new(advice, tools_listed_tx);
        let cancellation = CancellationToken::new();
        let mut config = StreamableHttpServerConfig::default();
        config.cancellation_token = cancellation.clone();
        let service = StreamableHttpService::new(
            move || Ok(handler.clone()),
            Arc::new(LocalSessionManager::default()),
            config,
        );
        let protected = axum::Router::new().nest_service(MCP_PATH, service).layer(
            axum::middleware::from_fn_with_state(authorization.clone(), require_bearer),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind Loki advisor MCP listener")?;
        let addr = listener
            .local_addr()
            .context("read Loki advisor MCP listener address")?;
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, protected)
                .with_graceful_shutdown(task_cancellation.cancelled_owned())
                .await
            {
                tracing::warn!("Loki advisor MCP listener stopped: {error}");
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

    async fn wait_until_tools_listed(&self) -> Result<()> {
        let mut listed = self.tools_listed.clone();
        if *listed.borrow() {
            return Ok(());
        }
        tokio::time::timeout(Duration::from_secs(30), listed.changed())
            .await
            .map_err(|_| anyhow!("Loki timed out loading the advise MCP tool"))?
            .map_err(|_| anyhow!("Loki advisor MCP server closed before tools/list"))?;
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
    request: HttpRequest,
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

#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub step: u64,
    pub text: String,
    pub activities: Vec<String>,
}

#[derive(Default)]
pub struct BoundaryTracker {
    trajectory: String,
    final_message: String,
    segment: String,
    lane: Option<SegmentLane>,
    tools: HashMap<String, agent_client_protocol::schema::v1::ToolCall>,
    terminals: HashMap<String, String>,
    next_step: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SegmentLane {
    Message,
    Thought,
}

impl BoundaryTracker {
    pub fn observe(&mut self, event: &UiEvent) -> Option<Checkpoint> {
        use agent_client_protocol::schema::v1::{
            SessionUpdate, ToolCall, ToolCallContent, ToolCallStatus,
        };
        let flush = |this: &mut Self| {
            this.lane.take()?;
            if this.segment.trim().is_empty() {
                this.segment.clear();
                return None;
            }
            Some(std::mem::take(&mut this.segment))
        };
        let append = |this: &mut Self, lane: SegmentLane, text: &str| {
            if this.lane != Some(lane) {
                if !this.segment.is_empty() {
                    this.segment.push('\n');
                }
                this.segment.push_str(match lane {
                    SegmentLane::Message => "message:\n",
                    SegmentLane::Thought => "thinking:\n",
                });
                this.lane = Some(lane);
            }
            this.segment.push_str(text);
        };
        let boundary: Option<(String, Vec<String>)> = match event {
            UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(chunk)) => {
                let text = crate::event::content_block_text(&chunk.content);
                append(self, SegmentLane::Message, &text);
                self.final_message.push_str(&text);
                None
            }
            UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(chunk)) => {
                append(
                    self,
                    SegmentLane::Thought,
                    &crate::event::content_block_text(&chunk.content),
                );
                None
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCall(call)) => {
                let terminal_backed = call
                    .content
                    .iter()
                    .any(|content| matches!(content, ToolCallContent::Terminal(_)));
                for content in &call.content {
                    if let ToolCallContent::Terminal(terminal) = content {
                        self.terminals
                            .insert(terminal.terminal_id.to_string(), tool_activity(call));
                    }
                }
                let complete = matches!(
                    call.status,
                    ToolCallStatus::Completed | ToolCallStatus::Failed
                );
                self.tools
                    .insert(call.tool_call_id.to_string(), call.clone());
                (complete && !terminal_backed).then(|| {
                    self.final_message.clear();
                    let activity = tool_activity(call);
                    (
                        join_boundary(flush(self), render_tool_delta(call)),
                        vec![activity],
                    )
                })
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCallUpdate(update)) => {
                let id = update.tool_call_id.to_string();
                let completed = matches!(
                    update.fields.status,
                    Some(ToolCallStatus::Completed | ToolCallStatus::Failed)
                );
                let rendered = {
                    let tool = self
                        .tools
                        .entry(id.clone())
                        .or_insert_with(|| ToolCall::new(id.clone(), "tool"));
                    tool.update(update.fields.clone());
                    (completed && !tool_has_terminal(tool)).then(|| render_tool_delta(tool))
                };
                rendered.map(|rendered| {
                    self.final_message.clear();
                    let activity = self
                        .tools
                        .get(&id)
                        .map_or_else(|| "tool".to_string(), tool_activity);
                    (join_boundary(flush(self), rendered), vec![activity])
                })
            }
            UiEvent::SessionUpdate(SessionUpdate::Plan(plan)) => {
                self.final_message.clear();
                Some((
                    join_boundary(flush(self), bounded_item(format!("plan update:\n{plan:?}"))),
                    vec!["plan update".to_string()],
                ))
            }
            UiEvent::TerminalOutput(snapshot) if snapshot.exit_status.is_some() => {
                let activity = self
                    .terminals
                    .get(&snapshot.terminal_id)
                    .cloned()
                    .unwrap_or_else(|| "terminal".to_string());
                let lines = snapshot.output.lines().count();
                Some((
                    join_boundary(
                        flush(self),
                        bounded_item(format!(
                            "terminal: {activity} [{:?}], {lines} output lines",
                            snapshot.exit_status
                        )),
                    ),
                    vec![activity],
                ))
            }
            UiEvent::PromptDone { stop_reason, .. } => {
                use agent_client_protocol::schema::v1::StopReason;
                // The concluding message otherwise never reaches a tool
                // boundary, so flush it as its own reviewable checkpoint.
                // Cancelled turns are either user aborts or Loki interrupts;
                // both discard the partial segment through reset_attempt.
                (!matches!(stop_reason, StopReason::Cancelled))
                    .then(|| flush(self))
                    .flatten()
                    .map(|segment| {
                        (
                            bounded_item(format!("final response:\n{segment}")),
                            vec!["final response".to_string()],
                        )
                    })
            }
            UiEvent::PromptFailed { .. } => None,
            _ => None,
        };
        boundary.map(|(text, activities)| {
            self.next_step += 1;
            self.trajectory.push_str(&text);
            self.trajectory.push('\n');
            self.trajectory = bounded(std::mem::take(&mut self.trajectory));
            Checkpoint {
                step: self.next_step,
                text,
                activities,
            }
        })
    }

    pub fn trajectory(&self) -> String {
        self.trajectory.clone()
    }

    pub fn final_message(&self) -> String {
        self.final_message.clone()
    }

    pub fn reset_attempt(&mut self) {
        self.final_message.clear();
        self.segment.clear();
        self.lane = None;
        self.tools.clear();
        self.terminals.clear();
    }
}

fn tool_has_terminal(tool: &agent_client_protocol::schema::v1::ToolCall) -> bool {
    use agent_client_protocol::schema::v1::ToolCallContent;
    tool.content
        .iter()
        .any(|content| matches!(content, ToolCallContent::Terminal(_)))
}

fn render_tool_delta(tool: &agent_client_protocol::schema::v1::ToolCall) -> String {
    use agent_client_protocol::schema::v1::{ToolCallContent, ToolCallStatus};
    let activity = tool_activity(tool);
    let primary = tool.raw_input.as_ref().and_then(primary_arg);
    let output = tool_output_text(tool);
    let lines = output.lines().count();
    let mut text = format!("tool: {activity}");
    if let Some(primary) = primary {
        text.push_str(&format!(" ({primary})"));
    }
    text.push_str(&format!(" [{:?}], {lines} result lines", tool.status));
    if matches!(tool.status, ToolCallStatus::Failed)
        && let Some(first) = tool
            .raw_output
            .as_ref()
            .and_then(first_error_value)
            .or_else(|| {
                output
                    .lines()
                    .find(|line| !line.trim().is_empty())
                    .map(str::to_string)
            })
    {
        text.push_str("\nerror: ");
        text.push_str(first.trim());
    }
    for content in &tool.content {
        if let ToolCallContent::Diff(diff) = content {
            text.push_str(&format!(
                "\ndiff {}:\n--- old\n{}\n+++ new\n{}",
                diff.path.display(),
                diff.old_text.as_deref().unwrap_or(""),
                diff.new_text
            ));
        }
    }
    bounded_item(text)
}

fn first_error_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => ["error", "stderr", "message"]
            .iter()
            .find_map(|key| map.get(*key))
            .and_then(|value| match value {
                serde_json::Value::String(text) => text.lines().next().map(str::to_string),
                value => Some(value.to_string()),
            })
            .or_else(|| map.values().find_map(first_error_value)),
        serde_json::Value::Array(values) => values.iter().find_map(first_error_value),
        _ => None,
    }
}

fn tool_output_text(tool: &agent_client_protocol::schema::v1::ToolCall) -> String {
    use agent_client_protocol::schema::v1::ToolCallContent;
    let mut parts = Vec::new();
    for content in &tool.content {
        if let ToolCallContent::Content(content) = content {
            parts.push(crate::event::content_block_text(&content.content));
        }
    }
    if let Some(output) = tool.raw_output.as_ref() {
        collect_json_text(output, &mut parts);
    }
    parts.join("\n")
}

fn collect_json_text(value: &serde_json::Value, parts: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) => parts.push(value.clone()),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_json_text(value, parts);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_json_text(value, parts);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

fn tool_activity(tool: &agent_client_protocol::schema::v1::ToolCall) -> String {
    let metadata = tool
        .meta
        .as_ref()
        .and_then(|meta| serde_json::to_value(meta).ok());
    metadata
        .as_ref()
        .and_then(|value| find_json_string(value, &["toolName", "tool_name", "name"]))
        .or_else(|| {
            tool.raw_input
                .as_ref()
                .and_then(|value| find_json_string(value, &["toolName", "tool_name"]))
        })
        .unwrap_or_else(|| tool.title.clone())
}

fn find_json_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for key in keys {
                if let Some(value) = map.get(*key).and_then(serde_json::Value::as_str) {
                    return Some(value.to_string());
                }
            }
            map.values().find_map(|value| find_json_string(value, keys))
        }
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|value| find_json_string(value, keys)),
        _ => None,
    }
}

fn primary_arg(value: &serde_json::Value) -> Option<String> {
    find_json_string(value, &["command", "path", "file_path", "pattern", "query"])
        .map(|value| value.chars().take(160).collect())
}

fn bounded_item(mut text: String) -> String {
    if text.len() <= MAX_DELTA_ITEM_BYTES {
        return text;
    }
    let split = text.floor_char_boundary(MAX_DELTA_ITEM_BYTES);
    text.truncate(split);
    text.push_str("\n…[item truncated]");
    text
}

fn join_boundary(previous: Option<String>, current: String) -> String {
    previous.map_or(current.clone(), |previous| format!("{previous}\n{current}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    Thor,
    Eitri,
}

impl Target {
    fn label(self) -> &'static str {
        match self {
            Self::Thor => "Thor",
            Self::Eitri => "Eitri",
        }
    }
}

enum Request {
    Begin {
        epoch: u64,
        task: String,
    },
    TargetContext {
        epoch: u64,
        target: Target,
        invocation: Option<u64>,
        text: String,
    },
    Review {
        id: u64,
        epoch: u64,
        target: Target,
        invocation: Option<u64>,
        checkpoint: Checkpoint,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct Handle {
    requests: mpsc::UnboundedSender<Request>,
    ids: Arc<AtomicU64>,
    epochs: Arc<AtomicU64>,
    eitri_invocations: Arc<AtomicU64>,
    advice_state: SharedAdviceState,
    abort: watch::Sender<bool>,
    finished: watch::Receiver<bool>,
    posted: watch::Receiver<u64>,
}

impl Handle {
    pub fn start(
        role: ResolvedRole,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        ui_tx: mpsc::UnboundedSender<UiEvent>,
        council_session: String,
    ) -> Self {
        let (requests, rx) = mpsc::unbounded_channel();
        let (abort, abort_rx) = watch::channel(false);
        let (finished_tx, finished) = watch::channel(false);
        let (posted_tx, posted) = watch::channel(0_u64);
        let advice_state = SharedAdviceState::default();
        let handle = Self {
            requests,
            ids: Arc::new(AtomicU64::new(1)),
            epochs: Arc::new(AtomicU64::new(1)),
            eitri_invocations: Arc::new(AtomicU64::new(1)),
            advice_state: advice_state.clone(),
            abort,
            finished,
            posted,
        };
        tokio::spawn(worker(
            role,
            cwd,
            additional_directories,
            ui_tx,
            rx,
            advice_state,
            abort_rx,
            finished_tx,
            posted_tx,
            council_session,
        ));
        handle
    }

    /// Watch that ticks whenever Loki queues a new advice note. Idle
    /// orchestrators use it to interject late advice between user turns.
    pub fn subscribe_advice(&self) -> watch::Receiver<u64> {
        self.posted.clone()
    }

    pub fn begin_turn(&self, task: String) -> u64 {
        let _ = self.abort.send(false);
        let epoch = self.epochs.fetch_add(1, Ordering::Relaxed);
        self.eitri_invocations.store(1, Ordering::Relaxed);
        {
            // Advice queued in earlier turns stays deliverable; only the
            // exact-duplicate guard is scoped to the new turn.
            let mut state = self
                .advice_state
                .lock()
                .expect("Loki advice state poisoned");
            state.seen.retain(|(seen_epoch, _)| *seen_epoch == epoch);
        }
        let _ = self.requests.send(Request::Begin { epoch, task });
        epoch
    }

    pub fn begin_eitri(&self, epoch: u64, context: String) -> u64 {
        let invocation = self.eitri_invocations.fetch_add(1, Ordering::Relaxed);
        let _ = self.requests.send(Request::TargetContext {
            epoch,
            target: Target::Eitri,
            invocation: Some(invocation),
            text: context,
        });
        invocation
    }

    /// Drain every queued advice note, oldest first, regardless of the turn
    /// it was produced in. Callers deliver the result at a turn boundary.
    pub fn take_deferred(&self) -> Vec<Advice> {
        let mut state = self
            .advice_state
            .lock()
            .expect("Loki advice state poisoned");
        state.deferred.drain(..).collect()
    }

    pub fn current_epoch(&self) -> u64 {
        self.epochs.load(Ordering::Relaxed).saturating_sub(1)
    }

    pub fn cancel_turn(&self) {
        let _ = self.abort.send(true);
    }

    pub fn observe(
        &self,
        epoch: u64,
        target: Target,
        invocation: Option<u64>,
        checkpoint: Checkpoint,
    ) {
        self.submit(epoch, target, invocation, checkpoint);
    }

    fn submit(&self, epoch: u64, target: Target, invocation: Option<u64>, checkpoint: Checkpoint) {
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        let _ = self.requests.send(Request::Review {
            id,
            epoch,
            target,
            invocation,
            checkpoint,
        });
    }

    pub async fn shutdown_and_wait(&self) {
        let _ = self.abort.send(true);
        let _ = self.requests.send(Request::Shutdown);
        let mut finished = self.finished.clone();
        if *finished.borrow() {
            return;
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), finished.changed()).await;
    }
}

fn bounded(mut text: String) -> String {
    if text.len() > MAX_CONTEXT_BYTES {
        let split = text.len() - MAX_CONTEXT_BYTES;
        let split = text.ceil_char_boundary(split);
        text = format!("…[earlier context omitted]\n{}", &text[split..]);
    }
    text
}

#[allow(clippy::too_many_arguments)]
async fn worker(
    role: ResolvedRole,
    cwd: PathBuf,
    additional_directories: Vec<PathBuf>,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    mut requests: mpsc::UnboundedReceiver<Request>,
    advice_state: SharedAdviceState,
    abort_rx: watch::Receiver<bool>,
    finished: watch::Sender<bool>,
    posted: watch::Sender<u64>,
    council_session: String,
) {
    let mut epoch = 0;
    let mut outer_task = String::new();
    let mut recent_trajectory = String::new();
    let mut pending_context: HashMap<(Target, Option<u64>), String> = HashMap::new();
    let mut session: Option<AgentHandle> = None;
    let advice = AdviceSlot {
        active: Arc::default(),
        state: advice_state,
        posted: Some(posted),
    };
    let server = match HttpServer::start(advice.clone()).await {
        Ok(server) => Some(server),
        Err(error) => {
            emit_warning(
                &ui_tx,
                &role,
                format!("Loki advisor tool could not start: {error:#}"),
            );
            None
        }
    };
    let mut primed = false;
    'worker: loop {
        let Some(request) = requests.recv().await else {
            break;
        };
        let mut batch = Vec::new();
        let mut incoming = VecDeque::from([request]);
        while let Some(request) = incoming.pop_front() {
            match request {
                Request::Begin {
                    epoch: next,
                    task: next_task,
                } => {
                    epoch = next;
                    outer_task = next_task.clone();
                    recent_trajectory.clear();
                    pending_context.clear();
                    pending_context.insert(
                        (Target::Thor, None),
                        format!("New outer user request:\n{next_task}"),
                    );
                }
                Request::TargetContext {
                    epoch: request_epoch,
                    target,
                    invocation,
                    text,
                } if request_epoch == epoch => {
                    pending_context
                        .entry((target, invocation))
                        .and_modify(|pending| {
                            pending.push_str("\n\n");
                            pending.push_str(&text);
                        })
                        .or_insert(text);
                }
                Request::Review {
                    id,
                    epoch: request_epoch,
                    target,
                    invocation,
                    checkpoint,
                } if request_epoch == epoch => {
                    batch.push(ReviewItem {
                        id,
                        target,
                        invocation,
                        checkpoint,
                    });
                    while let Ok(next) = requests.try_recv() {
                        incoming.push_back(next);
                    }
                }
                Request::TargetContext { .. } | Request::Review { .. } => {}
                Request::Shutdown => break 'worker,
            }
        }
        if batch.is_empty() || *abort_rx.borrow() {
            continue;
        }
        let Some(server) = server.as_ref() else {
            continue;
        };
        if session.is_none() {
            match connect(
                &role,
                &cwd,
                &additional_directories,
                abort_rx.clone(),
                server.advertised.clone(),
                &council_session,
            )
            .await
            {
                Ok(agent) => {
                    session = Some(agent);
                    if let Err(error) = server.wait_until_tools_listed().await {
                        emit_warning(
                            &ui_tx,
                            &role,
                            format!("Loki could not load advise: {error:#}"),
                        );
                        if let Some(old) = session.take() {
                            old.dismiss().await;
                        }
                    }
                }
                Err(error) => {
                    if !*abort_rx.borrow() {
                        emit_warning(&ui_tx, &role, format!("Loki could not start: {error:#}"));
                    }
                    continue;
                }
            }
        }
        let Some(agent) = session.as_mut() else {
            continue;
        };
        let spans = reviewed_spans(&batch);
        let mut context = Vec::new();
        for span in &spans {
            if let Some(value) = pending_context.remove(&(span.target, span.invocation)) {
                context.push(value);
            }
        }
        if !primed && !recent_trajectory.is_empty() {
            context.push(format!(
                "Current outer user request:\n{outer_task}\n\nRecent semantic trajectory before reconnect:\n{recent_trajectory}"
            ));
        }
        let prompt = review_prompt(&batch, &spans, &context, !primed);
        primed = true;
        let id = batch[0].id;
        tracing::info!(event = "review_started", council_session = %council_session, god = "Loki", model = %role.model.model, adapter = %role.launch.source_id, review_id = id, epoch, batch_size = batch.len(), review_target = %spans.iter().map(ReviewedSpan::marker).collect::<Vec<_>>().join(" | "), "Loki review started");
        advice.begin(id, epoch, spans.clone()).await;
        let result = agent
            .prompt(prompt, REVIEW_TIMEOUT, |event| match event {
                TurnEvent::Permission {
                    prompt,
                    access_mode,
                } => {
                    let decision =
                        crate::ragnarok::permission_decision_for_access(access_mode, &prompt);
                    let _ = prompt.responder.send(decision);
                }
                TurnEvent::Message(_)
                | TurnEvent::Thought(_)
                | TurnEvent::Tool { .. }
                | TurnEvent::Note(_) => {}
            })
            .await;
        let accepted = advice.finish(id).await;
        for item in &batch {
            recent_trajectory.push_str(&format!(
                "[{}]\n{}\n",
                spans
                    .iter()
                    .find(|span| {
                        span.target == item.target && span.invocation == item.invocation
                    })
                    .expect("batch span")
                    .marker(),
                item.checkpoint.text
            ));
        }
        recent_trajectory = bounded(recent_trajectory);
        if let Err(error) = result {
            if !*abort_rx.borrow() {
                emit_warning(&ui_tx, &role, format!("Loki review failed open: {error}"));
            }
            primed = false;
            if let Some(old) = session.take() {
                old.dismiss().await;
            }
        }
        tracing::info!(event = "review_finished", council_session = %council_session, god = "Loki", model = %role.model.model, adapter = %role.launch.source_id, review_id = id, epoch, batch_size = batch.len(), advice_target = accepted.as_ref().map(|a| a.target.label()), advice = accepted.as_ref().map(|a| a.note.as_str()), "Loki review finished");
    }
    if let Some(agent) = session {
        agent.dismiss().await;
    }
    let _ = finished.send(true);
}

struct ReviewItem {
    id: u64,
    target: Target,
    invocation: Option<u64>,
    checkpoint: Checkpoint,
}

fn reviewed_spans(batch: &[ReviewItem]) -> Vec<ReviewedSpan> {
    let mut spans: Vec<ReviewedSpan> = Vec::new();
    for item in batch {
        if let Some(span) = spans
            .iter_mut()
            .find(|span| span.target == item.target && span.invocation == item.invocation)
        {
            span.first_step = span.first_step.min(item.checkpoint.step);
            span.last_step = span.last_step.max(item.checkpoint.step);
            for activity in &item.checkpoint.activities {
                if !span.activities.contains(activity) {
                    span.activities.push(activity.clone());
                }
            }
        } else {
            spans.push(ReviewedSpan {
                target: item.target,
                invocation: item.invocation,
                first_step: item.checkpoint.step,
                last_step: item.checkpoint.step,
                activities: item.checkpoint.activities.clone(),
            });
        }
    }
    spans
}

async fn connect(
    role: &ResolvedRole,
    cwd: &std::path::Path,
    additional_directories: &[PathBuf],
    abort: watch::Receiver<bool>,
    advise_server: McpServer,
    council_session: &str,
) -> Result<AgentHandle> {
    let launch = Launch {
        program: role.launch.command.clone(),
        args: role.launch.args.clone(),
        env: role.launch.env.clone(),
    };
    AgentHandle::connect_with_role_config_and_mcp(
        &launch,
        cwd,
        additional_directories,
        abort,
        RuntimeAccessMode::ReadOnly,
        HashMap::new(),
        Some(RuntimeRoleConfig {
            label: "Loki".to_string(),
            model_id: role.model.model.clone(),
            model_value: role.model_value.clone(),
            adapter_source_id: role.launch.source_id.clone(),
            council_session: Some(council_session.to_string()),
        }),
        vec![advise_server],
    )
    .await
}

fn review_prompt(
    batch: &[ReviewItem],
    spans: &[ReviewedSpan],
    context: &[String],
    include_contract: bool,
) -> String {
    let mut prompt = String::new();
    if include_contract {
        prompt.push_str("You are Loki, Mjolnir's persistent read-only advisor. Take a different, user-aligned angle from Thor and Eitri and verify assumptions when useful. Do not restate failures they already know. Stay silent for style, uncertainty, optional improvements, incomplete work, or activity that is on track. The advise tool accepts one note per update and is fully asynchronous: your note is queued and delivered to Thor at the next natural turn boundary, never as an interruption, so reserve it for material correctness, safety, scope, or strategy problems that remain worth raising even if later work may have already addressed them. Never implement changes yourself.\n\n");
    }
    for context in context {
        prompt.push_str(context);
        prompt.push_str("\n\n");
    }
    prompt.push_str("### Chronological session update\n\n");
    for item in batch {
        let span = spans
            .iter()
            .find(|span| span.target == item.target && span.invocation == item.invocation)
            .expect("batch span");
        prompt.push_str(&format!(
            "[{}]\n{}\n\n",
            span.marker(),
            item.checkpoint.text
        ));
    }
    prompt.push_str("Consider only these new checkpoints in light of your existing context. Use advise at most once, selecting a target present above, only for material actionable guidance; otherwise finish silently.");
    bounded(prompt)
}

fn is_content_free(note: &str) -> bool {
    matches!(
        note.trim()
            .trim_end_matches(['.', '!'])
            .to_ascii_lowercase()
            .as_str(),
        "looks good" | "good" | "no issues" | "no issue" | "nothing to add" | "all good" | "lgtm"
    )
}

pub fn format_deferred(advice: &[Advice], current_epoch: u64) -> String {
    advice
        .iter()
        .map(|advice| advice.deferred_text(current_epoch))
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn identity(role: &ResolvedRole) -> LokiIdentity {
    LokiIdentity {
        role: "Loki".to_string(),
        connection_id: "loki".to_string(),
        source_id: Some(role.launch.source_id.clone()),
        model_name: Some(role.model.model.clone()),
        model_value: Some(role.model_value.clone()),
    }
}

fn emit_warning(ui_tx: &mpsc::UnboundedSender<UiEvent>, role: &ResolvedRole, message: String) {
    let _ = ui_tx.send(UiEvent::LokiActivity(LokiActivity::Warning {
        actor: identity(role),
        message,
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{SessionUpdate, ToolCall, ToolCallStatus};

    fn test_handle(epoch: u64) -> (Handle, mpsc::UnboundedReceiver<Request>) {
        let (requests, request_rx) = mpsc::unbounded_channel();
        let (abort, _) = watch::channel(false);
        let (_, finished) = watch::channel(false);
        let (_, posted) = watch::channel(0_u64);
        (
            Handle {
                requests,
                ids: Arc::new(AtomicU64::new(1)),
                epochs: Arc::new(AtomicU64::new(epoch.saturating_add(1))),
                eitri_invocations: Arc::new(AtomicU64::new(1)),
                advice_state: SharedAdviceState::default(),
                abort,
                finished,
                posted,
            },
            request_rx,
        )
    }

    #[tokio::test]
    async fn advice_tool_slot_accepts_one_material_note_per_update() {
        let slot = AdviceSlot::default();
        let span = ReviewedSpan {
            target: Target::Eitri,
            invocation: Some(1),
            first_step: 4,
            last_step: 4,
            activities: vec!["cargo test".to_string()],
        };
        slot.begin(4, 2, vec![span]).await;
        let advice = slot
            .accept(AdviseArgs {
                target: Target::Eitri,
                note: "fix the race".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(advice.epoch, 2);
        assert_eq!(advice.target, Target::Eitri);
        assert_eq!(advice.note, "fix the race");
        assert!(
            slot.accept(AdviseArgs {
                target: Target::Eitri,
                note: "second note".to_string(),
            })
            .await
            .is_err()
        );
        assert_eq!(slot.finish(4).await.unwrap().note, "fix the race");
    }

    #[test]
    fn tool_reviews_only_run_at_completed_or_failed_boundaries() {
        let mut tracker = BoundaryTracker::default();
        let tool = |status| {
            UiEvent::SessionUpdate(SessionUpdate::ToolCall(
                ToolCall::new("tool-1", "build").status(status),
            ))
        };

        assert!(tracker.observe(&tool(ToolCallStatus::Pending)).is_none());
        assert!(tracker.observe(&tool(ToolCallStatus::InProgress)).is_none());
        assert!(tracker.observe(&tool(ToolCallStatus::Completed)).is_some());
        assert!(tracker.observe(&tool(ToolCallStatus::Failed)).is_some());
    }

    #[test]
    fn completed_tool_delta_contains_bounded_input_and_output() {
        let mut tracker = BoundaryTracker::default();
        let event = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("tool-1", "run tests")
                .raw_input(serde_json::json!({"command": "cargo test"}))
                .raw_output(serde_json::json!({"exit": 1, "stderr": "boom"}))
                .status(ToolCallStatus::Failed),
        ));
        let delta = tracker.observe(&event).expect("completed tool boundary");
        assert!(delta.text.contains("tool: run tests (cargo test) [Failed]"));
        assert!(delta.text.contains("boom"));
        assert!(!delta.text.contains("stderr"));
    }

    #[test]
    fn every_completed_step_is_submitted_without_a_workspace_change_gate() {
        let (handle, mut requests) = test_handle(7);
        handle.observe(
            7,
            Target::Thor,
            None,
            Checkpoint {
                step: 1,
                text: "planning only".to_string(),
                activities: vec!["plan update".to_string()],
            },
        );
        let Request::Review {
            target, checkpoint, ..
        } = requests.try_recv().expect("review request")
        else {
            panic!("expected review request");
        };
        assert_eq!(target, Target::Thor);
        assert_eq!(checkpoint.text, "planning only");
    }

    #[test]
    fn reviewed_spans_combine_steps_and_deduplicate_activities() {
        let batch = vec![
            ReviewItem {
                id: 1,
                target: Target::Eitri,
                invocation: Some(2),
                checkpoint: Checkpoint {
                    step: 4,
                    text: "a".into(),
                    activities: vec!["rg".into()],
                },
            },
            ReviewItem {
                id: 2,
                target: Target::Eitri,
                invocation: Some(2),
                checkpoint: Checkpoint {
                    step: 5,
                    text: "b".into(),
                    activities: vec!["rg".into(), "cargo test".into()],
                },
            },
        ];
        let spans = reviewed_spans(&batch);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].first_step, 4);
        assert_eq!(spans[0].last_step, 5);
        assert_eq!(spans[0].activities, ["rg", "cargo test"]);
        assert_eq!(spans[0].marker(), "Eitri #2 · steps 4-5: rg, cargo test");
    }

    #[test]
    fn prompt_done_flushes_the_final_message_as_a_checkpoint() {
        use agent_client_protocol::schema::v1::{
            ContentBlock, ContentChunk, StopReason, TextContent,
        };

        let mut tracker = BoundaryTracker::default();
        let message = UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("done: the fix is in place")),
        )));
        assert!(tracker.observe(&message).is_none());

        let done = UiEvent::PromptDone {
            stop_reason: StopReason::EndTurn,
            usage: None,
        };
        let checkpoint = tracker.observe(&done).expect("final response checkpoint");
        assert!(
            checkpoint
                .text
                .contains("final response:\nmessage:\ndone: the fix is in place")
        );
        assert_eq!(checkpoint.activities, ["final response"]);

        // Nothing pending afterwards: a second completion yields no checkpoint.
        assert!(
            tracker
                .observe(&UiEvent::PromptDone {
                    stop_reason: StopReason::EndTurn,
                    usage: None,
                })
                .is_none()
        );
    }

    #[test]
    fn cancelled_prompt_done_does_not_flush_a_final_checkpoint() {
        use agent_client_protocol::schema::v1::{
            ContentBlock, ContentChunk, StopReason, TextContent,
        };

        let mut tracker = BoundaryTracker::default();
        let message = UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("partial")),
        )));
        assert!(tracker.observe(&message).is_none());
        assert!(
            tracker
                .observe(&UiEvent::PromptDone {
                    stop_reason: StopReason::Cancelled,
                    usage: None,
                })
                .is_none()
        );
    }

    #[test]
    fn message_and_thought_transitions_wait_for_a_semantic_checkpoint() {
        use agent_client_protocol::schema::v1::{ContentBlock, ContentChunk, TextContent};

        let mut tracker = BoundaryTracker::default();
        let message = UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("checking")),
        )));
        let thought = UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new(" next")),
        )));
        assert!(tracker.observe(&message).is_none());
        assert!(tracker.observe(&thought).is_none());

        let tool = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("tool", "cargo test").status(ToolCallStatus::Completed),
        ));
        let checkpoint = tracker.observe(&tool).expect("tool checkpoint");
        assert!(checkpoint.text.contains("message:\nchecking"));
        assert!(checkpoint.text.contains("thinking:\n next"));
        assert_eq!(checkpoint.step, 1);
    }

    #[test]
    fn successful_tool_projects_shape_without_raw_result_body() {
        let tool = ToolCall::new("tool", "search")
            .raw_input(serde_json::json!({
                "action": {"type": "mcpToolCall", "toolName": "explore_agent"},
                "query": "find config"
            }))
            .raw_output(serde_json::json!({"result": "large successful body\nsecond line"}))
            .status(ToolCallStatus::Completed);
        let projected = render_tool_delta(&tool);
        assert!(projected.contains("tool: explore_agent (find config) [Completed]"));
        assert!(projected.contains("2 result lines"));
        assert!(!projected.contains("large successful body"));
    }

    #[test]
    fn terminal_backed_tool_emits_only_the_terminal_exit_checkpoint() {
        use crate::event::TerminalOutputSnapshot;
        use agent_client_protocol::schema::v1::{Terminal, TerminalExitStatus, ToolCallContent};

        let mut tracker = BoundaryTracker::default();
        let pending = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("tool", "printf")
                .content(vec![ToolCallContent::Terminal(Terminal::new("term"))])
                .status(ToolCallStatus::InProgress),
        ));
        assert!(tracker.observe(&pending).is_none());
        let terminal = UiEvent::TerminalOutput(TerminalOutputSnapshot {
            terminal_id: "term".into(),
            output: "alpha\nbeta\n".into(),
            truncated: false,
            exit_status: Some(TerminalExitStatus::new().exit_code(0)),
        });
        let checkpoint = tracker.observe(&terminal).expect("terminal checkpoint");
        assert_eq!(checkpoint.step, 1);
        assert!(checkpoint.text.contains("2 output lines"));

        let completed = UiEvent::SessionUpdate(SessionUpdate::ToolCall(
            ToolCall::new("tool", "printf")
                .content(vec![ToolCallContent::Terminal(Terminal::new("term"))])
                .status(ToolCallStatus::Completed),
        ));
        assert!(tracker.observe(&completed).is_none());
    }

    #[tokio::test]
    async fn accepted_advice_queues_and_notifies_the_posted_watch() {
        let state = SharedAdviceState::default();
        let (posted_tx, mut posted) = watch::channel(0_u64);
        let slot = AdviceSlot {
            active: Arc::default(),
            state: state.clone(),
            posted: Some(posted_tx),
        };
        let span = ReviewedSpan {
            target: Target::Thor,
            invocation: None,
            first_step: 2,
            last_step: 2,
            activities: vec!["cargo test".into()],
        };
        slot.begin(8, 3, vec![span]).await;
        slot.accept(AdviseArgs {
            target: Target::Thor,
            note: "the test is destructive".into(),
        })
        .await
        .unwrap();

        assert!(posted.has_changed().unwrap());
        assert_eq!(*posted.borrow_and_update(), 1);
        let deferred = &state.lock().unwrap().deferred;
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].note, "the test is destructive");
    }

    #[test]
    fn take_deferred_drains_advice_across_turns_with_provenance_labels() {
        let (handle, _requests) = test_handle(4);
        {
            let mut state = handle.advice_state.lock().unwrap();
            let advice = |epoch, note: &str| Advice {
                epoch,
                target: Target::Thor,
                note: note.to_string(),
                span: ReviewedSpan {
                    target: Target::Thor,
                    invocation: None,
                    first_step: 1,
                    last_step: 1,
                    activities: vec!["cargo test".into()],
                },
            };
            state.deferred.push_back(advice(2, "old turn advice"));
            state.deferred.push_back(advice(4, "current turn advice"));
        }

        let taken = handle.take_deferred();
        assert_eq!(taken.len(), 2);
        assert!(handle.take_deferred().is_empty());

        let formatted = format_deferred(&taken, 4);
        assert!(formatted.contains("[reviewed in an earlier turn: Thor · step 1: cargo test]"));
        assert!(formatted.contains("old turn advice"));
        assert!(formatted.contains("[reviewed Thor · step 1: cargo test]"));
        assert!(formatted.contains("current turn advice"));
    }

    #[test]
    fn begin_turn_preserves_queued_advice_from_earlier_turns() {
        let (handle, mut requests) = test_handle(1);
        {
            let mut state = handle.advice_state.lock().unwrap();
            state.deferred.push_back(Advice {
                epoch: 1,
                target: Target::Thor,
                note: "still relevant".to_string(),
                span: ReviewedSpan {
                    target: Target::Thor,
                    invocation: None,
                    first_step: 1,
                    last_step: 1,
                    activities: vec!["edit".into()],
                },
            });
        }

        let epoch = handle.begin_turn("next task".to_string());
        assert_eq!(epoch, 2);
        assert!(matches!(
            requests.try_recv().expect("begin request"),
            Request::Begin { epoch: 2, .. }
        ));
        assert_eq!(handle.take_deferred().len(), 1);
    }
}
