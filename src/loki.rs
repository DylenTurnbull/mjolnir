//! Optional read-only council reviewer. One ACP session is kept for each outer
//! user turn; observations are serialized through it and decisions are
//! delivered mechanically to the active Thor or Eitri driver.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc, watch};

use crate::acp::{RuntimeAccessMode, RuntimeRoleConfig, WorkspaceTurnSnapshot};
use crate::council::ResolvedRole;
use crate::event::{ActorActivity, ActorIdentity, UiEvent};
use crate::ragnarok::{AgentHandle, Launch, TurnEvent};

const REVIEW_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MAX_CONTEXT_BYTES: usize = 96 * 1024;

/// Loki critiques waiting to be delivered to a target at a safe step boundary.
///
/// Review decisions arrive asynchronously, often after the target has already
/// started its next operation. Keep them ordered by review id and request at
/// most one cancellation when the next boundary is observed.
#[derive(Default)]
pub struct DeferredIntervention {
    critiques: BTreeMap<u64, String>,
    cancel_requested: bool,
}

impl DeferredIntervention {
    pub fn push(&mut self, id: u64, critique: String) {
        self.critiques.insert(id, critique);
    }

    pub fn is_pending(&self) -> bool {
        !self.critiques.is_empty()
    }

    /// Mark the next observed non-terminal step boundary for interruption.
    pub fn interrupt_at_boundary(&mut self) -> bool {
        if self.critiques.is_empty() || self.cancel_requested {
            return false;
        }
        self.cancel_requested = true;
        true
    }

    pub fn cancellation_was_requested(&self) -> bool {
        self.cancel_requested
    }

    /// Drain queued critiques in observation order for one continuation prompt.
    pub fn take(&mut self) -> Option<String> {
        if self.critiques.is_empty() {
            return None;
        }
        self.cancel_requested = false;
        let critiques = std::mem::take(&mut self.critiques);
        Some(
            critiques
                .into_values()
                .enumerate()
                .map(|(index, critique)| {
                    if index == 0 {
                        critique
                    } else {
                        format!("Additional Loki critique: {critique}")
                    }
                })
                .collect::<Vec<_>>()
                .join("\n\n"),
        )
    }

    pub fn clear(&mut self) {
        self.critiques.clear();
        self.cancel_requested = false;
    }
}

#[derive(Default)]
pub struct BoundaryTracker {
    trajectory: String,
    final_message: String,
    segment: String,
    lane: Option<SegmentLane>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SegmentLane {
    Message,
    Thought,
}

impl BoundaryTracker {
    pub fn observe(&mut self, event: &UiEvent) -> Option<String> {
        use agent_client_protocol::schema::v1::SessionUpdate;
        let flush = |this: &mut Self| {
            let lane = this.lane.take()?;
            if this.segment.trim().is_empty() {
                this.segment.clear();
                return None;
            }
            let kind = match lane {
                SegmentLane::Message => "completed message segment",
                SegmentLane::Thought => "completed thought segment",
            };
            Some(format!("{kind}: {}", std::mem::take(&mut this.segment)))
        };
        let boundary = match event {
            UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(chunk)) => {
                let text = crate::event::content_block_text(&chunk.content);
                let previous = (self.lane == Some(SegmentLane::Thought))
                    .then(|| flush(self))
                    .flatten();
                self.lane = Some(SegmentLane::Message);
                self.segment.push_str(&text);
                self.final_message.push_str(&text);
                previous
            }
            UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(chunk)) => {
                let previous = (self.lane == Some(SegmentLane::Message))
                    .then(|| flush(self))
                    .flatten();
                self.lane = Some(SegmentLane::Thought);
                self.segment
                    .push_str(&crate::event::content_block_text(&chunk.content));
                previous
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCall(call))
                if matches!(
                    call.status,
                    agent_client_protocol::schema::v1::ToolCallStatus::Completed
                        | agent_client_protocol::schema::v1::ToolCallStatus::Failed
                ) =>
            {
                self.final_message.clear();
                Some(join_boundary(
                    flush(self),
                    format!("tool call: {} [{:?}]", call.title, call.status),
                ))
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCallUpdate(update))
                if matches!(
                    update.fields.status,
                    Some(
                        agent_client_protocol::schema::v1::ToolCallStatus::Completed
                            | agent_client_protocol::schema::v1::ToolCallStatus::Failed
                    )
                ) =>
            {
                self.final_message.clear();
                Some(join_boundary(
                    flush(self),
                    format!(
                        "tool boundary: {} [{:?}]",
                        update.tool_call_id, update.fields.status
                    ),
                ))
            }
            UiEvent::SessionUpdate(SessionUpdate::Plan(plan)) => {
                self.final_message.clear();
                Some(join_boundary(
                    flush(self),
                    format!("plan transition: {plan:?}"),
                ))
            }
            UiEvent::TerminalOutput(snapshot) if snapshot.exit_status.is_some() => {
                Some(join_boundary(
                    flush(self),
                    format!(
                        "terminal boundary: {} {:?}",
                        snapshot.terminal_id, snapshot.exit_status
                    ),
                ))
            }
            UiEvent::PromptDone { stop_reason, .. } => Some(join_boundary(
                flush(self),
                format!("turn boundary: {stop_reason:?}"),
            )),
            UiEvent::PromptFailed { message } => Some(join_boundary(
                flush(self),
                format!("failed turn boundary: {message}"),
            )),
            _ => None,
        };
        if let Some(boundary) = boundary.as_ref() {
            self.trajectory.push_str(boundary);
            self.trajectory.push('\n');
            self.trajectory = bounded(std::mem::take(&mut self.trajectory));
        }
        boundary
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
    }
}

fn join_boundary(previous: Option<String>, current: String) -> String {
    previous.map_or(current.clone(), |previous| format!("{previous}\n{current}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone)]
pub enum Verdict {
    NoIntervention,
    Intervention(String),
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub id: u64,
    pub epoch: u64,
    pub target: Target,
    pub verdict: Verdict,
}

enum Request {
    Begin {
        epoch: u64,
        task: String,
    },
    Review {
        id: u64,
        epoch: u64,
        target: Target,
        workspace_diff: String,
        boundary: String,
        trajectory: String,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct Handle {
    requests: mpsc::UnboundedSender<Request>,
    decisions: broadcast::Sender<Decision>,
    ids: Arc<AtomicU64>,
    epochs: Arc<AtomicU64>,
    abort: watch::Sender<bool>,
    finished: watch::Receiver<bool>,
    turn_snapshot: Arc<RwLock<Option<(u64, WorkspaceTurnSnapshot)>>>,
    pub streaming_enabled: bool,
}

impl Handle {
    pub fn start(
        role: ResolvedRole,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        ui_tx: mpsc::UnboundedSender<UiEvent>,
        streaming_enabled: bool,
    ) -> Self {
        let (requests, rx) = mpsc::unbounded_channel();
        let (decisions, _) = broadcast::channel(512);
        let (abort, abort_rx) = watch::channel(false);
        let (finished_tx, finished) = watch::channel(false);
        let handle = Self {
            requests,
            decisions: decisions.clone(),
            ids: Arc::new(AtomicU64::new(1)),
            epochs: Arc::new(AtomicU64::new(1)),
            abort,
            finished,
            turn_snapshot: Arc::new(RwLock::new(None)),
            streaming_enabled,
        };
        tokio::spawn(worker(
            role,
            cwd,
            additional_directories,
            ui_tx,
            rx,
            decisions,
            abort_rx,
            finished_tx,
        ));
        handle
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Decision> {
        self.decisions.subscribe()
    }

    pub fn begin_turn(&self, task: String, snapshot: WorkspaceTurnSnapshot) -> u64 {
        let _ = self.abort.send(false);
        let epoch = self.epochs.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut current) = self.turn_snapshot.write() {
            *current = Some((epoch, snapshot));
        }
        let _ = self.requests.send(Request::Begin { epoch, task });
        epoch
    }

    pub fn current_epoch(&self) -> u64 {
        self.epochs.load(Ordering::Relaxed).saturating_sub(1)
    }

    pub fn cancel_turn(&self) {
        let _ = self.abort.send(true);
    }

    pub async fn observe(
        &self,
        epoch: u64,
        target: Target,
        boundary: String,
        trajectory: String,
    ) -> Option<u64> {
        if !self.streaming_enabled {
            return None;
        }
        let snapshot = self
            .turn_snapshot
            .read()
            .ok()
            .and_then(|current| current.as_ref().filter(|(turn, _)| *turn == epoch).cloned())
            .map(|(_, snapshot)| snapshot)?;
        let workspace_diff = snapshot.complete_diff().await?;
        Some(self.submit(epoch, target, workspace_diff, boundary, trajectory))
    }

    fn submit(
        &self,
        epoch: u64,
        target: Target,
        workspace_diff: String,
        boundary: String,
        trajectory: String,
    ) -> u64 {
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        let _ = self.requests.send(Request::Review {
            id,
            epoch,
            target,
            workspace_diff,
            boundary: bounded(boundary),
            trajectory: bounded(trajectory),
        });
        id
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

#[derive(Deserialize)]
struct WireDecision {
    decision: String,
    #[serde(default)]
    critique: Option<String>,
}

fn parse_decision(text: &str) -> Result<Verdict> {
    let trimmed = text.trim();
    let json = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .unwrap_or(trimmed)
        .strip_suffix("```")
        .unwrap_or(trimmed)
        .trim();
    let parsed: WireDecision = serde_json::from_str(json).context("parse Loki decision JSON")?;
    match parsed.decision.as_str() {
        "no_intervention" => Ok(Verdict::NoIntervention),
        "intervention" => parsed
            .critique
            .filter(|critique| !critique.trim().is_empty())
            .map(Verdict::Intervention)
            .ok_or_else(|| anyhow!("Loki intervention omitted critique")),
        other => Err(anyhow!("unknown Loki decision '{other}'")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn worker(
    role: ResolvedRole,
    cwd: PathBuf,
    additional_directories: Vec<PathBuf>,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    mut requests: mpsc::UnboundedReceiver<Request>,
    decisions: broadcast::Sender<Decision>,
    abort_rx: watch::Receiver<bool>,
    finished: watch::Sender<bool>,
) {
    let mut epoch = 0;
    let mut task = String::new();
    let mut session: Option<AgentHandle> = None;
    while let Some(request) = requests.recv().await {
        match request {
            Request::Begin {
                epoch: next,
                task: next_task,
            } => {
                if let Some(old) = session.take() {
                    old.dismiss().await;
                }
                epoch = next;
                task = next_task;
            }
            Request::Review {
                id,
                epoch: request_epoch,
                target,
                workspace_diff,
                boundary,
                trajectory,
            } if request_epoch == epoch => {
                if session.is_none() {
                    match connect(&role, &cwd, &additional_directories, abort_rx.clone()).await {
                        Ok(agent) => {
                            emit(
                                &ui_tx,
                                &role,
                                ActorActivity::Connected {
                                    actor: identity(&role),
                                },
                            );
                            session = Some(agent);
                        }
                        Err(error) => {
                            let message = format!("Loki could not start: {error:#}");
                            emit_warning(&ui_tx, &role, message.clone());
                            let _ = decisions.send(Decision {
                                id,
                                epoch,
                                target,
                                verdict: Verdict::Failed(message),
                            });
                            continue;
                        }
                    }
                }
                let prompt = review_prompt(&task, target, &workspace_diff, &boundary, &trajectory);
                let result = session
                    .as_mut()
                    .expect("connected")
                    .prompt(prompt, REVIEW_TIMEOUT, |event| match event {
                        TurnEvent::Thought(text) => emit(
                            &ui_tx,
                            &role,
                            ActorActivity::Thought {
                                actor: identity(&role),
                                text,
                            },
                        ),
                        TurnEvent::Tool {
                            title,
                            kind,
                            status,
                            ..
                        } => emit(
                            &ui_tx,
                            &role,
                            ActorActivity::Tool {
                                actor: identity(&role),
                                tool_id: format!("loki:{id}:{title}"),
                                title,
                                kind: kind.map(|value| format!("{value:?}")),
                                status: status.map(|value| format!("{value:?}")),
                            },
                        ),
                        TurnEvent::Note(message) => emit(
                            &ui_tx,
                            &role,
                            ActorActivity::Info {
                                actor: identity(&role),
                                message,
                            },
                        ),
                        TurnEvent::Permission {
                            prompt,
                            access_mode,
                        } => {
                            let decision = crate::ragnarok::permission_decision_for_access(
                                access_mode,
                                &prompt,
                            );
                            let _ = prompt.responder.send(decision);
                        }
                        TurnEvent::Message(_) => {}
                    })
                    .await;
                let verdict = match result {
                    Ok(outcome) => parse_decision(&outcome.text)
                        .unwrap_or_else(|error| Verdict::Failed(error.to_string())),
                    Err(error) => Verdict::Failed(error.to_string()),
                };
                match &verdict {
                    Verdict::Intervention(critique) => emit_warning(
                        &ui_tx,
                        &role,
                        format!("Loki intervenes against {}: {critique}", target.label()),
                    ),
                    Verdict::Failed(message) => {
                        emit_warning(&ui_tx, &role, format!("Loki review failed open: {message}"))
                    }
                    Verdict::NoIntervention => {}
                }
                let _ = decisions.send(Decision {
                    id,
                    epoch,
                    target,
                    verdict,
                });
            }
            Request::Review { .. } => {}
            Request::Shutdown => break,
        }
    }
    if let Some(agent) = session {
        agent.dismiss().await;
    }
    let _ = finished.send(true);
}

async fn connect(
    role: &ResolvedRole,
    cwd: &std::path::Path,
    additional_directories: &[PathBuf],
    abort: watch::Receiver<bool>,
) -> Result<AgentHandle> {
    let launch = Launch {
        program: role.launch.command.clone(),
        args: role.launch.args.clone(),
        env: role.launch.env.clone(),
    };
    AgentHandle::connect_with_role_config(
        &launch,
        cwd,
        additional_directories,
        abort,
        RuntimeAccessMode::ReadOnly,
        HashMap::new(),
        Some(RuntimeRoleConfig {
            label: "Loki".to_string(),
            model_value: role.model_value.clone(),
            force_high_reasoning: true,
        }),
    )
    .await
}

fn review_prompt(
    task: &str,
    target: Target,
    workspace_diff: &str,
    boundary: &str,
    trajectory: &str,
) -> String {
    format!(
        "You are Loki, a read-only streaming reviewer of {actor}. Any intervention you issue is queued until the target's next safe step boundary, then Mjolnir interrupts normal continuation and re-prompts it with your critique. Intervene only for a material correctness, safety, scope, or strategy problem. Do not intervene for style, optional improvements, or mere uncertainty. Return exactly one JSON object and no markdown: {{\"decision\":\"no_intervention\"}} or {{\"decision\":\"intervention\",\"critique\":\"specific actionable critique\"}}.\n\nOriginal task:\n{task}\n\nComplete workspace diff since this user prompt began (starting point for review):\n{workspace_diff}\n\nReview kind: streaming boundary review\nBoundary:\n{boundary}\n\nBounded trajectory:\n{trajectory}",
        actor = target.label(),
    )
}

fn identity(role: &ResolvedRole) -> ActorIdentity {
    ActorIdentity {
        role: "Loki".to_string(),
        connection_id: "loki".to_string(),
        source_id: Some(role.launch.source_id.clone()),
        model_name: Some(role.model.model.clone()),
        model_value: Some(role.model_value.clone()),
    }
}

fn emit(ui_tx: &mpsc::UnboundedSender<UiEvent>, _role: &ResolvedRole, activity: ActorActivity) {
    let _ = ui_tx.send(UiEvent::ActorActivity(activity));
}

fn emit_warning(ui_tx: &mpsc::UnboundedSender<UiEvent>, role: &ResolvedRole, message: String) {
    emit(
        ui_tx,
        role,
        ActorActivity::Warning {
            actor: identity(role),
            message,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{SessionUpdate, ToolCall, ToolCallStatus};

    fn run_git(root: &std::path::Path, args: &[&str]) {
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

    fn test_handle(
        epoch: u64,
        snapshot: WorkspaceTurnSnapshot,
    ) -> (Handle, mpsc::UnboundedReceiver<Request>) {
        let (requests, request_rx) = mpsc::unbounded_channel();
        let (decisions, _) = broadcast::channel(8);
        let (abort, _) = watch::channel(false);
        let (_, finished) = watch::channel(false);
        (
            Handle {
                requests,
                decisions,
                ids: Arc::new(AtomicU64::new(1)),
                epochs: Arc::new(AtomicU64::new(epoch.saturating_add(1))),
                abort,
                finished,
                turn_snapshot: Arc::new(RwLock::new(Some((epoch, snapshot)))),
                streaming_enabled: true,
            },
            request_rx,
        )
    }

    #[test]
    fn strict_decisions_require_material_critique() {
        assert!(matches!(
            parse_decision(r#"{"decision":"no_intervention"}"#).unwrap(),
            Verdict::NoIntervention
        ));
        assert!(parse_decision(r#"{"decision":"intervention"}"#).is_err());
        assert!(matches!(
            parse_decision(r#"{"decision":"intervention","critique":"wrong file"}"#).unwrap(),
            Verdict::Intervention(_)
        ));
    }

    #[test]
    fn intervention_waits_for_boundary_and_requests_one_cancellation() {
        let mut intervention = DeferredIntervention::default();

        intervention.push(7, "inspect the generated config".to_string());

        assert!(intervention.is_pending());
        assert!(!intervention.cancellation_was_requested());
        assert!(intervention.interrupt_at_boundary());
        assert!(intervention.cancellation_was_requested());
        assert!(!intervention.interrupt_at_boundary());
        assert_eq!(
            intervention.take().as_deref(),
            Some("inspect the generated config")
        );
        assert!(!intervention.is_pending());
        assert!(!intervention.cancellation_was_requested());
    }

    #[test]
    fn completed_target_can_be_reprompted_without_cancellation() {
        let mut intervention = DeferredIntervention::default();
        intervention.push(3, "fix the final answer".to_string());

        assert_eq!(intervention.take().as_deref(), Some("fix the final answer"));
        assert!(!intervention.cancellation_was_requested());
    }

    #[test]
    fn queued_critiques_are_ordered_and_clear_resets_boundary_state() {
        let mut intervention = DeferredIntervention::default();
        intervention.push(20, "second observed review".to_string());
        intervention.push(10, "first observed review".to_string());
        assert!(intervention.interrupt_at_boundary());

        assert_eq!(
            intervention.take().as_deref(),
            Some("first observed review\n\nAdditional Loki critique: second observed review")
        );

        intervention.push(30, "stale review".to_string());
        assert!(intervention.interrupt_at_boundary());
        intervention.clear();
        assert!(!intervention.is_pending());
        assert!(!intervention.cancellation_was_requested());
        assert!(!intervention.interrupt_at_boundary());
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

    #[tokio::test]
    async fn auto_review_requires_attributable_changes_and_starts_with_complete_diff() {
        let temp = tempfile::tempdir().expect("tempdir");
        run_git(temp.path(), &["init", "-q"]);
        run_git(
            temp.path(),
            &["config", "user.email", "mjolnir@example.test"],
        );
        run_git(temp.path(), &["config", "user.name", "Mjolnir Tests"]);
        let path = temp.path().join("notes.txt");
        tokio::fs::write(&path, "committed\n")
            .await
            .expect("seed file");
        run_git(temp.path(), &["add", "notes.txt"]);
        run_git(temp.path(), &["commit", "-qm", "seed"]);
        tokio::fs::write(&path, "dirty before user prompt\n")
            .await
            .expect("preexisting dirty state");

        let root = tokio::fs::canonicalize(temp.path()).await.expect("root");
        let snapshot =
            WorkspaceTurnSnapshot::capture(&[root], crate::acp::DEFAULT_FS_TEXT_BYTES).await;
        let (handle, mut requests) = test_handle(7, snapshot);

        assert!(
            handle
                .observe(
                    7,
                    Target::Thor,
                    "planning boundary".to_string(),
                    "planning only".to_string(),
                )
                .await
                .is_none()
        );
        assert!(requests.try_recv().is_err());

        tokio::fs::write(&path, "changed by this user prompt\n")
            .await
            .expect("turn change");
        assert!(
            handle
                .observe(
                    7,
                    Target::Eitri,
                    "edit boundary".to_string(),
                    "implementation trajectory".to_string(),
                )
                .await
                .is_some()
        );

        let Request::Review {
            workspace_diff,
            boundary,
            trajectory,
            ..
        } = requests.try_recv().expect("review request")
        else {
            panic!("expected review request");
        };
        assert!(workspace_diff.contains("dirty before user prompt"));
        assert!(workspace_diff.contains("changed by this user prompt"));
        assert!(!workspace_diff.contains("--- before\ncommitted\n"));
        let prompt = review_prompt(
            "change notes",
            Target::Eitri,
            &workspace_diff,
            &boundary,
            &trajectory,
        );
        assert!(
            prompt.find(&workspace_diff).expect("diff")
                < prompt.find("Boundary:\nedit boundary").expect("boundary")
        );
    }
}
