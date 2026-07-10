//! Optional read-only council reviewer. One ACP session is kept for each outer
//! user turn; observations are serialized through it and decisions are
//! delivered mechanically to the active Thor or Eitri driver.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc, watch};

use crate::acp::{RuntimeAccessMode, RuntimeRoleConfig};
use crate::council::ResolvedRole;
use crate::event::{ActorActivity, ActorIdentity, UiEvent};
use crate::ragnarok::{AgentHandle, Launch, TurnEvent};

const REVIEW_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MAX_CONTEXT_BYTES: usize = 96 * 1024;

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
    pub final_review: bool,
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
        boundary: String,
        trajectory: String,
        final_review: bool,
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
    pub streaming_enabled: bool,
    pub final_enabled: bool,
}

impl Handle {
    pub fn start(
        role: ResolvedRole,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        ui_tx: mpsc::UnboundedSender<UiEvent>,
        streaming_enabled: bool,
        final_enabled: bool,
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
            streaming_enabled,
            final_enabled,
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

    pub fn begin_turn(&self, task: String) -> u64 {
        let _ = self.abort.send(false);
        let epoch = self.epochs.fetch_add(1, Ordering::Relaxed);
        let _ = self.requests.send(Request::Begin { epoch, task });
        epoch
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
        boundary: String,
        trajectory: String,
    ) -> Option<u64> {
        self.streaming_enabled
            .then(|| self.submit(epoch, target, boundary, trajectory, false))
    }

    pub fn final_review(
        &self,
        epoch: u64,
        result: String,
        trajectory_and_diff: String,
    ) -> Option<u64> {
        self.final_enabled.then(|| {
            self.submit(
                epoch,
                Target::Thor,
                format!("Thor completed its initial answer:\n{result}"),
                trajectory_and_diff,
                true,
            )
        })
    }

    fn submit(
        &self,
        epoch: u64,
        target: Target,
        boundary: String,
        trajectory: String,
        final_review: bool,
    ) -> u64 {
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        let _ = self.requests.send(Request::Review {
            id,
            epoch,
            target,
            boundary: bounded(boundary),
            trajectory: bounded(trajectory),
            final_review,
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
                boundary,
                trajectory,
                final_review,
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
                                final_review,
                                verdict: Verdict::Failed(message),
                            });
                            continue;
                        }
                    }
                }
                let prompt = review_prompt(&task, target, &boundary, &trajectory, final_review);
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
                    final_review,
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
    boundary: &str,
    trajectory: &str,
    final_review: bool,
) -> String {
    format!(
        "You are Loki, a read-only reviewer of {actor}. Any intervention you issue forces Mjolnir to cancel the active request and re-prompt it, which is expensive and discards in-flight work. Intervene only for a material correctness, safety, scope, or strategy problem. Do not intervene for style, optional improvements, or mere uncertainty. Return exactly one JSON object and no markdown: {{\"decision\":\"no_intervention\"}} or {{\"decision\":\"intervention\",\"critique\":\"specific actionable critique\"}}.\n\nOriginal task:\n{task}\n\nReview kind: {kind}\nBoundary:\n{boundary}\n\nBounded trajectory and workspace context:\n{trajectory}",
        actor = target.label(),
        kind = if final_review {
            "discrete final review"
        } else {
            "streaming boundary review"
        },
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
}
