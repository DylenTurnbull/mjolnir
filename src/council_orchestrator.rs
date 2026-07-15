//! Shared Thor turn orchestration for interactive, headless, and remote sessions.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use agent_client_protocol::schema::v1::{SessionUpdate, StopReason, UsageUpdate};
use tokio::sync::{Mutex, mpsc};

use crate::{
    council_usage::{Record, Role},
    event::{InternalMessage, InternalMessageKind, UiCommand, UiEvent},
    loki,
    workspace_snapshot::{WorkspaceDelta, WorkspaceSnapshot},
};

#[derive(Clone, Default)]
struct ActiveTurn {
    epoch: u64,
    task: String,
    snapshot: Option<WorkspaceSnapshot>,
}

#[derive(Clone)]
pub struct Handle {
    turn: Arc<Mutex<ActiveTurn>>,
    review_enabled: Arc<AtomicBool>,
}

impl Handle {
    pub async fn begin_turn(&self, epoch: u64, task: String, snapshot: WorkspaceSnapshot) {
        *self.turn.lock().await = ActiveTurn {
            epoch,
            task,
            snapshot: Some(snapshot),
        };
    }

    pub fn set_review_enabled(&self, enabled: bool) {
        self.review_enabled.store(enabled, Ordering::Release);
    }
}

pub struct Config {
    pub reviewer: Option<loki::Handle>,
    pub runtime_commands: mpsc::UnboundedSender<UiCommand>,
    pub implementation_handoffs: Arc<AtomicUsize>,
    pub discrete_review: bool,
    pub log_context: Option<LogContext>,
}

#[derive(Clone)]
pub struct LogContext {
    pub council_session: String,
    pub model: String,
    pub adapter: String,
}

pub struct Running {
    pub handle: Handle,
    pub events: mpsc::UnboundedReceiver<UiEvent>,
    pub task: tokio::task::JoinHandle<()>,
}

pub fn spawn(mut runtime_events: mpsc::UnboundedReceiver<UiEvent>, config: Config) -> Running {
    let (events_tx, events) = mpsc::unbounded_channel();
    let turn = Arc::new(Mutex::new(ActiveTurn::default()));
    let review_enabled = Arc::new(AtomicBool::new(config.discrete_review));
    let handle = Handle {
        turn: turn.clone(),
        review_enabled: review_enabled.clone(),
    };
    let task = tokio::spawn(async move {
        let mut decisions = config.reviewer.as_ref().map(loki::Handle::subscribe);
        let mut trajectory = loki::BoundaryTracker::default();
        let mut intervention = loki::DeferredIntervention::default();
        let mut held_completion: Option<UiEvent> = None;
        let mut discrete_review_started = false;
        let mut loki_followup_started = false;
        let mut latest_usage_update: Option<UsageUpdate> = None;

        loop {
            tokio::select! {
                event = runtime_events.recv() => {
                    let Some(event) = event else { break; };
                    let active = turn.lock().await.clone();
                    let boundary = (active.epoch > 0).then(|| trajectory.observe(&event)).flatten();
                    let target_completed = target_completed(&event);
                    if let UiEvent::SessionUpdate(SessionUpdate::UsageUpdate(update)) = &event {
                        latest_usage_update = Some(update.clone());
                    }
                    if let UiEvent::PromptDone { usage, .. } = &event {
                        let _ = events_tx.send(UiEvent::CouncilUsage(Record {
                            role: Role::Thor,
                            purpose: None,
                            usage: usage.clone(),
                            update: latest_usage_update.take(),
                        }));
                    }
                    if matches!(event, UiEvent::PromptFailed { .. }) {
                        latest_usage_update = None;
                    }
                    if target_completed && let Some(reviewer) = config.reviewer.as_ref() {
                        reviewer.target_completed(active.epoch, loki::Target::Thor, None);
                    }
                    let interrupting = boundary.is_some()
                        && !target_completed
                        && intervention.interrupt_at_boundary();
                    if interrupting {
                        let _ = events_tx.send(UiEvent::Info(
                            "Thor · interrupting at step boundary for Loki review".to_string(),
                        ));
                        let _ = config.runtime_commands.send(UiCommand::CancelPrompt);
                    }
                    if let Some(boundary) = boundary
                        && !interrupting
                        && !(target_completed && intervention.is_pending())
                        && !loki_followup_started
                        && let Some(reviewer) = config.reviewer.as_ref()
                    {
                        reviewer.observe(active.epoch, loki::Target::Thor, None, boundary);
                    }

                    match &event {
                        UiEvent::PromptDone { stop_reason, .. } => {
                            let cancelled = matches!(stop_reason, StopReason::Cancelled);
                            if cancelled
                                && intervention.is_pending()
                                && !intervention.cancellation_was_requested()
                            {
                                intervention.clear();
                            } else if let Some(critique) = intervention.take() {
                                resume_with_advice(
                                    &events_tx,
                                    &config,
                                    &mut trajectory,
                                    active.epoch,
                                    &active.task,
                                    critique,
                                    "Thor · resumed after Loki intervention",
                                );
                                held_completion = None;
                                continue;
                            }
                            if cancelled {
                                let _ = events_tx.send(event);
                                finish_turn(
                                    &turn,
                                    config.reviewer.as_ref(),
                                    active.epoch,
                                    &mut trajectory,
                                    &mut intervention,
                                    &mut discrete_review_started,
                                    &mut loki_followup_started,
                                ).await;
                                continue;
                            }
                            held_completion = Some(event);
                        }
                        UiEvent::PromptFailed { .. } if intervention.is_pending() => {
                            let critique = intervention.take().expect("queued intervention");
                            resume_with_advice(
                                &events_tx,
                                &config,
                                &mut trajectory,
                                active.epoch,
                                &active.task,
                                critique,
                                "Thor · resumed after Loki intervention",
                            );
                            held_completion = None;
                            continue;
                        }
                        _ => {
                            let _ = events_tx.send(event);
                            if target_completed {
                                finish_turn(
                                    &turn,
                                    config.reviewer.as_ref(),
                                    active.epoch,
                                    &mut trajectory,
                                    &mut intervention,
                                    &mut discrete_review_started,
                                    &mut loki_followup_started,
                                ).await;
                            }
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
                    let active = turn.lock().await.clone();
                    if decision.epoch != active.epoch || decision.target != loki::Target::Thor {
                        continue;
                    }
                    if let Some(context) = config.log_context.as_ref() {
                        tracing::info!(
                            event = "advice_received",
                            advice_id = decision.id,
                            epoch = decision.epoch,
                            council_session = %context.council_session,
                            god = "Thor",
                            source = "Loki",
                            model = %context.model,
                            adapter = %context.adapter,
                            advice = %decision.critique,
                            "Thor received Loki advice"
                        );
                    }
                    intervention.push(decision.id, decision.critique);
                    if held_completion.take().is_some() {
                        let critique = intervention.take().expect("queued intervention");
                        resume_with_advice(
                            &events_tx,
                            &config,
                            &mut trajectory,
                            active.epoch,
                            &active.task,
                            critique,
                            "Thor · re-prompted after Loki intervention",
                        );
                    } else {
                        let _ = events_tx.send(UiEvent::Info(
                            "Thor · Loki intervention queued for the next step boundary".to_string(),
                        ));
                    }
                }
            }

            if held_completion.is_none() || intervention.is_pending() {
                continue;
            }
            let active = turn.lock().await.clone();
            let handoffs = config.implementation_handoffs.load(Ordering::Acquire);
            let review = review_enabled.load(Ordering::Acquire);
            let delta = if review && handoffs > 1 && !discrete_review_started {
                match active.snapshot.as_ref() {
                    Some(snapshot) => Some(snapshot.delta().await),
                    None => None,
                }
            } else {
                None
            };
            if should_start_discrete_review(
                review,
                discrete_review_started,
                handoffs,
                delta.as_ref().is_some_and(WorkspaceDelta::changed),
            ) {
                let initial_result = trajectory.final_message();
                let context = discrete_review_context(delta.as_ref(), trajectory.trajectory());
                held_completion = None;
                discrete_review_started = true;
                trajectory.reset_attempt();
                if let Some(reviewer) = config.reviewer.as_ref() {
                    reviewer.target_resumed(active.epoch, loki::Target::Thor, None);
                }
                let prompt = thor_discrete_review_prompt(&active.task, &initial_result, &context);
                let _ = events_tx.send(UiEvent::Info("reviewing the completed work…".to_string()));
                emit_internal(
                    &events_tx,
                    "Thor",
                    "Thor",
                    InternalMessageKind::DiscreteReview,
                    &prompt,
                );
                let _ = config.runtime_commands.send(UiCommand::SendPrompt {
                    text: prompt,
                    images: Vec::new(),
                });
                continue;
            }
            if !loki_followup_started && let Some(reviewer) = config.reviewer.as_ref() {
                let deferred = reviewer.take_deferred(active.epoch);
                if !deferred.is_empty() {
                    let advice = loki::format_deferred(&deferred);
                    held_completion = None;
                    loki_followup_started = true;
                    trajectory.reset_attempt();
                    emit_internal(
                        &events_tx,
                        "Loki",
                        "Thor",
                        InternalMessageKind::Continuation,
                        &advice,
                    );
                    let _ = config.runtime_commands.send(UiCommand::SendPrompt {
                        text: continuation_prompt(&advice),
                        images: Vec::new(),
                    });
                    continue;
                }
            }
            let event = held_completion.take().expect("completion held");
            let _ = events_tx.send(event);
            finish_turn(
                &turn,
                config.reviewer.as_ref(),
                active.epoch,
                &mut trajectory,
                &mut intervention,
                &mut discrete_review_started,
                &mut loki_followup_started,
            )
            .await;
        }
    });
    Running {
        handle,
        events,
        task,
    }
}

fn resume_with_advice(
    events: &mpsc::UnboundedSender<UiEvent>,
    config: &Config,
    trajectory: &mut loki::BoundaryTracker,
    epoch: u64,
    _task: &str,
    critique: String,
    status: &str,
) {
    if let Some(reviewer) = config.reviewer.as_ref() {
        reviewer.target_resumed(epoch, loki::Target::Thor, None);
    }
    trajectory.reset_attempt();
    let _ = events.send(UiEvent::Info(status.to_string()));
    emit_internal(
        events,
        "Loki",
        "Thor",
        InternalMessageKind::Continuation,
        &critique,
    );
    let _ = config.runtime_commands.send(UiCommand::SendPrompt {
        text: continuation_prompt(&critique),
        images: Vec::new(),
    });
}

async fn finish_turn(
    turn: &Mutex<ActiveTurn>,
    reviewer: Option<&loki::Handle>,
    epoch: u64,
    trajectory: &mut loki::BoundaryTracker,
    intervention: &mut loki::DeferredIntervention,
    discrete_review_started: &mut bool,
    loki_followup_started: &mut bool,
) {
    if let Some(reviewer) = reviewer {
        reviewer.end_turn(epoch);
    }
    *turn.lock().await = ActiveTurn::default();
    *trajectory = loki::BoundaryTracker::default();
    intervention.clear();
    *discrete_review_started = false;
    *loki_followup_started = false;
}

fn target_completed(event: &UiEvent) -> bool {
    matches!(
        event,
        UiEvent::PromptDone { .. } | UiEvent::PromptFailed { .. }
    )
}

fn continuation_prompt(critique: &str) -> String {
    format!(
        "<advisory guidance=\"weigh, don't blindly obey\">\n{critique}\n</advisory>\n\nContinue the interrupted turn. Address the material advice, then finish the existing task. Please continue from where you left off."
    )
}

fn should_start_discrete_review(
    enabled: bool,
    already_started: bool,
    implementation_handoffs: usize,
    workspace_changed: bool,
) -> bool {
    enabled && !already_started && implementation_handoffs > 1 && workspace_changed
}

fn thor_discrete_review_prompt(task: &str, initial_result: &str, context: &str) -> String {
    format!(
        "Perform Thor's discrete review for this same user turn. You own the research, planning, coordination, review, verification, and final response; do not act as a thin relay for Eitri. Re-read the original task, critically review the initial result and implementation evidence, investigate or verify anything necessary, and correct material issues. If code changes are still needed, delegate them to Eitri with code_agent and then review the new result. Return the final user-facing answer when the work is genuinely complete.\n\nOriginal task:\n{task}\n\nInitial result:\n{initial_result}\n\nBounded trajectory and workspace context:\n{context}"
    )
}

fn discrete_review_context(delta: Option<&WorkspaceDelta>, trajectory: String) -> String {
    let diff = match delta {
        Some(delta) => delta
            .review_patch()
            .map(str::to_string)
            .unwrap_or_else(|| "[no workspace changes attributable to this user turn]".to_string()),
        None => "[workspace turn snapshot unavailable]".to_string(),
    };
    let mut context = format!("Trajectory:\n{trajectory}\n\nWorkspace diff:\n{diff}");
    const LIMIT: usize = 128 * 1024;
    if context.len() > LIMIT {
        let split = context.ceil_char_boundary(context.len() - LIMIT);
        context = format!("…[earlier review context omitted]\n{}", &context[split..]);
    }
    context
}

fn emit_internal(
    events: &mpsc::UnboundedSender<UiEvent>,
    source: &str,
    target: &str,
    kind: InternalMessageKind,
    text: &str,
) {
    let _ = events.send(UiEvent::InternalMessage(InternalMessage {
        source: source.to_string(),
        target: target.to_string(),
        kind,
        text: text.to_string(),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn review_requires_multiple_implementation_handoffs_and_changes() {
        assert!(should_start_discrete_review(true, false, 2, true));
        assert!(!should_start_discrete_review(true, false, 1, true));
        assert!(!should_start_discrete_review(true, true, 2, true));
        assert!(!should_start_discrete_review(true, false, 2, false));
    }

    #[test]
    fn continuation_is_advisory_and_does_not_repeat_the_outer_task() {
        let prompt = continuation_prompt("fix the race");
        assert!(prompt.contains("guidance=\"weigh, don't blindly obey\""));
        assert!(prompt.contains("fix the race"));
        assert!(prompt.contains("Please continue from where you left off."));
    }
}
