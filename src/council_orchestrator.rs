//! Shared Thor turn orchestration for interactive, headless, and remote sessions.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use agent_client_protocol::schema::v1::{SessionUpdate, StopReason, UsageUpdate};
use tokio::sync::{Mutex, mpsc};

use crate::{
    code_agent::ActiveCodeWorkers,
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
    pub active_implementation_workers: ActiveCodeWorkers,
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
        let mut active_worker_updates = config.active_implementation_workers.subscribe();
        let mut advice_watch = config.reviewer.as_ref().map(loki::Handle::subscribe_advice);
        let mut trajectory = loki::BoundaryTracker::default();
        let mut held_completion = None;
        let mut discrete_review_started = false;
        let mut idle_epoch = None;
        let mut interjected_epoch = None;
        let mut observed_epoch = 0;
        let mut latest_usage_update: Option<UsageUpdate> = None;

        loop {
            tokio::select! {
                event = runtime_events.recv() => {
                    let Some(event) = event else { break; };
                    let active = turn.lock().await.clone();
                    if active.epoch != observed_epoch {
                        observed_epoch = active.epoch;
                        idle_epoch = None;
                        held_completion = None;
                        discrete_review_started = false;
                        trajectory = loki::BoundaryTracker::default();
                    }
                    if let Some(boundary) = (active.epoch > 0)
                        .then(|| trajectory.observe(&event))
                        .flatten()
                        && let Some(reviewer) = config.reviewer.as_ref()
                    {
                        reviewer.observe(active.epoch, loki::Target::Thor, None, boundary);
                    }
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

                    match &event {
                        UiEvent::PromptDone {
                            stop_reason: StopReason::Cancelled,
                            ..
                        } => {
                            let _ = events_tx.send(event);
                            reset_turn_state(
                                &mut trajectory,
                                &mut held_completion,
                                &mut discrete_review_started,
                            );
                            idle_epoch = None;
                            interjected_epoch = Some(active.epoch);
                        }
                        UiEvent::PromptDone { .. } => held_completion = Some(event),
                        UiEvent::PromptFailed { .. } => {
                            latest_usage_update = None;
                            let _ = events_tx.send(event);
                            reset_turn_state(
                                &mut trajectory,
                                &mut held_completion,
                                &mut discrete_review_started,
                            );
                            idle_epoch = None;
                            interjected_epoch = Some(active.epoch);
                        }
                        _ => {
                            let _ = events_tx.send(event);
                        }
                    }
                }
                advice_posted = async {
                    match advice_watch.as_mut() {
                        Some(watch) => watch.changed().await.ok(),
                        None => std::future::pending().await,
                    }
                } => {
                    if advice_posted.is_none() {
                        advice_watch = None;
                        continue;
                    }
                    let active = turn.lock().await.clone();
                    if idle_epoch != Some(active.epoch) || interjected_epoch == Some(active.epoch) {
                        continue;
                    }
                    let Some(reviewer) = config.reviewer.as_ref() else { continue; };
                    let outcome = reviewer.pull(loki::Consumer::Thor).await;
                    if outcome.is_empty() {
                        continue;
                    }
                    let advice = loki::format_pull_outcome(
                        &outcome,
                        active.epoch,
                        loki::Consumer::Thor,
                    );
                    log_advice(config.log_context.as_ref(), &advice, "interjection");
                    idle_epoch = None;
                    interjected_epoch = Some(active.epoch);
                    let _ = events_tx.send(UiEvent::Info(
                        "Loki · sharing post-turn review feedback".to_string(),
                    ));
                    emit_internal(
                        &events_tx,
                        "Loki",
                        "Thor",
                        InternalMessageKind::Interjection,
                        &advice,
                    );
                    let _ = config.runtime_commands.send(UiCommand::SendPrompt {
                        text: loki_interjection_prompt(&advice),
                        images: Vec::new(),
                    });
                }
                changed = active_worker_updates.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }

            if held_completion.is_none() {
                continue;
            }
            if *active_worker_updates.borrow() > 0 {
                continue;
            }
            let active = turn.lock().await.clone();
            let pulled = pull_advice(config.reviewer.as_ref(), active.epoch).await;
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
                let prompt = thor_discrete_review_prompt(
                    &active.task,
                    &initial_result,
                    &context,
                    pulled.as_ref().map(|(_, receipt)| receipt.as_str()),
                );
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
            if let Some((outcome, advice)) = pulled
                && !outcome.is_empty()
            {
                log_advice(config.log_context.as_ref(), &advice, "turn_boundary");
                held_completion = None;
                trajectory.reset_attempt();
                emit_internal(
                    &events_tx,
                    "Loki",
                    "Thor",
                    InternalMessageKind::Continuation,
                    &advice,
                );
                let _ = config.runtime_commands.send(UiCommand::SendPrompt {
                    text: loki_advice_prompt(&advice),
                    images: Vec::new(),
                });
                continue;
            }
            let event = held_completion.take().expect("completion held");
            let _ = events_tx.send(event);
            reset_turn_state(
                &mut trajectory,
                &mut held_completion,
                &mut discrete_review_started,
            );
            idle_epoch = Some(active.epoch);
        }
    });
    Running {
        handle,
        events,
        task,
    }
}

async fn pull_advice(
    reviewer: Option<&loki::Handle>,
    epoch: u64,
) -> Option<(loki::PullOutcome, String)> {
    let reviewer = reviewer?;
    let outcome = reviewer.pull(loki::Consumer::Thor).await;
    let receipt = loki::format_pull_outcome(&outcome, epoch, loki::Consumer::Thor);
    Some((outcome, receipt))
}

fn log_advice(context: Option<&LogContext>, advice: &str, delivery: &str) {
    if let Some(context) = context {
        tracing::info!(
            event = "advice_received",
            council_session = %context.council_session,
            god = "Thor",
            source = "Loki",
            model = %context.model,
            adapter = %context.adapter,
            delivery,
            advice,
            "Thor received Loki advice"
        );
    }
}

fn reset_turn_state(
    trajectory: &mut loki::BoundaryTracker,
    held_completion: &mut Option<UiEvent>,
    discrete_review_started: &mut bool,
) {
    *trajectory = loki::BoundaryTracker::default();
    *held_completion = None;
    *discrete_review_started = false;
}

fn loki_advice_prompt(advice: &str) -> String {
    format!(
        "<advisory source=\"Loki\" timing=\"asynchronous; may be superseded by later work\">\n{advice}\n</advisory>\n\nConsider this review feedback against the work already completed. Verify whether it still applies, address any material issue that remains, and then return the final user-facing answer."
    )
}

fn loki_interjection_prompt(advice: &str) -> String {
    format!(
        "<advisory source=\"Loki\" timing=\"post-turn; may be superseded by later work\">\n{advice}\n</advisory>\n\nLoki finished reviewing after your previous answer was already delivered. Re-open that completed work only as needed to verify whether this feedback still applies. If a material issue remains, address it and explain the correction; otherwise briefly say the completed work already covers it."
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

fn thor_discrete_review_prompt(
    task: &str,
    initial_result: &str,
    context: &str,
    loki_advice: Option<&str>,
) -> String {
    let advice = loki_advice
        .map(|advice| {
            format!("\n\nAsynchronous Loki advice (may be superseded by later work):\n{advice}")
        })
        .unwrap_or_default();
    format!(
        "Perform Thor's discrete review for this same user turn. You own the research, planning, coordination, review, verification, and final response; do not act as a thin relay for Eitri. Re-read the original task, critically review the initial result and implementation evidence, investigate or verify anything necessary, and correct material issues. If code changes are still needed, delegate them to Eitri with code_agent and then review the new result. Return the final user-facing answer when the work is genuinely complete.\n\nOriginal task:\n{task}\n\nInitial result:\n{initial_result}\n\nBounded trajectory and workspace context:\n{context}{advice}"
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
    fn asynchronous_advice_prompts_warn_that_feedback_may_be_superseded() {
        let advice = "turn 3, Thor step 2: verify the fallback";
        assert!(loki_advice_prompt(advice).contains("may be superseded"));
        assert!(loki_interjection_prompt(advice).contains("previous answer"));
    }

    #[tokio::test]
    async fn prompt_completion_waits_for_code_worker_reap() {
        let (runtime_tx, runtime_rx) = mpsc::unbounded_channel();
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        let workers = ActiveCodeWorkers::default();
        workers.set(1);
        let mut running = spawn(
            runtime_rx,
            Config {
                reviewer: None,
                runtime_commands: command_tx,
                implementation_handoffs: Arc::new(AtomicUsize::new(1)),
                active_implementation_workers: workers.clone(),
                discrete_review: false,
                log_context: None,
            },
        );

        runtime_tx
            .send(UiEvent::PromptDone {
                stop_reason: StopReason::EndTurn,
                usage: None,
            })
            .expect("send completion");
        assert!(matches!(
            running.events.recv().await,
            Some(UiEvent::CouncilUsage(_))
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), running.events.recv())
                .await
                .is_err(),
            "completion escaped while Eitri could still mutate"
        );

        workers.set(0);
        let completion =
            tokio::time::timeout(std::time::Duration::from_secs(1), running.events.recv())
                .await
                .expect("completion after reap")
                .expect("orchestrated event");
        assert!(matches!(completion, UiEvent::PromptDone { .. }));

        drop(runtime_tx);
        running.task.await.expect("orchestrator task");
    }
}
