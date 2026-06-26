//! Thor coordinator defaults and routing rules.
//!
//! The first implementation keeps Thor's routing policy explicit and testable
//! while the full coordinator runtime is built out. `mj` still launches one ACP
//! backend today, but the same defaults feed the future multi-worker catalog.

use std::collections::HashMap;
use std::path::PathBuf;

use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, PermissionOption, PermissionOptionKind, Plan, PlanEntry,
    PlanEntryPriority, PlanEntryStatus, SessionUpdate, StopReason, TextContent, ToolCallStatus,
    ToolCallUpdate, ToolCallUpdateFields, ToolKind,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::acp::{self, AcpRuntimeConfig};
use crate::config::SelectedAgent;
use crate::event::{PermissionDecision, PermissionPrompt, UiCommand, UiEvent};

pub const DEFAULT_COORDINATOR_MODEL: &str = "auto-strong";
pub const LM_ARENA_LEADERBOARD_URL: &str =
    "https://huggingface.co/spaces/lmarena-ai/arena-leaderboard";
pub const OPENROUTER_MODELS_URL: &str = "https://openrouter.ai/api/v1/models";
pub const PLAN_APPROVAL_TOOL_CALL_ID: &str = "thor-plan-approval";

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ThorPlanApproval {
    #[default]
    Always,
    AskToSkip,
    Never,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ThorOptimizationMode {
    /// Balanced default: pick capable models, avoid waste, and review risky work.
    #[default]
    Balanced,
    /// Accountant persona: minimize spend when the task is simple enough.
    Cost,
    /// Architect persona: optimize for solution quality, including alternate
    /// implementations on complex tasks when multiple workers are available.
    BestSolution,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ThorConfig {
    #[serde(default = "default_coordinator_model")]
    pub coordinator_model: String,
    #[serde(default = "default_leaderboard_url")]
    pub leaderboard_url: String,
    #[serde(default = "default_pricing_url")]
    pub pricing_url: String,
    #[serde(default)]
    pub plan_approval: ThorPlanApproval,
    #[serde(default)]
    pub optimization_mode: ThorOptimizationMode,
}

impl Default for ThorConfig {
    fn default() -> Self {
        Self {
            coordinator_model: default_coordinator_model(),
            leaderboard_url: default_leaderboard_url(),
            pricing_url: default_pricing_url(),
            plan_approval: ThorPlanApproval::Always,
            optimization_mode: ThorOptimizationMode::Balanced,
        }
    }
}

impl ThorConfig {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

fn default_coordinator_model() -> String {
    DEFAULT_COORDINATOR_MODEL.to_string()
}

fn default_leaderboard_url() -> String {
    LM_ARENA_LEADERBOARD_URL.to_string()
}

fn default_pricing_url() -> String {
    OPENROUTER_MODELS_URL.to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum TaskComplexity {
    Simple,
    Hard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum HarnessKind {
    ClaudeCode,
    Codex,
    Anvil,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct ModelScore {
    pub model: String,
    pub arena_score: f64,
    pub input_price_per_million: Option<f64>,
    pub output_price_per_million: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct HarnessCandidate {
    pub source_id: String,
    pub kind: HarnessKind,
    pub remaining_quota_known: bool,
    pub remaining_quota_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct RouteChoice {
    pub model: String,
    pub harness_source_id: String,
    pub harness_kind: HarnessKind,
}

pub fn default_anvil_agent() -> SelectedAgent {
    SelectedAgent {
        source_id: "anvil".to_string(),
        program: PathBuf::from("uvx"),
        args: vec!["brokk".to_string(), "acp".to_string()],
        env: HashMap::new(),
    }
}

pub struct ThorRuntimeConfig {
    pub thor: ThorConfig,
    pub worker: AcpRuntimeConfig,
    pub worker_label: String,
}

/// Run Thor as the only user-facing runtime. Thor owns submitted prompts,
/// presents a plan for approval, then delegates the approved task to the
/// configured ACP worker backend.
pub async fn run(
    cfg: ThorRuntimeConfig,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    mut ui_rx: mpsc::UnboundedReceiver<UiCommand>,
) -> anyhow::Result<()> {
    let (worker_event_tx, mut worker_event_rx) = mpsc::unbounded_channel();
    let (worker_cmd_tx, worker_cmd_rx) = mpsc::unbounded_channel();
    let worker_label = cfg.worker_label.clone();
    let worker_handle = tokio::spawn(async move {
        if let Err(e) = acp::run(cfg.worker, worker_event_tx, worker_cmd_rx).await {
            tracing::error!("thor worker runtime error: {e:#}");
        }
    });

    let mut active_route: Option<ThorRoutePlan> = None;
    let mut worker_closed = false;
    loop {
        tokio::select! {
            maybe_event = worker_event_rx.recv(), if !worker_closed => {
                match maybe_event {
                    Some(event) => forward_worker_event(event, &ui_tx, &mut active_route),
                    None => worker_closed = true,
                }
            }
            maybe_cmd = ui_rx.recv() => {
                let Some(cmd) = maybe_cmd else {
                    break;
                };
                match cmd {
                    UiCommand::SendPrompt { text, images } => {
                        let route = ThorRoutePlan::new(&cfg.thor, &worker_label, &text);
                        emit_plan(
                            &ui_tx,
                            &route,
                            PlanEntryStatus::Pending,
                            PlanEntryStatus::Pending,
                            PlanEntryStatus::Pending,
                            PlanEntryStatus::Pending,
                        );
                        if !approve_plan(&ui_tx, &route, cfg.thor.plan_approval).await {
                            emit_plan(
                                &ui_tx,
                                &route,
                                PlanEntryStatus::Pending,
                                PlanEntryStatus::Pending,
                                PlanEntryStatus::Pending,
                                PlanEntryStatus::Pending,
                            );
                            let _ = ui_tx.send(UiEvent::Info("Thor plan rejected; worker task was not started".to_string()));
                            let _ = ui_tx.send(UiEvent::PromptDone {
                                stop_reason: StopReason::Cancelled,
                                usage: None,
                            });
                            continue;
                        }
                        emit_plan(
                            &ui_tx,
                            &route,
                            PlanEntryStatus::InProgress,
                            PlanEntryStatus::Pending,
                            PlanEntryStatus::Pending,
                            PlanEntryStatus::Pending,
                        );
                        active_route = Some(route.clone());
                        if worker_cmd_tx.send(UiCommand::SendPrompt {
                            text: route.worker_prompt(),
                            images,
                        }).is_err() {
                            let _ = ui_tx.send(UiEvent::PromptFailed {
                                message: "Thor worker backend is unavailable".to_string(),
                            });
                        }
                    }
                    UiCommand::Shutdown => {
                        let _ = worker_cmd_tx.send(UiCommand::Shutdown);
                        break;
                    }
                    other => {
                        if worker_cmd_tx.send(other).is_err() {
                            let _ = ui_tx.send(UiEvent::Warning(
                                "Thor worker backend is unavailable".to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }

    let abort_handle = worker_handle.abort_handle();
    match tokio::time::timeout(std::time::Duration::from_secs(2), worker_handle).await {
        Ok(join_res) => {
            if let Err(e) = join_res {
                tracing::warn!("thor worker task join: {e}");
            }
        }
        Err(_) => {
            tracing::warn!("thor worker did not exit within 2s; aborting");
            abort_handle.abort();
        }
    }

    Ok(())
}

fn forward_worker_event(
    event: UiEvent,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    active_route: &mut Option<ThorRoutePlan>,
) {
    match event {
        UiEvent::Connected {
            prompt_images_supported,
            session_fork_supported,
            ..
        } => {
            let _ = ui_tx.send(UiEvent::Connected {
                agent_name: Some("Thor".to_string()),
                agent_version: None,
                prompt_images_supported,
                session_fork_supported,
            });
        }
        UiEvent::PromptDone { stop_reason, usage } => {
            if let Some(route) = active_route.take() {
                emit_plan(
                    ui_tx,
                    &route,
                    PlanEntryStatus::Completed,
                    PlanEntryStatus::Completed,
                    PlanEntryStatus::Completed,
                    PlanEntryStatus::Completed,
                );
            }
            let recap = match &usage {
                Some(usage) => format!(
                    "\n\nThor recap: worker turn completed with {stop_reason:?}. Usage: {} input / {} output tokens.",
                    usage.input_tokens, usage.output_tokens
                ),
                None => format!("\n\nThor recap: worker turn completed with {stop_reason:?}."),
            };
            let _ = ui_tx.send(UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(
                ContentChunk::new(ContentBlock::Text(TextContent::new(recap))),
            )));
            let _ = ui_tx.send(UiEvent::PromptDone { stop_reason, usage });
        }
        other => {
            let _ = ui_tx.send(other);
        }
    }
}

async fn approve_plan(
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    route: &ThorRoutePlan,
    approval: ThorPlanApproval,
) -> bool {
    if approval == ThorPlanApproval::Never {
        return true;
    }

    let (tx, rx) = oneshot::channel::<PermissionDecision>();
    let mut fields = ToolCallUpdateFields::new();
    fields.kind = Some(ToolKind::Think);
    fields.status = Some(ToolCallStatus::Pending);
    fields.title = Some("Thor execution plan".to_string());
    fields.raw_input = Some(serde_json::json!({
        "worker": route.worker_label,
        "model": route.model,
        "optimization_mode": optimization_mode_label(route.optimization_mode),
        "persona": route.persona,
        "summary": route.summary,
        "implementation_strategy": route.implementation_strategy,
        "review_strategy": route.review_strategy,
        "correction_strategy": route.correction_strategy,
        "subscription_strategy": route.subscription_strategy,
        "prompt": route.user_prompt,
    }));

    let prompt = PermissionPrompt {
        tool_call: ToolCallUpdate::new(PLAN_APPROVAL_TOOL_CALL_ID, fields),
        options: vec![
            PermissionOption::new(
                "approve",
                "Approve Thor plan",
                PermissionOptionKind::AllowOnce,
            ),
            PermissionOption::new("reject", "Reject", PermissionOptionKind::RejectOnce),
        ],
        responder: tx,
    };

    if ui_tx.send(UiEvent::PermissionRequest(prompt)).is_err() {
        return false;
    }

    matches!(
        rx.await,
        Ok(PermissionDecision::Selected(option_id)) if option_id == "approve"
    )
}

fn emit_plan(
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    route: &ThorRoutePlan,
    worker_status: PlanEntryStatus,
    review_status: PlanEntryStatus,
    correction_status: PlanEntryStatus,
    recap_status: PlanEntryStatus,
) {
    let entries = vec![
        PlanEntry::new(
            "Thor will route this request through the configured worker backend",
            PlanEntryPriority::High,
            PlanEntryStatus::Completed,
        ),
        PlanEntry::new(
            route.summary.clone(),
            PlanEntryPriority::High,
            worker_status,
        ),
        PlanEntry::new(
            format!(
                "Run mandatory adversarial review: {}",
                route.review_strategy
            ),
            PlanEntryPriority::High,
            review_status,
        ),
        PlanEntry::new(
            format!("Apply correction pass: {}", route.correction_strategy),
            PlanEntryPriority::High,
            correction_status,
        ),
        PlanEntry::new(
            "Thor will recap validation, risks, and worker usage after completion",
            PlanEntryPriority::Medium,
            recap_status,
        ),
    ];
    let _ = ui_tx.send(UiEvent::SessionUpdate(SessionUpdate::Plan(Plan::new(
        entries,
    ))));
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThorRoutePlan {
    worker_label: String,
    model: String,
    optimization_mode: ThorOptimizationMode,
    persona: &'static str,
    user_prompt: String,
    summary: String,
    implementation_strategy: String,
    review_strategy: String,
    correction_strategy: String,
    subscription_strategy: &'static str,
}

impl ThorRoutePlan {
    fn new(cfg: &ThorConfig, worker_label: &str, user_prompt: &str) -> Self {
        let complexity = infer_complexity(user_prompt);
        let model = placeholder_model_choice(cfg, complexity);
        let persona = persona_label(cfg.optimization_mode);
        let implementation_strategy = implementation_strategy(cfg.optimization_mode, complexity);
        let review_strategy = review_strategy(cfg.optimization_mode, complexity);
        let correction_strategy = correction_strategy(cfg.optimization_mode);
        let summary = format!(
            "Run the task with {worker_label} as {persona}, using {model} routing for a {} task",
            complexity_label(complexity),
        );
        Self {
            worker_label: worker_label.to_string(),
            model,
            optimization_mode: cfg.optimization_mode,
            persona,
            user_prompt: user_prompt.to_string(),
            summary,
            implementation_strategy,
            review_strategy,
            correction_strategy,
            subscription_strategy: subscription_strategy(),
        }
    }

    fn worker_prompt(&self) -> String {
        format!(
            "You are executing a task assigned by Thor, the mjolnir omni-agent coordinator.\n\n\
             Thor routing decision:\n\
             - Worker backend: {}\n\
             - Model choice: {}\n\n\
             Optimization:\n\
             - Mode: {}\n\
             - Persona: {}\n\
             - Implementation strategy: {}\n\
             - Review strategy: {}\n\
             - Correction strategy: {}\n\
             - Subscription strategy: {}\n\n\
             Required review and correction cycle:\n\
             1. Complete the implementation.\n\
             2. Review the result adversarially, looking for correctness, security, edge-case, regression, and test gaps.\n\
             3. Apply corrections from that review before finalizing.\n\
             4. Rerun relevant validation after corrections.\n\n\
             Follow the user's request exactly, keep changes scoped, run relevant validation, \
             and finish with a concise summary of changes, tests, risks, and usage if available.\n\n\
             User request:\n{}",
            self.worker_label,
            self.model,
            optimization_mode_label(self.optimization_mode),
            self.persona,
            self.implementation_strategy,
            self.review_strategy,
            self.correction_strategy,
            self.subscription_strategy,
            self.user_prompt
        )
    }
}

fn placeholder_model_choice(cfg: &ThorConfig, complexity: TaskComplexity) -> String {
    match (cfg.optimization_mode, complexity) {
        (ThorOptimizationMode::Cost, TaskComplexity::Simple) => {
            "cheapest sufficiently capable model".to_string()
        }
        (ThorOptimizationMode::BestSolution, TaskComplexity::Hard) => cfg.coordinator_model.clone(),
        (_, TaskComplexity::Simple) => "simple capable model".to_string(),
        (_, TaskComplexity::Hard) => cfg.coordinator_model.clone(),
    }
}

fn persona_label(mode: ThorOptimizationMode) -> &'static str {
    match mode {
        ThorOptimizationMode::Balanced => "Thor coordinator",
        ThorOptimizationMode::Cost => "accountant",
        ThorOptimizationMode::BestSolution => "architect",
    }
}

fn optimization_mode_label(mode: ThorOptimizationMode) -> &'static str {
    match mode {
        ThorOptimizationMode::Balanced => "balanced",
        ThorOptimizationMode::Cost => "cost",
        ThorOptimizationMode::BestSolution => "best-solution",
    }
}

fn implementation_strategy(mode: ThorOptimizationMode, complexity: TaskComplexity) -> String {
    match (mode, complexity) {
        (ThorOptimizationMode::Cost, TaskComplexity::Simple) => {
            "use one low-cost implementation pass unless risk increases".to_string()
        }
        (ThorOptimizationMode::Cost, TaskComplexity::Hard) => {
            "start with the strongest necessary worker, then constrain extra passes".to_string()
        }
        (ThorOptimizationMode::BestSolution, TaskComplexity::Hard) => {
            "run two independent implementation passes with different model families when available, then have Thor compare the results and choose the best solution".to_string()
        }
        (ThorOptimizationMode::BestSolution, TaskComplexity::Simple) => {
            "use one high-confidence implementation pass; avoid unnecessary duplication".to_string()
        }
        (ThorOptimizationMode::Balanced, TaskComplexity::Simple) => {
            "use one capable implementation pass".to_string()
        }
        (ThorOptimizationMode::Balanced, TaskComplexity::Hard) => {
            "use the strongest configured worker and reserve capacity for review".to_string()
        }
    }
}

fn review_strategy(mode: ThorOptimizationMode, complexity: TaskComplexity) -> String {
    match (mode, complexity) {
        (ThorOptimizationMode::Cost, TaskComplexity::Simple) => {
            "run a focused adversarial review sized to the low-cost implementation".to_string()
        }
        (ThorOptimizationMode::Cost, TaskComplexity::Hard) => {
            "run adversarial review while constraining extra spend where possible".to_string()
        }
        (ThorOptimizationMode::BestSolution, TaskComplexity::Hard) => {
            "review the selected result adversarially with a different vendor model when available"
                .to_string()
        }
        (_, TaskComplexity::Hard) => {
            "prefer adversarial review with a different vendor model".to_string()
        }
        _ => "run adversarial review before finalizing".to_string(),
    }
}

fn correction_strategy(mode: ThorOptimizationMode) -> String {
    match mode {
        ThorOptimizationMode::Cost => {
            "fix review findings that affect correctness, safety, or requested behavior before stopping".to_string()
        }
        ThorOptimizationMode::BestSolution => {
            "fold review findings into the selected solution and reject weaker alternate results".to_string()
        }
        ThorOptimizationMode::Balanced => {
            "apply review findings, then rerun relevant validation before final response".to_string()
        }
    }
}

fn subscription_strategy() -> &'static str {
    "use Claude Code and Codex subscription quota evenly and maximally before falling back to metered OpenRouter routing"
}

fn complexity_label(complexity: TaskComplexity) -> &'static str {
    match complexity {
        TaskComplexity::Simple => "simple",
        TaskComplexity::Hard => "hard",
    }
}

fn infer_complexity(prompt: &str) -> TaskComplexity {
    let lower = prompt.to_ascii_lowercase();
    if lower.len() < 160
        && ["format", "typo", "rename", "summarize", "explain"]
            .iter()
            .any(|word| lower.contains(word))
    {
        TaskComplexity::Simple
    } else {
        TaskComplexity::Hard
    }
}

#[allow(dead_code)]
pub fn classify_model_family(model: &str) -> HarnessKind {
    let lower = model.to_ascii_lowercase();
    if lower.contains("claude") || lower.contains("anthropic") {
        HarnessKind::ClaudeCode
    } else if lower.contains("gpt") || lower.contains("openai") || lower.contains("o3") {
        HarnessKind::Codex
    } else {
        HarnessKind::Anvil
    }
}

#[allow(dead_code)]
pub fn choose_model(
    models: &[ModelScore],
    complexity: TaskComplexity,
    optimization_mode: ThorOptimizationMode,
) -> Option<&ModelScore> {
    match (optimization_mode, complexity) {
        (ThorOptimizationMode::Cost, TaskComplexity::Simple) => models
            .iter()
            .min_by(|a, b| model_price(a).total_cmp(&model_price(b))),
        (ThorOptimizationMode::Cost, TaskComplexity::Hard)
        | (ThorOptimizationMode::Balanced, TaskComplexity::Simple) => models
            .iter()
            .min_by(|a, b| model_price(a).total_cmp(&model_price(b)))
            .or_else(|| {
                models
                    .iter()
                    .max_by(|a, b| a.arena_score.total_cmp(&b.arena_score))
            }),
        (ThorOptimizationMode::BestSolution, _)
        | (ThorOptimizationMode::Balanced, TaskComplexity::Hard) => models
            .iter()
            .max_by(|a, b| a.arena_score.total_cmp(&b.arena_score)),
    }
}

#[allow(dead_code)]
pub fn choose_route(
    models: &[ModelScore],
    harnesses: &[HarnessCandidate],
    complexity: TaskComplexity,
    optimization_mode: ThorOptimizationMode,
) -> Option<RouteChoice> {
    let model = choose_model(models, complexity, optimization_mode)?;
    let preferred = classify_model_family(&model.model);
    let harness = harnesses
        .iter()
        .filter(|h| h.kind == preferred)
        .find(|h| h.remaining_quota_available)
        .or_else(|| {
            harnesses
                .iter()
                .filter(|h| h.kind == HarnessKind::Anvil)
                .find(|h| h.remaining_quota_available)
        })
        .or_else(|| harnesses.iter().find(|h| h.remaining_quota_available))?;
    Some(RouteChoice {
        model: model.model.clone(),
        harness_source_id: harness.source_id.clone(),
        harness_kind: harness.kind,
    })
}

#[allow(dead_code)]
fn model_price(model: &ModelScore) -> f64 {
    match (
        model.input_price_per_million,
        model.output_price_per_million,
    ) {
        (Some(input), Some(output)) => input + output,
        (Some(input), None) => input,
        (None, Some(output)) => output,
        (None, None) => f64::INFINITY,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn harness(source_id: &str, kind: HarnessKind, available: bool) -> HarnessCandidate {
        HarnessCandidate {
            source_id: source_id.to_string(),
            kind,
            remaining_quota_known: true,
            remaining_quota_available: available,
        }
    }

    #[test]
    fn default_anvil_agent_uses_uvx_brokk_acp() {
        let agent = default_anvil_agent();
        assert_eq!(agent.source_id, "anvil");
        assert_eq!(agent.program, PathBuf::from("uvx"));
        assert_eq!(agent.args, vec!["brokk", "acp"]);
    }

    #[test]
    fn balanced_hard_tasks_choose_strongest_model() {
        let models = vec![
            ModelScore {
                model: "cheap".to_string(),
                arena_score: 900.0,
                input_price_per_million: Some(0.1),
                output_price_per_million: Some(0.1),
            },
            ModelScore {
                model: "strong".to_string(),
                arena_score: 1300.0,
                input_price_per_million: Some(10.0),
                output_price_per_million: Some(30.0),
            },
        ];

        assert_eq!(
            choose_model(
                &models,
                TaskComplexity::Hard,
                ThorOptimizationMode::Balanced
            )
            .expect("model")
            .model,
            "strong"
        );
    }

    #[test]
    fn architect_mode_uses_strong_models_for_solution_candidates() {
        let models = vec![
            ModelScore {
                model: "cheap".to_string(),
                arena_score: 900.0,
                input_price_per_million: Some(0.1),
                output_price_per_million: Some(0.1),
            },
            ModelScore {
                model: "best-fit".to_string(),
                arena_score: 1400.0,
                input_price_per_million: Some(20.0),
                output_price_per_million: Some(40.0),
            },
        ];

        assert_eq!(
            choose_model(
                &models,
                TaskComplexity::Simple,
                ThorOptimizationMode::BestSolution
            )
            .expect("model")
            .model,
            "best-fit"
        );
    }

    #[test]
    fn architect_plan_runs_two_versions_and_has_thor_select_winner() {
        let cfg = ThorConfig {
            optimization_mode: ThorOptimizationMode::BestSolution,
            ..ThorConfig::default()
        };
        let route = ThorRoutePlan::new(
            &cfg,
            "anvil",
            "redesign the runtime routing for a complex multi-agent workflow",
        );
        let prompt = route.worker_prompt();

        assert!(prompt.contains("Persona: architect"));
        assert!(prompt.contains("two independent implementation passes"));
        assert!(prompt.contains("Thor compare the results and choose the best solution"));
        assert!(prompt.contains("different vendor model"));
        assert!(prompt.contains("Required review and correction cycle"));
        assert!(prompt.contains("Apply corrections from that review"));
    }

    #[test]
    fn accountant_mode_simple_tasks_choose_cheapest_priced_model() {
        let models = vec![
            ModelScore {
                model: "strong".to_string(),
                arena_score: 1300.0,
                input_price_per_million: Some(10.0),
                output_price_per_million: Some(30.0),
            },
            ModelScore {
                model: "small".to_string(),
                arena_score: 900.0,
                input_price_per_million: Some(0.1),
                output_price_per_million: Some(0.1),
            },
        ];

        assert_eq!(
            choose_model(&models, TaskComplexity::Simple, ThorOptimizationMode::Cost)
                .expect("model")
                .model,
            "small"
        );
    }

    #[test]
    fn claude_models_prefer_claude_code_when_quota_available() {
        let models = vec![ModelScore {
            model: "anthropic/claude-example".to_string(),
            arena_score: 1200.0,
            input_price_per_million: Some(3.0),
            output_price_per_million: Some(15.0),
        }];
        let harnesses = vec![
            harness("anvil", HarnessKind::Anvil, true),
            harness("claude-code", HarnessKind::ClaudeCode, true),
        ];

        let route = choose_route(
            &models,
            &harnesses,
            TaskComplexity::Hard,
            ThorOptimizationMode::Balanced,
        )
        .expect("route");
        assert_eq!(route.harness_kind, HarnessKind::ClaudeCode);
        assert_eq!(route.harness_source_id, "claude-code");
    }

    #[test]
    fn gpt_models_prefer_codex_and_fall_back_to_anvil_without_quota() {
        let models = vec![ModelScore {
            model: "openai/gpt-example".to_string(),
            arena_score: 1200.0,
            input_price_per_million: Some(2.0),
            output_price_per_million: Some(8.0),
        }];
        let harnesses = vec![
            harness("codex", HarnessKind::Codex, false),
            harness("anvil", HarnessKind::Anvil, true),
        ];

        let route = choose_route(
            &models,
            &harnesses,
            TaskComplexity::Hard,
            ThorOptimizationMode::Balanced,
        )
        .expect("route");
        assert_eq!(route.harness_kind, HarnessKind::Anvil);
        assert_eq!(route.harness_source_id, "anvil");
    }

    #[test]
    fn route_plan_wraps_user_prompt_with_thor_instructions() {
        let route = ThorRoutePlan::new(&ThorConfig::default(), "anvil", "fix the parser");
        let prompt = route.worker_prompt();

        assert!(prompt.contains("Thor, the mjolnir omni-agent coordinator"));
        assert!(prompt.contains("Worker backend: anvil"));
        assert!(prompt.contains("User request:\nfix the parser"));
    }

    #[test]
    fn short_mechanical_prompts_are_simple_and_large_prompts_are_hard() {
        assert_eq!(
            infer_complexity("fix typo in README"),
            TaskComplexity::Simple
        );
        assert_eq!(
            infer_complexity(
                "redesign the runtime so prompts are coordinated through Thor and worker ACP sessions"
            ),
            TaskComplexity::Hard
        );
    }

    #[test]
    fn emit_plan_surfaces_implementation_review_correction_and_recap_statuses() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let route = ThorRoutePlan::new(&ThorConfig::default(), "anvil", "fix it");

        emit_plan(
            &tx,
            &route,
            PlanEntryStatus::InProgress,
            PlanEntryStatus::Pending,
            PlanEntryStatus::Pending,
            PlanEntryStatus::Pending,
        );

        let event = rx.try_recv().expect("plan event");
        let UiEvent::SessionUpdate(SessionUpdate::Plan(plan)) = event else {
            panic!("expected plan event");
        };
        assert_eq!(plan.entries.len(), 5);
        assert_eq!(plan.entries[1].status, PlanEntryStatus::InProgress);
        assert_eq!(plan.entries[2].status, PlanEntryStatus::Pending);
        assert_eq!(plan.entries[3].status, PlanEntryStatus::Pending);
        assert_eq!(plan.entries[4].status, PlanEntryStatus::Pending);
        assert!(plan.entries[1].content.contains("Run the task with anvil"));
        assert!(plan.entries[2].content.contains("adversarial review"));
        assert!(plan.entries[3].content.contains("correction pass"));
    }
}
