//! Thor host-agent configuration and routing policy.
//!
//! Thor is not an in-process subagent. `mj` launches a selected ACP agent as
//! the Thor host and injects a local MCP bridge into that ACP session. The host
//! model gets the user's prompt plus these instructions, then uses MCP tools to
//! list and run other configured ACP agents.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::config::{CUSTOM_AGENT_SOURCE_PREFIX, Config, SelectedAgent};

pub const DEFAULT_COORDINATOR_MODEL: &str = "auto-strong";
pub const LM_ARENA_LEADERBOARD_URL: &str =
    "https://huggingface.co/spaces/lmarena-ai/arena-leaderboard";
pub const OPENROUTER_MODELS_URL: &str = "https://openrouter.ai/api/v1/models";
pub const THOR_MCP_SERVER_NAME: &str = "thor-acp-bridge";

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
    /// Architect persona: optimize for solution quality by comparing alternate
    /// implementations when the task is complex enough.
    BestSolution,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ThorReasoning {
    Low,
    Medium,
    #[default]
    High,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ThorConfig {
    #[serde(default)]
    pub onboarding_complete: bool,
    #[serde(default)]
    pub enabled_worker_source_ids: Vec<String>,
    #[serde(default = "default_coordinator_model")]
    pub coordinator_model: String,
    #[serde(default)]
    pub coordinator_reasoning: ThorReasoning,
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
            onboarding_complete: false,
            enabled_worker_source_ids: Vec::new(),
            coordinator_model: default_coordinator_model(),
            coordinator_reasoning: ThorReasoning::High,
            leaderboard_url: default_leaderboard_url(),
            pricing_url: default_pricing_url(),
            plan_approval: ThorPlanApproval::Always,
            optimization_mode: ThorOptimizationMode::Balanced,
        }
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

pub fn available_worker_catalog(config: &Config) -> Vec<SelectedAgent> {
    let mut agents = Vec::new();
    if let Some(agent) = config.agent.clone() {
        agents.push(agent);
    }
    for custom in &config.custom_agents {
        let source_id = format!("{CUSTOM_AGENT_SOURCE_PREFIX}{}", custom.name);
        if agents.iter().any(|agent| agent.source_id == source_id) {
            continue;
        }
        agents.push(SelectedAgent {
            source_id,
            program: custom.program.clone(),
            args: custom.args.clone(),
            env: HashMap::new(),
        });
    }
    if !agents.iter().any(|agent| agent.source_id == "anvil") {
        agents.push(default_anvil_agent());
    }
    agents
}

pub fn worker_catalog(config: &Config) -> Vec<SelectedAgent> {
    let agents = available_worker_catalog(config);
    if config.thor.enabled_worker_source_ids.is_empty() {
        return agents;
    }

    let filtered = agents
        .iter()
        .filter(|agent| {
            config
                .thor
                .enabled_worker_source_ids
                .iter()
                .any(|source_id| source_id == &agent.source_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        agents
    } else {
        filtered
    }
}

pub fn host_prompt(thor: &ThorConfig, user_prompt: &str) -> String {
    format!(
        "\
You are Thor, the mjolnir omni-agent coordinator.

You are running inside an ACP host agent. You are not a local in-process
subagent. `mj` has provided an MCP server named `{server_name}` with tools for
listing configured ACP workers, reading model/pricing metadata, and delegating
prompts to them.

Operating mode:
- optimization: {optimization}
- coordinator model preference: {model}
- coordinator reasoning: {reasoning}
- model strength source: {leaderboard}
- pricing source: {pricing}

Policy:
- Keep the UX aggressively simple: no model picker or agent picker unless the
  user explicitly asks.
- Start routing decisions by calling `thor_get_model_catalog`; refresh it when
  cached pricing/strength data is stale or missing.
- Use `thor_validate_acp_agents` before relying on a worker set that has not
  been validated in this session.
- Before assigning work, call `thor_refresh_quota` or
  `thor_list_acp_agents` with `refreshQuota: true` so mj can query configured
  Claude SDK and Codex appserver quota probes. Treat returned quota data and
  worker-run usage metadata as the source of truth for subscription capacity.
  Prefer known available Claude Code/Codex quota before metered OpenRouter
  routes; avoid exhausted workers.
- Use `thor_run_acp_agents` when work should happen in parallel, including
  architect-mode alternate implementations and adversarial reviews.
- Present a concise plan before doing work unless the user has configured plan
  approval to skip it.
- For cost/accountant mode, use cheaper models when the task is sufficiently
  simple.
- For best-solution/architect mode, run two independent versions on complex
  tasks with different vendor models when viable, then choose the best result.
- Prefer Claude Code for Claude models and Codex for GPT models when their
  subscription quota is available; otherwise prefer Anvil/OpenRouter pricing.
- Always bake in adversarial review and correction: implementation, review by a
  different vendor model when possible, correction pass, then final recap.
- Recap what changed and report token/model usage returned by worker tools.
- Use the structured worker progress/tool-call/usage fields returned by the MCP
  tools instead of pasting raw worker transcripts back to the user.

User request:
{user_prompt}",
        server_name = THOR_MCP_SERVER_NAME,
        optimization = optimization_label(thor.optimization_mode),
        model = thor.coordinator_model,
        reasoning = reasoning_label(thor.coordinator_reasoning),
        leaderboard = thor.leaderboard_url,
        pricing = thor.pricing_url,
    )
}

#[allow(dead_code)]
pub fn choose_model(
    models: &[ModelScore],
    complexity: TaskComplexity,
    mode: ThorOptimizationMode,
) -> Option<ModelScore> {
    match mode {
        ThorOptimizationMode::Cost if complexity == TaskComplexity::Simple => models
            .iter()
            .filter(|model| {
                model.input_price_per_million.is_some() && model.output_price_per_million.is_some()
            })
            .min_by(|a, b| model_cost(a).total_cmp(&model_cost(b)))
            .cloned()
            .or_else(|| strongest_model(models)),
        ThorOptimizationMode::BestSolution => strongest_model(models),
        _ => match complexity {
            TaskComplexity::Simple => models
                .iter()
                .filter(|model| model.arena_score >= 900.0)
                .min_by(|a, b| model_cost(a).total_cmp(&model_cost(b)))
                .cloned()
                .or_else(|| strongest_model(models)),
            TaskComplexity::Hard => strongest_model(models),
        },
    }
}

#[allow(dead_code)]
pub fn choose_route(
    models: &[ModelScore],
    harnesses: &[HarnessCandidate],
    complexity: TaskComplexity,
    mode: ThorOptimizationMode,
) -> Option<RouteChoice> {
    let model = choose_model(models, complexity, mode)?;
    let preferred = preferred_harness_kind(&model.model);
    let harness = harnesses
        .iter()
        .filter(|harness| harness.remaining_quota_available)
        .find(|harness| Some(harness.kind) == preferred)
        .or_else(|| {
            harnesses
                .iter()
                .filter(|harness| harness.remaining_quota_available)
                .find(|harness| harness.kind == HarnessKind::Anvil)
        })
        .or_else(|| {
            harnesses
                .iter()
                .find(|harness| harness.remaining_quota_available)
        })?;
    Some(RouteChoice {
        model: model.model,
        harness_source_id: harness.source_id.clone(),
        harness_kind: harness.kind,
    })
}

#[allow(dead_code)]
pub fn infer_complexity(prompt: &str) -> TaskComplexity {
    let lower = prompt.to_ascii_lowercase();
    let hard_keywords = [
        "architecture",
        "redesign",
        "multi-agent",
        "orchestr",
        "security",
        "refactor",
        "migration",
        "complex",
    ];
    if prompt.len() > 240 || hard_keywords.iter().any(|keyword| lower.contains(keyword)) {
        TaskComplexity::Hard
    } else {
        TaskComplexity::Simple
    }
}

#[allow(dead_code)]
fn strongest_model(models: &[ModelScore]) -> Option<ModelScore> {
    models
        .iter()
        .max_by(|a, b| a.arena_score.total_cmp(&b.arena_score))
        .cloned()
}

#[allow(dead_code)]
fn model_cost(model: &ModelScore) -> f64 {
    model.input_price_per_million.unwrap_or(f64::INFINITY)
        + model.output_price_per_million.unwrap_or(f64::INFINITY)
}

#[allow(dead_code)]
fn preferred_harness_kind(model: &str) -> Option<HarnessKind> {
    let lower = model.to_ascii_lowercase();
    if lower.contains("claude") || lower.contains("anthropic") {
        Some(HarnessKind::ClaudeCode)
    } else if lower.contains("gpt") || lower.contains("openai") {
        Some(HarnessKind::Codex)
    } else {
        None
    }
}

fn optimization_label(mode: ThorOptimizationMode) -> &'static str {
    match mode {
        ThorOptimizationMode::Balanced => "balanced",
        ThorOptimizationMode::Cost => "cost/accountant",
        ThorOptimizationMode::BestSolution => "best-solution/architect",
    }
}

fn reasoning_label(reasoning: ThorReasoning) -> &'static str {
    match reasoning {
        ThorReasoning::Low => "low",
        ThorReasoning::Medium => "medium",
        ThorReasoning::High => "high",
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
    fn host_prompt_makes_mcp_bridge_the_coordination_surface() {
        let prompt = host_prompt(&ThorConfig::default(), "fix the parser");
        assert!(prompt.contains("running inside an ACP host agent"));
        assert!(prompt.contains(THOR_MCP_SERVER_NAME));
        assert!(prompt.contains("listing configured ACP workers"));
        assert!(prompt.contains("coordinator reasoning: high"));
        assert!(prompt.contains("Always bake in adversarial review and correction"));
        assert!(prompt.contains("User request:\nfix the parser"));
    }

    #[test]
    fn worker_catalog_honors_enabled_worker_source_ids() {
        let config = Config {
            thor: ThorConfig {
                enabled_worker_source_ids: vec!["custom:reviewer".to_string()],
                ..ThorConfig::default()
            },
            custom_agents: vec![crate::config::CustomAgent {
                name: "reviewer".to_string(),
                program: PathBuf::from("reviewer-acp"),
                args: Vec::new(),
                description: String::new(),
            }],
            ..Config::default()
        };

        let workers = worker_catalog(&config);
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].source_id, "custom:reviewer");
    }

    #[test]
    fn architect_prompt_selects_best_of_two_versions() {
        let cfg = ThorConfig {
            optimization_mode: ThorOptimizationMode::BestSolution,
            ..ThorConfig::default()
        };
        let prompt = host_prompt(&cfg, "redesign the runtime routing");
        assert!(prompt.contains("best-solution/architect"));
        assert!(prompt.contains("run two independent versions"));
        assert!(prompt.contains("choose the best result"));
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
}
