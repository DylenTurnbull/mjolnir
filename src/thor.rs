//! Thor host-agent configuration and routing policy.
//!
//! Thor is not an in-process subagent. `mj` launches a selected ACP agent as
//! the Thor host and injects a local MCP bridge into that ACP session. The host
//! model gets the user's prompt plus these instructions, then uses MCP tools to
//! list and run other configured ACP agents.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::config::{
    CUSTOM_AGENT_SOURCE_PREFIX, Config, ConfiguredAcpServer, SelectedAgent, ThorQuotaBackend,
};

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
    pub configured_acp_servers: Vec<ConfiguredAcpServer>,
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
            configured_acp_servers: Vec::new(),
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

pub fn default_anvil_agent() -> SelectedAgent {
    SelectedAgent {
        source_id: "anvil".to_string(),
        program: PathBuf::from("uvx"),
        args: vec!["brokk".to_string(), "acp".to_string()],
        env: HashMap::new(),
    }
}

pub fn default_anvil_server() -> ConfiguredAcpServer {
    let agent = default_anvil_agent();
    ConfiguredAcpServer {
        source_id: agent.source_id,
        name: "Anvil".to_string(),
        program: agent.program,
        args: agent.args,
        env: agent.env,
        description: "Brokk ACP server via uvx".to_string(),
        setup_hint: "install uv; Brokk/Anvil signs in when required".to_string(),
        setup_install: "install uv".to_string(),
        setup_auth: "Brokk/Anvil signs in when required".to_string(),
        setup_url: "https://github.com/BrokkAi/brokk".to_string(),
        quota_backend: ThorQuotaBackend::None,
    }
}

pub fn available_worker_catalog(config: &Config) -> Vec<SelectedAgent> {
    configured_acp_servers(config)
        .into_iter()
        .map(|server| server.selected_agent())
        .collect()
}

pub fn configured_acp_servers(config: &Config) -> Vec<ConfiguredAcpServer> {
    if !config.thor.configured_acp_servers.is_empty() {
        return config
            .thor
            .configured_acp_servers
            .iter()
            .cloned()
            .map(apply_known_server_defaults)
            .collect();
    }
    let mut agents = Vec::new();
    if let Some(agent) = config.agent.clone() {
        agents.push(configured_from_selected(
            agent,
            "Configured agent".to_string(),
            String::new(),
            ThorQuotaBackend::None,
        ));
    }
    for custom in &config.custom_agents {
        let source_id = format!("{CUSTOM_AGENT_SOURCE_PREFIX}{}", custom.name);
        if agents.iter().any(|agent| agent.source_id == source_id) {
            continue;
        }
        agents.push(ConfiguredAcpServer {
            source_id,
            name: custom.name.clone(),
            program: custom.program.clone(),
            args: custom.args.clone(),
            env: HashMap::new(),
            description: custom.description.clone(),
            setup_hint: String::new(),
            setup_install: String::new(),
            setup_auth: String::new(),
            setup_url: String::new(),
            quota_backend: ThorQuotaBackend::None,
        });
    }
    if !agents.iter().any(|agent| agent.source_id == "anvil") {
        agents.push(default_anvil_server());
    }
    agents
        .into_iter()
        .map(apply_known_server_defaults)
        .collect()
}

fn configured_from_selected(
    agent: SelectedAgent,
    name: String,
    description: String,
    quota_backend: ThorQuotaBackend,
) -> ConfiguredAcpServer {
    ConfiguredAcpServer {
        source_id: agent.source_id,
        name,
        program: agent.program,
        args: agent.args,
        env: agent.env,
        description,
        setup_hint: String::new(),
        setup_install: String::new(),
        setup_auth: String::new(),
        setup_url: String::new(),
        quota_backend,
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum KnownServerProvider {
    Anvil,
    Claude,
    Codex,
    Gemini,
    OpenCode,
    Goose,
    Cursor,
    GitHubCopilot,
}

fn apply_known_server_defaults(mut server: ConfiguredAcpServer) -> ConfiguredAcpServer {
    let Some(provider) = known_server_provider(&server) else {
        return server;
    };
    let defaults = known_server_defaults(provider);
    let setup_hint = known_server_setup_hint(provider, &server, defaults.setup_hint);
    let setup_install = known_server_setup_install(provider, &server, defaults.setup_install);
    let description = known_server_description(provider, &server, defaults.description);
    if server_name_is_placeholder(&server) {
        server.name = defaults.name.to_string();
    }
    if server.description.trim().is_empty() {
        server.description = description.to_string();
    }
    if server.setup_hint.trim().is_empty() {
        server.setup_hint = setup_hint.to_string();
    }
    if server.setup_install.trim().is_empty() {
        server.setup_install = setup_install.to_string();
    }
    if server.setup_auth.trim().is_empty() {
        server.setup_auth = defaults.setup_auth.to_string();
    }
    if server.setup_url.trim().is_empty() {
        server.setup_url = defaults.setup_url.to_string();
    }
    if server.quota_backend == ThorQuotaBackend::None {
        server.quota_backend = defaults.quota_backend;
    }
    server
}

fn known_server_description<'a>(
    provider: KnownServerProvider,
    server: &ConfiguredAcpServer,
    default: &'a str,
) -> &'a str {
    if provider == KnownServerProvider::Anvil && !anvil_server_uses_uvx_brokk(server) {
        "Anvil ACP server"
    } else {
        default
    }
}

fn known_server_setup_hint<'a>(
    provider: KnownServerProvider,
    server: &ConfiguredAcpServer,
    default: &'a str,
) -> &'a str {
    if provider == KnownServerProvider::Anvil && !anvil_server_uses_uvx_brokk(server) {
        "install Anvil; Brokk/Anvil signs in when required"
    } else {
        default
    }
}

fn known_server_setup_install<'a>(
    provider: KnownServerProvider,
    server: &ConfiguredAcpServer,
    default: &'a str,
) -> &'a str {
    if provider == KnownServerProvider::Anvil && !anvil_server_uses_uvx_brokk(server) {
        "install Anvil"
    } else {
        default
    }
}

fn anvil_server_uses_uvx_brokk(server: &ConfiguredAcpServer) -> bool {
    let program = server
        .program
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .trim_end_matches(".exe")
        .to_ascii_lowercase();
    program == "uvx" && server.args.iter().any(|arg| arg == "brokk")
}

fn server_name_is_placeholder(server: &ConfiguredAcpServer) -> bool {
    let name = server.name.trim();
    name.is_empty() || name == "Configured agent" || name == server.source_id
}

struct KnownServerDefaults {
    name: &'static str,
    description: &'static str,
    setup_hint: &'static str,
    setup_install: &'static str,
    setup_auth: &'static str,
    setup_url: &'static str,
    quota_backend: ThorQuotaBackend,
}

fn known_server_defaults(provider: KnownServerProvider) -> KnownServerDefaults {
    match provider {
        KnownServerProvider::Anvil => KnownServerDefaults {
            name: "Anvil",
            description: "Brokk ACP server via uvx",
            setup_hint: "install uv; Brokk/Anvil signs in when required",
            setup_install: "install uv",
            setup_auth: "Brokk/Anvil signs in when required",
            setup_url: "https://github.com/BrokkAi/brokk",
            quota_backend: ThorQuotaBackend::None,
        },
        KnownServerProvider::Claude => KnownServerDefaults {
            name: "Claude Code",
            description: "Claude Code ACP server",
            setup_hint: "install Node.js/npm; install and sign in to Claude Code",
            setup_install: "install Node.js/npm",
            setup_auth: "install and sign in to Claude Code",
            setup_url: "https://docs.anthropic.com/en/docs/claude-code",
            quota_backend: ThorQuotaBackend::ClaudeCli,
        },
        KnownServerProvider::Codex => KnownServerDefaults {
            name: "Codex",
            description: "Codex ACP server",
            setup_hint: "install Node.js/npm; sign in to Codex",
            setup_install: "install Node.js/npm",
            setup_auth: "sign in to Codex",
            setup_url: "https://developers.openai.com/codex",
            quota_backend: ThorQuotaBackend::CodexAppserver,
        },
        KnownServerProvider::Gemini => KnownServerDefaults {
            name: "Gemini",
            description: "Gemini CLI ACP server",
            setup_hint: "install Node.js/npm; sign in with Gemini CLI",
            setup_install: "install Node.js/npm",
            setup_auth: "sign in with Gemini CLI",
            setup_url: "https://github.com/google-gemini/gemini-cli",
            quota_backend: ThorQuotaBackend::None,
        },
        KnownServerProvider::OpenCode => KnownServerDefaults {
            name: "OpenCode",
            description: "OpenCode ACP server",
            setup_hint: "install OpenCode CLI; configure OpenCode provider credentials",
            setup_install: "install OpenCode CLI",
            setup_auth: "configure OpenCode provider credentials",
            setup_url: "https://opencode.ai",
            quota_backend: ThorQuotaBackend::None,
        },
        KnownServerProvider::Goose => KnownServerDefaults {
            name: "Goose",
            description: "Goose ACP server",
            setup_hint: "install Goose; configure a Goose provider",
            setup_install: "install Goose",
            setup_auth: "configure a Goose provider",
            setup_url: "https://block.github.io/goose",
            quota_backend: ThorQuotaBackend::None,
        },
        KnownServerProvider::Cursor => KnownServerDefaults {
            name: "Cursor Agent",
            description: "Cursor Agent ACP server",
            setup_hint: "install Cursor Agent; sign in to Cursor",
            setup_install: "install Cursor Agent",
            setup_auth: "sign in to Cursor",
            setup_url: "https://cursor.com",
            quota_backend: ThorQuotaBackend::None,
        },
        KnownServerProvider::GitHubCopilot => KnownServerDefaults {
            name: "GitHub Copilot",
            description: "GitHub Copilot ACP server",
            setup_hint: "install Node.js/npm; sign in to GitHub Copilot",
            setup_install: "install Node.js/npm",
            setup_auth: "sign in to GitHub Copilot",
            setup_url: "https://github.com/features/copilot",
            quota_backend: ThorQuotaBackend::None,
        },
    }
}

fn known_server_provider(server: &ConfiguredAcpServer) -> Option<KnownServerProvider> {
    let mut parts = vec![
        server.source_id.as_str(),
        server.name.as_str(),
        server.description.as_str(),
        server.setup_url.as_str(),
    ];
    if let Some(program) = server.program.to_str() {
        parts.push(program);
    }
    parts.extend(server.args.iter().map(String::as_str));
    known_server_provider_from_parts(parts)
}

fn known_server_provider_from_parts<'a>(
    parts: impl IntoIterator<Item = &'a str>,
) -> Option<KnownServerProvider> {
    let haystack = parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if haystack.is_empty() {
        return None;
    }
    if haystack.contains("github-copilot") || haystack.contains("copilot-cli") {
        return Some(KnownServerProvider::GitHubCopilot);
    }
    if haystack.contains("claude") {
        return Some(KnownServerProvider::Claude);
    }
    if haystack.contains("codex") {
        return Some(KnownServerProvider::Codex);
    }
    if haystack.contains("gemini") {
        return Some(KnownServerProvider::Gemini);
    }
    if haystack.contains("opencode") || haystack.contains("open-code") {
        return Some(KnownServerProvider::OpenCode);
    }
    if haystack.contains("goose") {
        return Some(KnownServerProvider::Goose);
    }
    if haystack.contains("cursor") {
        return Some(KnownServerProvider::Cursor);
    }
    if haystack.contains("anvil") || haystack.contains("brokk") {
        return Some(KnownServerProvider::Anvil);
    }
    None
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
    let task_title = crate::session_titles::title_from_prompt(user_prompt).unwrap_or_default();
    format!(
        "\
Task title: {task_title}

User request:
{user_prompt}

Use `Task title` above, not the Thor system instructions below, if you set,
infer, or update the saved session title. Never title the saved session after
Thor, mjolnir, coordination, or these instructions.

You are Thor, the mjolnir omni-agent coordinator.

You are running inside an ACP host agent. You are not a local in-process
subagent. `mj` has provided an MCP server named `{server_name}` with tools for
listing configured ACP workers, reading model/pricing metadata, submitting a
structured plan, and delegating prompts to workers.

Operating mode:
- optimization: {optimization}
- coordinator model preference: {model}
- coordinator reasoning: {reasoning}
- model strength source: {leaderboard}
- pricing source: {pricing}

Rust-enforced workflow:
- Gather facts first: call `thor_list_acp_agents` with `refreshQuota: true` and
  `validate: true`, then call `thor_get_model_catalog`.
- Decide task complexity, strategy, worker/model choices, and prompts yourself.
  Rust provides facts and guardrails; it does not classify task difficulty or
  pick routes for you.
- Submit your structured plan with `thor_submit_plan` before any worker run.
- The plan must include implementation, adversarial review, and correction
  phases. `mj` rejects phase-skipping and unknown worker/job IDs.
- Run planned implementation jobs, then planned review jobs, then planned
  correction jobs. Use `phase` and `jobId` values from the accepted plan.

Policy:
- Keep the UX aggressively simple: no model picker or agent picker unless the
  user explicitly asks.
- Start routing decisions by calling `thor_get_model_catalog`; refresh it when
  cached pricing/strength data is stale or missing.
- Use `thor_validate_acp_agents`, or `thor_list_acp_agents` with `validate:
  true`, before relying on a worker set that has not been validated in this
  session.
- Before assigning work, call `thor_refresh_quota` or
  `thor_list_acp_agents` with `refreshQuota: true` so mj can query provider
  quota directly through Claude Code `/usage` and Codex appserver
  `account/rateLimits/read`. Treat only returned direct quota data as
  subscription capacity.
  Prefer known available Claude Code/Codex quota before metered OpenRouter
  routes; avoid exhausted workers.
- Use `thor_run_acp_agents` when work should happen in parallel, including
  architect-mode alternate implementations and adversarial reviews.
- Delegated worker tools only allow `reject` or `accept_edits` permission
  modes and only run inside mj's current workspace. Do not request bypassed
  permissions or arbitrary filesystem roots.
- Present a concise plan before doing work unless the user has configured plan
  approval to skip it; use the same plan content you pass to `thor_submit_plan`.
- Keep the transcript alive while you coordinate: before long-running fact
  gathering or worker calls, emit a short user-visible progress sentence, and
  after each implementation/review/correction phase, summarize the phase result
  before starting the next phase.
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
  tools instead of pasting raw worker transcripts back to the user.",
        server_name = THOR_MCP_SERVER_NAME,
        task_title = task_title,
        optimization = optimization_label(thor.optimization_mode),
        model = thor.coordinator_model,
        reasoning = reasoning_label(thor.coordinator_reasoning),
        leaderboard = thor.leaderboard_url,
        pricing = thor.pricing_url,
    )
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
        assert!(prompt.starts_with("Task title: fix the parser\n\nUser request:\nfix the parser"));
        assert!(prompt.contains("running inside an ACP host agent"));
        assert!(prompt.contains(THOR_MCP_SERVER_NAME));
        assert!(prompt.contains("listing configured ACP workers"));
        assert!(prompt.contains("Rust-enforced workflow"));
        assert!(prompt.contains("thor_submit_plan"));
        assert!(prompt.contains("coordinator reasoning: high"));
        assert!(prompt.contains("Never title the saved session after"));
        assert!(prompt.contains("Always bake in adversarial review and correction"));
        assert!(prompt.contains("Keep the transcript alive while you coordinate"));
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
    fn configured_acp_servers_are_worker_source_of_truth() {
        let config = Config {
            thor: ThorConfig {
                configured_acp_servers: vec![ConfiguredAcpServer {
                    source_id: "claude-acp".to_string(),
                    name: "Claude".to_string(),
                    program: PathBuf::from("npx"),
                    args: vec!["@agentclientprotocol/claude-agent-acp".to_string()],
                    env: HashMap::new(),
                    description: String::new(),
                    setup_hint: String::new(),
                    setup_install: String::new(),
                    setup_auth: String::new(),
                    setup_url: String::new(),
                    quota_backend: ThorQuotaBackend::ClaudeCli,
                }],
                ..ThorConfig::default()
            },
            agent: Some(default_anvil_agent()),
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
        assert_eq!(workers[0].source_id, "claude-acp");
    }

    #[test]
    fn configured_acp_servers_repair_known_provider_placeholders() {
        let config = Config {
            thor: ThorConfig {
                configured_acp_servers: vec![ConfiguredAcpServer {
                    source_id: "anvil".to_string(),
                    name: "Configured agent".to_string(),
                    program: PathBuf::from("uvx"),
                    args: vec!["brokk".to_string(), "acp".to_string()],
                    env: HashMap::new(),
                    description: String::new(),
                    setup_hint: String::new(),
                    setup_install: String::new(),
                    setup_auth: String::new(),
                    setup_url: String::new(),
                    quota_backend: ThorQuotaBackend::None,
                }],
                ..ThorConfig::default()
            },
            ..Config::default()
        };

        let servers = configured_acp_servers(&config);

        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "Anvil");
        assert_eq!(servers[0].setup_install, "install uv");
        assert_eq!(servers[0].setup_auth, "Brokk/Anvil signs in when required");
        assert_eq!(servers[0].setup_url, "https://github.com/BrokkAi/brokk");
    }

    #[test]
    fn configured_acp_servers_infer_quota_backend_without_renaming_custom_agent() {
        let config = Config {
            thor: ThorConfig {
                configured_acp_servers: vec![ConfiguredAcpServer {
                    source_id: "custom:codex alt".to_string(),
                    name: "codex alt".to_string(),
                    program: PathBuf::from("npx"),
                    args: vec![
                        "npm".to_string(),
                        "run".to_string(),
                        "start".to_string(),
                        "--prefix".to_string(),
                        "/Users/example/code/acp-upstream/codex-acp".to_string(),
                    ],
                    env: HashMap::new(),
                    description: String::new(),
                    setup_hint: String::new(),
                    setup_install: String::new(),
                    setup_auth: String::new(),
                    setup_url: String::new(),
                    quota_backend: ThorQuotaBackend::None,
                }],
                ..ThorConfig::default()
            },
            ..Config::default()
        };

        let servers = configured_acp_servers(&config);

        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "codex alt");
        assert_eq!(servers[0].quota_backend, ThorQuotaBackend::CodexAppserver);
        assert_eq!(servers[0].setup_auth, "sign in to Codex");
    }

    #[test]
    fn configured_acp_servers_use_command_specific_anvil_setup_copy() {
        let config = Config {
            thor: ThorConfig {
                configured_acp_servers: vec![ConfiguredAcpServer {
                    source_id: "custom:anvil dev".to_string(),
                    name: "anvil dev".to_string(),
                    program: PathBuf::from("/Users/example/.cargo/bin/anvil"),
                    args: Vec::new(),
                    env: HashMap::new(),
                    description: String::new(),
                    setup_hint: String::new(),
                    setup_install: String::new(),
                    setup_auth: String::new(),
                    setup_url: String::new(),
                    quota_backend: ThorQuotaBackend::None,
                }],
                ..ThorConfig::default()
            },
            ..Config::default()
        };

        let servers = configured_acp_servers(&config);

        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "anvil dev");
        assert_eq!(servers[0].setup_install, "install Anvil");
        assert_eq!(
            servers[0].setup_hint,
            "install Anvil; Brokk/Anvil signs in when required"
        );
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
}
