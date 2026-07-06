//! `/ragnarok` multi-agent tournament orchestration.
//!
//! The active chat runtime stays untouched. A Ragnarok run opens separate ACP
//! connections for Thor, implementation competitors, adversarial reviewers, and
//! the final judge. Competitors work in fresh linked Git worktrees.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::{
    McpServer, McpServerStdio, PermissionOption, PermissionOptionKind, SessionConfigOption,
    SessionConfigValueId, SessionUpdate, StopReason, ToolCallUpdate, ToolKind,
};
use anyhow::{Context, Result, anyhow, bail};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::acp::{self, AcpRuntimeConfig};
use crate::app::{config_option_choices, config_option_current_value_id, is_model_config_option};
use crate::config::{CUSTOM_AGENT_SOURCE_PREFIX, Config};
use crate::event::{
    ElicitationOutcome, PermissionDecision, PromptImage, RagnarokAnimationFrame,
    SessionConfigTarget, UiCommand, UiEvent, content_block_text,
};
use crate::install;
use crate::labels::{
    permission_option_kind_label, stop_reason_label, tool_kind_label, tool_status_label,
};
use crate::picker::{self, PickerPreferences};
use crate::probe;
use crate::registry;
use crate::scores::{self, ModelScore, ResolvedModelScore};
use crate::worktree::{self, CreatedWorktree};

const MODEL_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const THOR_TIMEOUT: Duration = Duration::from_secs(8 * 60);
const THOR_IDLE_TIMEOUT: Duration = Duration::from_secs(75);
const THOR_IDLE_NOTICE_INTERVAL: Duration = Duration::from_secs(15);
const MAX_THOR_ROUTING_ATTEMPTS: usize = 6;
const IMPLEMENTATION_TIMEOUT: Duration = Duration::from_secs(90 * 60);
const REVIEW_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const JUDGE_TIMEOUT: Duration = Duration::from_secs(12 * 60);
const RUNTIME_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_THOR_CANDIDATES: usize = 50;
const MAX_PROMPT_BLOCK_CHARS: usize = 120_000;

#[derive(Debug, Clone)]
pub struct RagnarokOptions {
    pub agent_stderr: Option<PathBuf>,
    pub fs_max_text_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub prompt: String,
    pub fun: bool,
    pub cwd: PathBuf,
    pub app_config: Config,
    pub options: RagnarokOptions,
    pub cancel_token: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLaunch {
    pub source_id: String,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelChoice {
    pub value: String,
    pub name: String,
    pub description: Option<String>,
    pub score: ModelScore,
    pub match_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub candidate_id: String,
    pub launch: AgentLaunch,
    pub model: ModelChoice,
}

#[derive(Debug, Clone)]
struct Competitor {
    id: String,
    candidate: Candidate,
    worktree: CreatedWorktree,
}

#[derive(Debug)]
struct ImplementationResult {
    competitor: Competitor,
    result: AgentTurnResult,
    status: String,
    diff_summary: String,
}

#[derive(Debug)]
struct ReviewResult {
    assignment: ReviewAssignment,
    result: std::result::Result<AgentTurnResult, String>,
}

#[derive(Debug)]
struct AgentTurnResult {
    final_text: String,
    stop_reason: Option<StopReason>,
}

#[derive(Debug, Clone)]
enum PermissionPolicy {
    AllowPlanApprovals,
    AskThor(Box<ThorPermissionContext>),
}

#[derive(Debug, Clone)]
struct ThorPermissionContext {
    cfg: RunConfig,
    thor: Candidate,
    actor_label: String,
    worktree: PathBuf,
}

#[derive(Debug, Deserialize)]
struct ThorRouting {
    competitors: usize,
    #[serde(default)]
    candidate_ids: Vec<String>,
    #[serde(default)]
    rationale: String,
}

#[derive(Debug, Deserialize)]
struct ThorReviewPlan {
    assignments: Vec<ReviewAssignment>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReviewAssignment {
    reviewer_id: String,
    target_id: String,
    #[serde(default)]
    reason: String,
}

#[derive(Debug, Deserialize)]
struct ThorJudgment {
    clear_winner: bool,
    #[serde(default)]
    winner_id: Option<String>,
    #[serde(default)]
    runner_up_id: Option<String>,
    #[serde(default)]
    top_two_ids: Vec<String>,
    #[serde(default)]
    rationale: String,
    #[serde(default)]
    recommendation: String,
    #[serde(default)]
    review_audit: Vec<ReviewAudit>,
}

#[derive(Debug, Deserialize)]
struct ReviewAudit {
    reviewer_id: String,
    target_id: String,
    valid: bool,
    #[serde(default)]
    note: String,
}

#[derive(Debug, Deserialize)]
struct ThorPermissionChoice {
    #[serde(default)]
    option_id: Option<String>,
    #[serde(default)]
    rationale: String,
}

#[derive(Clone)]
struct RagnarokProgressState {
    completed: Arc<AtomicUsize>,
    total: usize,
    detail: Arc<Mutex<String>>,
}

#[derive(Clone)]
struct RagnarokProgressSnapshot {
    completed: usize,
    total: usize,
    detail: String,
}

impl RagnarokProgressState {
    fn new(total: usize, detail: impl Into<String>) -> Self {
        Self {
            completed: Arc::new(AtomicUsize::new(0)),
            total,
            detail: Arc::new(Mutex::new(detail.into())),
        }
    }

    fn finish_one(&self, detail: impl Into<String>) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.set_detail(detail);
    }

    fn set_detail(&self, detail: impl Into<String>) {
        if let Ok(mut value) = self.detail.lock() {
            *value = detail.into();
        }
    }

    fn snapshot(&self) -> RagnarokProgressSnapshot {
        let detail = self
            .detail
            .lock()
            .map(|value| value.clone())
            .unwrap_or_else(|_| "progress detail unavailable".to_string());
        RagnarokProgressSnapshot {
            completed: self.completed.load(Ordering::Relaxed).min(self.total),
            total: self.total,
            detail,
        }
    }
}

pub async fn run(cfg: RunConfig, ui_tx: mpsc::UnboundedSender<UiEvent>) {
    if let Err(error) = run_inner(cfg, ui_tx.clone()).await {
        emit_warning(&ui_tx, format!("ragnarok failed: {error:#}"));
    }
}

async fn run_inner(cfg: RunConfig, ui_tx: mpsc::UnboundedSender<UiEvent>) -> Result<()> {
    check_cancelled(&cfg)?;
    emit_info(
        &ui_tx,
        format!(
            "ragnarok: summoning Thor for {}",
            preview_one_line(&cfg.prompt, 96)
        ),
    );

    let score_store = load_ragnarok_scores(&cfg.app_config).await;
    let launches = configured_launches(&cfg.app_config, &ui_tx).await?;
    if launches.is_empty() {
        bail!("no configured ACP agents are available for Ragnarok");
    }
    emit_info(
        &ui_tx,
        format!(
            "ragnarok: probing {} configured ACP agent(s) for ready Elo-scored models",
            launches.len()
        ),
    );

    let discovery_progress = RagnarokProgressState::new(
        launches.len(),
        format!("probing {} ACP agent(s)", launches.len()),
    );
    let discovery_active = cfg.fun.then(|| {
        spawn_progress_animator(
            ui_tx.clone(),
            "model discovery".to_string(),
            discovery_progress.clone(),
        )
    });
    let mut candidates = discover_candidates(
        launches,
        &cfg.cwd,
        &score_store,
        &cfg.cancel_token,
        &ui_tx,
        Some(discovery_progress),
    )
    .await;
    stop_animator(discovery_active).await;
    candidates = ranked_unique_candidate_list(candidates);
    if candidates.len() < 2 {
        bail!(
            "need at least two ready configured models with LMArena Elo scores; found {}",
            candidates.len()
        );
    }

    let primary_thor = candidates
        .first()
        .context("candidate list unexpectedly empty")?;
    emit_info(
        &ui_tx,
        format!(
            "ragnarok: primary Thor candidate is {} / {} ({} Elo)",
            primary_thor.launch.source_id, primary_thor.model.name, primary_thor.model.score.elo
        ),
    );

    let (thor, routing) = route_with_thor_fallback(&cfg, &candidates, &ui_tx).await?;
    check_cancelled(&cfg)?;
    let competitors = create_competitors(&cfg.cwd, routing, &ui_tx)?;
    if competitors.len() < 2 {
        bail!("Thor selected fewer than two valid competitors");
    }

    let implementations = run_implementations(&cfg, &thor, competitors, &ui_tx).await;
    check_cancelled(&cfg)?;
    let successful: Vec<ImplementationResult> = implementations
        .into_iter()
        .filter(|result| result.result.stop_reason.is_some())
        .collect();
    if successful.len() < 2 {
        bail!(
            "fewer than two competitors completed implementation turns; completed {}",
            successful.len()
        );
    }

    let assignments = plan_reviews(&cfg, &thor, &successful, &ui_tx).await?;
    check_cancelled(&cfg)?;
    let reviews = run_reviews(&cfg, &successful, assignments, &ui_tx).await;
    check_cancelled(&cfg)?;
    let judgment = judge(&cfg, &thor, &successful, &reviews, &ui_tx).await?;
    report_judgment(&ui_tx, &successful, &reviews, &judgment)?;
    Ok(())
}

async fn load_ragnarok_scores(cfg: &Config) -> scores::ScoreStore {
    let file = scores::load_scores_file(
        &scores::default_cache_path(),
        scores::CACHE_TTL,
        cfg.scores
            .url
            .as_deref()
            .unwrap_or(scores::DEFAULT_SCORES_URL),
    )
    .await;
    let store = scores::ScoreStore::default();
    store.install(scores::ScoreCatalog::build(
        &file,
        cfg.scores.overrides.clone(),
        true,
    ));
    store
}

fn check_cancelled(cfg: &RunConfig) -> Result<()> {
    if cfg.cancel_token.is_cancelled() {
        bail!("cancelled");
    }
    Ok(())
}

async fn configured_launches(
    cfg: &Config,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<Vec<AgentLaunch>> {
    let mut launches = Vec::new();
    let mut seen = HashSet::new();

    if let Some(agent) = &cfg.agent {
        push_launch(
            &mut launches,
            &mut seen,
            AgentLaunch {
                source_id: agent.source_id.clone(),
                program: agent.program.clone(),
                args: agent.args.clone(),
                env: agent.env.clone(),
            },
        );
    }

    for custom in &cfg.custom_agents {
        push_launch(
            &mut launches,
            &mut seen,
            AgentLaunch {
                source_id: format!("{CUSTOM_AGENT_SOURCE_PREFIX}{}", custom.name),
                program: custom.program.clone(),
                args: custom.args.clone(),
                env: HashMap::new(),
            },
        );
    }

    let mut allowed: HashSet<String> = cfg.favorite_agents.iter().cloned().collect();
    if let Some(agent) = &cfg.agent {
        allowed.insert(agent.source_id.clone());
    }
    allowed.extend(
        cfg.custom_agents
            .iter()
            .map(|agent| format!("{CUSTOM_AGENT_SOURCE_PREFIX}{}", agent.name)),
    );

    let registry = load_registry(ui_tx).await;
    let plan = picker::launch_plan(
        &registry,
        &registry::current_platform(),
        &install::default_install_root(),
        picker_preferences(cfg),
    );
    for (source_id, command) in plan {
        if !allowed.contains(&source_id) {
            continue;
        }
        match command {
            Some(command) => push_launch(
                &mut launches,
                &mut seen,
                AgentLaunch {
                    source_id,
                    program: command.program,
                    args: command.args,
                    env: command.env,
                },
            ),
            None if !seen.contains(&source_id) => emit_warning(
                ui_tx,
                format!("ragnarok: configured agent {source_id} has no installed registry launch"),
            ),
            None => {}
        }
    }

    Ok(launches)
}

fn push_launch(launches: &mut Vec<AgentLaunch>, seen: &mut HashSet<String>, launch: AgentLaunch) {
    if seen.insert(launch.source_id.clone()) {
        launches.push(launch);
    }
}

async fn load_registry(ui_tx: &mpsc::UnboundedSender<UiEvent>) -> registry::Registry {
    let cache_path = registry::default_cache_path();
    match registry::load_with_cache(&cache_path, registry::CACHE_TTL, registry::REGISTRY_URL).await
    {
        Ok(registry) => registry,
        Err(error) => {
            emit_warning(
                ui_tx,
                format!(
                    "ragnarok: registry unavailable; using configured direct agents only: {error:#}"
                ),
            );
            registry::Registry::default()
        }
    }
}

fn picker_preferences(cfg: &Config) -> PickerPreferences {
    PickerPreferences {
        default_agent: cfg.agent.as_ref().map(|agent| picker::PickerOutcome {
            source_id: agent.source_id.clone(),
            program: agent.program.clone(),
            args: agent.args.clone(),
            env: agent.env.clone(),
        }),
        favorite_source_ids: cfg.favorite_agents.clone(),
        custom_agents: cfg
            .custom_agents
            .iter()
            .map(|agent| picker::CustomAgent {
                name: agent.name.clone(),
                program: agent.program.clone(),
                args: agent.args.clone(),
                description: agent.description.clone(),
            })
            .collect(),
    }
}

async fn discover_candidates(
    launches: Vec<AgentLaunch>,
    cwd: &Path,
    score_store: &scores::ScoreStore,
    cancel_token: &CancellationToken,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    progress: Option<RagnarokProgressState>,
) -> Vec<Candidate> {
    let mut tasks = FuturesUnordered::new();
    for launch in launches {
        let cwd = cwd.to_path_buf();
        let score_store = score_store.clone();
        tasks.push(async move {
            let source_id = launch.source_id.clone();
            let result = probe::session_models(
                launch.program.clone(),
                launch.args.clone(),
                launch.env.clone(),
                cwd,
                MODEL_DISCOVERY_TIMEOUT,
            )
            .await;
            (launch, source_id, score_store, result)
        });
    }

    let mut candidates = Vec::new();
    loop {
        let next = tokio::select! {
            () = cancel_token.cancelled() => break,
            next = tasks.next() => next,
        };
        let Some((launch, source_id, score_store, result)) = next else {
            break;
        };
        match result {
            Ok(models) => {
                let mut eligible = 0usize;
                for model in models {
                    let description = model.description.as_deref().unwrap_or_default();
                    let Some(ResolvedModelScore { score, match_key }) =
                        score_store.model_score(&source_id, &model.value, &model.name, description)
                    else {
                        continue;
                    };
                    eligible += 1;
                    candidates.push(Candidate {
                        candidate_id: String::new(),
                        launch: launch.clone(),
                        model: ModelChoice {
                            value: model.value,
                            name: model.name,
                            description: model.description,
                            score,
                            match_key,
                        },
                    });
                }
                emit_info(
                    ui_tx,
                    format!("ragnarok: {source_id} ready with {eligible} Elo-scored model(s)"),
                );
                if let Some(progress) = &progress {
                    progress.finish_one(format!("{source_id}: {eligible} eligible Elo model(s)"));
                }
            }
            Err(error) => {
                emit_warning(
                    ui_tx,
                    format!("ragnarok: skipping {source_id}; not ready: {error}"),
                );
                if let Some(progress) = &progress {
                    progress.finish_one(format!("{source_id}: skipped ({error})"));
                }
            }
        }
    }
    candidates
}

pub(crate) fn ranked_unique_candidate_list(mut candidates: Vec<Candidate>) -> Vec<Candidate> {
    candidates.sort_by(|a, b| {
        b.model
            .score
            .elo
            .cmp(&a.model.score.elo)
            .then_with(|| a.model.score.provisional.cmp(&b.model.score.provisional))
            .then_with(|| a.launch.source_id.cmp(&b.launch.source_id))
            .then_with(|| a.model.name.cmp(&b.model.name))
    });

    let mut seen_keys = HashSet::new();
    let mut ranked = Vec::new();
    for mut candidate in candidates {
        if !seen_keys.insert(candidate.model.match_key.clone()) {
            continue;
        }
        candidate.candidate_id = format!("M{}", ranked.len() + 1);
        ranked.push(candidate);
    }
    ranked
}

async fn route_with_thor_fallback(
    cfg: &RunConfig,
    candidates: &[Candidate],
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<(Candidate, Vec<Candidate>)> {
    let thor_order = thor_candidate_order(candidates);
    let attempts = thor_order.len().min(MAX_THOR_ROUTING_ATTEMPTS);
    let mut failures = Vec::new();
    for (index, thor) in thor_order.into_iter().take(attempts).enumerate() {
        check_cancelled(cfg)?;
        if index == 0 {
            emit_info(
                ui_tx,
                format!(
                    "ragnarok: Thor routing using {} / {} ({} Elo)",
                    thor.launch.source_id, thor.model.name, thor.model.score.elo
                ),
            );
        } else {
            emit_warning(
                ui_tx,
                format!(
                    "ragnarok: Thor routing retry {} using {} / {} ({} Elo)",
                    index + 1,
                    thor.launch.source_id,
                    thor.model.name,
                    thor.model.score.elo
                ),
            );
        }
        match route_competitors(cfg, &thor, candidates, ui_tx).await {
            Ok(routing) => return Ok((thor, routing)),
            Err(error) => {
                let message = format!(
                    "{} / {} failed: {error:#}",
                    thor.launch.source_id, thor.model.name
                );
                emit_warning(ui_tx, format!("ragnarok: Thor routing {message}"));
                failures.push(message);
            }
        }
    }

    bail!(
        "Thor routing failed across {} candidate(s): {}",
        failures.len(),
        failures.join("; ")
    )
}

fn thor_candidate_order(candidates: &[Candidate]) -> Vec<Candidate> {
    let mut ordered = Vec::new();
    let mut seen_ids = HashSet::new();
    let mut seen_sources = HashSet::new();

    if let Some(primary) = candidates.first() {
        seen_ids.insert(primary.candidate_id.clone());
        seen_sources.insert(primary.launch.source_id.clone());
        ordered.push(primary.clone());
    }

    for candidate in candidates {
        if seen_sources.insert(candidate.launch.source_id.clone())
            && seen_ids.insert(candidate.candidate_id.clone())
        {
            ordered.push(candidate.clone());
        }
    }

    for candidate in candidates {
        if seen_ids.insert(candidate.candidate_id.clone()) {
            ordered.push(candidate.clone());
        }
    }

    ordered
}

async fn route_competitors(
    cfg: &RunConfig,
    thor: &Candidate,
    candidates: &[Candidate],
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<Vec<Candidate>> {
    let slate = diverse_routing_slate(candidates);
    emit_info(
        ui_tx,
        format!(
            "ragnarok: Thor routing slate shows {} diversified model(s): {}",
            slate.len(),
            source_counts(&slate)
        ),
    );
    let shown = slate
        .iter()
        .map(candidate_line)
        .collect::<Vec<_>>()
        .join("\n");
    let max = slate.len().min(10);
    let prompt = format!(
        "You are Thor, the router agent for Mjolnir's /ragnarok command.\n\
         You have access to Mjolnir's MCP tools through the mjolnir-ragnarok MCP server; use them when useful to inspect configured agents or coordinate ACP work.\n\
         Decide how many competitors should implement the user's task and choose the exact eligible candidate IDs.\n\
         Routing guidance:\n\
         - Elo is the primary quality signal; higher Elo usually means a stronger model, and every row includes its Elo\n\
         - pick the best competitors for this specific task, not mechanically the first N rows\n\
         - use model/task fit and ACP-agent diversity when quality is close\n\
         - do not repeatedly favor one ACP source if another ready Elo-scored source is comparably useful\n\
         - if you choose a lower-Elo candidate over a higher-Elo candidate, explain the task-fit or diversity reason in rationale\n\
         Constraints:\n\
         - competitors must be between 2 and {max}\n\
         - candidate_ids length must exactly equal competitors\n\
         - choose only IDs from the eligible list below\n\
         - every selected candidate must have a distinct model_key\n\
         - all listed candidates are already configured, ready, and Elo-scored\n\
         - the eligible list is a diversified slate by ACP source, so do not assume adjacent IDs are the only strong alternatives\n\
         - respond with JSON only: {{\"competitors\": number, \"candidate_ids\": [\"M1\", \"M2\"], \"rationale\": \"short\"}}\n\
         User task:\n{task}\n\nEligible diversified model slate:\n{shown}",
        task = cfg.prompt
    );

    let routing: ThorRouting = ask_thor_json(
        cfg,
        thor,
        &cfg.cwd,
        &[],
        prompt,
        "routing",
        THOR_TIMEOUT,
        ui_tx,
    )
    .await?;
    let selected = validate_routing(&routing, &slate)?;
    emit_info(
        ui_tx,
        format!(
            "ragnarok: Thor selected {} competitor(s): {}",
            selected.len(),
            selected
                .iter()
                .map(|candidate| format!(
                    "{}={}/{}({} Elo)",
                    candidate.candidate_id,
                    candidate.launch.source_id,
                    candidate.model.name,
                    candidate.model.score.elo
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    if !routing.rationale.trim().is_empty() {
        emit_info(
            ui_tx,
            format!(
                "ragnarok: Thor routing rationale: {}",
                routing.rationale.trim()
            ),
        );
    }
    Ok(selected)
}

pub(crate) fn diverse_routing_slate(candidates: &[Candidate]) -> Vec<Candidate> {
    let mut source_order = Vec::new();
    let mut by_source: HashMap<String, VecDeque<Candidate>> = HashMap::new();
    for candidate in candidates {
        let source_id = candidate.launch.source_id.clone();
        by_source.entry(source_id.clone()).or_insert_with(|| {
            source_order.push(source_id);
            VecDeque::new()
        });
        by_source
            .get_mut(&candidate.launch.source_id)
            .expect("source inserted")
            .push_back(candidate.clone());
    }

    let mut slate = Vec::new();
    while slate.len() < MAX_THOR_CANDIDATES {
        let mut added = false;
        for source_id in &source_order {
            if slate.len() >= MAX_THOR_CANDIDATES {
                break;
            }
            let Some(queue) = by_source.get_mut(source_id) else {
                continue;
            };
            let Some(candidate) = queue.pop_front() else {
                continue;
            };
            slate.push(candidate);
            added = true;
        }
        if !added {
            break;
        }
    }
    slate
}

fn source_counts(candidates: &[Candidate]) -> String {
    let mut order = Vec::new();
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for candidate in candidates {
        let source_id = candidate.launch.source_id.as_str();
        if !counts.contains_key(source_id) {
            order.push(source_id);
        }
        *counts.entry(source_id).or_default() += 1;
    }
    order
        .into_iter()
        .map(|source_id| format!("{source_id}={}", counts[source_id]))
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_routing(routing: &ThorRouting, candidates: &[Candidate]) -> Result<Vec<Candidate>> {
    if !(2..=10).contains(&routing.competitors) {
        bail!(
            "Thor selected invalid competitor count {}",
            routing.competitors
        );
    }
    let eligible = candidates
        .iter()
        .take(MAX_THOR_CANDIDATES)
        .collect::<Vec<_>>();
    if routing.competitors > eligible.len() {
        bail!(
            "Thor selected {} competitor(s), but only {} eligible candidate(s) are available",
            routing.competitors,
            eligible.len()
        );
    }
    if routing.candidate_ids.len() != routing.competitors {
        bail!(
            "Thor selected {} competitor(s), but returned {} candidate id(s)",
            routing.competitors,
            routing.candidate_ids.len()
        );
    }

    let by_id = eligible
        .iter()
        .map(|candidate| (candidate.candidate_id.as_str(), *candidate))
        .collect::<HashMap<_, _>>();
    let eligible_ids = eligible
        .iter()
        .map(|candidate| candidate.candidate_id.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let mut seen_ids = HashSet::new();
    let mut seen_model_keys = HashSet::new();
    let mut selected = Vec::new();
    for candidate_id in &routing.candidate_ids {
        let candidate_id = candidate_id.trim();
        if candidate_id.is_empty() {
            bail!("Thor returned an empty candidate id");
        }
        if !seen_ids.insert(candidate_id.to_string()) {
            bail!("Thor selected candidate {candidate_id} more than once");
        }
        let Some(candidate) = by_id.get(candidate_id) else {
            bail!(
                "Thor selected unknown candidate id {candidate_id}; eligible ids: {eligible_ids}"
            );
        };
        if !seen_model_keys.insert(candidate.model.match_key.clone()) {
            bail!(
                "Thor selected duplicate underlying model key {}",
                candidate.model.match_key
            );
        }
        selected.push((*candidate).clone());
    }

    Ok(selected)
}

fn create_competitors(
    cwd: &Path,
    candidates: Vec<Candidate>,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<Vec<Competitor>> {
    let mut competitors = Vec::new();
    for (idx, candidate) in candidates.into_iter().enumerate() {
        let id = format!("C{}", idx + 1);
        let worktree = worktree::create_for_cwd_unprompted(cwd)
            .with_context(|| format!("create worktree for {id}"))?;
        emit_info(
            ui_tx,
            format!(
                "ragnarok: {id} enters at {} using {} / {} ({} Elo)",
                worktree.session_cwd.display(),
                candidate.launch.source_id,
                candidate.model.name,
                candidate.model.score.elo
            ),
        );
        competitors.push(Competitor {
            id,
            candidate,
            worktree,
        });
    }
    Ok(competitors)
}

async fn run_implementations(
    cfg: &RunConfig,
    thor: &Candidate,
    competitors: Vec<Competitor>,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Vec<ImplementationResult> {
    let progress = RagnarokProgressState::new(
        competitors.len(),
        format!("{} competitor worktree(s) fighting", competitors.len()),
    );
    let active = cfg.fun.then(|| {
        spawn_progress_animator(
            ui_tx.clone(),
            "implementation melee".to_string(),
            progress.clone(),
        )
    });
    let mut tasks = FuturesUnordered::new();
    for competitor in competitors {
        let prompt = implementation_prompt(&cfg.prompt, &competitor);
        let options = cfg.options.clone();
        let permission_policy = PermissionPolicy::AskThor(Box::new(ThorPermissionContext {
            cfg: cfg.clone(),
            thor: thor.clone(),
            actor_label: format!("{} implement", competitor.id),
            worktree: competitor.worktree.session_cwd.clone(),
        }));
        let ui_tx = ui_tx.clone();
        tasks.push(async move {
            let label = format!("{} implement", competitor.id);
            let turn = run_agent_turn(
                &competitor.candidate.launch,
                &competitor.candidate.model,
                competitor.worktree.session_cwd.clone(),
                Vec::new(),
                cfg.cancel_token.clone(),
                prompt,
                permission_policy,
                &options,
                ui_tx.clone(),
                label,
                IMPLEMENTATION_TIMEOUT,
                None,
                None,
            )
            .await;
            let (result, status) = match turn {
                Ok(result) => {
                    let status = stop_reason_status(result.stop_reason);
                    (result, status)
                }
                Err(error) => (
                    AgentTurnResult {
                        final_text: String::new(),
                        stop_reason: None,
                    },
                    format!("failed: {error:#}"),
                ),
            };
            let diff_summary = git_summary(&competitor.worktree.session_cwd);
            ImplementationResult {
                competitor,
                result,
                status,
                diff_summary,
            }
        });
    }

    let mut out = Vec::new();
    while let Some(result) = tasks.next().await {
        progress.finish_one(format!(
            "{} implementation finished ({})",
            result.competitor.id, result.status
        ));
        emit_info(
            ui_tx,
            format!(
                "ragnarok: {} implementation finished ({})",
                result.competitor.id, result.status
            ),
        );
        out.push(result);
    }
    stop_animator(active).await;
    out.sort_by(|a, b| a.competitor.id.cmp(&b.competitor.id));
    out
}

fn implementation_prompt(user_prompt: &str, competitor: &Competitor) -> String {
    format!(
        "You are competitor {id} in a Ragnarok implementation tournament.\n\
         Implement the user's request in this dedicated Git worktree only:\n{cwd}\n\n\
         Requirements:\n\
         - Make the code changes needed for the request.\n\
         - Run focused validation when practical.\n\
         - Permission requests are routed to Thor; request only actions needed inside this worktree.\n\
         - Do not create commits, branches, or pull requests.\n\
         - End with a concise summary of changed files and validation.\n\n\
         User request:\n{user_prompt}",
        id = competitor.id,
        cwd = competitor.worktree.session_cwd.display(),
    )
}

async fn plan_reviews(
    cfg: &RunConfig,
    thor: &Candidate,
    implementations: &[ImplementationResult],
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<Vec<ReviewAssignment>> {
    let competitor_list = implementations
        .iter()
        .map(implementation_line)
        .collect::<Vec<_>>()
        .join("\n");
    let prompt = format!(
        "You are Thor assigning adversarial reviews for Ragnarok.\n\
         Choose reviewer/target pairs so every implementation is reviewed exactly once.\n\
         A reviewer cannot review its own implementation. Reviewers may review more than once only if needed.\n\
         Respond with JSON only: {{\"assignments\":[{{\"reviewer_id\":\"C1\",\"target_id\":\"C2\",\"reason\":\"short\"}}]}}\n\n\
         Implementations:\n{competitor_list}"
    );
    let plan: ThorReviewPlan = ask_thor_json(
        cfg,
        thor,
        &cfg.cwd,
        &[],
        prompt,
        "review assignment",
        THOR_TIMEOUT,
        ui_tx,
    )
    .await?;
    let assignments = validate_review_assignments(&plan.assignments, implementations)?;
    emit_info(
        ui_tx,
        format!(
            "ragnarok: Thor assigned reviews: {}",
            assignments
                .iter()
                .map(|assignment| format!("{}->{}", assignment.reviewer_id, assignment.target_id))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    Ok(assignments)
}

fn validate_review_assignments(
    assignments: &[ReviewAssignment],
    implementations: &[ImplementationResult],
) -> Result<Vec<ReviewAssignment>> {
    let ids: HashSet<&str> = implementations
        .iter()
        .map(|implementation| implementation.competitor.id.as_str())
        .collect();
    let mut target_ids = HashSet::new();
    for assignment in assignments {
        if !ids.contains(assignment.reviewer_id.as_str()) {
            bail!("Thor assigned unknown reviewer {}", assignment.reviewer_id);
        }
        if !ids.contains(assignment.target_id.as_str()) {
            bail!("Thor assigned unknown target {}", assignment.target_id);
        }
        if assignment.reviewer_id == assignment.target_id {
            bail!("Thor assigned {} to review itself", assignment.reviewer_id);
        }
        if !target_ids.insert(assignment.target_id.clone()) {
            bail!(
                "Thor assigned duplicate review target {}",
                assignment.target_id
            );
        }
    }
    for id in &ids {
        if !target_ids.contains(*id) {
            bail!("Thor did not assign a review for target {id}");
        }
    }
    Ok(assignments.to_vec())
}

async fn run_reviews(
    cfg: &RunConfig,
    implementations: &[ImplementationResult],
    assignments: Vec<ReviewAssignment>,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Vec<ReviewResult> {
    let progress = RagnarokProgressState::new(
        assignments.len(),
        format!("{} adversarial review(s) assigned", assignments.len()),
    );
    let active = cfg.fun.then(|| {
        spawn_progress_animator(
            ui_tx.clone(),
            "adversarial review brawl".to_string(),
            progress.clone(),
        )
    });
    let by_id: HashMap<&str, &ImplementationResult> = implementations
        .iter()
        .map(|implementation| (implementation.competitor.id.as_str(), implementation))
        .collect();
    let mut tasks = FuturesUnordered::new();
    for assignment in assignments {
        let reviewer = by_id
            .get(assignment.reviewer_id.as_str())
            .expect("validated reviewer")
            .competitor
            .candidate
            .clone();
        let target = *by_id
            .get(assignment.target_id.as_str())
            .expect("validated target");
        let prompt = review_prompt(&cfg.prompt, &assignment, target);
        let options = cfg.options.clone();
        let ui_tx = ui_tx.clone();
        tasks.push(async move {
            let label = format!(
                "{} reviews {}",
                assignment.reviewer_id, assignment.target_id
            );
            let result = run_agent_turn(
                &reviewer.launch,
                &reviewer.model,
                target.competitor.worktree.session_cwd.clone(),
                Vec::new(),
                cfg.cancel_token.clone(),
                prompt,
                PermissionPolicy::AllowPlanApprovals,
                &options,
                ui_tx,
                label,
                REVIEW_TIMEOUT,
                None,
                None,
            )
            .await
            .map_err(|error| format!("{error:#}"));
            ReviewResult { assignment, result }
        });
    }

    let mut out = Vec::new();
    while let Some(review) = tasks.next().await {
        let suffix = match &review.result {
            Ok(result) => stop_reason_status(result.stop_reason),
            Err(error) => format!("failed: {error}"),
        };
        progress.finish_one(format!(
            "{} -> {} finished ({suffix})",
            review.assignment.reviewer_id, review.assignment.target_id
        ));
        emit_info(
            ui_tx,
            format!(
                "ragnarok: review {} -> {} finished ({suffix})",
                review.assignment.reviewer_id, review.assignment.target_id
            ),
        );
        out.push(review);
    }
    stop_animator(active).await;
    out.sort_by(|a, b| a.assignment.target_id.cmp(&b.assignment.target_id));
    out
}

fn review_prompt(
    user_prompt: &str,
    assignment: &ReviewAssignment,
    target: &ImplementationResult,
) -> String {
    format!(
        "You are competitor {reviewer} performing an adversarial review of competitor {target_id}.\n\
         You are running in the target implementation worktree:\n{cwd}\n\n\
         Review goals:\n\
         - Inspect the implementation for correctness against the original prompt.\n\
         - Identify bugs, missing constraints, risky behavior, and missing validation.\n\
         - Do not modify files. If permission is requested for edits or shell execution, expect it to be rejected.\n\
         - Be adversarial but honest. Do not invent issues.\n\n\
         Original prompt:\n{prompt}\n\n\
         Target implementation status:\n{status}\n\n\
         Target git summary:\n{summary}\n\n\
         End with: verdict, strongest issues, and whether this implementation should win.",
        reviewer = assignment.reviewer_id,
        target_id = assignment.target_id,
        cwd = target.competitor.worktree.session_cwd.display(),
        prompt = user_prompt,
        status = target.status,
        summary = target.diff_summary,
    )
}

async fn judge(
    cfg: &RunConfig,
    thor: &Candidate,
    implementations: &[ImplementationResult],
    reviews: &[ReviewResult],
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<ThorJudgment> {
    let implementation_block = implementations
        .iter()
        .map(implementation_judgment_block)
        .collect::<Vec<_>>()
        .join("\n\n");
    let review_block = reviews
        .iter()
        .map(review_judgment_block)
        .collect::<Vec<_>>()
        .join("\n\n");
    let prompt = format!(
        "You are Thor judging a Ragnarok tournament.\n\
         You have access to Mjolnir's MCP tools through the mjolnir-ragnarok MCP server; use them when useful to audit ACP work or inspect available agent state.\n\
         Audit the adversarial reviews for honesty and validity, then choose the implementation closest to the user's request.\n\
         If there is a clear winner, set clear_winner=true and winner_id.\n\
         If no clear winner exists, set clear_winner=false and provide exactly two ids in top_two_ids.\n\
         Respond with JSON only: {{\"clear_winner\":true,\"winner_id\":\"C1\",\"runner_up_id\":\"C2\",\"top_two_ids\":[],\"rationale\":\"short\",\"recommendation\":\"short\",\"review_audit\":[{{\"reviewer_id\":\"C1\",\"target_id\":\"C2\",\"valid\":true,\"note\":\"short\"}}]}}\n\n\
         Original prompt:\n{task}\n\n\
         Implementations:\n{implementation_block}\n\n\
         Reviews:\n{review_block}",
        task = cfg.prompt,
    );
    ask_thor_json(
        cfg,
        thor,
        &cfg.cwd,
        &[],
        prompt,
        "final judgment",
        JUDGE_TIMEOUT,
        ui_tx,
    )
    .await
}

fn report_judgment(
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    implementations: &[ImplementationResult],
    reviews: &[ReviewResult],
    judgment: &ThorJudgment,
) -> Result<()> {
    let by_id: HashMap<&str, &ImplementationResult> = implementations
        .iter()
        .map(|implementation| (implementation.competitor.id.as_str(), implementation))
        .collect();
    emit_info(ui_tx, "ragnarok: THOR JUDGMENT".to_string());
    if judgment.clear_winner {
        let winner_id = judgment
            .winner_id
            .as_deref()
            .context("Thor declared a clear winner without winner_id")?;
        let winner = by_id
            .get(winner_id)
            .ok_or_else(|| anyhow!("Thor chose unknown winner {winner_id}"))?;
        emit_info(
            ui_tx,
            format!(
                "ragnarok: winner {winner_id} -> {} / {} at {}",
                winner.competitor.candidate.launch.source_id,
                winner.competitor.candidate.model.name,
                winner.competitor.worktree.session_cwd.display()
            ),
        );
        if let Some(runner_up_id) = judgment.runner_up_id.as_deref()
            && let Some(runner_up) = by_id.get(runner_up_id)
        {
            emit_info(
                ui_tx,
                format!(
                    "ragnarok: runner-up {runner_up_id} -> {} / {} at {}",
                    runner_up.competitor.candidate.launch.source_id,
                    runner_up.competitor.candidate.model.name,
                    runner_up.competitor.worktree.session_cwd.display()
                ),
            );
        }
    } else {
        let top_two = top_two_ids(judgment)?;
        emit_info(
            ui_tx,
            format!(
                "ragnarok: no clear winner; Thor presents {} for the user to decide",
                top_two.join(" and ")
            ),
        );
        for id in top_two {
            let implementation = by_id
                .get(id.as_str())
                .ok_or_else(|| anyhow!("Thor chose unknown top-two competitor {id}"))?;
            emit_info(
                ui_tx,
                format!(
                    "ragnarok: option {id} -> {} / {} at {}",
                    implementation.competitor.candidate.launch.source_id,
                    implementation.competitor.candidate.model.name,
                    implementation.competitor.worktree.session_cwd.display()
                ),
            );
        }
    }
    if !judgment.rationale.trim().is_empty() {
        emit_info(
            ui_tx,
            format!("ragnarok: rationale: {}", judgment.rationale.trim()),
        );
    }
    if !judgment.recommendation.trim().is_empty() {
        emit_info(
            ui_tx,
            format!(
                "ragnarok: recommendation: {}",
                judgment.recommendation.trim()
            ),
        );
    }
    for audit in &judgment.review_audit {
        emit_info(
            ui_tx,
            format!(
                "ragnarok: review audit {} -> {} valid={} {}",
                audit.reviewer_id,
                audit.target_id,
                audit.valid,
                audit.note.trim()
            ),
        );
    }
    for review in reviews {
        if let Err(error) = &review.result {
            emit_warning(
                ui_tx,
                format!(
                    "ragnarok: review {} -> {} failed: {error}",
                    review.assignment.reviewer_id, review.assignment.target_id
                ),
            );
        }
    }
    Ok(())
}

fn top_two_ids(judgment: &ThorJudgment) -> Result<Vec<String>> {
    if judgment.top_two_ids.len() == 2 {
        return Ok(judgment.top_two_ids.clone());
    }
    match (&judgment.winner_id, &judgment.runner_up_id) {
        (Some(a), Some(b)) if a != b => Ok(vec![a.clone(), b.clone()]),
        _ => bail!("Thor did not provide two alternatives"),
    }
}

fn thor_mcp_servers(cfg: &RunConfig, additional_directories: &[PathBuf]) -> Result<Vec<McpServer>> {
    let exe = std::env::current_exe().context("resolve current executable for Thor MCP server")?;
    let mut args = vec![
        "--cwd".to_string(),
        cfg.cwd.display().to_string(),
        "--fs-max-text-bytes".to_string(),
        cfg.options.fs_max_text_bytes.to_string(),
    ];
    for directory in additional_directories {
        args.push("--additional-directory".to_string());
        args.push(directory.display().to_string());
    }
    if let Some(agent_stderr) = &cfg.options.agent_stderr {
        args.push("--agent-stderr".to_string());
        args.push(agent_stderr.display().to_string());
    }
    args.push("mcp".to_string());

    Ok(vec![McpServer::Stdio(
        McpServerStdio::new("mjolnir-ragnarok", exe).args(args),
    )])
}

#[allow(clippy::too_many_arguments)]
async fn ask_thor_json<T>(
    cfg: &RunConfig,
    thor: &Candidate,
    cwd: &Path,
    mcp_additional_directories: &[PathBuf],
    prompt: String,
    phase: &str,
    timeout: Duration,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let progress = RagnarokProgressState::new(1, format!("Thor {phase}: preparing ACP turn"));
    let active = cfg
        .fun
        .then(|| spawn_progress_animator(ui_tx.clone(), format!("Thor {phase}"), progress.clone()));
    let result: Result<T> = async {
        let first = run_agent_turn(
            &thor.launch,
            &thor.model,
            cwd.to_path_buf(),
            thor_mcp_servers(cfg, mcp_additional_directories)?,
            cfg.cancel_token.clone(),
            prompt.clone(),
            PermissionPolicy::AllowPlanApprovals,
            &cfg.options,
            ui_tx.clone(),
            format!("Thor {phase}"),
            timeout,
            Some(THOR_IDLE_TIMEOUT),
            Some(progress.clone()),
        )
        .await?;
        match parse_json_object::<T>(&first.final_text) {
            Ok(value) => {
                progress.finish_one(format!("Thor {phase}: valid JSON received"));
                Ok(value)
            }
            Err(first_error) => {
                emit_warning(
                    ui_tx,
                    format!("ragnarok: Thor returned invalid JSON for {phase}; retrying once"),
                );
                progress.set_detail(format!("Thor {phase}: invalid JSON; retrying"));
                let retry_prompt = format!(
                    "Your previous {phase} answer was not valid JSON ({first_error}).\n\
                     Return ONLY the requested JSON object. No markdown, no prose.\n\n\
                     Original instruction:\n{prompt}"
                );
                let second = run_agent_turn(
                    &thor.launch,
                    &thor.model,
                    cwd.to_path_buf(),
                    thor_mcp_servers(cfg, mcp_additional_directories)?,
                    cfg.cancel_token.clone(),
                    retry_prompt,
                    PermissionPolicy::AllowPlanApprovals,
                    &cfg.options,
                    ui_tx.clone(),
                    format!("Thor {phase} retry"),
                    timeout,
                    Some(THOR_IDLE_TIMEOUT),
                    Some(progress.clone()),
                )
                .await?;
                let parsed = parse_json_object::<T>(&second.final_text)
                    .with_context(|| format!("Thor did not return valid JSON for {phase}"))?;
                progress.finish_one(format!("Thor {phase}: valid retry JSON received"));
                Ok(parsed)
            }
        }
    }
    .await;
    stop_animator(active).await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_agent_turn(
    launch: &AgentLaunch,
    model: &ModelChoice,
    cwd: PathBuf,
    mcp_servers: Vec<McpServer>,
    cancel_token: CancellationToken,
    prompt: String,
    permission_policy: PermissionPolicy,
    options: &RagnarokOptions,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    label: String,
    timeout: Duration,
    idle_timeout: Option<Duration>,
    progress: Option<RagnarokProgressState>,
) -> Result<AgentTurnResult> {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let runtime_cfg = AcpRuntimeConfig {
        command: launch.program.clone(),
        args: launch.args.clone(),
        cwd: cwd.clone(),
        additional_directories: Vec::new(),
        mcp_servers,
        resume_session: None,
        env: launch.env.clone(),
        agent_stderr: options.agent_stderr.clone(),
        fs_max_text_bytes: options.fs_max_text_bytes,
    };
    let runtime = tokio::spawn(async move { acp::run(runtime_cfg, event_tx, cmd_rx).await });
    let runtime_abort = runtime.abort_handle();
    let mut session_started = false;
    let mut sent_model_update = false;
    let mut model_ready = false;
    let mut sent_prompt = false;
    let mut final_text = String::new();
    let mut stop_reason = None;
    let timeout = tokio::time::sleep(timeout);
    tokio::pin!(timeout);
    let mut idle_tick = tokio::time::interval(Duration::from_secs(5));
    idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_activity = Instant::now();
    let mut next_idle_notice = THOR_IDLE_NOTICE_INTERVAL;

    emit_info(
        &ui_tx,
        format!(
            "ragnarok: {label} connecting {} / {} in {}",
            launch.source_id,
            model.name,
            cwd.display()
        ),
    );
    if let Some(progress) = &progress {
        progress.set_detail(format!("{label}: connecting ACP"));
    }

    let result = loop {
        tokio::select! {
            _ = &mut timeout => {
                break Err(anyhow!("{label} timed out"));
            }
            () = cancel_token.cancelled() => {
                break Err(anyhow!("{label} cancelled"));
            }
            _ = idle_tick.tick(), if sent_prompt && (idle_timeout.is_some() || progress.is_some()) => {
                let idle_for = last_activity.elapsed();
                let idle_for_text = format_elapsed(idle_for);
                let watchdog_text = idle_timeout
                    .map(format_elapsed)
                    .map(|limit| format!(" / watchdog {limit}"))
                    .unwrap_or_default();
                if let Some(progress) = &progress {
                    progress.set_detail(format!(
                        "{label}: waiting for ACP activity; idle {idle_for_text}{watchdog_text}"
                    ));
                }
                if let Some(limit) = idle_timeout
                    && idle_for >= limit
                {
                    break Err(anyhow!(
                        "{label} idle for {idle_for_text} after prompt launch; no ACP output, tool call, permission, or completion"
                    ));
                }
                if idle_for >= next_idle_notice {
                    emit_info(
                        &ui_tx,
                        format!(
                            "ragnarok: {label} still waiting for ACP activity (idle {idle_for_text}{watchdog_text})"
                        ),
                    );
                    next_idle_notice += THOR_IDLE_NOTICE_INTERVAL;
                }
            }
            maybe_event = event_rx.recv() => {
                let Some(event) = maybe_event else {
                    break Err(anyhow!("{label} ACP runtime closed before prompt completed"));
                };
                if sent_prompt {
                    last_activity = Instant::now();
                    next_idle_notice = THOR_IDLE_NOTICE_INTERVAL;
                    if let Some(progress) = &progress {
                        progress.set_detail(format!("{label}: ACP event received"));
                    }
                }
                match event {
                    UiEvent::Connected { .. } => {
                        if let Some(progress) = &progress {
                            progress.set_detail(format!("{label}: ACP connected"));
                        }
                        emit_info(&ui_tx, format!("ragnarok: {label} ACP connected"));
                    }
                    UiEvent::SessionStarted { .. } => {
                        session_started = true;
                        if let Some(progress) = &progress {
                            progress.set_detail(format!("{label}: session started"));
                        }
                        emit_info(&ui_tx, format!("ragnarok: {label} session started"));
                    }
                    UiEvent::SessionConfigOptions { options, targets } => {
                        match handle_model_config(&cmd_tx, &options, &targets, model, sent_model_update) {
                            Ok(ModelConfigProgress::Ready { value, confirmed_update }) => {
                                model_ready = true;
                                let status = if confirmed_update {
                                    "confirmed"
                                } else {
                                    "already current"
                                };
                                emit_info(
                                    &ui_tx,
                                    format!("ragnarok: {label} model {status}: {value}"),
                                );
                                if let Some(progress) = &progress {
                                    progress.set_detail(format!(
                                        "{label}: model {status}: {value}"
                                    ));
                                }
                            }
                            Ok(ModelConfigProgress::UpdateSent { from, to }) => {
                                sent_model_update = true;
                                let from = from.unwrap_or_else(|| "unknown".to_string());
                                emit_info(
                                    &ui_tx,
                                    format!("ragnarok: {label} switching model {from} -> {to}"),
                                );
                                if let Some(progress) = &progress {
                                    progress.set_detail(format!(
                                        "{label}: switching model {from} -> {to}"
                                    ));
                                }
                            }
                            Err(error) => break Err(error),
                        }
                    }
                    UiEvent::SessionUpdate(update) => {
                        collect_update(&mut final_text, &label, update, sent_prompt, &ui_tx);
                    }
                    UiEvent::PermissionRequest(permission) => {
                        let decision = match permission_decision(
                            &permission_policy,
                            &permission.tool_call,
                            &permission.options,
                            &ui_tx,
                        )
                        .await
                        {
                            Ok(decision) => decision,
                            Err(error) => {
                                emit_warning(
                                    &ui_tx,
                                    format!("ragnarok: {label} permission delegation failed: {error:#}"),
                                );
                                None
                            }
                        };
                        let decision_text = decision
                            .as_deref()
                            .map(|id| format!("selected {id}"))
                            .unwrap_or_else(|| "cancelled".to_string());
                        emit_info(
                            &ui_tx,
                            format!(
                                "ragnarok: {label} permission {decision_text}: {}",
                                permission.tool_call.fields.title.as_deref().unwrap_or("tool")
                            ),
                        );
                        let _ = permission.responder.send(match decision {
                            Some(option_id) => PermissionDecision::Selected(option_id),
                            None => PermissionDecision::Cancelled,
                        });
                    }
                    UiEvent::ElicitationRequest(prompt) => {
                        emit_warning(
                            &ui_tx,
                            format!("ragnarok: {label} declined elicitation: {}", prompt.message),
                        );
                        let _ = prompt.responder.send(ElicitationOutcome::Decline);
                    }
                    UiEvent::PromptDone { stop_reason: reason, .. } => {
                        stop_reason = Some(reason);
                        if let Some(progress) = &progress {
                            progress.set_detail(format!(
                                "{label}: prompt finished with {}",
                                stop_reason_label(reason)
                            ));
                        }
                        emit_info(
                            &ui_tx,
                            format!(
                                "ragnarok: {label} prompt finished with {}",
                                stop_reason_label(reason)
                            ),
                        );
                        break Ok(());
                    }
                    UiEvent::PromptFailed { message }
                    | UiEvent::SessionForkFailed { message }
                    | UiEvent::Fatal(message) => {
                        break Err(anyhow!("{label}: {message}"));
                    }
                    UiEvent::Warning(message) => {
                        emit_warning(&ui_tx, format!("ragnarok: {label}: {message}"));
                    }
                    UiEvent::Info(message) => {
                        emit_info(&ui_tx, format!("ragnarok: {label}: {message}"));
                    }
                    UiEvent::TerminalOutput(_)
                    | UiEvent::CancelPendingPermissions
                    | UiEvent::RagnarokAnimation(_)
                    | UiEvent::RemotePermissionDecision { .. }
                    | UiEvent::ClaudeUsage(_) => {}
                }

                if session_started && model_ready && !sent_prompt {
                    cmd_tx
                        .send(UiCommand::SendPrompt {
                            text: prompt.clone(),
                            images: Vec::<PromptImage>::new(),
                        })
                        .with_context(|| format!("send {label} prompt"))?;
                    sent_prompt = true;
                    last_activity = Instant::now();
                    next_idle_notice = THOR_IDLE_NOTICE_INTERVAL;
                    if let Some(progress) = &progress {
                        progress.set_detail(format!(
                            "{label}: prompt launched; waiting for ACP output"
                        ));
                    }
                    emit_info(
                        &ui_tx,
                        format!("ragnarok: {label} prompt launched; awaiting response"),
                    );
                }
            }
        }
    };

    let _ = cmd_tx.send(UiCommand::Shutdown);
    match tokio::time::timeout(RUNTIME_TEARDOWN_TIMEOUT, runtime).await {
        Ok(Ok(Ok(()))) | Ok(Ok(Err(_))) => {}
        Ok(Err(error)) => tracing::warn!("ragnarok {label} runtime join failed: {error}"),
        Err(_) => {
            tracing::warn!("ragnarok {label} runtime did not exit within timeout");
            runtime_abort.abort();
        }
    }

    result?;
    if !matches!(
        stop_reason.unwrap_or(StopReason::Cancelled),
        StopReason::EndTurn | StopReason::MaxTokens | StopReason::MaxTurnRequests
    ) {
        bail!(
            "{label} stopped with {}",
            stop_reason_label(stop_reason.unwrap_or(StopReason::Cancelled))
        );
    }
    Ok(AgentTurnResult {
        final_text,
        stop_reason,
    })
}

enum ModelConfigProgress {
    Ready {
        value: String,
        confirmed_update: bool,
    },
    UpdateSent {
        from: Option<String>,
        to: String,
    },
}

struct ModelTargetMatch {
    target: SessionConfigTarget,
    value: SessionConfigValueId,
    current_value: Option<String>,
    current_matches: bool,
}

fn handle_model_config(
    cmd_tx: &mpsc::UnboundedSender<UiCommand>,
    options: &[SessionConfigOption],
    targets: &[SessionConfigTarget],
    model: &ModelChoice,
    sent_model_update: bool,
) -> Result<ModelConfigProgress> {
    let selection = find_model_target(options, targets, model).ok_or_else(|| {
        anyhow!(
            "selected model {} ({}) is not exposed by this ACP session",
            model.name,
            model.value
        )
    })?;
    let value = selection.value.to_string();
    if selection.current_matches {
        return Ok(ModelConfigProgress::Ready {
            value,
            confirmed_update: sent_model_update,
        });
    }
    if sent_model_update {
        bail!(
            "model update to {} did not become current after session config refresh",
            value
        );
    }
    cmd_tx
        .send(UiCommand::SetSessionConfigOption {
            target: selection.target,
            value: selection.value,
        })
        .context("send model config update")?;
    Ok(ModelConfigProgress::UpdateSent {
        from: selection.current_value,
        to: value,
    })
}

fn find_model_target(
    options: &[SessionConfigOption],
    targets: &[SessionConfigTarget],
    model: &ModelChoice,
) -> Option<ModelTargetMatch> {
    for (index, option) in options.iter().enumerate() {
        if !is_model_config_option(option) {
            continue;
        }
        let choices = config_option_choices(option)?;
        let choice = choices
            .iter()
            .find(|choice| choice.value.to_string() == model.value)
            .or_else(|| {
                choices.iter().find(|choice| {
                    choice.name == model.name && choice.description == model.description
                })
            })?;
        let target =
            targets
                .get(index)
                .cloned()
                .unwrap_or_else(|| SessionConfigTarget::ConfigOption {
                    config_id: option.id.clone(),
                });
        let value = choice.value.clone();
        let current_value =
            config_option_current_value_id(option).map(|current| current.to_string());
        let current_matches = current_value
            .as_deref()
            .map(|current| current == value.to_string())
            .unwrap_or(false);
        return Some(ModelTargetMatch {
            target,
            value,
            current_value,
            current_matches,
        });
    }
    None
}

fn collect_update(
    final_text: &mut String,
    label: &str,
    update: SessionUpdate,
    sent_prompt: bool,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) if sent_prompt => {
            let text = content_block_text(&chunk.content);
            final_text.push_str(&text);
        }
        SessionUpdate::AgentThoughtChunk(chunk) if sent_prompt => {
            let _ = content_block_text(&chunk.content);
        }
        SessionUpdate::ToolCall(tool_call) => {
            emit_info(
                ui_tx,
                format!(
                    "ragnarok: {label} tool {} [{}]",
                    tool_call.title,
                    tool_kind_label(tool_call.kind)
                ),
            );
        }
        SessionUpdate::ToolCallUpdate(update) => {
            let _ = update.fields.status.map(tool_status_label);
        }
        _ => {}
    }
}

async fn permission_decision(
    policy: &PermissionPolicy,
    tool_call: &ToolCallUpdate,
    options: &[PermissionOption],
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<Option<String>> {
    match policy {
        PermissionPolicy::AllowPlanApprovals => {
            if is_plan_approval_request(tool_call) {
                Ok(choose_allow_once_option(options))
            } else {
                Ok(None)
            }
        }
        PermissionPolicy::AskThor(ctx) => {
            Box::pin(ask_thor_permission_decision(ctx, tool_call, options, ui_tx)).await
        }
    }
}

async fn ask_thor_permission_decision(
    ctx: &ThorPermissionContext,
    tool_call: &ToolCallUpdate,
    options: &[PermissionOption],
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<Option<String>> {
    let prompt = format!(
        "You are Thor handling an ACP permission request during a Ragnarok implementation.\n\
         You have access to Mjolnir's MCP tools through the mjolnir-ragnarok MCP server; use them if useful before deciding.\n\
         Decide which permission option Mjolnir should send back to the competitor.\n\n\
         Hard policy:\n\
         - The competitor may edit and run validation inside its dedicated worktree.\n\
         - Reject or cancel requests that commit, push, open PRs, alter unrelated directories, exfiltrate secrets, change credentials, or perform destructive actions outside the worktree.\n\
         - Select an actual option_id from the list below. Use a reject option when rejection is the right answer.\n\
         - Respond with JSON only: {{\"option_id\":\"allow\",\"rationale\":\"short\"}}. Use null option_id only when no listed option is appropriate.\n\n\
         Original user request:\n{task}\n\n\
         Competitor turn: {actor}\n\
         Dedicated worktree: {worktree}\n\n\
         Permission request:\n{request}\n\n\
         Options:\n{options}",
        task = ctx.cfg.prompt,
        actor = ctx.actor_label,
        worktree = ctx.worktree.display(),
        request = permission_request_block(tool_call),
        options = permission_options_block(options),
    );

    let choice: ThorPermissionChoice = ask_thor_json(
        &ctx.cfg,
        &ctx.thor,
        &ctx.cfg.cwd,
        std::slice::from_ref(&ctx.worktree),
        prompt,
        "permission decision",
        THOR_TIMEOUT,
        ui_tx,
    )
    .await?;
    if !choice.rationale.trim().is_empty() {
        emit_info(
            ui_tx,
            format!(
                "ragnarok: Thor permission rationale for {}: {}",
                ctx.actor_label,
                choice.rationale.trim()
            ),
        );
    }
    validate_permission_choice(choice, options)
}

fn is_plan_approval_request(tool_call: &ToolCallUpdate) -> bool {
    let title = tool_call
        .fields
        .title
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let is_known_plan_gate = matches!(
        title.as_str(),
        "approve plan" | "approve plan before execution"
    );
    is_known_plan_gate
        && matches!(
            tool_call.fields.kind,
            None | Some(ToolKind::Other | ToolKind::Think)
        )
}

fn choose_allow_once_option(options: &[PermissionOption]) -> Option<String> {
    options
        .iter()
        .find(|option| option.kind == PermissionOptionKind::AllowOnce)
        .map(|option| option.option_id.to_string())
}

fn permission_request_block(tool_call: &ToolCallUpdate) -> String {
    let title = tool_call
        .fields
        .title
        .as_deref()
        .unwrap_or("permission request");
    let kind = tool_call
        .fields
        .kind
        .map(tool_kind_label)
        .unwrap_or("unknown");
    let raw_input = tool_call
        .fields
        .raw_input
        .as_ref()
        .and_then(|value| serde_json::to_string_pretty(value).ok())
        .map(|value| truncate_chars(&value, 8_000))
        .unwrap_or_else(|| "-".to_string());
    format!(
        "tool_call_id: {}\ntitle: {title}\nkind: {kind}\nraw_input:\n{raw_input}",
        tool_call.tool_call_id
    )
}

fn permission_options_block(options: &[PermissionOption]) -> String {
    options
        .iter()
        .map(|option| {
            format!(
                "- option_id={} kind={} name={}",
                option.option_id,
                permission_option_kind_label(option.kind),
                option.name
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn validate_permission_choice(
    choice: ThorPermissionChoice,
    options: &[PermissionOption],
) -> Result<Option<String>> {
    let Some(option_id) = choice.option_id.map(|id| id.trim().to_string()) else {
        return Ok(None);
    };
    if option_id.is_empty() {
        return Ok(None);
    }
    if options
        .iter()
        .any(|option| option.option_id.to_string() == option_id)
    {
        return Ok(Some(option_id));
    }
    bail!("Thor chose unknown permission option_id {option_id}");
}

fn parse_json_object<T>(text: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    if let Ok(value) = serde_json::from_str(text.trim()) {
        return Ok(value);
    }
    let start = text.find('{').context("no JSON object start")?;
    let end = text.rfind('}').context("no JSON object end")?;
    serde_json::from_str(&text[start..=end]).context("parse JSON object")
}

fn candidate_line(candidate: &Candidate) -> String {
    format!(
        "{}: agent={} model={} value={} elo={} model_key={}",
        candidate.candidate_id,
        candidate.launch.source_id,
        candidate.model.name,
        candidate.model.value,
        candidate.model.score.elo,
        candidate.model.match_key
    )
}

fn implementation_line(implementation: &ImplementationResult) -> String {
    format!(
        "{}: agent={} model={} elo={} worktree={} status={}",
        implementation.competitor.id,
        implementation.competitor.candidate.launch.source_id,
        implementation.competitor.candidate.model.name,
        implementation.competitor.candidate.model.score.elo,
        implementation.competitor.worktree.session_cwd.display(),
        implementation.status
    )
}

fn implementation_judgment_block(implementation: &ImplementationResult) -> String {
    format!(
        "{}\nImplementation report:\n{}\nGit summary:\n{}",
        implementation_line(implementation),
        truncate_chars(
            &implementation.result.final_text,
            MAX_PROMPT_BLOCK_CHARS / 2,
        ),
        truncate_chars(&implementation.diff_summary, MAX_PROMPT_BLOCK_CHARS / 2),
    )
}

fn review_judgment_block(review: &ReviewResult) -> String {
    match &review.result {
        Ok(result) => format!(
            "reviewer={} target={} reason={}\n{}",
            review.assignment.reviewer_id,
            review.assignment.target_id,
            review.assignment.reason,
            truncate_chars(&result.final_text, MAX_PROMPT_BLOCK_CHARS / 2),
        ),
        Err(error) => format!(
            "reviewer={} target={} failed={error}",
            review.assignment.reviewer_id, review.assignment.target_id
        ),
    }
}

fn git_summary(cwd: &Path) -> String {
    let status = git_output(cwd, ["status", "--short"]).unwrap_or_else(|error| error);
    let diff_stat = git_output(cwd, ["diff", "--stat"]).unwrap_or_else(|error| error);
    let diff_names = git_output(cwd, ["diff", "--name-only"]).unwrap_or_else(|error| error);
    format!(
        "status:\n{}\n\ndiff stat:\n{}\n\ndiff files:\n{}",
        empty_dash(status),
        empty_dash(diff_stat),
        empty_dash(diff_names)
    )
}

fn git_output<const N: usize>(cwd: &Path, args: [&str; N]) -> std::result::Result<String, String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .map_err(|error| format!("git failed to start: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn empty_dash(value: String) -> String {
    if value.trim().is_empty() {
        "-".to_string()
    } else {
        value
    }
}

fn stop_reason_status(reason: Option<StopReason>) -> String {
    reason
        .map(stop_reason_label)
        .unwrap_or("failed")
        .to_string()
}

fn spawn_progress_animator(
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    phase: String,
    progress: RagnarokProgressState,
) -> (Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    spawn_animator_inner(ui_tx, phase, Some(progress))
}

fn spawn_animator_inner(
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    phase: String,
    progress: Option<RagnarokProgressState>,
) -> (Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let active = Arc::new(AtomicBool::new(true));
    let thread_active = active.clone();
    let handle = tokio::spawn(async move {
        let mut idx = 0usize;
        let started = std::time::Instant::now();
        while thread_active.load(Ordering::Relaxed) {
            let snapshot = progress.as_ref().map(RagnarokProgressState::snapshot);
            let lines = ragnarok_combat_frame(idx, &phase, started.elapsed(), snapshot.as_ref());
            let _ = ui_tx.send(UiEvent::RagnarokAnimation(RagnarokAnimationFrame {
                active: true,
                phase: phase.clone(),
                frame_index: idx,
                lines,
            }));
            idx = idx.wrapping_add(1);
            tokio::time::sleep(Duration::from_millis(220)).await;
        }
        let _ = ui_tx.send(UiEvent::RagnarokAnimation(RagnarokAnimationFrame {
            active: false,
            phase,
            frame_index: idx,
            lines: Vec::new(),
        }));
    });
    (active, handle)
}

fn ragnarok_combat_frame(
    frame: usize,
    phase: &str,
    elapsed: Duration,
    progress: Option<&RagnarokProgressSnapshot>,
) -> Vec<String> {
    let (ladder, status) = ragnarok_phase_hud(phase, elapsed);
    let progress = ragnarok_progress_line(progress);
    let caption = format!("ᚱᚢᚾᛖ phase: {phase}  ᚦᛟᚱ");
    let mut lines = match frame % 10 {
        0 => vec![
            "        [TH]                         [AG]        ".to_string(),
            "       /|##|\\        ....           /|??|\\       ".to_string(),
            "        /  \\                         /  \\        ".to_string(),
            "  =====/====\\=======================/====\\====  ".to_string(),
            caption,
        ],
        1 => vec![
            "           [TH]                    [AG]          ".to_string(),
            "          /|##|\\   --->           /|??|\\         ".to_string(),
            "           /  \\                    /  \\          ".to_string(),
            "  ========/====\\==================/====\\======  ".to_string(),
            caption,
        ],
        2 => vec![
            "              [TH]              [AG]             ".to_string(),
            "        _===> /|##|\\            /|??|\\            ".to_string(),
            "              /  \\              /  \\             ".to_string(),
            "  ===========/====\\============/====\\=========  ".to_string(),
            caption,
        ],
        3 => vec![
            "                 [TH]====#     [AG]              ".to_string(),
            "                /|##|\\        /|??|\\             ".to_string(),
            "                 /  \\          /  \\              ".to_string(),
            "  ==============/====\\========/====\\==========  ".to_string(),
            caption,
        ],
        4 => vec![
            "                    [TH] ### [AG]                 ".to_string(),
            "                   /|##|\\BAM/|??|\\                ".to_string(),
            "                    /  \\     /  \\                 ".to_string(),
            "  ================/====\\===/====\\=============  ".to_string(),
            caption,
        ],
        5 => vec![
            "                    [TH]      _[AG]_              ".to_string(),
            "                   /|##|\\  CRASH |??|             ".to_string(),
            "                    /  \\      _/ \\_               ".to_string(),
            "  ================/====\\===/=====\\============  ".to_string(),
            caption,
        ],
        6 => vec![
            "                 [TH]             \\[AG]/          ".to_string(),
            "                /|##|\\       --->  |??|           ".to_string(),
            "                 /  \\              / \\            ".to_string(),
            "  ==============/====\\============/===\\========  ".to_string(),
            caption,
        ],
        7 => vec![
            "            [TH]                         [AG]     ".to_string(),
            "           /|##|\\          <---         /|??|\\    ".to_string(),
            "            /  \\                        /  \\     ".to_string(),
            "  =========/====\\======================/====\\===  ".to_string(),
            caption,
        ],
        8 => vec![
            "        [TH]                 [AG]                 ".to_string(),
            "       /|##|\\    ((parry))  /|??|\\                ".to_string(),
            "        /  \\                 /  \\                 ".to_string(),
            "  =====/====\\===============/====\\=============  ".to_string(),
            caption,
        ],
        _ => vec![
            "        [TH]  lightning reloads      [AG]         ".to_string(),
            "       /|##|\\        * * *          /|??|\\        ".to_string(),
            "        /  \\                       _/  \\_         ".to_string(),
            "  =====/====\\=====================/======\\=====  ".to_string(),
            caption,
        ],
    };
    lines.insert(0, progress);
    lines.insert(0, status);
    lines.insert(0, ladder);
    lines
}

fn ragnarok_phase_hud(phase: &str, elapsed: Duration) -> (String, String) {
    let lower = phase.to_ascii_lowercase();
    let elapsed = format_elapsed(elapsed);
    if lower.contains("routing") {
        (
            "discover ✓  |  route ⚔  |  implement ·  |  review ·  |  judge ·".to_string(),
            format!("stage 2/5 ROUTING  · Thor selecting competitors · elapsed {elapsed}"),
        )
    } else if lower.contains("permission") {
        (
            "discover ✓  |  route ✓  |  implement ⚔  |  review ·  |  judge ·".to_string(),
            format!("gate THOR PERMISSION  · deciding ACP approval · elapsed {elapsed}"),
        )
    } else if lower.contains("implementation") {
        (
            "discover ✓  |  route ✓  |  implement ⚔  |  review ·  |  judge ·".to_string(),
            format!(
                "stage 3/5 IMPLEMENT  · competitors fight in parallel worktrees · elapsed {elapsed}"
            ),
        )
    } else if lower.contains("review") {
        (
            "discover ✓  |  route ✓  |  implement ✓  |  review ⚔  |  judge ·".to_string(),
            format!("stage 4/5 REVIEW  · adversarial reviewers inspect rivals · elapsed {elapsed}"),
        )
    } else if lower.contains("judgment") || lower.contains("judge") {
        (
            "discover ✓  |  route ✓  |  implement ✓  |  review ✓  |  judge ⚔".to_string(),
            format!(
                "stage 5/5 JUDGE  · Thor auditing reviews and choosing winner · elapsed {elapsed}"
            ),
        )
    } else {
        (
            "discover ⚔  |  route ·  |  implement ·  |  review ·  |  judge ·".to_string(),
            format!("stage 1/5 DISCOVER  · probing ACP agents and Elo models · elapsed {elapsed}"),
        )
    }
}

fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    format!("{:02}:{:02}", seconds / 60, seconds % 60)
}

fn ragnarok_progress_line(progress: Option<&RagnarokProgressSnapshot>) -> String {
    let Some(progress) = progress else {
        return "progress [--------------------] live ACP turn active".to_string();
    };
    let total = progress.total.max(1);
    let completed = progress.completed.min(total);
    let filled = completed * 20 / total;
    let empty = 20usize.saturating_sub(filled);
    format!(
        "progress [{}{}] {}/{} · {}",
        "#".repeat(filled),
        "-".repeat(empty),
        completed,
        progress.total,
        progress.detail
    )
}

async fn stop_animator(active: Option<(Arc<AtomicBool>, tokio::task::JoinHandle<()>)>) {
    if let Some((flag, handle)) = active {
        flag.store(false, Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }
}

fn emit_info(ui_tx: &mpsc::UnboundedSender<UiEvent>, message: String) {
    let _ = ui_tx.send(UiEvent::Info(message));
}

fn emit_warning(ui_tx: &mpsc::UnboundedSender<UiEvent>, message: String) {
    let _ = ui_tx.send(UiEvent::Warning(message));
}

fn preview_one_line(text: &str, max_chars: usize) -> String {
    truncate_chars(
        &text.split_whitespace().collect::<Vec<_>>().join(" "),
        max_chars,
    )
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out = text
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::ToolCallUpdateFields;

    fn launch(source_id: &str) -> AgentLaunch {
        AgentLaunch {
            source_id: source_id.to_string(),
            program: PathBuf::from("agent"),
            args: Vec::new(),
            env: HashMap::new(),
        }
    }

    fn candidate(agent: &str, name: &str, elo: u32, key: &str) -> Candidate {
        Candidate {
            candidate_id: String::new(),
            launch: launch(agent),
            model: ModelChoice {
                value: name.to_string(),
                name: name.to_string(),
                description: None,
                score: ModelScore {
                    elo,
                    provisional: false,
                },
                match_key: key.to_string(),
            },
        }
    }

    #[test]
    fn ranked_candidates_drop_duplicate_model_keys_and_assign_ids() {
        let ranked = ranked_unique_candidate_list(vec![
            candidate("a", "low", 1200, "vendor/low"),
            candidate("b", "same", 1500, "vendor/top"),
            candidate("a", "top", 1400, "vendor/top"),
        ]);

        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].candidate_id, "M1");
        assert_eq!(ranked[0].launch.source_id, "b");
        assert_eq!(ranked[0].model.match_key, "vendor/top");
        assert_eq!(ranked[1].candidate_id, "M2");
    }

    #[test]
    fn diverse_routing_slate_interleaves_acp_sources() {
        let mut candidates = vec![
            candidate("anvil", "anvil-1", 1500, "anvil-1"),
            candidate("anvil", "anvil-2", 1499, "anvil-2"),
            candidate("anvil", "anvil-3", 1498, "anvil-3"),
            candidate("codex-acp", "codex-1", 1400, "codex-1"),
            candidate("claude-acp", "claude-1", 1390, "claude-1"),
        ];
        for (index, candidate) in candidates.iter_mut().enumerate() {
            candidate.candidate_id = format!("M{}", index + 1);
        }

        let slate = diverse_routing_slate(&candidates);

        let sources = slate
            .iter()
            .map(|candidate| candidate.launch.source_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            sources,
            vec!["anvil", "codex-acp", "claude-acp", "anvil", "anvil"]
        );
    }

    #[test]
    fn thor_candidate_order_tries_other_sources_before_same_source_depth() {
        let mut candidates = vec![
            candidate("anvil", "anvil-1", 1500, "anvil-1"),
            candidate("anvil", "anvil-2", 1499, "anvil-2"),
            candidate("codex-acp", "codex-1", 1450, "codex-1"),
            candidate("claude-acp", "claude-1", 1440, "claude-1"),
            candidate("anvil", "anvil-3", 1430, "anvil-3"),
        ];
        for (index, candidate) in candidates.iter_mut().enumerate() {
            candidate.candidate_id = format!("M{}", index + 1);
        }

        let order = thor_candidate_order(&candidates);
        let ids = order
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["M1", "M3", "M4", "M2", "M5"]);
    }

    #[test]
    fn ragnarok_combat_frame_reports_phase_timer_and_progress() {
        let progress = RagnarokProgressSnapshot {
            completed: 2,
            total: 5,
            detail: "C2 implementation finished".to_string(),
        };

        let lines = ragnarok_combat_frame(
            4,
            "implementation melee",
            Duration::from_secs(65),
            Some(&progress),
        );

        assert!(
            lines
                .iter()
                .any(|line| line.contains("stage 3/5 IMPLEMENT"))
        );
        assert!(lines.iter().any(|line| line.contains("elapsed 01:05")));
        assert!(lines.iter().any(|line| line.contains("2/5")));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("C2 implementation finished"))
        );
        assert!(lines.iter().any(|line| line.contains("BAM")));
    }

    #[test]
    fn routing_validation_respects_thor_candidate_ids() {
        let mut candidates = vec![
            candidate("a", "top", 1500, "top"),
            candidate("b", "middle", 1490, "middle"),
            candidate("c", "low", 1300, "low"),
        ];
        candidates[0].candidate_id = "M1".to_string();
        candidates[1].candidate_id = "M2".to_string();
        candidates[2].candidate_id = "M3".to_string();

        let selected = validate_routing(
            &ThorRouting {
                competitors: 2,
                candidate_ids: vec!["M1".to_string(), "M3".to_string()],
                rationale: String::new(),
            },
            &candidates,
        )
        .expect("selected candidates");

        assert_eq!(selected[0].model.name, "top");
        assert_eq!(selected[1].model.name, "low");
    }

    #[test]
    fn routing_validation_rejects_duplicate_underlying_model() {
        let mut candidates = vec![
            candidate("a", "first", 1500, "same-model"),
            candidate("b", "second", 1490, "same-model"),
        ];
        candidates[0].candidate_id = "M1".to_string();
        candidates[1].candidate_id = "M2".to_string();

        let error = validate_routing(
            &ThorRouting {
                competitors: 2,
                candidate_ids: vec!["M1".to_string(), "M2".to_string()],
                rationale: String::new(),
            },
            &candidates,
        )
        .expect_err("duplicate model key should be rejected");

        assert!(error.to_string().contains("duplicate underlying model key"));
    }

    #[test]
    fn review_assignment_validation_rejects_self_review() {
        let implementation = ImplementationResult {
            competitor: Competitor {
                id: "C1".to_string(),
                candidate: candidate("a", "one", 1500, "one"),
                worktree: CreatedWorktree {
                    project_root: PathBuf::from("/repo"),
                    worktree_root: PathBuf::from("/repo/wt"),
                    session_cwd: PathBuf::from("/repo/wt"),
                    was_created: true,
                },
            },
            result: AgentTurnResult {
                final_text: String::new(),
                stop_reason: Some(StopReason::EndTurn),
            },
            status: "ok".to_string(),
            diff_summary: String::new(),
        };

        let err = validate_review_assignments(
            &[ReviewAssignment {
                reviewer_id: "C1".to_string(),
                target_id: "C1".to_string(),
                reason: String::new(),
            }],
            &[implementation],
        )
        .expect_err("self review must fail");

        assert!(err.to_string().contains("review itself"));
    }

    #[test]
    fn parse_json_object_accepts_markdown_wrapped_json() {
        #[derive(Debug, Deserialize, PartialEq, Eq)]
        struct Value {
            competitors: usize,
        }

        let parsed: Value = parse_json_object("```json\n{\"competitors\":2}\n```").unwrap();
        assert_eq!(parsed, Value { competitors: 2 });
    }

    #[tokio::test]
    async fn plan_approval_policy_allows_anvil_gate_once() {
        let tool_call = ToolCallUpdate::new(
            "plan",
            ToolCallUpdateFields::new().title("Approve plan before execution"),
        );
        let options = vec![
            PermissionOption::new("allow", "Approve", PermissionOptionKind::AllowOnce),
            PermissionOption::new("reject", "Reject", PermissionOptionKind::RejectOnce),
        ];
        let (ui_tx, _ui_rx) = mpsc::unbounded_channel();

        let decision = permission_decision(
            &PermissionPolicy::AllowPlanApprovals,
            &tool_call,
            &options,
            &ui_tx,
        )
        .await
        .expect("permission decision");

        assert_eq!(decision.as_deref(), Some("allow"));
    }

    #[tokio::test]
    async fn plan_approval_policy_rejects_actionable_spoofed_plan_title() {
        let tool_call = ToolCallUpdate::new(
            "shell",
            ToolCallUpdateFields::new()
                .title("Approve plan before execution")
                .kind(ToolKind::Execute),
        );
        let options = vec![
            PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
            PermissionOption::new("reject", "Reject", PermissionOptionKind::RejectOnce),
        ];
        let (ui_tx, _ui_rx) = mpsc::unbounded_channel();

        let decision = permission_decision(
            &PermissionPolicy::AllowPlanApprovals,
            &tool_call,
            &options,
            &ui_tx,
        )
        .await
        .expect("permission decision");

        assert_eq!(decision, None);
    }

    #[tokio::test]
    async fn plan_approval_policy_rejects_other_permission_requests() {
        let tool_call = ToolCallUpdate::new(
            "shell",
            ToolCallUpdateFields::new().title("Run shell command"),
        );
        let options = vec![
            PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
            PermissionOption::new("reject", "Reject", PermissionOptionKind::RejectOnce),
        ];
        let (ui_tx, _ui_rx) = mpsc::unbounded_channel();

        let decision = permission_decision(
            &PermissionPolicy::AllowPlanApprovals,
            &tool_call,
            &options,
            &ui_tx,
        )
        .await
        .expect("permission decision");

        assert_eq!(decision, None);
    }

    #[test]
    fn permission_choice_validation_accepts_only_listed_option_ids() {
        let options = vec![
            PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
            PermissionOption::new("reject", "Reject", PermissionOptionKind::RejectOnce),
        ];

        let accepted = validate_permission_choice(
            ThorPermissionChoice {
                option_id: Some("reject".to_string()),
                rationale: String::new(),
            },
            &options,
        )
        .expect("valid option id");
        assert_eq!(accepted.as_deref(), Some("reject"));

        let err = validate_permission_choice(
            ThorPermissionChoice {
                option_id: Some("root".to_string()),
                rationale: String::new(),
            },
            &options,
        )
        .expect_err("unknown option must fail");
        assert!(err.to_string().contains("unknown permission option_id"));
    }
}
