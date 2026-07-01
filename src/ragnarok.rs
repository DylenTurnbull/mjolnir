//! Local Ragnarok coordinator.
//!
//! `/ragnarok` is deliberately not forwarded to the active ACP session. The
//! UI supervisor starts this runner, which creates separate managed ACP
//! sessions through the same connection machinery that backs `mj mcp`.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use agent_client_protocol::schema::v1::{SessionConfigOption, StopReason};
use anyhow::Result;
use tokio::sync::mpsc;

use crate::acp::AcpRuntimeConfig;
use crate::app::{config_option_choices, config_option_current_value_id, is_model_config_option};
use crate::config::{self, Config, SelectedAgent};
use crate::event::UiEvent;
use crate::install;
use crate::mcp::{self, ManagedAcpConnection, ManagedTurnResult};
use crate::picker::{self, PickerOutcome, PickerPreferences};
use crate::registry;

const MAX_SCOUTED_AGENTS: usize = 5;
const SIMPLE_BRACKET_SIZE: usize = 2;
const COMPLEX_BRACKET_SIZE: usize = 3;
const TURN_TIMEOUT: Duration = Duration::from_secs(180);
const ANIMATION_TICK: Duration = Duration::from_millis(280);
const MAX_OUTPUT_CHARS: usize = 6_000;
const MAX_REVIEW_CHARS: usize = 4_000;

#[derive(Debug, Clone)]
pub(crate) struct RagnarokConfig {
    pub cwd: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub fs_max_text_bytes: u64,
}

#[derive(Debug, Clone)]
struct LaunchCandidate {
    source_id: String,
    label: String,
    program: PathBuf,
    args: Vec<String>,
    env: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct ModelChoice {
    config_id: String,
    value: String,
    name: String,
    description: Option<String>,
    current: bool,
}

struct Contender {
    id: usize,
    label: String,
    source_id: String,
    model: Option<ModelChoice>,
    conn: ManagedAcpConnection,
    output: String,
}

#[derive(Debug, Clone)]
struct Review {
    reviewer: String,
    text: String,
}

#[derive(Debug, Clone, Copy)]
enum TaskComplexity {
    Simple,
    Complex,
}

impl TaskComplexity {
    fn bracket_size(self) -> usize {
        match self {
            Self::Simple => SIMPLE_BRACKET_SIZE,
            Self::Complex => COMPLEX_BRACKET_SIZE,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Complex => "complex",
        }
    }
}

pub(crate) async fn run(task: String, cfg: RagnarokConfig, ui_tx: mpsc::UnboundedSender<UiEvent>) {
    let result = run_inner(task, cfg, &ui_tx).await;
    if let Err(error) = result {
        let _ = ui_tx.send(UiEvent::RagnarokFailed {
            message: format!("Ragnarok failed: {error:#}"),
        });
    }
}

async fn run_inner(
    task: String,
    cfg: RagnarokConfig,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Result<()> {
    let complexity = classify_task(&task);
    emit_frame(
        ui_tx,
        0,
        format!(
            "Thor sizes up the task: {}. Opening a {}-fighter bracket.",
            complexity.label(),
            complexity.bracket_size()
        ),
    );

    let candidates = discover_candidates().await;
    if candidates.is_empty() {
        anyhow::bail!("no configured ACP agents are available for Ragnarok");
    }
    let judge_candidate = candidates.first().cloned();

    let mut contenders = scout_contenders(&task, &cfg, complexity, candidates, ui_tx).await;
    if contenders.is_empty() {
        anyhow::bail!("no competitor ACP sessions could be started");
    }
    if contenders.len() == 1 {
        emit_frame(
            ui_tx,
            1,
            "Only one fighter answered the horn; Thor will still demand a final pass.",
        );
    }

    for (idx, contender) in contenders.iter_mut().enumerate() {
        let prompt = competitor_prompt(&task, contender);
        let status = format!("{} charges into the arena.", contender_title(contender));
        emit_frame(ui_tx, idx + 2, status);
        let overrides = model_override(contender.model.as_ref());
        let turn = run_turn(
            &contender.conn,
            prompt,
            overrides,
            ui_tx,
            format!(
                "{} is forging a candidate answer",
                contender_title(contender)
            ),
        )
        .await;
        contender.output = turn_text(turn);
    }

    let candidate_brief = candidate_brief(&contenders, MAX_OUTPUT_CHARS);
    let mut reviews = Vec::new();
    for contender in &contenders {
        let prompt = review_prompt(&task, &candidate_brief, contender);
        let turn = run_turn(
            &contender.conn,
            prompt,
            HashMap::new(),
            ui_tx,
            format!(
                "{} parries and looks for weak spots",
                contender_title(contender)
            ),
        )
        .await;
        reviews.push(Review {
            reviewer: contender_title(contender),
            text: turn_text(turn),
        });
    }

    let final_text = judge(&task, &cfg, judge_candidate, &contenders, &reviews, ui_tx).await;

    for contender in &contenders {
        contender.conn.disconnect().await;
    }

    let _ = ui_tx.send(UiEvent::RagnarokFinished { final_text });
    Ok(())
}

async fn scout_contenders(
    task: &str,
    cfg: &RagnarokConfig,
    complexity: TaskComplexity,
    candidates: Vec<LaunchCandidate>,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> Vec<Contender> {
    let target = complexity.bracket_size();
    let mut contenders = Vec::new();
    for (idx, candidate) in candidates.into_iter().take(MAX_SCOUTED_AGENTS).enumerate() {
        if contenders.len() >= target {
            break;
        }
        let label = candidate.label.clone();
        let runtime_cfg = runtime_config(&candidate, cfg);
        let connected = animate_while(ui_tx, format!("Thor scouts {label}"), async move {
            mcp::connect_managed(runtime_cfg).await
        })
        .await;
        let Ok((conn, connected)) = connected else {
            emit_frame(
                ui_tx,
                idx + 1,
                format!("{label} misses the call to battle."),
            );
            continue;
        };
        let _ = (
            &connected.agent_name,
            &connected.agent_version,
            &connected.session_id,
            connected.prompt_images_supported,
            connected.session_fork_supported,
        );
        let options = conn.config_options().await;
        let model = choose_model(task, &options);
        let model_label = model
            .as_ref()
            .map(|m| format!(" on {}", m.name))
            .unwrap_or_default();
        emit_frame(ui_tx, idx + 2, format!("Thor drafts {label}{model_label}."));
        contenders.push(Contender {
            id: contenders.len() + 1,
            label,
            source_id: candidate.source_id,
            model,
            conn,
            output: String::new(),
        });
    }
    contenders
}

async fn judge(
    task: &str,
    cfg: &RagnarokConfig,
    candidate: Option<LaunchCandidate>,
    contenders: &[Contender],
    reviews: &[Review],
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
) -> String {
    let Some(candidate) = candidate else {
        return fallback_judgment(contenders, reviews);
    };
    let runtime_cfg = runtime_config(&candidate, cfg);
    let connected = animate_while(
        ui_tx,
        "Thor climbs onto the judges' dais".to_string(),
        async move { mcp::connect_managed(runtime_cfg).await },
    )
    .await;
    let Ok((conn, _)) = connected else {
        return fallback_judgment(contenders, reviews);
    };
    let prompt = judge_prompt(task, contenders, reviews);
    let turn = run_turn(
        &conn,
        prompt,
        HashMap::new(),
        ui_tx,
        "Thor weighs every blow and counterblow".to_string(),
    )
    .await;
    conn.disconnect().await;
    let text = turn_text(turn);
    if text.trim().is_empty() {
        fallback_judgment(contenders, reviews)
    } else {
        text
    }
}

async fn run_turn(
    conn: &ManagedAcpConnection,
    prompt: String,
    config_overrides: HashMap<String, String>,
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    status: String,
) -> ManagedTurnResult {
    match conn.submit_prompt(prompt, config_overrides).await {
        Ok(_) => {
            animate_while(ui_tx, status, async move {
                conn.wait_result_rejecting_permissions(TURN_TIMEOUT).await
            })
            .await
        }
        Err(error) => ManagedTurnResult {
            final_text: String::new(),
            final_text_truncated: false,
            stop_reason: None,
            usage: None,
            error: Some(error),
        },
    }
}

async fn animate_while<T, F>(ui_tx: &mpsc::UnboundedSender<UiEvent>, status: String, future: F) -> T
where
    F: Future<Output = T>,
{
    tokio::pin!(future);
    let mut frame = 0usize;
    let mut tick = tokio::time::interval(ANIMATION_TICK);
    loop {
        tokio::select! {
            result = &mut future => return result,
            _ = tick.tick() => {
                emit_frame(ui_tx, frame, status.clone());
                frame = frame.wrapping_add(1);
            }
        }
    }
}

fn emit_frame(ui_tx: &mpsc::UnboundedSender<UiEvent>, frame: usize, status: impl Into<String>) {
    let art = COMBAT_FRAMES[frame % COMBAT_FRAMES.len()];
    let _ = ui_tx.send(UiEvent::RagnarokUpdate {
        text: format!("{art}\n\n{}", status.into()),
    });
}

const COMBAT_FRAMES: [&str; 6] = [
    r"       THOR
        \o/
         |
        / \
  [A] --==>        <==-- [B]",
    r"       THOR
         o  __
        /|\/
        / \
  [A]  ---==>    <==---  [B]",
    r"       THOR
      __ o
        \/|\
        / \
  [A]   ----==> <==----  [B]",
    r"       THOR
        \o/
     ____|____
        / \
  [A] <==--  !!  --==> [B]",
    r"       THOR
      \  o  /
       \ | /
        / \
  [A] ==--== sparks ==--== [B]",
    r"       THOR
         o
       --|--
        / \
  [A] <===>  clang!  <===> [B]",
];

async fn discover_candidates() -> Vec<LaunchCandidate> {
    let cfg = Config::load(&config::default_config_path()).unwrap_or_default();
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();

    if let Some(agent) = &cfg.agent {
        push_selected_candidate(&mut candidates, &mut seen, agent);
    }

    let registry = load_registry().await;
    let preferences = picker_preferences_from_config(&cfg);
    for (source_id, command) in picker::launch_plan(
        &registry,
        &registry::current_platform(),
        &install::default_install_root(),
        preferences,
    ) {
        let Some(command) = command else {
            continue;
        };
        if seen.insert(source_id.clone()) {
            candidates.push(LaunchCandidate {
                label: source_id.clone(),
                source_id,
                program: command.program,
                args: command.args,
                env: command.env,
            });
        }
    }

    for custom in &cfg.custom_agents {
        let source_id = format!("{}{}", config::CUSTOM_AGENT_SOURCE_PREFIX, custom.name);
        if seen.insert(source_id.clone()) {
            candidates.push(LaunchCandidate {
                source_id,
                label: custom.name.clone(),
                program: custom.program.clone(),
                args: custom.args.clone(),
                env: HashMap::new(),
            });
        }
    }

    candidates
}

fn push_selected_candidate(
    candidates: &mut Vec<LaunchCandidate>,
    seen: &mut HashSet<String>,
    agent: &SelectedAgent,
) {
    if seen.insert(agent.source_id.clone()) {
        candidates.push(LaunchCandidate {
            source_id: agent.source_id.clone(),
            label: agent.source_id.clone(),
            program: agent.program.clone(),
            args: agent.args.clone(),
            env: agent.env.clone(),
        });
    }
}

async fn load_registry() -> registry::Registry {
    match registry::load_with_cache(
        &registry::default_cache_path(),
        registry::CACHE_TTL,
        registry::REGISTRY_URL,
    )
    .await
    {
        Ok(registry) => registry,
        Err(error) => {
            tracing::warn!("Ragnarok registry load failed: {error:#}");
            registry::Registry::default()
        }
    }
}

fn picker_preferences_from_config(cfg: &Config) -> PickerPreferences {
    PickerPreferences {
        default_agent: cfg.agent.as_ref().map(selected_to_picker_outcome),
        favorite_source_ids: cfg.favorite_agents.clone(),
        custom_agents: cfg
            .custom_agents
            .iter()
            .map(|custom| picker::CustomAgent {
                name: custom.name.clone(),
                program: custom.program.clone(),
                args: custom.args.clone(),
                description: custom.description.clone(),
            })
            .collect(),
    }
}

fn selected_to_picker_outcome(agent: &SelectedAgent) -> PickerOutcome {
    PickerOutcome {
        source_id: agent.source_id.clone(),
        program: agent.program.clone(),
        args: agent.args.clone(),
        env: agent.env.clone(),
    }
}

fn runtime_config(candidate: &LaunchCandidate, cfg: &RagnarokConfig) -> AcpRuntimeConfig {
    AcpRuntimeConfig {
        command: candidate.program.clone(),
        args: candidate.args.clone(),
        cwd: cfg.cwd.clone(),
        additional_directories: cfg.additional_directories.clone(),
        resume_session: None,
        env: candidate.env.clone(),
        // Secondary tournament sessions should not race each other truncating a
        // user-specified stderr capture file.
        agent_stderr: None,
        fs_max_text_bytes: cfg.fs_max_text_bytes,
    }
}

fn classify_task(task: &str) -> TaskComplexity {
    let lower = task.to_lowercase();
    let mut score = task.split_whitespace().count() / 30;
    for keyword in [
        "architecture",
        "security",
        "regression",
        "refactor",
        "performance",
        "concurrency",
        "database",
        "migration",
        "review",
        "implement",
        "fix",
        "debug",
    ] {
        if lower.contains(keyword) {
            score += 1;
        }
    }
    if score >= 2 {
        TaskComplexity::Complex
    } else {
        TaskComplexity::Simple
    }
}

fn choose_model(task: &str, options: &[SessionConfigOption]) -> Option<ModelChoice> {
    let mut choices = Vec::new();
    for option in options
        .iter()
        .filter(|option| is_model_config_option(option))
    {
        let current = config_option_current_value_id(option).map(|id| id.to_string());
        if let Some(option_choices) = config_option_choices(option) {
            for choice in option_choices {
                let value = choice.value.to_string();
                choices.push(ModelChoice {
                    config_id: option.id.to_string(),
                    current: current.as_deref() == Some(value.as_str()),
                    value,
                    name: choice.name,
                    description: choice.description,
                });
            }
        }
    }
    choices
        .into_iter()
        .max_by_key(|choice| model_score(task, choice))
}

fn model_score(task: &str, choice: &ModelChoice) -> i32 {
    let haystack = format!(
        "{} {} {}",
        choice.value,
        choice.name,
        choice.description.as_deref().unwrap_or_default()
    )
    .to_lowercase();
    let speed_task = task.to_lowercase().contains("quick")
        || task.to_lowercase().contains("fast")
        || task.to_lowercase().contains("small");
    let mut score = if choice.current { 4 } else { 0 };
    for (needle, value) in [
        ("gpt-5", 12),
        ("opus", 11),
        ("o3", 10),
        ("sonnet", 8),
        ("pro", 7),
        ("max", 7),
        ("thinking", 6),
        ("reason", 6),
    ] {
        if haystack.contains(needle) {
            score += value;
        }
    }
    if speed_task {
        for (needle, value) in [("mini", 8), ("flash", 8), ("haiku", 8), ("fast", 6)] {
            if haystack.contains(needle) {
                score += value;
            }
        }
    }
    score
}

fn model_override(model: Option<&ModelChoice>) -> HashMap<String, String> {
    model
        .map(|model| HashMap::from([(model.config_id.clone(), model.value.clone())]))
        .unwrap_or_default()
}

fn competitor_prompt(task: &str, contender: &Contender) -> String {
    format!(
        "Ragnarok competitor brief.\n\
         You were selected by Thor as {name}.\n\
         Task:\n{task}\n\n\
         Produce the strongest candidate answer or implementation plan you can.\n\
         Constraints:\n\
         - Do not modify files.\n\
         - Do not ask for permissions.\n\
         - Be concrete and technically defensible.\n\
         - Include risks, tests, or validation when relevant.\n\
         Return only your candidate response.",
        name = contender_title(contender),
    )
}

fn review_prompt(task: &str, candidate_brief: &str, contender: &Contender) -> String {
    format!(
        "Ragnarok review round.\n\
         You are {name}. Review the candidate answers below for the task.\n\
         Task:\n{task}\n\n\
         Candidates:\n{candidate_brief}\n\n\
         Find correctness bugs, missing cases, weak assumptions, and practical risks.\n\
         Name the candidate you think should win, but prioritize concrete critique.",
        name = contender_title(contender),
    )
}

fn judge_prompt(task: &str, contenders: &[Contender], reviews: &[Review]) -> String {
    format!(
        "You are Thor, coordinator of Ragnarok.\n\
         Task:\n{task}\n\n\
         Candidate answers:\n{candidates}\n\n\
         Peer reviews:\n{reviews}\n\n\
         Pick the winning version or synthesize a better final from the strongest parts.\n\
         Present the answer to the user. Keep the selection rationale brief and concrete.",
        candidates = candidate_brief(contenders, MAX_OUTPUT_CHARS),
        reviews = review_brief(reviews),
    )
}

fn candidate_brief(contenders: &[Contender], limit: usize) -> String {
    contenders
        .iter()
        .map(|contender| {
            format!(
                "Candidate {id}: {title}\n{body}\n",
                id = contender.id,
                title = contender_title(contender),
                body = truncate_chars(&contender.output, limit),
            )
        })
        .collect::<Vec<_>>()
        .join("\n---\n")
}

fn review_brief(reviews: &[Review]) -> String {
    reviews
        .iter()
        .map(|review| {
            format!(
                "Review by {reviewer}\n{body}\n",
                reviewer = review.reviewer,
                body = truncate_chars(&review.text, MAX_REVIEW_CHARS),
            )
        })
        .collect::<Vec<_>>()
        .join("\n---\n")
}

fn fallback_judgment(contenders: &[Contender], reviews: &[Review]) -> String {
    let Some(best) = contenders
        .iter()
        .filter(|contender| !contender.output.trim().is_empty())
        .max_by_key(|contender| contender.output.len())
    else {
        return "Ragnarok could not produce a usable candidate answer.".to_string();
    };
    format!(
        "Ragnarok winner: {}\n\n{}\n\nPeer review notes considered:\n{}",
        contender_title(best),
        best.output,
        review_brief(reviews)
    )
}

fn contender_title(contender: &Contender) -> String {
    let model = contender
        .model
        .as_ref()
        .map(|model| format!(" / {}", model.name))
        .unwrap_or_default();
    format!("{}{} ({})", contender.label, model, contender.source_id)
}

fn turn_text(turn: ManagedTurnResult) -> String {
    if let Some(error) = turn.error {
        return format!("Tournament turn failed: {error}");
    }
    let mut text = turn.final_text;
    if turn.final_text_truncated {
        text.push_str("\n\n[truncated by tournament output limit]");
    }
    if !matches!(
        turn.stop_reason,
        Some(StopReason::EndTurn | StopReason::MaxTokens | StopReason::MaxTurnRequests)
    ) {
        text.push_str("\n\n[turn ended without a normal stop reason]");
    }
    if let Some(usage) = turn.usage {
        text.push_str(&format!(
            "\n\n[usage: {} input, {} output]",
            usage.input_tokens, usage.output_tokens
        ));
    }
    text
}

fn truncate_chars(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let mut out: String = text.chars().take(limit).collect();
    out.push_str("\n[truncated]");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complexity_promotes_risky_tasks() {
        assert!(matches!(
            classify_task("fix a security regression in concurrent session handling"),
            TaskComplexity::Complex
        ));
        assert!(matches!(
            classify_task("rename this heading"),
            TaskComplexity::Simple
        ));
    }

    #[test]
    fn truncate_chars_preserves_short_text() {
        assert_eq!(truncate_chars("abc", 5), "abc");
        assert_eq!(truncate_chars("abcdef", 3), "abc\n[truncated]");
    }
}
