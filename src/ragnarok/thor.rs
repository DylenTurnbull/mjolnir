//! Thor: spawned as a real ACP connection with `mj mcp` wired in as its own
//! MCP tool (`McpServer::Stdio` — spec-mandatory for every ACP agent, so no
//! capability negotiation is needed). Thor genuinely drives the
//! tournament — connecting competitors, feeding them the task, gating
//! their permission requests, assigning adversarial reviews, and judging a
//! winner — through real tool calls. This module spawns Thor's connection,
//! derives a best-effort combat-UI event stream by watching Thor's own
//! tool-call telemetry, and parses Thor's closing verdict.
//!
//! Tool-call telemetry is a *cosmetic* layer, not a load-bearing one: the
//! one event that actually matters (`TournamentDone`) comes from Thor's own
//! closing text, accumulated the same way for every turn regardless of
//! whether any individual tool call could be classified. If classification
//! misses a beat, the combat UI is a little quieter; the tournament still
//! runs and still produces a verdict.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::{
    McpServer, McpServerStdio, PermissionOption, PermissionOptionKind, SessionUpdate, StopReason,
    ToolCallStatus, ToolCallUpdate, ToolKind,
};
use tokio::sync::mpsc;

use crate::acp::{self, AcpRuntimeConfig};
use crate::event::{PermissionDecision, UiCommand, UiEvent};

use super::eligibility::{self, EligibleCompetitor};
use super::event::{
    JudgingOutcome, JudgingVerdict, RagnarokCommand, RagnarokEvent, ThorJudgment, TurnOutcome,
};

/// What the caller (`/ragnarok`'s dispatch in `ui.rs`) hands the
/// orchestrator. Deliberately thin — everything tournament-specific (which
/// agents, what models, how many competitors) is worked out inside
/// `run_tournament`, not configured from outside.
pub struct RagnarokConfig {
    pub task: String,
    pub project_root: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub agent_stderr: Option<PathBuf>,
    pub fs_max_text_bytes: u64,
    /// Reused from `AppState.score_store` rather than reloaded — it's
    /// already populated at startup and cheap to clone (`Arc`-backed).
    pub score_store: crate::scores::ScoreStore,
}

pub struct RagnarokTimeouts {
    pub eligibility_probe: Duration,
    /// Bounds Thor's *entire* turn — spawn, implement, review, judge all
    /// happen inside one long ACP prompt turn (see module docs on why one
    /// turn, not phase-by-phase prompting). Generous because a 10-competitor
    /// tournament with a full review round can genuinely take a while.
    pub thor_turn: Duration,
}

impl Default for RagnarokTimeouts {
    fn default() -> Self {
        Self {
            eligibility_probe: eligibility::ELIGIBILITY_PROBE_TIMEOUT,
            thor_turn: Duration::from_secs(60 * 60),
        }
    }
}

/// Spawn a tournament as a background task. Returns immediately; the caller
/// polls `RagnarokEvent`s off the receiver and can send
/// `RagnarokCommand::Cancel` to tear the whole thing down without waiting
/// for a verdict.
pub fn spawn(
    cfg: RagnarokConfig,
    timeouts: RagnarokTimeouts,
) -> (
    mpsc::UnboundedReceiver<RagnarokEvent>,
    mpsc::UnboundedSender<RagnarokCommand>,
) {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_tournament(cfg, timeouts, event_tx, cmd_rx));
    (event_rx, cmd_tx)
}

async fn run_tournament(
    cfg: RagnarokConfig,
    timeouts: RagnarokTimeouts,
    event_tx: mpsc::UnboundedSender<RagnarokEvent>,
    mut cmd_rx: mpsc::UnboundedReceiver<RagnarokCommand>,
) {
    let loaded_cfg = match crate::config::Config::load(&crate::config::default_config_path()) {
        Ok(c) => c,
        Err(e) => {
            let _ = event_tx.send(RagnarokEvent::Aborted {
                reason: format!("couldn't load mj config: {e:#}"),
            });
            return;
        }
    };
    ensure_score_catalog_ready(&loaded_cfg, &cfg.score_store).await;

    let candidates = eligibility::candidates_from_config(&loaded_cfg);
    let scan = eligibility::scan(
        candidates,
        &cfg.project_root,
        &cfg.score_store,
        timeouts.eligibility_probe,
    )
    .await;

    if scan.eligible.len().saturating_sub(1) < super::MIN_COMPETITORS {
        let mut reason = format!(
            "found {} eligible (agent, model) pair(s) with an Elo score; need at least {} total ({} competitors plus one Thor judge). \
             Configure another ACP agent, or wait for a model to appear on the LMArena leaderboard.",
            scan.eligible.len(),
            super::MIN_COMPETITORS + 1,
            super::MIN_COMPETITORS,
        );
        if !scan.skipped.is_empty() {
            let details: Vec<String> = scan
                .skipped
                .iter()
                .map(|s| format!("{}: {}", s.agent_label, s.reason))
                .collect();
            reason.push_str(&format!(" Skipped: {}.", details.join("; ")));
        }
        let _ = event_tx.send(RagnarokEvent::Aborted { reason });
        return;
    }
    // Thor is voiced by the single highest-Elo eligible entry and never
    // competes -- judging its own work would be exactly the conflict of
    // interest constraint #6 rules out between competitors.
    let (thor_voice, competitor_pool) = split_thor_voice_and_competitors(&scan.eligible);
    let _ = event_tx.send(RagnarokEvent::EligibilityScanned {
        eligible_count: competitor_pool.len(),
    });
    let _ = event_tx.send(RagnarokEvent::ThorActivity {
        summary: format!(
            "summoning thor via {}/{} ({} elo)",
            thor_voice.agent_label, thor_voice.model_name, thor_voice.elo
        ),
    });
    let mut allowed_ids: Vec<String> = competitor_pool
        .iter()
        .map(|c| c.agent_source_id.clone())
        .collect();
    allowed_ids.sort();
    allowed_ids.dedup();

    let mj_exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            let _ = event_tx.send(RagnarokEvent::Aborted {
                reason: format!("couldn't resolve the mj binary path: {e}"),
            });
            return;
        }
    };

    let runtime_cfg = AcpRuntimeConfig {
        command: thor_voice.program.clone(),
        args: thor_voice.args.clone(),
        cwd: cfg.project_root.clone(),
        additional_directories: cfg.additional_directories.clone(),
        resume_session: None,
        env: thor_voice.env.clone(),
        agent_stderr: cfg.agent_stderr.clone(),
        fs_max_text_bytes: cfg.fs_max_text_bytes,
        // Stdio MCP is spec-mandatory for every ACP agent -- no capability
        // check needed before wiring this in. `--cwd` is a *top-level* mj
        // flag (not global), so it must precede the `mcp` subcommand token.
        mcp_servers: vec![McpServer::Stdio(McpServerStdio::new("mj", mj_exe).args(
            vec![
                "--cwd".to_string(),
                cfg.project_root.display().to_string(),
                "mcp".to_string(),
                "--allowed-agents".to_string(),
                allowed_ids.join(","),
                "--require-worktree-cwd".to_string(),
            ],
        ))],
    };

    let (thor_event_tx, mut thor_event_rx) = mpsc::unbounded_channel();
    let (thor_cmd_tx, thor_cmd_rx) = mpsc::unbounded_channel();
    let thor_runtime =
        tokio::spawn(async move { acp::run(runtime_cfg, thor_event_tx, thor_cmd_rx).await });

    let prompt = build_prompt(&cfg.task, &competitor_pool);
    let mut driver = ThorDriver::new(&competitor_pool, &event_tx);
    let mut prompt_sent = false;
    let mut final_text = String::new();
    let mut aborted_reason: Option<String> = None;
    let mut reported_thought = false;
    let mut reported_message = false;

    let drive = async {
        loop {
            tokio::select! {
                biased;
                // The only command is Cancel; resolving a tied verdict is
                // handled entirely client-side (`ui.rs`) since by the time a
                // pick is possible, Thor's turn has already ended.
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(RagnarokCommand::Cancel) | None => return,
                    }
                }
                event = thor_event_rx.recv() => {
                    let Some(event) = event else { return };
                    match event {
                        UiEvent::SessionStarted { .. } if !prompt_sent => {
                            prompt_sent = true;
                            let _ = event_tx.send(RagnarokEvent::ThorActivity {
                                summary: "thor connected; tournament prompt sent".to_string(),
                            });
                            let _ = thor_cmd_tx.send(UiCommand::SendPrompt {
                                text: prompt.clone(),
                                images: Vec::new(),
                            });
                        }
                        UiEvent::SessionUpdate(update) => {
                            match &update {
                                SessionUpdate::AgentMessageChunk(chunk) => {
                                    let text = crate::event::content_block_text(&chunk.content);
                                    final_text.push_str(&text);
                                    if !reported_message
                                        && let Some(summary) = summarize_thor_text("thor says", &text)
                                    {
                                        reported_message = true;
                                        let _ = event_tx.send(RagnarokEvent::ThorActivity { summary });
                                    }
                                }
                                SessionUpdate::AgentThoughtChunk(chunk) if !reported_thought => {
                                    let text = crate::event::content_block_text(&chunk.content);
                                    if let Some(summary) =
                                        summarize_thor_text("thor thinking", &text)
                                    {
                                        reported_thought = true;
                                        let _ =
                                            event_tx.send(RagnarokEvent::ThorActivity { summary });
                                    }
                                }
                                _ => {}
                            }
                            driver.observe(update);
                        }
                        UiEvent::PermissionRequest(prompt) => {
                            // Thor can still ask for its own ACP tool
                            // permissions while supervising. Auto-allow only
                            // non-mutating/MCP-shaped requests; reject
                            // execute/edit/delete/move so the judge cannot
                            // silently mutate the user's real checkout.
                            // Competitor permissions are handled separately
                            // through Thor's respond_permission MCP calls
                            // (observed, not answered, by `driver`).
                            let decision =
                                thor_permission_decision(&prompt.tool_call, &prompt.options);
                            let _ = prompt.responder.send(decision);
                        }
                        UiEvent::PromptDone { stop_reason, .. } => {
                            if matches!(stop_reason, StopReason::Cancelled) {
                                aborted_reason = Some("thor's turn was cancelled".to_string());
                            }
                            return;
                        }
                        UiEvent::PromptFailed { message } | UiEvent::Fatal(message) => {
                            aborted_reason = Some(format!("thor's connection failed: {message}"));
                            return;
                        }
                        _ => {}
                    }
                }
            }
        }
    };

    if tokio::time::timeout(timeouts.thor_turn, drive)
        .await
        .is_err()
    {
        aborted_reason = Some("thor's turn timed out".to_string());
    }

    let _ = thor_cmd_tx.send(UiCommand::Shutdown);
    let _ = tokio::time::timeout(Duration::from_secs(5), thor_runtime).await;

    match aborted_reason {
        Some(reason) => {
            let _ = event_tx.send(RagnarokEvent::Aborted { reason });
        }
        None => {
            let verdict = validate_judgment(&final_text);
            let _ = event_tx.send(RagnarokEvent::TournamentDone { verdict });
        }
    }
}

fn split_thor_voice_and_competitors(
    eligible: &[EligibleCompetitor],
) -> (EligibleCompetitor, Vec<EligibleCompetitor>) {
    let thor_voice = eligible
        .first()
        .expect("caller checks at least one eligible entry")
        .clone();
    let competitor_pool = eligible
        .iter()
        .skip(1)
        .cloned()
        .collect::<Vec<EligibleCompetitor>>();
    (thor_voice, competitor_pool)
}

async fn ensure_score_catalog_ready(
    loaded_cfg: &crate::config::Config,
    score_store: &crate::scores::ScoreStore,
) {
    if score_store.is_active() || !loaded_cfg.scores.enabled {
        return;
    }
    let cache_path = crate::scores::default_cache_path();
    let url = loaded_cfg
        .scores
        .url
        .as_deref()
        .unwrap_or(crate::scores::DEFAULT_SCORES_URL);
    let file = crate::scores::load_scores_file(&cache_path, crate::scores::CACHE_TTL, url).await;
    score_store.install(crate::scores::ScoreCatalog::build(
        &file,
        loaded_cfg.scores.overrides.clone(),
        loaded_cfg.scores.enabled,
    ));
}

fn thor_permission_decision(
    tool_call: &ToolCallUpdate,
    options: &[PermissionOption],
) -> PermissionDecision {
    let allow = matches!(
        tool_call.fields.kind.unwrap_or(ToolKind::Other),
        ToolKind::Read | ToolKind::Search | ToolKind::Think | ToolKind::Fetch | ToolKind::Other
    );
    if allow {
        return choose_permission_option(
            options,
            &[
                PermissionOptionKind::AllowOnce,
                PermissionOptionKind::AllowAlways,
            ],
        )
        .map(PermissionDecision::Selected)
        .unwrap_or(PermissionDecision::Cancelled);
    }
    choose_permission_option(
        options,
        &[
            PermissionOptionKind::RejectOnce,
            PermissionOptionKind::RejectAlways,
        ],
    )
    .map(PermissionDecision::Selected)
    .unwrap_or(PermissionDecision::Cancelled)
}

fn choose_permission_option(
    options: &[PermissionOption],
    kinds: &[PermissionOptionKind],
) -> Option<String> {
    kinds.iter().find_map(|kind| {
        options
            .iter()
            .find(|option| option.kind == *kind)
            .map(|option| option.option_id.to_string())
    })
}

/// Builds Thor's one and only prompt: the task, the pool it must pick from,
/// and the full protocol. Long by necessity -- this *is* the orchestration
/// logic for everything past the eligibility scan, expressed as
/// instructions rather than Rust control flow.
fn build_prompt(task: &str, eligible: &[EligibleCompetitor]) -> String {
    let pool = eligible
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "{}. agent=\"{}\" ({}) model=\"{}\" elo={}",
                i + 1,
                c.agent_source_id,
                c.agent_label,
                c.model_value,
                c.elo
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"You are Thor, running a competitive multi-agent implementation tournament ("Ragnarok"). You have the `mj` MCP server available as a tool, exposing: create_worktree, connect, set_config_option, submit_prompt, poll_progress, respond_permission, get_result, cancel_prompt, disconnect, list_connections. You may use your own read/search tools for inspection, but do not use terminal or editing tools; those permission requests are denied by the supervisor.

TASK (send this exact text, verbatim, to every competitor -- do not edit, summarize, or add your own commentary to it):
---
{task}
---

ELIGIBLE POOL (you may only pick agents/models from this list -- the server enforces allowed agent ids and worktree cwd; you must enforce the listed model choices):
{pool}

PROTOCOL -- follow these steps in order:

1. Decide how many competitors K to run (between {min} and {max} inclusive) based on this task's complexity. A trivial task might only need {min}; a large, ambiguous, or high-stakes task benefits from more. Use your own judgement.

2. Pick K entries from the eligible pool above. Every competitor must use a DIFFERENT model. Prefer different agents when Elo scores are close, but a single agent offering multiple distinct models is fine if that's what the pool has. Prefer higher-Elo entries, but you may trade some Elo for agent diversity when it's close.

3. For each of your K picks, in this order, before polling any of them:
   a. Call create_worktree with branch_name "ragnarok/<a short slug for this task>/c<N>" where N is 1, 2, 3... in the order you pick competitors (so the first is c1, second is c2, etc.) -- this exact naming matters, later steps depend on it.
   b. Call connect with that competitor's agent (from the pool, by its `agent=` value) and cwd set to the worktree path create_worktree just returned.
   c. Call set_config_option on that connection to select the competitor's assigned model (from the pool, by its `model=` value).
   d. Call submit_prompt on that connection with the TASK text above, verbatim.
   Do all of a-d for every competitor before moving to step 4 -- they run concurrently once connected, so spawning all of them first is what makes this actually parallel.

4. Poll every competitor via poll_progress / get_result until each either finishes or clearly fails. While polling, you WILL see permission requests (session/request_permission surfaced through poll_progress). Every one of these MUST be answered with respond_permission -- approve actions that are reasonable and in-scope for the task (file edits, running tests, installing a stated dependency), deny anything destructive, unrelated to the task, or outside the competitor's own worktree. Never leave one unanswered. This is your most important responsibility: you are the only thing standing between an unattended competitor and its own mistakes.

5. Once every competitor has a result, assign reviews using EXACTLY this rule (do not invent your own pairing): competitor c<i> reviews competitor c<((i mod K)+1)>'s work. For example, at K=4: c1 reviews c2, c2 reviews c3, c3 reviews c4, c4 reviews c1. At K=5: c1 reviews c2, c2 reviews c3, c3 reviews c4, c4 reviews c5, c5 reviews c1. This guarantees nobody reviews their own work and every implementation gets exactly one review.

6. For each review pairing, connect the REVIEWER's own agent/model again in its own isolated review worktree: call create_worktree with branch_name "ragnarok/<the same slug>/review-c<N>" for the reviewer slot, then connect with cwd set to that review worktree path. Never connect a reviewer to the project root or to the reviewee's worktree. Prompt it with the original task plus the reviewee's actual changed files or implementation summary gathered through safe read/search inspection, and ask it to adversarially critique correctness, completeness, and quality against the original task. Same respond_permission obligation applies to reviewer connections.

7. Once all reviews are back, judge. Read every competitor's actual diff yourself (not just their own summary) and read every review. Fact-check each review's specific claims against the real diff -- do not take a review's word for it; note if a review is exaggerated, wrong, or dishonest. Pick a winner based on which implementation best satisfies the original task. If you are genuinely torn between two, say so rather than forcing a pick.

8. End your turn with a short paragraph of reasoning, then a single fenced ```json code block matching exactly this shape (no other fields, no comments inside the JSON):

```json
{{
  "verdict": "clear_winner",
  "winner_slot": "c2",
  "finalist_slots": [],
  "review_assessments": [
    {{"reviewer_slot": "c1", "reviewee_slot": "c2", "credible": true, "note": "one line on whether this review's claims held up against the real diff"}}
  ],
  "reasoning": "one or two sentences on why this competitor won"
}}
```

Or, if genuinely tied:

```json
{{
  "verdict": "tied_best_two",
  "winner_slot": null,
  "finalist_slots": ["c2", "c4"],
  "review_assessments": [],
  "reasoning": "why you couldn't separate these two"
}}
```

Include a review_assessments entry for every review that happened, not just the ones relevant to your finalists."#,
        task = task,
        pool = pool,
        min = super::MIN_COMPETITORS,
        max = super::MAX_COMPETITORS,
    )
}

/// Extract the fenced ```json block from Thor's closing text (falling back
/// to the first balanced `{...}` span), and validate+map it to a
/// `JudgingOutcome`. Malformed or structurally invalid replies fall back to
/// presenting `c1`/`c2` (the top-2-by-Elo picks, per the naming convention
/// step 3 of the prompt instructs Thor to use) as tied finalists --
/// reusing constraint #8's own "let the user decide" as the safe default,
/// never retried against the LLM (no better odds, and retrying burns the
/// time the parallel design is explicitly trying to save).
fn validate_judgment(raw: &str) -> JudgingOutcome {
    let Some(judgment) =
        extract_json_block(raw).and_then(|block| serde_json::from_str::<ThorJudgment>(&block).ok())
    else {
        return fallback("thor's closing reply did not contain a valid, parseable JSON block");
    };
    match judgment.verdict {
        JudgingVerdict::ClearWinner => match judgment.winner_slot.clone() {
            Some(slot) if !slot.trim().is_empty() => JudgingOutcome::ClearWinner {
                winner_slot: slot,
                judgment,
            },
            _ => fallback("thor declared a clear winner but didn't name a slot"),
        },
        JudgingVerdict::TiedBestTwo => {
            let mut slots: Vec<String> = judgment
                .finalist_slots
                .iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            slots.dedup();
            if slots.len() == 2 {
                JudgingOutcome::TiedBestTwo {
                    finalist_slots: [slots.remove(0), slots.remove(0)],
                    judgment: Some(judgment),
                }
            } else {
                fallback("thor declared a tie but didn't name exactly 2 distinct finalist slots")
            }
        }
    }
}

fn fallback(reason: &str) -> JudgingOutcome {
    JudgingOutcome::FellBack {
        reason: reason.to_string(),
        finalist_slots: ["c1".to_string(), "c2".to_string()],
    }
}

/// Finds a ```json fenced block first; falls back to the first balanced
/// `{...}` span in the text (some models omit the fence under instruction
/// pressure but still emit valid JSON). Returns `None` for pure prose.
fn extract_json_block(text: &str) -> Option<String> {
    if let Some(start) = text.find("```json") {
        let after = &text[start + "```json".len()..];
        if let Some(end) = after.find("```") {
            return Some(after[..end].trim().to_string());
        }
    }
    let start = text.find('{')?;
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    for (offset, &b) in bytes[start..].iter().enumerate() {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(text[start..start + offset + 1].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

/// Best-effort classification of one of Thor's own tool calls, and the
/// resulting `RagnarokEvent` derivation. See module docs: this is a
/// cosmetic layer over real telemetry, not something the tournament's
/// actual outcome depends on.
struct ThorDriver<'a> {
    eligible: &'a [EligibleCompetitor],
    event_tx: &'a mpsc::UnboundedSender<RagnarokEvent>,
    /// `tool_call_id` -> its `raw_input`, retained across updates so a
    /// later `ToolCallUpdate` (which may omit `raw_input`) can still be
    /// classified using the input first seen on the initial `ToolCall`.
    pending_input: HashMap<String, serde_json::Value>,
    /// mj-mcp `connection_id` -> (our own `c<N>` slot label, when we first
    /// saw it), learned from a completed `connect` call's `raw_output`.
    /// Absent for reviewer connections (see `on_connect`) — only
    /// competitors get a roster slot.
    connection_slots: HashMap<String, (String, Instant)>,
    /// Competitor worktree paths seen from completed `create_worktree` calls.
    /// Review worktrees are deliberately excluded so their follow-up
    /// `connect` calls are treated as the review phase, not new roster rows.
    competitor_worktrees: std::collections::HashSet<String>,
    /// Set once the first reviewer-shaped `connect` fires, so
    /// `ReviewPhaseStarted` is sent exactly once.
    review_phase_announced: bool,
    /// `tool_call_id`s already classified on completion, so a repeated
    /// `ToolCallUpdate` for the same call (e.g. a second `Completed`
    /// notification) doesn't emit duplicate events.
    settled: std::collections::HashSet<String>,
    /// Tool calls already announced when they started. Classified completion
    /// events can arrive much later; announcing starts keeps the arena from
    /// looking dead while Thor is inside a long MCP call.
    announced_tool_calls: std::collections::HashSet<String>,
    /// mj-mcp `connection_id` -> last activity summary sent, so repeated
    /// polling of an unchanged state doesn't re-emit the same
    /// `CompetitorActivity` on every `poll_progress` call.
    last_activity: HashMap<String, String>,
}

impl<'a> ThorDriver<'a> {
    fn new(
        eligible: &'a [EligibleCompetitor],
        event_tx: &'a mpsc::UnboundedSender<RagnarokEvent>,
    ) -> Self {
        Self {
            eligible,
            event_tx,
            pending_input: HashMap::new(),
            last_activity: HashMap::new(),
            connection_slots: HashMap::new(),
            competitor_worktrees: std::collections::HashSet::new(),
            review_phase_announced: false,
            settled: std::collections::HashSet::new(),
            announced_tool_calls: std::collections::HashSet::new(),
        }
    }

    fn observe(&mut self, update: SessionUpdate) {
        match update {
            SessionUpdate::ToolCall(call) => {
                let id = call.tool_call_id.to_string();
                self.announce_tool_call(&id, &call.title);
                if let Some(input) = &call.raw_input {
                    self.pending_input.insert(id.clone(), input.clone());
                }
                if matches!(
                    call.status,
                    ToolCallStatus::Completed | ToolCallStatus::Failed
                ) {
                    self.settle(&id, call.raw_output.as_ref());
                }
            }
            SessionUpdate::ToolCallUpdate(update) => {
                let id = update.tool_call_id.to_string();
                if let Some(title) = &update.fields.title {
                    self.announce_tool_call(&id, title);
                }
                if let Some(input) = &update.fields.raw_input {
                    self.pending_input.insert(id.clone(), input.clone());
                }
                if matches!(
                    update.fields.status,
                    Some(ToolCallStatus::Completed) | Some(ToolCallStatus::Failed)
                ) {
                    self.settle(&id, update.fields.raw_output.as_ref());
                }
            }
            _ => {}
        }
    }

    fn announce_tool_call(&mut self, id: &str, title: &str) {
        if title.trim().is_empty() || !self.announced_tool_calls.insert(id.to_string()) {
            return;
        }
        let _ = self.event_tx.send(RagnarokEvent::ThorActivity {
            summary: format!("thor tool: {}", truncate_for_feed(title.trim(), 72)),
        });
    }

    fn settle(&mut self, tool_call_id: &str, raw_output: Option<&serde_json::Value>) {
        if !self.settled.insert(tool_call_id.to_string()) {
            return;
        }
        let Some(input) = self.pending_input.get(tool_call_id).cloned() else {
            return;
        };
        // `get_result`'s output has a distinctive shape (`turn_status` +
        // `final_text` together, from `GetResultView` in mcp.rs) that no
        // other tool's result carries -- checked against raw_output first
        // since `get_result`'s raw_input alone is indistinguishable from
        // poll_progress/cancel_prompt/disconnect/list_config_options.
        if let Some(output) = raw_output
            && output.get("turn_status").and_then(|v| v.as_str()).is_some()
            && output.get("final_text").is_some()
        {
            self.on_get_result(&input, output);
            return;
        }
        // `poll_progress`'s output (`PollResult` in mcp.rs) has its own
        // distinctive shape -- `connection_status` + `items` together --
        // that neither `get_result`'s `GetResultView` (no `items` array,
        // `final_text` not `final_text_so_far`) nor anything else shares.
        if let Some(output) = raw_output
            && output.get("connection_status").is_some()
            && output.get("items").and_then(|v| v.as_array()).is_some()
        {
            self.on_poll_progress(&input, output);
            return;
        }
        match classify(&input) {
            ClassifiedCall::CreateWorktree => self.on_create_worktree(raw_output),
            ClassifiedCall::Connect { agent } => self.on_connect(&input, &agent, raw_output),
            ClassifiedCall::SetConfigOption {
                connection_id,
                value,
            } => self.on_model_chosen(&connection_id, &value),
            ClassifiedCall::RespondPermission { connection_id } => {
                self.on_permission_decision(&connection_id, raw_output)
            }
            ClassifiedCall::Unclassified => {}
        }
    }

    fn on_create_worktree(&mut self, raw_output: Option<&serde_json::Value>) {
        let Some(output) = raw_output else {
            return;
        };
        let branch_name = output
            .get("branch_name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let Some(path) = output.get("worktree_path").and_then(|v| v.as_str()) else {
            return;
        };
        if is_competitor_branch_name(branch_name) {
            self.competitor_worktrees.insert(path.to_string());
        }
    }

    fn on_connect(
        &mut self,
        raw_input: &serde_json::Value,
        agent: &str,
        raw_output: Option<&serde_json::Value>,
    ) {
        let Some(connection_id) = raw_output
            .and_then(|o| o.get("connection_id"))
            .and_then(|v| v.as_str())
        else {
            return;
        };
        let cwd = raw_input.get("cwd").and_then(|v| v.as_str());
        let is_competitor = cwd.is_some_and(|c| self.competitor_worktrees.contains(c));

        if !is_competitor {
            // A reviewer connection: no roster slot (see the struct doc for
            // why per-pair attribution isn't attempted), just the
            // tournament-wide phase signal, sent once.
            if !self.connection_slots.is_empty() && !self.review_phase_announced {
                self.review_phase_announced = true;
                let _ = self.event_tx.send(RagnarokEvent::ReviewPhaseStarted);
            }
            return;
        }

        let slot = format!("c{}", self.connection_slots.len() + 1);
        self.connection_slots
            .insert(connection_id.to_string(), (slot.clone(), Instant::now()));
        let elo = self
            .eligible
            .iter()
            .find(|c| c.agent_source_id == agent)
            .map(|c| c.elo)
            .unwrap_or(0);
        let _ = self.event_tx.send(RagnarokEvent::CompetitorStarted {
            slot,
            agent_label: agent.to_string(),
            model_name: "(selecting model...)".to_string(),
            elo,
        });
    }

    fn on_model_chosen(&mut self, connection_id: &str, model_value: &str) {
        let Some((slot, _)) = self.connection_slots.get(connection_id) else {
            return;
        };
        // model_value alone doesn't say which agent it belongs to; scan for
        // any eligible entry with this exact model value; imprecise if two
        // agents share a model name, but this only affects a cosmetic label.
        let elo = self
            .eligible
            .iter()
            .find(|c| c.model_value == model_value)
            .map(|c| c.elo)
            .unwrap_or(0);
        let _ = self.event_tx.send(RagnarokEvent::CompetitorModelChosen {
            slot: slot.clone(),
            model_name: model_value.to_string(),
            elo,
        });
    }

    fn on_permission_decision(
        &mut self,
        connection_id: &str,
        raw_output: Option<&serde_json::Value>,
    ) {
        let Some((slot, _)) = self.connection_slots.get(connection_id) else {
            return;
        };
        let approved = raw_output
            .and_then(|o| o.get("approved"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let summary = raw_output
            .and_then(|o| o.get("title"))
            .and_then(|v| v.as_str())
            .unwrap_or("a permission request")
            .to_string();
        let _ = self.event_tx.send(RagnarokEvent::PermissionDecision {
            slot: slot.clone(),
            approved,
            summary,
        });
    }

    /// `raw_input` here is `GetResultArgs`-shaped (`{connection_id,
    /// wait_ms?}`); `raw_output` is the `GetResultView` already confirmed
    /// present by the caller. Only a *terminal* `turn_status` ("done" /
    /// "failed") is a real finish -- Thor may call `get_result` repeatedly
    /// while polling a still-running turn, and each of those is its own
    /// tool call (so `settled` alone wouldn't dedup them).
    fn on_get_result(&mut self, raw_input: &serde_json::Value, raw_output: &serde_json::Value) {
        let Some(connection_id) = raw_input.get("connection_id").and_then(|v| v.as_str()) else {
            return;
        };
        let Some((slot, started_at)) = self.connection_slots.get(connection_id).cloned() else {
            return;
        };
        let turn_status = raw_output
            .get("turn_status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let outcome = match turn_status {
            "done" => TurnOutcome::Completed,
            "failed" => TurnOutcome::Failed {
                message: raw_output
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("turn failed")
                    .to_string(),
            },
            _ => return, // still running/idle/awaiting_permission: not finished yet
        };
        let _ = self.event_tx.send(RagnarokEvent::CompetitorFinished {
            slot,
            outcome,
            duration: started_at.elapsed(),
        });
    }

    /// `raw_input` is `PollArgs`-shaped (`{connection_id, since_seq?}`);
    /// `raw_output` is the `PollResult` already confirmed present by the
    /// caller. Pulls a one-line summary out of the *last* item in
    /// `items` (real `ProgressItem` telemetry: a tool call's title, or a
    /// snippet of agent message/thought text) and sends it only if it
    /// differs from the last summary sent for this connection, so
    /// unchanged polling doesn't spam the roster with duplicates.
    fn on_poll_progress(&mut self, raw_input: &serde_json::Value, raw_output: &serde_json::Value) {
        let Some(connection_id) = raw_input.get("connection_id").and_then(|v| v.as_str()) else {
            return;
        };
        let Some((slot, _)) = self.connection_slots.get(connection_id).cloned() else {
            return;
        };
        let Some(last_item) = raw_output
            .get("items")
            .and_then(|v| v.as_array())
            .and_then(|items| items.last())
        else {
            return;
        };
        let Some(summary) = summarize_progress_item(last_item) else {
            return;
        };
        if self.last_activity.get(connection_id) == Some(&summary) {
            return;
        }
        self.last_activity
            .insert(connection_id.to_string(), summary.clone());
        let _ = self
            .event_tx
            .send(RagnarokEvent::CompetitorActivity { slot, summary });
    }
}

fn is_competitor_branch_name(branch_name: &str) -> bool {
    branch_name
        .rsplit('/')
        .next()
        .and_then(|last| last.strip_prefix('c'))
        .is_some_and(|digits| !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit()))
}

/// One-line, honest summary of a `ProgressItem` (see mcp.rs), truncated for
/// a roster row. `None` for item kinds that don't have anything worth
/// showing as ongoing activity (permission requests/warnings surface
/// through their own dedicated events elsewhere).
fn summarize_progress_item(item: &serde_json::Value) -> Option<String> {
    match item.get("type").and_then(|v| v.as_str())? {
        "tool_call" | "tool_call_update" => {
            let title = item.get("title").and_then(|v| v.as_str())?;
            Some(truncate_for_feed(title, 48))
        }
        "agent_message" => {
            let text = item.get("text").and_then(|v| v.as_str())?;
            Some(truncate_for_feed(text.trim(), 48))
        }
        "agent_thought" => {
            let text = item.get("text").and_then(|v| v.as_str())?;
            Some(format!("thinking: {}", truncate_for_feed(text.trim(), 48)))
        }
        _ => None,
    }
}

fn summarize_thor_text(prefix: &str, text: &str) -> Option<String> {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return None;
    }
    Some(format!("{prefix}: {}", truncate_for_feed(&collapsed, 96)))
}

fn truncate_for_feed(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_string()
    } else {
        format!("{}...", text.chars().take(max_chars).collect::<String>())
    }
}

enum ClassifiedCall {
    CreateWorktree,
    Connect {
        agent: String,
    },
    SetConfigOption {
        connection_id: String,
        value: String,
    },
    RespondPermission {
        connection_id: String,
    },
    Unclassified,
}

/// Classifies one tool call purely from its `raw_input` JSON shape --
/// deliberately not from `title` (rendering varies by agent) or from
/// hardcoding a tool name (nothing in ACP guarantees one is present
/// alongside `raw_input`). Each of mj-mcp's own arg structs has a
/// sufficiently distinct field combination that shape-sniffing is reliable
/// for the handful of calls this module actually cares about (`get_result`
/// is classified separately in `settle`, by its distinctive *output*
/// shape); calls this can't place (poll_progress/cancel_prompt/disconnect/
/// list_config_options, which all share a `{"connection_id": ...}`-only
/// shape) are left `Unclassified` rather than guessed at.
fn classify(raw_input: &serde_json::Value) -> ClassifiedCall {
    let obj = match raw_input.as_object() {
        Some(o) => o,
        None => return ClassifiedCall::Unclassified,
    };
    let str_field = |key: &str| obj.get(key).and_then(|v| v.as_str());

    if obj.contains_key("branch_name") && !obj.contains_key("connection_id") {
        return ClassifiedCall::CreateWorktree;
    }
    if obj.contains_key("perm_id")
        && let Some(connection_id) = str_field("connection_id")
    {
        return ClassifiedCall::RespondPermission {
            connection_id: connection_id.to_string(),
        };
    }
    if let (Some(connection_id), Some(value)) = (str_field("connection_id"), str_field("value"))
        && obj.contains_key("config_id")
    {
        return ClassifiedCall::SetConfigOption {
            connection_id: connection_id.to_string(),
            value: value.to_string(),
        };
    }
    if !obj.contains_key("connection_id")
        && let Some(agent) = str_field("agent")
    {
        return ClassifiedCall::Connect {
            agent: agent.to_string(),
        };
    }
    ClassifiedCall::Unclassified
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::ToolCallUpdateFields;
    use serde_json::json;

    fn competitor(agent: &str, model: &str, elo: u32) -> EligibleCompetitor {
        EligibleCompetitor {
            agent_source_id: agent.to_string(),
            agent_label: agent.to_string(),
            program: PathBuf::from("/bin/true"),
            args: Vec::new(),
            env: HashMap::new(),
            model_value: model.to_string(),
            model_name: model.to_string(),
            elo,
        }
    }

    fn permission_option(id: &str, kind: PermissionOptionKind) -> PermissionOption {
        PermissionOption::new(id.to_string(), id.to_string(), kind)
    }

    fn permission_tool_call(kind: ToolKind) -> ToolCallUpdate {
        let mut fields = ToolCallUpdateFields::new();
        fields.kind = Some(kind);
        ToolCallUpdate::new("permission-test", fields)
    }

    #[test]
    fn split_thor_voice_removes_highest_ranked_pair_from_competitors() {
        let ranked = vec![
            competitor("thor-agent", "best", 3000),
            competitor("agent-a", "model-a", 2500),
            competitor("agent-b", "model-b", 2400),
        ];

        let (thor_voice, pool) = split_thor_voice_and_competitors(&ranked);

        assert_eq!(thor_voice.agent_source_id, "thor-agent");
        assert_eq!(thor_voice.model_value, "best");
        let pairs: Vec<(String, String)> = pool
            .into_iter()
            .map(|c| (c.agent_source_id, c.model_value))
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("agent-a".to_string(), "model-a".to_string()),
                ("agent-b".to_string(), "model-b".to_string()),
            ]
        );
    }

    #[test]
    fn thor_permission_allows_other_tool_with_allow_once() {
        let decision = thor_permission_decision(
            &permission_tool_call(ToolKind::Other),
            &[
                permission_option("allow-always", PermissionOptionKind::AllowAlways),
                permission_option("allow-once", PermissionOptionKind::AllowOnce),
            ],
        );

        match decision {
            PermissionDecision::Selected(id) => assert_eq!(id, "allow-once"),
            PermissionDecision::Cancelled => panic!("expected allow decision"),
        }
    }

    #[test]
    fn thor_permission_rejects_execute_tool_even_when_allow_is_available() {
        let decision = thor_permission_decision(
            &permission_tool_call(ToolKind::Execute),
            &[
                permission_option("allow-once", PermissionOptionKind::AllowOnce),
                permission_option("reject-once", PermissionOptionKind::RejectOnce),
            ],
        );

        match decision {
            PermissionDecision::Selected(id) => assert_eq!(id, "reject-once"),
            PermissionDecision::Cancelled => panic!("expected explicit reject decision"),
        }
    }

    #[test]
    fn thor_permission_cancels_unsafe_tool_when_no_reject_option_exists() {
        let decision = thor_permission_decision(
            &permission_tool_call(ToolKind::Edit),
            &[permission_option(
                "allow-once",
                PermissionOptionKind::AllowOnce,
            )],
        );

        assert!(matches!(decision, PermissionDecision::Cancelled));
    }

    #[test]
    fn competitor_branch_names_are_c_number_slots_only() {
        assert!(is_competitor_branch_name("ragnarok/run/c1"));
        assert!(is_competitor_branch_name("ragnarok/run/c10"));
        assert!(!is_competitor_branch_name("ragnarok/run/review-c1"));
        assert!(!is_competitor_branch_name("ragnarok/run/c"));
        assert!(!is_competitor_branch_name("ragnarok/run/c1-review"));
    }

    #[test]
    fn thor_driver_announces_tool_call_start_before_completion() {
        let eligible = vec![competitor("agent-a", "model-a", 2500)];
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut driver = ThorDriver::new(&eligible, &tx);

        let call = agent_client_protocol::schema::v1::ToolCall::new("call-1", "create worktree c1");
        driver.observe(SessionUpdate::ToolCall(call));

        match rx.try_recv().expect("activity event") {
            RagnarokEvent::ThorActivity { summary } => {
                assert_eq!(summary, "thor tool: create worktree c1");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn summarize_thor_text_collapses_whitespace_and_ignores_empty_chunks() {
        assert_eq!(
            summarize_thor_text("thor thinking", "  I\nneed\ttools  "),
            Some("thor thinking: I need tools".to_string())
        );
        assert_eq!(summarize_thor_text("thor thinking", " \n\t "), None);
    }

    // -- extract_json_block --

    #[test]
    fn extract_json_block_finds_fenced_block() {
        let text = "Here's my verdict.\n```json\n{\"verdict\": \"clear_winner\"}\n```\nDone.";
        assert_eq!(
            extract_json_block(text),
            Some("{\"verdict\": \"clear_winner\"}".to_string())
        );
    }

    #[test]
    fn extract_json_block_finds_bare_object_without_fence() {
        let text = "My verdict: {\"verdict\": \"clear_winner\", \"nested\": {\"a\": 1}} -- done.";
        assert_eq!(
            extract_json_block(text),
            Some("{\"verdict\": \"clear_winner\", \"nested\": {\"a\": 1}}".to_string())
        );
    }

    #[test]
    fn extract_json_block_returns_none_for_pure_prose() {
        assert_eq!(extract_json_block("I couldn't decide, sorry."), None);
    }

    // -- validate_judgment --

    #[test]
    fn validate_judgment_accepts_well_formed_clear_winner() {
        let raw = r#"Reasoning here.
```json
{"verdict": "clear_winner", "winner_slot": "c2", "finalist_slots": [], "review_assessments": [], "reasoning": "c2 was more correct"}
```"#;
        match validate_judgment(raw) {
            JudgingOutcome::ClearWinner { winner_slot, .. } => assert_eq!(winner_slot, "c2"),
            other => panic!("expected ClearWinner, got {other:?}"),
        }
    }

    #[test]
    fn validate_judgment_accepts_well_formed_tied_best_two() {
        let raw = r#"```json
{"verdict": "tied_best_two", "winner_slot": null, "finalist_slots": ["c1", "c3"], "review_assessments": [], "reasoning": "too close"}
```"#;
        match validate_judgment(raw) {
            JudgingOutcome::TiedBestTwo { finalist_slots, .. } => {
                assert_eq!(finalist_slots, ["c1".to_string(), "c3".to_string()]);
            }
            other => panic!("expected TiedBestTwo, got {other:?}"),
        }
    }

    #[test]
    fn validate_judgment_falls_back_on_malformed_json() {
        match validate_judgment("no json here at all") {
            JudgingOutcome::FellBack { finalist_slots, .. } => {
                assert_eq!(finalist_slots, ["c1".to_string(), "c2".to_string()]);
            }
            other => panic!("expected FellBack, got {other:?}"),
        }
    }

    #[test]
    fn validate_judgment_falls_back_when_clear_winner_names_no_slot() {
        let raw = r#"```json
{"verdict": "clear_winner", "winner_slot": null, "finalist_slots": [], "review_assessments": [], "reasoning": "x"}
```"#;
        assert!(matches!(
            validate_judgment(raw),
            JudgingOutcome::FellBack { .. }
        ));
    }

    #[test]
    fn validate_judgment_falls_back_when_tie_has_wrong_finalist_count() {
        let raw = r#"```json
{"verdict": "tied_best_two", "winner_slot": null, "finalist_slots": ["c1"], "review_assessments": [], "reasoning": "x"}
```"#;
        assert!(matches!(
            validate_judgment(raw),
            JudgingOutcome::FellBack { .. }
        ));
    }

    #[test]
    fn validate_judgment_falls_back_when_tie_has_duplicate_finalists() {
        let raw = r#"```json
{"verdict": "tied_best_two", "winner_slot": null, "finalist_slots": ["c1", "c1"], "review_assessments": [], "reasoning": "x"}
```"#;
        assert!(matches!(
            validate_judgment(raw),
            JudgingOutcome::FellBack { .. }
        ));
    }

    // -- classify --

    #[test]
    fn classify_recognizes_connect_by_agent_field_without_connection_id() {
        let input = json!({"agent": "claude-acp", "cwd": "/tmp/x"});
        assert!(matches!(
            classify(&input),
            ClassifiedCall::Connect { agent } if agent == "claude-acp"
        ));
    }

    #[test]
    fn classify_recognizes_create_worktree_by_branch_name_without_connection_id() {
        let input = json!({"branch_name": "ragnarok/abc123/c1"});
        assert!(matches!(classify(&input), ClassifiedCall::CreateWorktree));
    }

    #[test]
    fn classify_recognizes_respond_permission_by_perm_id() {
        let input = json!({"connection_id": "conn-1", "perm_id": "perm-1", "option_id": "allow"});
        assert!(matches!(
            classify(&input),
            ClassifiedCall::RespondPermission { connection_id } if connection_id == "conn-1"
        ));
    }

    #[test]
    fn classify_recognizes_set_config_option_by_config_id_and_value() {
        let input = json!({"connection_id": "conn-1", "config_id": "model", "value": "opus"});
        match classify(&input) {
            ClassifiedCall::SetConfigOption {
                connection_id,
                value,
            } => {
                assert_eq!(connection_id, "conn-1");
                assert_eq!(value, "opus");
            }
            _other => panic!("expected SetConfigOption, got a different variant"),
        }
    }

    #[test]
    fn classify_leaves_connection_id_only_shape_unclassified() {
        let input = json!({"connection_id": "conn-1"});
        assert!(matches!(classify(&input), ClassifiedCall::Unclassified));
    }

    #[test]
    fn classify_leaves_non_object_input_unclassified() {
        assert!(matches!(
            classify(&json!("not an object")),
            ClassifiedCall::Unclassified
        ));
    }

    // -- build_prompt --

    #[test]
    fn build_prompt_includes_task_verbatim_and_eligible_pool() {
        let eligible = vec![
            competitor("claude-acp", "opus", 2200),
            competitor("codex-acp", "gpt-6", 2100),
        ];
        let prompt = build_prompt("build a rate limiter", &eligible);
        assert!(prompt.contains("build a rate limiter"));
        assert!(prompt.contains("agent=\"claude-acp\""));
        assert!(prompt.contains("model=\"opus\""));
        assert!(prompt.contains("elo=2200"));
        assert!(prompt.contains("agent=\"codex-acp\""));
    }
}
