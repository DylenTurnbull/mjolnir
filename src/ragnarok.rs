//! `/ragnarok` — a coding-agent battle royale.
//!
//! One prompt, many rival agents. **Thor** (a router driven by the strongest
//! available model) sizes the task and decides how many champions enter the
//! arena. Each champion is a *distinct model* — ideally a *distinct ACP agent*
//! — chosen from the agents that are already configured and installed, ranked
//! by LMArena Elo (models with no Elo are ineligible). Every champion gets its
//! own git worktree and implements the task in parallel. Thor then has each
//! champion adversarially review a rival's work (never its own), judges those
//! reviews for honesty, and crowns the implementation closest to the prompt —
//! or, when it cannot separate the top two, hands the choice to the user.
//!
//! While all of that runs, the terminal shows an animated, extremely silly
//! ASCII combat scene so the wait is at least entertaining.
//!
//! The heavy lifting reuses existing primitives: [`crate::picker::launch_plan`]
//! for the ready-agent set, [`crate::probe::session_models`] for each agent's
//! selectable models, [`crate::scores::ScoreStore`] for Elo, [`crate::acp::run`]
//! for driving each ACP connection, and [`crate::worktree`] for isolation.

use std::collections::HashSet;
use std::io::Stdout;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use agent_client_protocol::schema::v1::{
    PermissionOption, PermissionOptionKind, SessionConfigValueId, SessionUpdate, StopReason,
    ToolCallUpdate, ToolKind,
};
use anyhow::Result;
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use ratatui::Terminal;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph, Wrap};
use tokio::sync::mpsc;

use crate::acp::{self, AcpRuntimeConfig};
use crate::event::{
    ElicitationOutcome, PermissionDecision, SessionConfigTarget, UiCommand, UiEvent,
    content_block_text,
};
use crate::palette::TerminalTheme;
use crate::picker::{self, LaunchCommand, PickerPreferences};
use crate::probe;
use crate::registry::Registry;
use crate::scores::ScoreStore;
use crate::term::TrackedBackend;
use crate::worktree::{self, CreatedWorktree};

// ---- tunables ------------------------------------------------------------

/// UI frame cadence (~11 fps): brisk enough to feel animated, cheap enough to
/// leave the CPU to the competing agents.
const FRAME_MS: u64 = 90;
/// How long we let one champion work before declaring it fell in battle.
const COMBAT_TIMEOUT: Duration = Duration::from_secs(20 * 60);
/// How long an adversarial review may take.
const REVIEW_TIMEOUT: Duration = Duration::from_secs(6 * 60);
/// How long any single Thor deliberation (routing / judging) may take.
const THOR_TIMEOUT: Duration = Duration::from_secs(4 * 60);
/// How long to probe a single agent for its model list.
const PROBE_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard ceiling on champions, per the spec ("it can be 10").
const MAX_COMPETITORS: usize = 10;
/// Fallback champion count when Thor cannot be reached / parsed.
const FALLBACK_COMPETITORS: usize = 3;
/// Keep the scrolling battle log bounded.
const MAX_LOG_LINES: usize = 400;

// ---- public entry --------------------------------------------------------

/// Everything [`run`] needs, assembled by `main`.
pub struct RagnarokConfig {
    pub prompt: String,
    pub cwd: PathBuf,
    pub theme: TerminalTheme,
    pub score_store: ScoreStore,
    pub registry: Registry,
    pub install_root: PathBuf,
    pub platform: String,
    pub preferences: PickerPreferences,
    pub agent_stderr: Option<PathBuf>,
}

/// Take over `terminal` with the fullscreen combat view, run the tournament to
/// completion (or until the user concedes), and return once the user leaves.
pub async fn run(
    terminal: &mut Terminal<TrackedBackend<Stdout>>,
    cfg: RagnarokConfig,
) -> Result<()> {
    let theme = cfg.theme;
    let state = Arc::new(Mutex::new(RagnarokState::new(&cfg.prompt)));
    let cancel = Arc::new(AtomicBool::new(false));

    // Drive the whole tournament off the UI thread so the animation never
    // stalls behind a network call or a child process.
    let orchestrator = tokio::spawn(orchestrate(state.clone(), cancel.clone(), cfg));
    tokio::pin!(orchestrator);
    let mut orchestrator_done = false;

    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(FRAME_MS));
    let started = SystemTime::now();

    loop {
        // Draw the current snapshot.
        {
            let snapshot = lock(&state);
            let frame = elapsed_frames(started);
            terminal.draw(|f| draw(f, &snapshot, theme, frame))?;
            if snapshot.exit_requested {
                break;
            }
        }

        tokio::select! {
            biased;
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(event)) => handle_event(&state, &cancel, event),
                    Some(Err(_)) => {}
                    None => break,
                }
            }
            joined = &mut orchestrator, if !orchestrator_done => {
                orchestrator_done = true;
                if let Err(e) = joined {
                    let mut st = lock(&state);
                    st.fail(format!("the arena collapsed: {e}"));
                }
            }
            _ = tick.tick() => {}
        }
    }

    // On the way out, make sure the orchestrator and every child agent stop.
    cancel.store(true, Ordering::SeqCst);
    if !orchestrator_done {
        let _ = tokio::time::timeout(Duration::from_secs(6), &mut orchestrator).await;
    }
    Ok(())
}

/// Lock helper that shrugs off poisoning — a panicked competitor task must not
/// freeze the whole UI.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn elapsed_frames(started: SystemTime) -> u64 {
    started
        .elapsed()
        .map(|d| d.as_millis() as u64 / FRAME_MS)
        .unwrap_or(0)
}

// ---- shared state --------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Recruiting,
    Routing,
    Combat,
    Reviewing,
    Judging,
    Done,
    NeedsUserPick,
    Error,
}

impl Phase {
    fn title(self) -> &'static str {
        match self {
            Phase::Recruiting => "MUSTERING CHAMPIONS",
            Phase::Routing => "THOR READS THE RUNES",
            Phase::Combat => "COMBAT",
            Phase::Reviewing => "TRIAL BY RIVAL",
            Phase::Judging => "THE JUDGEMENT OF THOR",
            Phase::Done => "A CHAMPION IS CROWNED",
            Phase::NeedsUserPick => "YOU DECIDE",
            Phase::Error => "RAGNAROK AVERTED",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Phase::Done | Phase::NeedsUserPick | Phase::Error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombatantStatus {
    Waiting,
    Fighting,
    Finished,
    Failed,
}

#[derive(Debug, Clone)]
pub struct Competitor {
    pub name: String,
    pub agent_source_id: String,
    pub agent_display: String,
    pub model_value: String,
    pub model_name: String,
    pub elo: u32,
    pub status: CombatantStatus,
    /// Tool calls observed = "blows landed". Drives the power bar.
    pub blows: u32,
    pub last_action: String,
    pub final_message: String,
    pub files_changed: usize,
    pub diff_lines: usize,
    pub stop_reason: Option<String>,
    pub error: Option<String>,
    pub worktree_path: Option<PathBuf>,
}

impl Competitor {
    /// Cosmetic 0..=100 "power" that grows as the champion lands blows, so an
    /// active fighter visibly charges up.
    fn power(&self) -> u16 {
        match self.status {
            CombatantStatus::Failed => 0,
            CombatantStatus::Finished => 100,
            _ => (self.blows.saturating_mul(9)).min(96) as u16,
        }
    }

    fn is_finished_ok(&self) -> bool {
        self.status == CombatantStatus::Finished
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewStatus {
    Pending,
    Reviewing,
    Done,
    Failed,
}

#[derive(Debug, Clone)]
pub struct Review {
    pub reviewer: usize,
    pub target: usize,
    pub status: ReviewStatus,
    pub score: Option<u8>,
    pub summary: String,
    /// Thor's honesty rating for this review, filled in during judging.
    pub honesty: Option<u8>,
}

#[derive(Debug, Clone)]
pub enum Verdict {
    Winner {
        competitor: usize,
        rationale: String,
        confidence: u8,
    },
    Tie {
        left: usize,
        right: usize,
        rationale: String,
    },
}

#[derive(Debug, Clone)]
pub struct LogLine {
    pub text: String,
    pub kind: LogKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogKind {
    Thor,
    Attack,
    Info,
    Good,
    Bad,
}

pub struct RagnarokState {
    pub prompt: String,
    pub phase: Phase,
    pub thor_line: String,
    pub competitors: Vec<Competitor>,
    pub reviews: Vec<Review>,
    pub verdict: Option<Verdict>,
    pub log: Vec<LogLine>,
    pub error: Option<String>,
    pub finished: bool,
    pub exit_requested: bool,
    pub aborting: bool,
    /// Cursor for the tie-break picker (0 = left, 1 = right).
    pub pick_cursor: usize,
    /// Set once the user crowns a winner in a tie.
    pub user_pick: Option<usize>,
    /// Toggle the transcript/detail panel.
    pub show_detail: bool,
    pub detail_index: usize,
}

impl RagnarokState {
    fn new(prompt: &str) -> Self {
        Self {
            prompt: prompt.to_string(),
            phase: Phase::Recruiting,
            thor_line: "Thor lifts Mjölnir and bellows for champions…".to_string(),
            competitors: Vec::new(),
            reviews: Vec::new(),
            verdict: None,
            log: Vec::new(),
            error: None,
            finished: false,
            exit_requested: false,
            aborting: false,
            pick_cursor: 0,
            user_pick: None,
            show_detail: false,
            detail_index: 0,
        }
    }

    fn set_phase(&mut self, phase: Phase) {
        self.phase = phase;
    }

    fn thor(&mut self, line: impl Into<String>) {
        let line = line.into();
        self.push_log(LogKind::Thor, format!("THOR: {line}"));
        self.thor_line = line;
    }

    fn push_log(&mut self, kind: LogKind, text: impl Into<String>) {
        self.log.push(LogLine {
            text: text.into(),
            kind,
        });
        if self.log.len() > MAX_LOG_LINES {
            let overflow = self.log.len() - MAX_LOG_LINES;
            self.log.drain(0..overflow);
        }
    }

    fn fail(&mut self, message: impl Into<String>) {
        let message = message.into();
        self.error = Some(message.clone());
        self.push_log(LogKind::Bad, message);
        self.phase = Phase::Error;
        self.finished = true;
    }

    /// The winning competitor index once known (Thor's pick or the user's).
    pub fn winner_index(&self) -> Option<usize> {
        if let Some(idx) = self.user_pick {
            return Some(idx);
        }
        match &self.verdict {
            Some(Verdict::Winner { competitor, .. }) => Some(*competitor),
            _ => None,
        }
    }
}

// ---- orchestration -------------------------------------------------------

async fn orchestrate(
    state: Arc<Mutex<RagnarokState>>,
    cancel: Arc<AtomicBool>,
    cfg: RagnarokConfig,
) {
    // Phase 0 — muster the champions who are actually ready to fight.
    {
        let mut st = lock(&state);
        st.set_phase(Phase::Recruiting);
        st.thor("Who among you dares? Only the installed and the rated may enter!");
        st.push_log(
            LogKind::Info,
            "Scouting configured agents for eligible models…",
        );
    }

    let pool = discover_pool(&state, &cfg).await;
    if cancelled(&cancel) {
        return;
    }
    let distinct_models = distinct_model_count(&pool);
    if distinct_models < 2 {
        let mut st = lock(&state);
        st.fail(format!(
            "Ragnarok needs at least 2 rival models with an Elo score; found {distinct_models}. \
             Install/enable more ACP agents (or keep scores enabled) and try again.",
        ));
        return;
    }

    // Thor's voice is the strongest scored champion available.
    let judge = pool[0].clone();
    {
        let mut st = lock(&state);
        st.push_log(
            LogKind::Info,
            format!(
                "{} eligible champions across {} agents. Thor speaks through {} ({} · {} elo).",
                pool.len(),
                distinct_agent_count(&pool),
                judge.model_name,
                judge.agent_display,
                judge.elo,
            ),
        );
    }

    // A throwaway "council chamber" worktree isolates every reasoning call
    // (Thor's routing/judging and the adversarial reviews) from the user's real
    // checkout — those agents run read-only-ish but must never dirty the repo.
    let council_cwd = {
        let cwd = cfg.cwd.clone();
        match tokio::task::spawn_blocking(move || worktree::create_detached(&cwd)).await {
            Ok(Ok(wt)) => wt.session_cwd,
            _ => cfg.cwd.clone(),
        }
    };

    // Phase 1 — Thor sizes the quest and picks the field.
    {
        let mut st = lock(&state);
        st.set_phase(Phase::Routing);
        st.thor("I weigh the burden of this quest…");
    }
    let requested = thor_route(&cfg, &judge, &pool, &council_cwd, &cancel).await;
    let count = clamp_competitor_count(
        requested.count.unwrap_or(FALLBACK_COMPETITORS),
        distinct_models,
    );
    {
        let mut st = lock(&state);
        if let Some(reason) = &requested.reasoning {
            st.thor(reason.clone());
        }
        st.thor(format!(
            "This quest rates {} in weight — I summon {count} champions to the field!",
            requested
                .complexity
                .map(|c| format!("{c}/10"))
                .unwrap_or_else(|| "unknown".to_string()),
        ));
    }

    let field = select_competitors(&pool, count);
    {
        let mut st = lock(&state);
        st.competitors = field
            .iter()
            .enumerate()
            .map(|(i, entry)| Competitor {
                name: champion_name(i, &entry.model_name),
                agent_source_id: entry.source_id.clone(),
                agent_display: entry.agent_display.clone(),
                model_value: entry.model_value.clone(),
                model_name: entry.model_name.clone(),
                elo: entry.elo,
                status: CombatantStatus::Waiting,
                blows: 0,
                last_action: "awaiting the horn".to_string(),
                final_message: String::new(),
                files_changed: 0,
                diff_lines: 0,
                stop_reason: None,
                error: None,
                worktree_path: None,
            })
            .collect();
        let entrances: Vec<String> = st
            .competitors
            .iter()
            .map(|c| {
                format!(
                    "⚔ {} enters — {} · {} · {} elo",
                    c.name, c.agent_display, c.model_name, c.elo
                )
            })
            .collect();
        for line in entrances {
            st.push_log(LogKind::Info, line);
        }
    }
    if cancelled(&cancel) {
        return;
    }

    // Phase 2 — parallel combat, each champion in its own worktree.
    {
        let mut st = lock(&state);
        st.set_phase(Phase::Combat);
        st.thor("To arms! Build, and let the worthiest work stand!");
    }
    run_combat(&state, &cancel, &cfg, &field).await;
    if cancelled(&cancel) {
        return;
    }

    let finished: Vec<usize> = {
        let st = lock(&state);
        (0..st.competitors.len())
            .filter(|&i| st.competitors[i].is_finished_ok())
            .collect()
    };
    if finished.is_empty() {
        let mut st = lock(&state);
        st.fail("No champion left the battlefield standing — every implementation failed.");
        return;
    }

    // Phase 3 — adversarial cross-review (a derangement over survivors).
    {
        let mut st = lock(&state);
        st.set_phase(Phase::Reviewing);
        st.thor("Now turn your blades on each other's work — but never your own!");
    }
    run_reviews(&state, &cancel, &cfg, &council_cwd, &finished).await;
    if cancelled(&cancel) {
        return;
    }

    // Phase 4 — Thor judges reviews for honesty and crowns a winner.
    {
        let mut st = lock(&state);
        st.set_phase(Phase::Judging);
        st.thor("I weigh the honesty of every critique and the merit of every work…");
    }
    let judgement = thor_judge(&cfg, &judge, &state, &council_cwd, &cancel).await;
    apply_judgement(&state, judgement, &finished);
}

fn cancelled(cancel: &AtomicBool) -> bool {
    cancel.load(Ordering::SeqCst)
}

/// One eligible `(agent, model, elo)` triple.
#[derive(Debug, Clone)]
struct PoolEntry {
    source_id: String,
    agent_display: String,
    launch: LaunchCommand,
    model_value: String,
    model_name: String,
    elo: u32,
}

/// Enumerate every ready agent's scored models, in parallel, sorted by Elo.
async fn discover_pool(state: &Arc<Mutex<RagnarokState>>, cfg: &RagnarokConfig) -> Vec<PoolEntry> {
    let plan = picker::launch_plan(
        &cfg.registry,
        &cfg.platform,
        &cfg.install_root,
        cfg.preferences.clone(),
    );
    let ready: Vec<(String, LaunchCommand)> = plan
        .into_iter()
        .filter_map(|(source_id, cmd)| cmd.map(|cmd| (source_id, cmd)))
        .collect();

    if ready.is_empty() {
        return Vec::new();
    }

    let futures = ready.into_iter().map(|(source_id, launch)| {
        let score = cfg.score_store.clone();
        let cwd = cfg.cwd.clone();
        let display = agent_display_name(&cfg.registry, &source_id);
        let state = state.clone();
        async move {
            {
                let mut st = lock(&state);
                st.push_log(LogKind::Info, format!("Probing {display} for its models…"));
            }
            let models = probe::session_models(
                launch.program.clone(),
                launch.args.clone(),
                launch.env.clone(),
                cwd,
                PROBE_TIMEOUT,
            )
            .await;
            let mut out = Vec::new();
            match models {
                Ok(models) => {
                    for m in models {
                        if let Some(elo) = score.score_numeric(
                            &source_id,
                            &m.value,
                            &m.name,
                            m.description.as_deref().unwrap_or_default(),
                        ) {
                            out.push(PoolEntry {
                                source_id: source_id.clone(),
                                agent_display: display.clone(),
                                launch: launch.clone(),
                                model_value: m.value,
                                model_name: m.name,
                                elo,
                            });
                        }
                    }
                }
                Err(e) => {
                    let mut st = lock(&state);
                    st.push_log(
                        LogKind::Bad,
                        format!("{display} could not be mustered: {e}"),
                    );
                }
            }
            out
        }
    });

    let mut pool: Vec<PoolEntry> = futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect();

    // Dedup identical (agent, model); keep the strongest ranking first.
    pool.sort_by(|a, b| {
        b.elo
            .cmp(&a.elo)
            .then_with(|| a.source_id.cmp(&b.source_id))
            .then_with(|| a.model_value.cmp(&b.model_value))
    });
    let mut seen = HashSet::new();
    pool.retain(|e| seen.insert((e.source_id.clone(), e.model_value.clone())));
    pool
}

async fn run_combat(
    state: &Arc<Mutex<RagnarokState>>,
    cancel: &Arc<AtomicBool>,
    cfg: &RagnarokConfig,
    field: &[PoolEntry],
) {
    // Raise every worktree SEQUENTIALLY first: `git worktree add` and the
    // random-name generator are not safe to race (parallel creation collides on
    // the same generated name). Once each champion has its own isolated
    // checkout, they build in parallel.
    let mut prepared: Vec<Option<CreatedWorktree>> = Vec::with_capacity(field.len());
    for idx in 0..field.len() {
        if cancelled(cancel) {
            prepared.push(None);
            continue;
        }
        let cwd = cfg.cwd.clone();
        let created = tokio::task::spawn_blocking(move || worktree::create_detached(&cwd)).await;
        match created {
            Ok(Ok(wt)) => {
                let mut st = lock(state);
                if let Some(c) = st.competitors.get_mut(idx) {
                    c.status = CombatantStatus::Fighting;
                    c.worktree_path = Some(wt.worktree_root.clone());
                    c.last_action = "charging into battle".to_string();
                }
                let name = competitor_name(&st, idx);
                st.push_log(
                    LogKind::Attack,
                    format!("{name} draws steel and begins to build!"),
                );
                prepared.push(Some(wt));
            }
            Ok(Err(e)) => {
                let mut st = lock(state);
                mark_failed(&mut st, idx, format!("could not raise a worktree: {e}"));
                prepared.push(None);
            }
            Err(e) => {
                let mut st = lock(state);
                mark_failed(&mut st, idx, format!("worktree task panicked: {e}"));
                prepared.push(None);
            }
        }
    }

    let mut handles = Vec::new();
    for (idx, (entry, worktree)) in field.iter().zip(prepared).enumerate() {
        let Some(worktree) = worktree else {
            continue;
        };
        let state = state.clone();
        let cancel = cancel.clone();
        let launch = entry.launch.clone();
        let model_value = entry.model_value.clone();
        let prompt = combat_prompt(&cfg.prompt);
        let agent_stderr = cfg.agent_stderr.clone();
        handles.push(tokio::spawn(async move {
            run_one_competitor(
                state,
                cancel,
                idx,
                launch,
                model_value,
                prompt,
                worktree,
                agent_stderr,
            )
            .await;
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_one_competitor(
    state: Arc<Mutex<RagnarokState>>,
    cancel: Arc<AtomicBool>,
    idx: usize,
    launch: LaunchCommand,
    model_value: String,
    prompt: String,
    worktree: CreatedWorktree,
    agent_stderr: Option<PathBuf>,
) {
    let worktree_root = worktree.worktree_root.clone();
    let session_cwd = worktree.session_cwd.clone();

    let progress_state = state.clone();
    let on_progress = move |progress: TurnProgress| {
        let mut st = lock(&progress_state);
        apply_progress(&mut st, idx, progress);
    };

    let turn = tokio::time::timeout(
        COMBAT_TIMEOUT,
        drive_agent_turn(DriveRequest {
            launch,
            cwd: session_cwd,
            model_value: Some(model_value),
            prompt,
            permission: PermPolicy::AcceptAll,
            agent_stderr,
            cancel: cancel.clone(),
            on_progress,
        }),
    )
    .await;

    // Capture the resulting diff regardless of how the turn ended.
    let diff = tokio::task::spawn_blocking(move || worktree::diff(&worktree_root))
        .await
        .ok()
        .and_then(|r| r.ok());

    let mut st = lock(&state);
    match turn {
        Ok(result) => {
            if let Some(c) = st.competitors.get_mut(idx) {
                c.final_message = result.final_text;
                c.stop_reason = result.stop_reason.map(stop_reason_label);
                if let Some(diff) = &diff {
                    c.files_changed = diff.files_changed;
                    c.diff_lines = diff.patch.lines().count();
                }
                let produced_work = diff.as_ref().map(|d| !d.is_empty()).unwrap_or(false);
                if let Some(err) = result.error {
                    c.status = CombatantStatus::Failed;
                    c.error = Some(err);
                } else if !produced_work {
                    c.status = CombatantStatus::Failed;
                    c.error = Some("left the arena without changing a single file".to_string());
                } else {
                    c.status = CombatantStatus::Finished;
                }
            }
        }
        Err(_) => {
            if let Some(c) = st.competitors.get_mut(idx) {
                c.status = CombatantStatus::Failed;
                c.error = Some("collapsed from exhaustion (timed out)".to_string());
                if let Some(diff) = &diff {
                    c.files_changed = diff.files_changed;
                    c.diff_lines = diff.patch.lines().count();
                }
            }
        }
    }
    let name = competitor_name(&st, idx);
    match st.competitors.get(idx).map(|c| c.status) {
        Some(CombatantStatus::Finished) => {
            let files = st.competitors[idx].files_changed;
            st.push_log(
                LogKind::Good,
                format!("{name} lands the finishing blow — {files} files reforged!"),
            );
        }
        _ => {
            let why = st
                .competitors
                .get(idx)
                .and_then(|c| c.error.clone())
                .unwrap_or_default();
            st.push_log(LogKind::Bad, format!("{name} falls: {why}"));
        }
    }
}

async fn run_reviews(
    state: &Arc<Mutex<RagnarokState>>,
    cancel: &Arc<AtomicBool>,
    cfg: &RagnarokConfig,
    council_cwd: &std::path::Path,
    finished: &[usize],
) {
    let assignments = review_assignments(finished);
    if assignments.is_empty() {
        let mut st = lock(state);
        st.thor("Only one work survives — there is nothing left to challenge it.");
        return;
    }
    {
        let mut st = lock(state);
        st.reviews = assignments
            .iter()
            .map(|&(reviewer, target)| Review {
                reviewer,
                target,
                status: ReviewStatus::Pending,
                score: None,
                summary: String::new(),
                honesty: None,
            })
            .collect();
        for &(reviewer, target) in &assignments {
            let rn = competitor_name(&st, reviewer);
            let tn = competitor_name(&st, target);
            st.push_log(
                LogKind::Info,
                format!("Thor sets {rn} to scrutinise {tn}'s work."),
            );
        }
    }

    let mut handles = Vec::new();
    for (slot, &(reviewer, target)) in assignments.iter().enumerate() {
        let state = state.clone();
        let cancel = cancel.clone();
        let cfg_prompt = cfg.prompt.clone();
        let agent_stderr = cfg.agent_stderr.clone();
        let cwd = council_cwd.to_path_buf();
        // Snapshot what the reviewer needs about its target. Launch commands are
        // not kept in state, so we resolve the reviewer's from the picker plan.
        let (model_value, reviewer_name, target_ctx) = {
            let st = lock(&state);
            let rc = &st.competitors[reviewer];
            let tc = &st.competitors[target];
            (
                rc.model_value.clone(),
                rc.name.clone(),
                TargetContext {
                    name: tc.name.clone(),
                    summary: tc.final_message.clone(),
                    files_changed: tc.files_changed,
                    worktree: tc.worktree_path.clone(),
                },
            )
        };
        let launch = cfg_launch_for(cfg, &state, reviewer);
        handles.push(tokio::spawn(async move {
            run_one_review(
                state,
                cancel,
                slot,
                launch,
                model_value,
                reviewer_name,
                cfg_prompt,
                target_ctx,
                cwd,
                agent_stderr,
            )
            .await;
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }
}

/// Reviewer needs its own launch command; we recover it from the discovery
/// pool via the competitor's source id + model (stored on the competitor).
fn cfg_launch_for(
    cfg: &RagnarokConfig,
    state: &Arc<Mutex<RagnarokState>>,
    competitor: usize,
) -> Option<LaunchCommand> {
    let (source_id, _model) = {
        let st = lock(state);
        let c = st.competitors.get(competitor)?;
        (c.agent_source_id.clone(), c.model_value.clone())
    };
    picker::launch_plan(
        &cfg.registry,
        &cfg.platform,
        &cfg.install_root,
        cfg.preferences.clone(),
    )
    .into_iter()
    .find_map(|(sid, cmd)| (sid == source_id).then_some(cmd).flatten())
}

struct TargetContext {
    name: String,
    summary: String,
    files_changed: usize,
    worktree: Option<PathBuf>,
}

#[allow(clippy::too_many_arguments)]
async fn run_one_review(
    state: Arc<Mutex<RagnarokState>>,
    cancel: Arc<AtomicBool>,
    slot: usize,
    launch: Option<LaunchCommand>,
    model_value: String,
    reviewer_name: String,
    quest: String,
    target: TargetContext,
    cwd: PathBuf,
    agent_stderr: Option<PathBuf>,
) {
    {
        let mut st = lock(&state);
        if let Some(r) = st.reviews.get_mut(slot) {
            r.status = ReviewStatus::Reviewing;
        }
    }
    let Some(launch) = launch else {
        let mut st = lock(&state);
        if let Some(r) = st.reviews.get_mut(slot) {
            r.status = ReviewStatus::Failed;
            r.summary = "reviewer's launch command could not be resolved".to_string();
        }
        return;
    };

    let diff = target
        .worktree
        .as_ref()
        .and_then(|wt| worktree::diff(wt).ok())
        .map(|d| d.patch)
        .unwrap_or_default();
    let prompt = review_prompt(&reviewer_name, &quest, &target, &diff);

    let noop = |_p: TurnProgress| {};
    let result = tokio::time::timeout(
        REVIEW_TIMEOUT,
        drive_agent_turn(DriveRequest {
            launch,
            cwd,
            model_value: Some(model_value),
            prompt,
            permission: PermPolicy::AcceptAll,
            agent_stderr,
            cancel: cancel.clone(),
            on_progress: noop,
        }),
    )
    .await;

    let mut st = lock(&state);
    match result {
        Ok(turn) if turn.error.is_none() => {
            let parsed = parse_review_response(&turn.final_text);
            if let Some(r) = st.reviews.get_mut(slot) {
                r.status = ReviewStatus::Done;
                r.score = parsed.score;
                r.summary = if parsed.summary.trim().is_empty() {
                    truncate(&turn.final_text, 600)
                } else {
                    parsed.summary
                };
            }
            let tname = st
                .reviews
                .get(slot)
                .map(|r| competitor_name(&st, r.target))
                .unwrap_or_default();
            st.push_log(
                LogKind::Attack,
                format!("{reviewer_name} delivers a verdict on {tname}."),
            );
        }
        Ok(turn) => {
            if let Some(r) = st.reviews.get_mut(slot) {
                r.status = ReviewStatus::Failed;
                r.summary = turn.error.unwrap_or_else(|| "review failed".to_string());
            }
        }
        Err(_) => {
            if let Some(r) = st.reviews.get_mut(slot) {
                r.status = ReviewStatus::Failed;
                r.summary = "review timed out".to_string();
            }
        }
    }
}

async fn thor_route(
    cfg: &RagnarokConfig,
    judge: &PoolEntry,
    pool: &[PoolEntry],
    council_cwd: &std::path::Path,
    cancel: &Arc<AtomicBool>,
) -> RouteDecision {
    let prompt = route_prompt(&cfg.prompt, pool);
    let noop = |_p: TurnProgress| {};
    let result = tokio::time::timeout(
        THOR_TIMEOUT,
        drive_agent_turn(DriveRequest {
            launch: judge.launch.clone(),
            cwd: council_cwd.to_path_buf(),
            model_value: Some(judge.model_value.clone()),
            prompt,
            permission: PermPolicy::AcceptAll,
            agent_stderr: cfg.agent_stderr.clone(),
            cancel: cancel.clone(),
            on_progress: noop,
        }),
    )
    .await;
    match result {
        Ok(turn) if turn.error.is_none() => parse_route_response(&turn.final_text),
        _ => RouteDecision::default(),
    }
}

async fn thor_judge(
    cfg: &RagnarokConfig,
    judge: &PoolEntry,
    state: &Arc<Mutex<RagnarokState>>,
    council_cwd: &std::path::Path,
    cancel: &Arc<AtomicBool>,
) -> Option<JudgeDecision> {
    let prompt = {
        let st = lock(state);
        judge_prompt(&cfg.prompt, &st)
    };
    let noop = |_p: TurnProgress| {};
    let result = tokio::time::timeout(
        THOR_TIMEOUT,
        drive_agent_turn(DriveRequest {
            launch: judge.launch.clone(),
            cwd: council_cwd.to_path_buf(),
            model_value: Some(judge.model_value.clone()),
            prompt,
            permission: PermPolicy::AcceptAll,
            agent_stderr: cfg.agent_stderr.clone(),
            cancel: cancel.clone(),
            on_progress: noop,
        }),
    )
    .await;
    let count = { lock(state).competitors.len() };
    match result {
        Ok(turn) if turn.error.is_none() => parse_judge_response(&turn.final_text, count),
        _ => None,
    }
}

fn apply_judgement(
    state: &Arc<Mutex<RagnarokState>>,
    judgement: Option<JudgeDecision>,
    finished: &[usize],
) {
    let mut st = lock(state);
    let Some(judgement) = judgement else {
        // Thor was silent — fall back to the highest-scored survivor.
        fallback_verdict(&mut st, finished);
        return;
    };

    // Record Thor's honesty ratings on the reviews.
    for (reviewer, honesty) in &judgement.honesty {
        for r in st.reviews.iter_mut() {
            if r.reviewer == *reviewer {
                r.honesty = Some(*honesty);
            }
        }
    }

    match (judgement.winner, judgement.top_two) {
        (Some(winner), _) if finished.contains(&winner) && judgement.confidence >= 60 => {
            let name = competitor_name(&st, winner);
            st.verdict = Some(Verdict::Winner {
                competitor: winner,
                rationale: judgement.rationale.clone(),
                confidence: judgement.confidence,
            });
            st.thor(format!(
                "The victor is {name}! {}",
                truncate(&judgement.rationale, 200)
            ));
            st.set_phase(Phase::Done);
            st.finished = true;
        }
        (_, Some((left, right)))
            if finished.contains(&left) && finished.contains(&right) && left != right =>
        {
            let ln = competitor_name(&st, left);
            let rn = competitor_name(&st, right);
            st.verdict = Some(Verdict::Tie {
                left,
                right,
                rationale: judgement.rationale.clone(),
            });
            st.thor(format!(
                "I cannot separate {ln} and {rn}. Mortal, the choice is yours."
            ));
            st.set_phase(Phase::NeedsUserPick);
            st.finished = true;
        }
        (Some(winner), _) if finished.contains(&winner) => {
            // A winner with low confidence: present it, but note the doubt.
            let name = competitor_name(&st, winner);
            st.verdict = Some(Verdict::Winner {
                competitor: winner,
                rationale: judgement.rationale.clone(),
                confidence: judgement.confidence,
            });
            st.thor(format!(
                "I lean toward {name}, though not without doubt. {}",
                truncate(&judgement.rationale, 160)
            ));
            st.set_phase(Phase::Done);
            st.finished = true;
        }
        _ => fallback_verdict(&mut st, finished),
    }
}

fn fallback_verdict(st: &mut RagnarokState, finished: &[usize]) {
    // Prefer the survivor with the most reviewer approval, then most files.
    let mut best: Option<usize> = None;
    let mut best_key = (i32::MIN, 0usize);
    for &idx in finished {
        let review_score: i32 = st
            .reviews
            .iter()
            .filter(|r| r.target == idx)
            .filter_map(|r| r.score.map(|s| s as i32))
            .sum();
        let files = st
            .competitors
            .get(idx)
            .map(|c| c.files_changed)
            .unwrap_or(0);
        let key = (review_score, files);
        if key > best_key {
            best_key = key;
            best = Some(idx);
        }
    }
    if let Some(winner) = best {
        let name = competitor_name(st, winner);
        st.verdict = Some(Verdict::Winner {
            competitor: winner,
            rationale: "Selected by fallback ranking (reviewer approval + scope) after Thor could not be reached.".to_string(),
            confidence: 50,
        });
        st.thor(format!("By the old ways, {name} carries the day."));
        st.set_phase(Phase::Done);
    } else {
        st.fail("No survivor could be judged.");
        return;
    }
    st.finished = true;
}

fn mark_failed(st: &mut RagnarokState, idx: usize, why: String) {
    if let Some(c) = st.competitors.get_mut(idx) {
        c.status = CombatantStatus::Failed;
        c.error = Some(why.clone());
    }
    let name = competitor_name(st, idx);
    st.push_log(
        LogKind::Bad,
        format!("{name} never made it to the field: {why}"),
    );
}

fn apply_progress(st: &mut RagnarokState, idx: usize, progress: TurnProgress) {
    match progress {
        TurnProgress::Started => {
            if let Some(c) = st.competitors.get_mut(idx) {
                c.last_action = "surveying the battlefield".to_string();
            }
        }
        TurnProgress::Thinking => {
            if let Some(c) = st.competitors.get_mut(idx) {
                c.last_action = "plotting a cunning strike".to_string();
            }
        }
        TurnProgress::Action(title) => {
            let name = st
                .competitors
                .get(idx)
                .map(|c| c.name.clone())
                .unwrap_or_default();
            if let Some(c) = st.competitors.get_mut(idx) {
                c.blows = c.blows.saturating_add(1);
                c.last_action = truncate(&title, 48);
            }
            let blows = st.competitors.get(idx).map(|c| c.blows).unwrap_or(0);
            if blows % 3 == 1 {
                st.push_log(LogKind::Attack, attack_line(&name, &title, blows));
            }
        }
    }
}

// ---- ACP one-shot driving ------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum PermPolicy {
    /// Accept every permission request — champions run fully unattended.
    AcceptAll,
    /// Accept only edit/delete/move (kept for completeness / future use).
    #[allow(dead_code)]
    AcceptEdits,
}

#[derive(Debug, Default)]
struct TurnResult {
    final_text: String,
    stop_reason: Option<StopReason>,
    error: Option<String>,
}

enum TurnProgress {
    Started,
    Thinking,
    Action(String),
}

/// Tracks model selection so the prompt is never sent while a session config
/// update is still in flight (which the ACP runtime rejects).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelGate {
    /// A model was requested but not yet set.
    NeedSet,
    /// `SetSessionConfigOption` was sent; waiting for the confirming options.
    AwaitingConfirm,
    /// Model is set (or none was requested) — the prompt may be sent.
    Ready,
}

struct DriveRequest<F: FnMut(TurnProgress)> {
    launch: LaunchCommand,
    cwd: PathBuf,
    model_value: Option<String>,
    prompt: String,
    permission: PermPolicy,
    agent_stderr: Option<PathBuf>,
    cancel: Arc<AtomicBool>,
    on_progress: F,
}

/// Spawn one ACP agent, open a session in `cwd`, optionally select a model,
/// send a single prompt, and drive it to completion. This is the reusable
/// primitive behind both champions and Thor — modeled on `crate::headless`.
async fn drive_agent_turn<F: FnMut(TurnProgress)>(mut req: DriveRequest<F>) -> TurnResult {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<UiEvent>();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<UiCommand>();

    let runtime_cfg = AcpRuntimeConfig {
        command: req.launch.program.clone(),
        args: req.launch.args.clone(),
        cwd: req.cwd.clone(),
        additional_directories: Vec::new(),
        resume_session: None,
        env: req.launch.env.clone(),
        agent_stderr: req.agent_stderr.clone(),
        fs_max_text_bytes: acp::DEFAULT_FS_TEXT_BYTES,
    };
    let runtime = tokio::spawn(async move { acp::run(runtime_cfg, event_tx, cmd_rx).await });

    let mut result = TurnResult::default();
    let mut session_started = false;
    // Model-selection gate. The ACP runtime rejects a prompt sent while a config
    // update is still in flight ("config update already in flight"), so we must
    // set the model, WAIT for the confirming `SessionConfigOptions`, and only
    // then prompt. If no model is requested, the gate starts open.
    let mut gate = if req.model_value.is_some() {
        ModelGate::NeedSet
    } else {
        ModelGate::Ready
    };
    let mut prompt_sent = false;

    while let Some(event) = event_rx.recv().await {
        if req.cancel.load(Ordering::SeqCst) {
            let _ = cmd_tx.send(UiCommand::Shutdown);
            result.error.get_or_insert_with(|| "conceded".to_string());
            break;
        }
        match event {
            UiEvent::SessionConfigOptions { options, targets } => {
                match gate {
                    ModelGate::NeedSet => {
                        // First options snapshot: request the model, then wait
                        // for the confirming options event before prompting.
                        match req
                            .model_value
                            .as_deref()
                            .and_then(|m| find_model_target(&options, &targets, m))
                        {
                            Some((target, value)) => {
                                let _ = cmd_tx
                                    .send(UiCommand::SetSessionConfigOption { target, value });
                                gate = ModelGate::AwaitingConfirm;
                            }
                            // Model not offered here — proceed with the default.
                            None => gate = ModelGate::Ready,
                        }
                    }
                    ModelGate::AwaitingConfirm => {
                        // The runtime only emits this after `drive_config_update`
                        // completes, so the config update is done and it is idle
                        // again — safe to prompt now.
                        gate = ModelGate::Ready;
                    }
                    ModelGate::Ready => {}
                }
                maybe_send_prompt(
                    &cmd_tx,
                    &req.prompt,
                    session_started,
                    gate,
                    &mut prompt_sent,
                );
            }
            UiEvent::SessionStarted { .. } => {
                session_started = true;
                (req.on_progress)(TurnProgress::Started);
                maybe_send_prompt(
                    &cmd_tx,
                    &req.prompt,
                    session_started,
                    gate,
                    &mut prompt_sent,
                );
            }
            UiEvent::SessionUpdate(update) => {
                apply_update(&update, prompt_sent, &mut result, &mut req.on_progress);
            }
            UiEvent::PermissionRequest(prompt) => {
                let decision =
                    decide_permission(req.permission, &prompt.tool_call, &prompt.options);
                let _ = prompt.responder.send(match decision {
                    Some(id) => PermissionDecision::Selected(id),
                    None => PermissionDecision::Cancelled,
                });
            }
            UiEvent::ElicitationRequest(prompt) => {
                let _ = prompt.responder.send(ElicitationOutcome::Decline);
            }
            // A warning while awaiting the model-set confirmation means the
            // config update finished (likely failed) — proceed rather than wait
            // forever for a confirming options event that won't come.
            UiEvent::Warning(_) if gate == ModelGate::AwaitingConfirm => {
                gate = ModelGate::Ready;
                maybe_send_prompt(
                    &cmd_tx,
                    &req.prompt,
                    session_started,
                    gate,
                    &mut prompt_sent,
                );
            }
            UiEvent::PromptDone { stop_reason, .. } => {
                result.stop_reason = Some(stop_reason);
                let _ = cmd_tx.send(UiCommand::Shutdown);
                break;
            }
            UiEvent::PromptFailed { message }
            | UiEvent::SessionForkFailed { message }
            | UiEvent::Fatal(message) => {
                result.error = Some(message);
                let _ = cmd_tx.send(UiCommand::Shutdown);
                break;
            }
            _ => {}
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(3), runtime).await;
    result
}

fn maybe_send_prompt(
    cmd_tx: &mpsc::UnboundedSender<UiCommand>,
    prompt: &str,
    session_started: bool,
    gate: ModelGate,
    prompt_sent: &mut bool,
) {
    if session_started && gate == ModelGate::Ready && !*prompt_sent {
        *prompt_sent = true;
        let _ = cmd_tx.send(UiCommand::SendPrompt {
            text: prompt.to_string(),
            images: Vec::new(),
        });
    }
}

fn apply_update<F: FnMut(TurnProgress)>(
    update: &SessionUpdate,
    prompt_sent: bool,
    result: &mut TurnResult,
    on_progress: &mut F,
) {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) if prompt_sent => {
            result
                .final_text
                .push_str(&content_block_text(&chunk.content));
        }
        SessionUpdate::AgentThoughtChunk(_) if prompt_sent => {
            on_progress(TurnProgress::Thinking);
        }
        SessionUpdate::ToolCall(tc) => {
            on_progress(TurnProgress::Action(tc.title.clone()));
        }
        SessionUpdate::ToolCallUpdate(u) => {
            if let Some(title) = &u.fields.title {
                on_progress(TurnProgress::Action(title.clone()));
            }
        }
        _ => {}
    }
}

/// Find the model config option and the choice whose value matches `model`,
/// returning the `(target, value)` pair to send in `SetSessionConfigOption`.
fn find_model_target(
    options: &[agent_client_protocol::schema::v1::SessionConfigOption],
    targets: &[SessionConfigTarget],
    model: &str,
) -> Option<(SessionConfigTarget, SessionConfigValueId)> {
    for (i, option) in options.iter().enumerate() {
        if !crate::app::is_model_config_option(option) {
            continue;
        }
        let choices = crate::app::config_option_choices(option)?;
        if let Some(choice) = choices.iter().find(|c| c.value.to_string() == model) {
            let target = targets.get(i)?.clone();
            return Some((target, choice.value.clone()));
        }
    }
    None
}

fn decide_permission(
    policy: PermPolicy,
    tool_call: &ToolCallUpdate,
    options: &[PermissionOption],
) -> Option<String> {
    let allow = match policy {
        PermPolicy::AcceptAll => true,
        PermPolicy::AcceptEdits => matches!(
            tool_call.fields.kind,
            Some(ToolKind::Edit | ToolKind::Delete | ToolKind::Move)
        ),
    };
    if !allow {
        return None;
    }
    options
        .iter()
        .find(|o| o.kind == PermissionOptionKind::AllowAlways)
        .or_else(|| {
            options
                .iter()
                .find(|o| o.kind == PermissionOptionKind::AllowOnce)
        })
        .or_else(|| options.first())
        .map(|o| o.option_id.to_string())
}

// ---- pure decision helpers (unit-tested) ---------------------------------

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct RouteDecision {
    count: Option<usize>,
    complexity: Option<u8>,
    reasoning: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JudgeDecision {
    winner: Option<usize>,
    top_two: Option<(usize, usize)>,
    confidence: u8,
    rationale: String,
    honesty: Vec<(usize, u8)>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ReviewDecision {
    score: Option<u8>,
    summary: String,
}

/// Constrain Thor's requested field size to `[2, min(10, available)]`.
fn clamp_competitor_count(requested: usize, available_distinct_models: usize) -> usize {
    let ceiling = available_distinct_models.min(MAX_COMPETITORS);
    requested.clamp(2, ceiling.max(2)).min(ceiling)
}

/// Distinct *models* available. Two `(agent, model)` pairs that resolve to the
/// same leaderboard model share an Elo, so we use Elo as the model identity —
/// this is what enforces "a different model for sure": the same underlying model
/// exposed by two agents counts once.
fn distinct_model_count(pool: &[PoolEntry]) -> usize {
    pool.iter().map(|e| e.elo).collect::<HashSet<_>>().len()
}

fn distinct_agent_count(pool: &[PoolEntry]) -> usize {
    pool.iter()
        .map(|e| e.source_id.as_str())
        .collect::<HashSet<_>>()
        .len()
}

/// Choose `n` champions, each a *distinct model* (keyed by Elo, so the same
/// model on two agents never fields twice), while *preferring distinct agents*.
/// `pool` must be sorted by Elo descending. For each new model (Elo) we take,
/// among its candidate hosts, an agent we have not used yet if one exists.
fn select_competitors(pool: &[PoolEntry], n: usize) -> Vec<PoolEntry> {
    let mut chosen: Vec<PoolEntry> = Vec::new();
    let mut used_elos: HashSet<u32> = HashSet::new();
    let mut used_agents: HashSet<String> = HashSet::new();

    for entry in pool {
        if chosen.len() >= n {
            break;
        }
        if used_elos.contains(&entry.elo) {
            continue;
        }
        // Among every host of this model, prefer one on an unused agent.
        let host = pool
            .iter()
            .filter(|e| e.elo == entry.elo)
            .find(|e| !used_agents.contains(&e.source_id))
            .unwrap_or(entry);
        used_elos.insert(host.elo);
        used_agents.insert(host.source_id.clone());
        chosen.push(host.clone());
    }
    chosen
}

/// A derangement over the finished competitors: each reviews the next survivor
/// (rotation by one), so every implementation is reviewed exactly once and no
/// one reviews their own work.
fn review_assignments(finished: &[usize]) -> Vec<(usize, usize)> {
    if finished.len() < 2 {
        return Vec::new();
    }
    let n = finished.len();
    (0..n)
        .map(|i| (finished[i], finished[(i + 1) % n]))
        .collect()
}

/// Pull the outermost `{ … }` object out of an LLM reply (which may wrap it in
/// prose or a code fence) and parse it as JSON.
fn extract_json_object(text: &str) -> Option<serde_json::Value> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end < start {
        return None;
    }
    serde_json::from_str(&text[start..=end]).ok()
}

fn parse_route_response(text: &str) -> RouteDecision {
    let Some(value) = extract_json_object(text) else {
        return RouteDecision::default();
    };
    RouteDecision {
        count: value
            .get("competitors")
            .or_else(|| value.get("count"))
            .and_then(json_usize),
        complexity: value.get("complexity").and_then(json_u8),
        reasoning: value
            .get("reasoning")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    }
}

fn parse_judge_response(text: &str, count: usize) -> Option<JudgeDecision> {
    let value = extract_json_object(text)?;
    let winner = value
        .get("winner")
        .and_then(json_usize)
        .filter(|&w| w < count);
    let top_two = value
        .get("top_two")
        .and_then(|v| v.as_array())
        .and_then(|a| {
            let l = a.first().and_then(json_usize)?;
            let r = a.get(1).and_then(json_usize)?;
            (l < count && r < count && l != r).then_some((l, r))
        });
    let confidence = value
        .get("confidence")
        .and_then(json_u8)
        .unwrap_or(0)
        .min(100);
    let rationale = value
        .get("rationale")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let honesty = value
        .get("review_honesty")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| {
                    let idx = k.parse::<usize>().ok()?;
                    let score = json_u8(v)?;
                    (idx < count).then_some((idx, score))
                })
                .collect()
        })
        .unwrap_or_default();
    Some(JudgeDecision {
        winner,
        top_two,
        confidence,
        rationale,
        honesty,
    })
}

fn parse_review_response(text: &str) -> ReviewDecision {
    let Some(value) = extract_json_object(text) else {
        return ReviewDecision {
            score: None,
            summary: truncate(text, 600),
        };
    };
    let score = value.get("score").and_then(json_u8).map(|s| s.min(100));
    let mut parts: Vec<String> = Vec::new();
    if let Some(summary) = value.get("summary").and_then(|v| v.as_str()) {
        parts.push(summary.to_string());
    }
    for key in ["weaknesses", "correctness_concerns", "strengths"] {
        if let Some(arr) = value.get(key).and_then(|v| v.as_array()) {
            let items: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| format!("• {s}"))
                .collect();
            if !items.is_empty() {
                parts.push(format!("{key}:\n{}", items.join("\n")));
            }
        }
    }
    ReviewDecision {
        score,
        summary: parts.join("\n"),
    }
}

fn json_usize(value: &serde_json::Value) -> Option<usize> {
    if let Some(n) = value.as_u64() {
        return usize::try_from(n).ok();
    }
    value.as_str().and_then(|s| s.trim().parse::<usize>().ok())
}

fn json_u8(value: &serde_json::Value) -> Option<u8> {
    if let Some(n) = value.as_u64() {
        return u8::try_from(n.min(255)).ok();
    }
    if let Some(f) = value.as_f64() {
        return Some(f.round().clamp(0.0, 255.0) as u8);
    }
    value.as_str().and_then(|s| s.trim().parse::<u8>().ok())
}

// ---- prompt construction -------------------------------------------------

fn combat_prompt(quest: &str) -> String {
    format!(
        "You are a competitor in a coding tournament. Implement the following task \
completely and correctly in this repository. Make real, working code changes — \
edit files, add tests where sensible, and ensure the project still builds. Do not \
ask questions; make reasonable decisions and finish. When done, end with a short \
summary of what you changed and why.\n\n=== TASK ===\n{quest}"
    )
}

fn route_prompt(quest: &str, pool: &[PoolEntry]) -> String {
    let mut roster = String::new();
    for e in pool.iter().take(24) {
        roster.push_str(&format!(
            "- {} via {} (elo {})\n",
            e.model_name, e.agent_display, e.elo
        ));
    }
    format!(
        "You are Thor, router of a coding-agent tournament. Judge how complex the \
following task is on a 1-10 scale, then decide how many rival champions should \
compete: a trivial task needs 2, an epic one may warrant up to {MAX_COMPETITORS}. \
More competitors means broader coverage but more cost, so scale with genuine \
difficulty. There are {pool_len} eligible champions available:\n{roster}\n\
Reply with ONLY a JSON object: {{\"complexity\": <1-10>, \"competitors\": <2-{MAX_COMPETITORS}>, \
\"reasoning\": \"<one short sentence>\"}}.\n\n=== TASK ===\n{quest}",
        pool_len = pool.len(),
    )
}

fn review_prompt(reviewer_name: &str, quest: &str, target: &TargetContext, diff: &str) -> String {
    format!(
        "You are {reviewer_name}, a rival competitor asked to adversarially review \
another champion's implementation of a shared task. Be brutally honest and \
specific: find real correctness bugs, missing requirements, and sloppy work — but \
be fair, do not invent flaws. You may NOT review your own work; this is a rival's.\n\n\
=== SHARED TASK ===\n{quest}\n\n=== RIVAL ({rival}) SUMMARY ===\n{summary}\n\n\
=== RIVAL DIFF ({files} files) ===\n{diff}\n\n\
Reply with ONLY a JSON object: {{\"score\": <0-100 how well it solves the task>, \
\"summary\": \"<2-4 sentence honest verdict>\", \"weaknesses\": [\"...\"], \
\"correctness_concerns\": [\"...\"], \"strengths\": [\"...\"]}}.",
        rival = target.name,
        summary = truncate(&target.summary, 1500),
        files = target.files_changed,
        diff = truncate(diff, 12000),
    )
}

fn judge_prompt(quest: &str, st: &RagnarokState) -> String {
    let mut champions = String::new();
    for (i, c) in st.competitors.iter().enumerate() {
        champions.push_str(&format!(
            "\n[{i}] {} ({} · {} · elo {}) — status: {:?}, files changed: {}\nsummary: {}\n",
            c.name,
            c.agent_display,
            c.model_name,
            c.elo,
            c.status,
            c.files_changed,
            truncate(&c.final_message, 1200),
        ));
    }
    let mut reviews = String::new();
    for r in &st.reviews {
        reviews.push_str(&format!(
            "\nreviewer [{}] on [{}] (score {:?}): {}\n",
            r.reviewer,
            r.target,
            r.score,
            truncate(&r.summary, 800),
        ));
    }
    format!(
        "You are Thor, judging a coding-agent tournament. Below are the champions' \
implementations and the adversarial reviews they wrote about each other. First \
judge each review for HONESTY and validity (did the reviewer make fair, accurate \
points, or exaggerate/lie?). Then decide which implementation best and most \
correctly satisfies the task. Pick a single winner if one clearly stands out; if \
the two best are genuinely too close to separate, return them both as top_two and \
leave winner null.\n\n=== TASK ===\n{quest}\n\n=== CHAMPIONS ==={champions}\n\
=== REVIEWS ==={reviews}\n\n\
Reply with ONLY a JSON object: {{\"winner\": <index or null>, \"top_two\": [<index>, <index>] or null, \
\"confidence\": <0-100>, \"rationale\": \"<2-4 sentences>\", \
\"review_honesty\": {{\"<reviewer_index>\": <0-100>, ...}}}}.",
    )
}

// ---- naming + text helpers -----------------------------------------------

const EPITHETS: &[&str] = &[
    "the Bold",
    "the Unrelenting",
    "Stormforge",
    "the Swift",
    "Ironquill",
    "the Cunning",
    "Doomscribe",
    "the Tireless",
    "Brightblade",
    "the Merciless",
];

fn champion_name(index: usize, model_name: &str) -> String {
    let short = short_model_label(model_name);
    let epithet = EPITHETS[index % EPITHETS.len()];
    format!("{short} {epithet}")
}

/// Provider/region/backend words to strip from a model id so the salient model
/// name survives (e.g. `bedrock::us.anthropic.claude-opus-4-6-v1` → `claude-opus-4-6-v1`).
const MODEL_NOISE: &[&str] = &[
    "bedrock",
    "azure",
    "vertex",
    "us",
    "eu",
    "apac",
    "global",
    "anthropic",
    "openai",
    "google",
    "meta",
    "zai",
    "qwen",
    "mistral",
    "cohere",
    "deepseek",
];

fn short_model_label(model_name: &str) -> String {
    // Take the most specific segment after backend separators (`:` / `/`).
    // Spaces are left intact so human display names like "Claude Opus 4.8"
    // are not truncated to "4.8".
    let tail = model_name
        .rsplit([':', '/'])
        .find(|s| !s.trim().is_empty())
        .unwrap_or(model_name)
        .trim();
    // …then drop leading provider/region words from a dotted id.
    let segments: Vec<&str> = tail.split('.').filter(|s| !s.is_empty()).collect();
    let start = segments
        .iter()
        .position(|s| !MODEL_NOISE.contains(&s.to_ascii_lowercase().as_str()))
        .unwrap_or(0);
    let cleaned = segments[start..].join(".");
    let cleaned = if cleaned.trim().is_empty() {
        tail.to_string()
    } else {
        cleaned
    };
    truncate(&cleaned, 24)
}

fn agent_display_name(registry: &Registry, source_id: &str) -> String {
    if source_id == "anvil" {
        return "Anvil".to_string();
    }
    if let Some(name) = source_id.strip_prefix("custom:") {
        return name.to_string();
    }
    if let Some(agent) = registry.agents.iter().find(|a| a.id == source_id)
        && !agent.name.is_empty()
    {
        return agent.name.clone();
    }
    // Titlecase the id as a friendly fallback.
    let mut chars = source_id.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => source_id.to_string(),
    }
}

fn competitor_name(st: &RagnarokState, idx: usize) -> String {
    st.competitors
        .get(idx)
        .map(|c| c.name.clone())
        .unwrap_or_else(|| format!("Champion {idx}"))
}

const ATTACK_TEMPLATES: &[&str] = &[
    "{name} hurls a flurry of keystrokes — {action}!",
    "{name} parries with a well-placed semicolon while {action}.",
    "{name} unleashes a refactor combo: {action}!",
    "{name} summons a storm of unit tests — {action}!",
    "{name} lands a critical hit: {action}!",
    "{name} channels raw compiler fury into {action}.",
];

fn attack_line(name: &str, action: &str, seed: u32) -> String {
    let template = ATTACK_TEMPLATES[(seed as usize) % ATTACK_TEMPLATES.len()];
    template
        .replace("{name}", name)
        .replace("{action}", &truncate(action, 40))
}

fn stop_reason_label(reason: StopReason) -> String {
    crate::labels::stop_reason_label(reason).to_string()
}

fn truncate(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let mut out: String = trimmed.chars().take(max_chars.saturating_sub(1)).collect();
    out.push('…');
    out
}

// ---- input handling ------------------------------------------------------

fn handle_event(state: &Arc<Mutex<RagnarokState>>, cancel: &Arc<AtomicBool>, event: Event) {
    let Event::Key(key) = event else {
        return;
    };
    if key.kind == KeyEventKind::Release {
        return;
    }
    let mut st = lock(state);
    let ctrl_c =
        key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c'));

    match key.code {
        KeyCode::Char('q') | KeyCode::Esc if !ctrl_c => request_exit(&mut st, cancel),
        _ if ctrl_c => request_exit(&mut st, cancel),
        KeyCode::Tab => {
            st.show_detail = !st.show_detail;
        }
        KeyCode::Left | KeyCode::Char('h') if st.phase == Phase::NeedsUserPick => {
            st.pick_cursor = 0;
        }
        KeyCode::Right | KeyCode::Char('l') if st.phase == Phase::NeedsUserPick => {
            st.pick_cursor = 1;
        }
        KeyCode::Up | KeyCode::Char('k') if st.show_detail => {
            st.detail_index = st.detail_index.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') if st.show_detail => {
            let max = st.competitors.len().saturating_sub(1);
            st.detail_index = (st.detail_index + 1).min(max);
        }
        KeyCode::Enter if st.phase == Phase::NeedsUserPick => {
            if let Some(Verdict::Tie { left, right, .. }) = &st.verdict {
                let winner = if st.pick_cursor == 0 { *left } else { *right };
                st.user_pick = Some(winner);
                let name = competitor_name(&st, winner);
                st.thor(format!("You crown {name}. So be it — a worthy choice."));
                st.set_phase(Phase::Done);
            }
        }
        _ => {}
    }
}

fn request_exit(st: &mut RagnarokState, cancel: &Arc<AtomicBool>) {
    if st.phase.is_terminal() || st.finished {
        st.exit_requested = true;
    } else if st.aborting {
        // Second request: force out even if the wind-down is slow.
        st.exit_requested = true;
    } else {
        st.aborting = true;
        cancel.store(true, Ordering::SeqCst);
        st.thor("You call a truce. The champions lay down their arms…");
        st.push_log(LogKind::Info, "Conceding — press q again to leave.");
    }
}

// ---- rendering -----------------------------------------------------------

fn draw(f: &mut ratatui::Frame, st: &RagnarokState, theme: TerminalTheme, frame: u64) {
    let area = f.area();
    f.render_widget(Clear, area);
    if area.height < 12 || area.width < 40 {
        let msg = Paragraph::new("Terminal too small for the arena — enlarge the window.")
            .style(Style::default().fg(theme.warning))
            .wrap(Wrap { trim: true });
        f.render_widget(msg, area);
        return;
    }

    let detail = st.show_detail && !st.competitors.is_empty();
    let cards_height = card_row_height(st.competitors.len());
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),            // header + quest
            Constraint::Length(7),            // animated arena banner
            Constraint::Length(cards_height), // champion cards
            Constraint::Min(4),               // battle log OR detail panel
            Constraint::Length(1),            // footer
        ])
        .split(area);

    draw_header(f, rows[0], st, theme);
    draw_arena(f, rows[1], st, theme, frame);
    draw_cards(f, rows[2], st, theme, frame);
    if detail {
        draw_detail(f, rows[3], st, theme);
    } else {
        draw_log(f, rows[3], st, theme);
    }
    draw_footer(f, rows[4], st, theme);
}

fn card_row_height(n: usize) -> u16 {
    // Cards flow into as many stacked rows of up to 5 as needed.
    let per_row = 5usize;
    let rows = n.div_ceil(per_row).max(1);
    (rows as u16 * 6).min(18)
}

fn draw_header(f: &mut ratatui::Frame, area: Rect, st: &RagnarokState, theme: TerminalTheme) {
    let title = Line::from(vec![
        Span::styled("⚡ ", Style::default().fg(theme.warning)),
        Span::styled(
            "R A G N A R O K",
            Style::default()
                .fg(theme.header)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" ⚡  ", Style::default().fg(theme.warning)),
        Span::styled(
            st.phase.title(),
            Style::default()
                .fg(phase_color(st.phase, theme))
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    let quest = Line::from(vec![
        Span::styled("Quest: ", Style::default().fg(theme.muted)),
        Span::styled(truncate(&st.prompt, 200), Style::default().fg(theme.text)),
    ]);
    let thor = Line::from(vec![Span::styled(
        format!("⚒ {}", truncate(&st.thor_line, 220)),
        Style::default().fg(theme.secondary),
    )]);
    let para = Paragraph::new(vec![title, quest, thor])
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(theme.subtle)),
        )
        .wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

fn phase_color(phase: Phase, theme: TerminalTheme) -> Color {
    match phase {
        Phase::Combat => theme.error,
        Phase::Reviewing => theme.tool,
        Phase::Judging => theme.accent,
        Phase::Done => theme.success,
        Phase::NeedsUserPick => theme.warning,
        Phase::Error => theme.error,
        _ => theme.primary,
    }
}

/// The animated banner: Thor and flickering lightning up top, with a marquee of
/// clashing glyphs that drifts by wall-clock frame.
fn draw_arena(
    f: &mut ratatui::Frame,
    area: Rect,
    st: &RagnarokState,
    theme: TerminalTheme,
    frame: u64,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Double)
        .border_style(Style::default().fg(phase_color(st.phase, theme)))
        .title(Span::styled(
            " the arena ",
            Style::default()
                .fg(theme.header)
                .add_modifier(Modifier::BOLD),
        ));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let width = inner.width as usize;
    let bolt = LIGHTNING[(frame as usize / 2) % LIGHTNING.len()];
    let thor_face = THOR_FACES[(frame as usize / 3) % THOR_FACES.len()];

    let mut lines: Vec<Line> = Vec::new();
    lines.push(centered_line(
        format!("{bolt}  {thor_face}  {bolt}"),
        width,
        Style::default()
            .fg(theme.warning)
            .add_modifier(Modifier::BOLD),
    ));

    // The clash marquee scrolls; content depends on the phase.
    let marquee = arena_marquee(st, frame, width);
    lines.push(centered_line(
        marquee,
        width,
        Style::default().fg(phase_color(st.phase, theme)),
    ));

    // Once decided, show Thor's verdict (rationale + confidence) right here;
    // otherwise a running tally so the scene reflects real progress.
    if let Some((verdict, style)) = verdict_banner(st, theme) {
        lines.push(centered_line(verdict, width, style));
    } else {
        let tally = arena_tally(st);
        lines.push(centered_line(
            tally,
            width,
            Style::default().fg(theme.muted),
        ));
    }

    let para = Paragraph::new(lines).alignment(Alignment::Center);
    f.render_widget(para, inner);
}

/// A one-line verdict summary drawn from the `Verdict` once the tournament is
/// decided — the winner's rationale + Thor's confidence, or the tie rationale.
fn verdict_banner(st: &RagnarokState, theme: TerminalTheme) -> Option<(String, Style)> {
    match st.verdict.as_ref()? {
        Verdict::Winner {
            competitor,
            rationale,
            confidence,
        } => {
            let name = competitor_name(st, *competitor);
            Some((
                format!(
                    "🏆 {name} wins ({confidence}% sure) — {}",
                    truncate(rationale, 120)
                ),
                Style::default()
                    .fg(theme.success)
                    .add_modifier(Modifier::BOLD),
            ))
        }
        Verdict::Tie {
            left,
            right,
            rationale,
        } => {
            let ln = competitor_name(st, *left);
            let rn = competitor_name(st, *right);
            Some((
                format!("⚖ {ln} vs {rn} — {}", truncate(rationale, 120)),
                Style::default()
                    .fg(theme.warning)
                    .add_modifier(Modifier::BOLD),
            ))
        }
    }
}

const LIGHTNING: &[&str] = &["ϟ", "⚡", "Ϟ", "↯", "⚡"];
const THOR_FACES: &[&str] = &["(⌐■_■)⚒", "(•̀o•́)⚒", "(⚡▄⚡)⚒", "(ಠ_ಠ)⚒"];
const SPARKS: &[char] = &['✶', '✦', '✧', '⋆', '✷', '∗'];

fn arena_marquee(st: &RagnarokState, frame: u64, width: usize) -> String {
    let scene = match st.phase {
        Phase::Recruiting | Phase::Routing => "≪ champions gather at the gates ≫",
        Phase::Combat => "⚔ CLASH ⚔ ⚔ CLASH ⚔ ⚔ CLASH ⚔",
        Phase::Reviewing => "🔍 rivals scrutinise rivals 🔍",
        Phase::Judging => "⚖ Thor weighs the works ⚖",
        Phase::Done => "🏆 A CHAMPION STANDS TRIUMPHANT 🏆",
        Phase::NeedsUserPick => "≟ two titans, one crown — you decide ≟",
        Phase::Error => "… the horns fall silent …",
    };
    let spark = SPARKS[(frame as usize) % SPARKS.len()];
    let banner = format!("{spark} {scene} {spark}");
    // Scroll by shifting a padded band.
    if banner.chars().count() + 4 >= width || width == 0 {
        return banner;
    }
    let pad = width - banner.chars().count();
    let offset = (frame as usize) % pad;
    let left = " ".repeat(offset);
    format!("{left}{banner}")
}

fn arena_tally(st: &RagnarokState) -> String {
    if st.competitors.is_empty() {
        return "mustering…".to_string();
    }
    let fighting = st
        .competitors
        .iter()
        .filter(|c| c.status == CombatantStatus::Fighting)
        .count();
    let finished = st
        .competitors
        .iter()
        .filter(|c| c.status == CombatantStatus::Finished)
        .count();
    let fallen = st
        .competitors
        .iter()
        .filter(|c| c.status == CombatantStatus::Failed)
        .count();
    let blows: u32 = st.competitors.iter().map(|c| c.blows).sum();
    format!(
        "⚔ {fighting} fighting · ✓ {finished} standing · ✗ {fallen} fallen · {blows} blows struck"
    )
}

fn draw_cards(
    f: &mut ratatui::Frame,
    area: Rect,
    st: &RagnarokState,
    theme: TerminalTheme,
    frame: u64,
) {
    if st.competitors.is_empty() {
        let para = Paragraph::new("Awaiting Thor's summons…")
            .style(Style::default().fg(theme.muted))
            .alignment(Alignment::Center);
        f.render_widget(para, area);
        return;
    }

    let per_row = 5usize;
    let n = st.competitors.len();
    let row_count = n.div_ceil(per_row);
    let row_areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Ratio(1, row_count as u32); row_count])
        .split(area);

    for (r, row_area) in row_areas.iter().enumerate() {
        let start = r * per_row;
        let end = (start + per_row).min(n);
        let count = end - start;
        if count == 0 {
            continue;
        }
        let col_areas = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Ratio(1, count as u32); count])
            .split(*row_area);
        for (c, col_area) in col_areas.iter().enumerate() {
            let idx = start + c;
            draw_one_card(f, *col_area, st, idx, theme, frame);
        }
    }
}

fn draw_one_card(
    f: &mut ratatui::Frame,
    area: Rect,
    st: &RagnarokState,
    idx: usize,
    theme: TerminalTheme,
    frame: u64,
) {
    let Some(c) = st.competitors.get(idx) else {
        return;
    };
    let is_winner = st.winner_index() == Some(idx);
    let border = match c.status {
        CombatantStatus::Finished => theme.success,
        CombatantStatus::Failed => theme.error,
        CombatantStatus::Fighting => theme.warning,
        CombatantStatus::Waiting => theme.subtle,
    };
    let border = if is_winner { theme.success } else { border };
    let title = format!(
        " {} {} ",
        combatant_glyph(c.status, frame),
        truncate(&c.name, 22)
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(if is_winner {
            BorderType::Double
        } else {
            BorderType::Rounded
        })
        .border_style(Style::default().fg(border))
        .title(Span::styled(
            title,
            Style::default().fg(border).add_modifier(Modifier::BOLD),
        ));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![Span::styled(
        format!(
            "{} · {}",
            truncate(&c.agent_display, 12),
            truncate(&c.model_name, 16)
        ),
        Style::default().fg(theme.muted),
    )]));
    lines.push(Line::from(vec![
        Span::styled("elo ", Style::default().fg(theme.subtle)),
        Span::styled(c.elo.to_string(), Style::default().fg(theme.accent)),
        Span::raw("  "),
        Span::styled(
            format!("×{} blows", c.blows),
            Style::default().fg(theme.tool),
        ),
    ]));
    lines.push(power_bar(c, theme, inner.width as usize));
    let status_line = if is_winner {
        Line::from(Span::styled(
            "🏆 CHAMPION",
            Style::default()
                .fg(theme.success)
                .add_modifier(Modifier::BOLD),
        ))
    } else {
        Line::from(Span::styled(status_text(c), Style::default().fg(border)))
    };
    lines.push(status_line);
    lines.push(Line::from(Span::styled(
        truncate(&c.last_action, inner.width.saturating_sub(2) as usize),
        Style::default().fg(theme.thought),
    )));

    let para = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(para, inner);
}

fn combatant_glyph(status: CombatantStatus, frame: u64) -> char {
    match status {
        CombatantStatus::Finished => '🏁',
        CombatantStatus::Failed => '☠',
        CombatantStatus::Waiting => '⏳',
        CombatantStatus::Fighting => {
            const SWING: &[char] = &['/', '-', '\\', '|', '⚔'];
            SWING[(frame as usize) % SWING.len()]
        }
    }
}

fn status_text(c: &Competitor) -> String {
    match c.status {
        CombatantStatus::Waiting => "awaiting the horn".to_string(),
        CombatantStatus::Fighting => "⚔ in the fray".to_string(),
        CombatantStatus::Finished => format!("✓ {} files", c.files_changed),
        CombatantStatus::Failed => format!(
            "✗ {}",
            c.error
                .as_deref()
                .map(|e| truncate(e, 20))
                .unwrap_or_else(|| "fell".to_string())
        ),
    }
}

fn power_bar(c: &Competitor, theme: TerminalTheme, width: usize) -> Line<'static> {
    let bar_width = width.saturating_sub(2).clamp(4, 20);
    let filled = ((c.power() as usize * bar_width) / 100).min(bar_width);
    let empty = bar_width - filled;
    let color = match c.status {
        CombatantStatus::Failed => theme.error,
        CombatantStatus::Finished => theme.success,
        _ => theme.warning,
    };
    Line::from(vec![
        Span::styled("█".repeat(filled), Style::default().fg(color)),
        Span::styled("░".repeat(empty), Style::default().fg(theme.subtle)),
    ])
}

fn draw_log(f: &mut ratatui::Frame, area: Rect, st: &RagnarokState, theme: TerminalTheme) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme.subtle))
        .title(Span::styled(
            " battle log ",
            Style::default().fg(theme.header),
        ));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let visible = inner.height as usize;
    let start = st.log.len().saturating_sub(visible);
    let lines: Vec<Line> = st.log[start..]
        .iter()
        .map(|l| {
            let color = match l.kind {
                LogKind::Thor => theme.secondary,
                LogKind::Attack => theme.warning,
                LogKind::Good => theme.success,
                LogKind::Bad => theme.error,
                LogKind::Info => theme.muted,
            };
            Line::from(Span::styled(l.text.clone(), Style::default().fg(color)))
        })
        .collect();
    let para = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(para, inner);
}

fn draw_detail(f: &mut ratatui::Frame, area: Rect, st: &RagnarokState, theme: TerminalTheme) {
    let idx = st.detail_index.min(st.competitors.len().saturating_sub(1));
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme.primary))
        .title(Span::styled(
            format!(
                " champion {}/{} — Tab: back to log, ↑/↓: switch ",
                idx + 1,
                st.competitors.len()
            ),
            Style::default().fg(theme.header),
        ));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let Some(c) = st.competitors.get(idx) else {
        return;
    };
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![Span::styled(
        c.name.clone(),
        Style::default()
            .fg(theme.header)
            .add_modifier(Modifier::BOLD),
    )]));
    lines.push(Line::from(Span::styled(
        format!(
            "{} · {} · elo {} · {} files · {} diff lines · {:?}",
            c.agent_display, c.model_name, c.elo, c.files_changed, c.diff_lines, c.status
        ),
        Style::default().fg(theme.muted),
    )));
    if let Some(path) = &c.worktree_path {
        lines.push(Line::from(Span::styled(
            format!("worktree: {}", path.display()),
            Style::default().fg(theme.subtle),
        )));
    }
    // Reviews written about this champion.
    for r in st.reviews.iter().filter(|r| r.target == idx) {
        let reviewer = competitor_name(st, r.reviewer);
        lines.push(Line::from(Span::styled(
            format!(
                "— review by {reviewer} (score {}{}):",
                r.score
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "?".to_string()),
                r.honesty
                    .map(|h| format!(", honesty {h}"))
                    .unwrap_or_default(),
            ),
            Style::default().fg(theme.tool),
        )));
        for line in r.summary.lines().take(6) {
            lines.push(Line::from(Span::styled(
                format!("  {line}"),
                Style::default().fg(theme.quote),
            )));
        }
    }
    lines.push(Line::from(Span::styled(
        "— summary —",
        Style::default().fg(theme.subtle),
    )));
    for line in c.final_message.lines().take(40) {
        lines.push(Line::from(Span::styled(
            line.to_string(),
            Style::default().fg(theme.text),
        )));
    }
    let para = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(para, inner);
}

fn draw_footer(f: &mut ratatui::Frame, area: Rect, st: &RagnarokState, theme: TerminalTheme) {
    let mut spans = Vec::new();
    match st.phase {
        Phase::NeedsUserPick => {
            spans.push(Span::styled("← →", Style::default().fg(theme.accent)));
            spans.push(Span::styled(" choose  ", Style::default().fg(theme.muted)));
            spans.push(Span::styled("Enter", Style::default().fg(theme.accent)));
            spans.push(Span::styled(
                " crown winner  ",
                Style::default().fg(theme.muted),
            ));
        }
        _ if st.phase.is_terminal() => {
            spans.push(Span::styled("q", Style::default().fg(theme.accent)));
            spans.push(Span::styled(
                " leave the arena  ",
                Style::default().fg(theme.muted),
            ));
        }
        _ => {
            spans.push(Span::styled("q", Style::default().fg(theme.accent)));
            spans.push(Span::styled(" concede  ", Style::default().fg(theme.muted)));
        }
    }
    spans.push(Span::styled("Tab", Style::default().fg(theme.accent)));
    spans.push(Span::styled(
        if st.show_detail {
            " battle log"
        } else {
            " inspect champions"
        },
        Style::default().fg(theme.muted),
    ));
    if let Some(idx) = st.winner_index()
        && let Some(path) = st
            .competitors
            .get(idx)
            .and_then(|c| c.worktree_path.as_ref())
    {
        spans.push(Span::styled(
            format!("   ▶ winner in {}", path.display()),
            Style::default().fg(theme.success),
        ));
    }
    f.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn centered_line(text: String, width: usize, style: Style) -> Line<'static> {
    // Alignment::Center handles horizontal centering; we just clip overly long
    // content so it never wraps and breaks the fixed-height banner.
    let clipped = if text.chars().count() > width && width > 1 {
        text.chars().take(width).collect()
    } else {
        text
    };
    Line::from(Span::styled(clipped, style))
}

// ---- tests ---------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(source: &str, model: &str, elo: u32) -> PoolEntry {
        PoolEntry {
            source_id: source.to_string(),
            agent_display: source.to_string(),
            launch: LaunchCommand {
                program: PathBuf::from("echo"),
                args: vec![],
                env: Default::default(),
            },
            model_value: model.to_string(),
            model_name: model.to_string(),
            elo,
        }
    }

    #[test]
    fn clamp_keeps_count_within_two_and_available() {
        assert_eq!(clamp_competitor_count(1, 8), 2);
        assert_eq!(clamp_competitor_count(5, 8), 5);
        assert_eq!(clamp_competitor_count(99, 8), 8);
        assert_eq!(clamp_competitor_count(99, 20), MAX_COMPETITORS);
        // Only two models available: any request collapses to 2.
        assert_eq!(clamp_competitor_count(7, 2), 2);
    }

    #[test]
    fn select_prefers_distinct_agents_and_dedups_same_model() {
        // Sorted desc by elo, as discover_pool guarantees. The same model
        // (elo 1456) is exposed by two agents — it must field only once, and
        // we should spread across agents.
        let pool = vec![
            entry("codex", "gpt", 1463),
            entry("anvil", "opus", 1456),
            entry("claude", "opus", 1456),
            entry("anvil", "sonnet", 1450),
        ];
        let chosen = select_competitors(&pool, 3);
        assert_eq!(chosen.len(), 3);
        // Distinct models (distinct elo) — the duplicate Opus is deduped.
        let elos: HashSet<u32> = chosen.iter().map(|e| e.elo).collect();
        assert_eq!(elos, HashSet::from([1463, 1456, 1450]));
        // Agent diversity is preferred: codex (gpt) and anvil (opus) both used
        // before falling back to anvil again for sonnet.
        assert_eq!(chosen[0].source_id, "codex");
        assert_eq!(chosen[1].source_id, "anvil");
        assert_eq!(chosen[1].model_value, "opus");
    }

    #[test]
    fn select_fields_distinct_models_from_a_single_agent() {
        let pool = vec![
            entry("anvil", "opus", 1460),
            entry("anvil", "sonnet", 1450),
            entry("anvil", "haiku", 1400),
        ];
        // One agent, three distinct models: still fields all three.
        let chosen = select_competitors(&pool, 3);
        assert_eq!(chosen.len(), 3);
        let elos: HashSet<u32> = chosen.iter().map(|e| e.elo).collect();
        assert_eq!(elos.len(), 3);
    }

    #[test]
    fn select_never_repeats_a_model_across_agents() {
        // Same model (same elo) on two agents, ask for 3 — only one distinct
        // model exists, so exactly one competitor comes back.
        let pool = vec![entry("anvil", "opus", 1456), entry("claude", "opus", 1456)];
        let chosen = select_competitors(&pool, 3);
        assert_eq!(chosen.len(), 1);
        // And distinct_model_count agrees it is a single model.
        assert_eq!(distinct_model_count(&pool), 1);
    }

    #[test]
    fn review_assignments_are_a_derangement() {
        let finished = vec![0, 2, 3, 5];
        let assignments = review_assignments(&finished);
        assert_eq!(assignments.len(), finished.len());
        // No self-review.
        assert!(assignments.iter().all(|(r, t)| r != t));
        // Every survivor reviewed exactly once.
        let targets: HashSet<usize> = assignments.iter().map(|(_, t)| *t).collect();
        assert_eq!(targets, finished.iter().copied().collect());
        // Every survivor reviews exactly once.
        let reviewers: HashSet<usize> = assignments.iter().map(|(r, _)| *r).collect();
        assert_eq!(reviewers, finished.iter().copied().collect());
    }

    #[test]
    fn review_assignments_empty_for_single_survivor() {
        assert!(review_assignments(&[4]).is_empty());
        assert!(review_assignments(&[]).is_empty());
    }

    #[test]
    fn route_response_parses_and_survives_prose() {
        let text = "Sure! Here is my call:\n```json\n{\"complexity\": 7, \"competitors\": 4, \"reasoning\": \"broad task\"}\n```\nGood luck.";
        let decision = parse_route_response(text);
        assert_eq!(decision.count, Some(4));
        assert_eq!(decision.complexity, Some(7));
        assert_eq!(decision.reasoning.as_deref(), Some("broad task"));
    }

    #[test]
    fn route_response_defaults_on_garbage() {
        assert_eq!(
            parse_route_response("no json here"),
            RouteDecision::default()
        );
    }

    #[test]
    fn judge_response_parses_winner_and_honesty() {
        let text = r#"{"winner": 1, "top_two": null, "confidence": 82, "rationale": "cleanest", "review_honesty": {"0": 90, "1": 40}}"#;
        let decision = parse_judge_response(text, 3).expect("parse");
        assert_eq!(decision.winner, Some(1));
        assert_eq!(decision.confidence, 82);
        assert!(decision.honesty.contains(&(0u8 as usize, 90)));
        assert!(decision.honesty.contains(&(1usize, 40)));
    }

    #[test]
    fn judge_response_parses_tie() {
        let text =
            r#"{"winner": null, "top_two": [0, 2], "confidence": 30, "rationale": "too close"}"#;
        let decision = parse_judge_response(text, 3).expect("parse");
        assert_eq!(decision.winner, None);
        assert_eq!(decision.top_two, Some((0, 2)));
    }

    #[test]
    fn judge_response_rejects_out_of_range_indices() {
        let text = r#"{"winner": 9, "top_two": [7, 8], "confidence": 99, "rationale": "x"}"#;
        let decision = parse_judge_response(text, 3).expect("parse");
        assert_eq!(decision.winner, None);
        assert_eq!(decision.top_two, None);
    }

    #[test]
    fn review_response_collects_summary_and_weaknesses() {
        let text = r#"{"score": 71, "summary": "solid", "weaknesses": ["no tests", "leaks a file handle"]}"#;
        let decision = parse_review_response(text);
        assert_eq!(decision.score, Some(71));
        assert!(decision.summary.contains("solid"));
        assert!(decision.summary.contains("no tests"));
    }

    #[test]
    fn champion_names_are_stable_and_short() {
        let name = champion_name(0, "Claude Opus 4.8");
        assert!(name.starts_with("Claude Opus"), "got: {name}");
        assert!(name.contains("the Bold"));
    }

    #[test]
    fn short_model_label_extracts_salient_name_from_backend_ids() {
        // Bedrock-style ids drop provider/region noise so champions are distinct.
        assert_eq!(
            short_model_label("bedrock::us.anthropic.claude-opus-4-6-v1"),
            "claude-opus-4-6-v1"
        );
        assert_eq!(
            short_model_label("bedrock::us.anthropic.claude-opus-4-7"),
            "claude-opus-4-7"
        );
        assert_eq!(short_model_label("codex::gpt-5.5"), "gpt-5.5");
        assert_eq!(
            short_model_label("anthropic/claude-opus-4-8"),
            "claude-opus-4-8"
        );
        // Human display names keep their spaces/version.
        assert_eq!(short_model_label("Claude Opus 4.8"), "Claude Opus 4.8");
        // Two different bedrock Opus versions yield different labels.
        assert_ne!(
            champion_name(0, "bedrock::us.anthropic.claude-opus-4-6-v1"),
            champion_name(2, "bedrock::us.anthropic.claude-opus-4-7"),
        );
    }

    #[test]
    fn prompt_gate_waits_for_config_confirmation() {
        use tokio::sync::mpsc;
        // No model requested → gate opens immediately, prompt sent once started.
        let (tx, mut rx) = mpsc::unbounded_channel::<UiCommand>();
        let mut sent = false;
        maybe_send_prompt(&tx, "hi", true, ModelGate::Ready, &mut sent);
        assert!(sent);
        assert!(matches!(rx.try_recv(), Ok(UiCommand::SendPrompt { .. })));

        // While a model set is pending/awaiting, the prompt must NOT be sent
        // (this is what prevented the "config update already in flight" error).
        let (tx, mut rx) = mpsc::unbounded_channel::<UiCommand>();
        let mut sent = false;
        maybe_send_prompt(&tx, "hi", true, ModelGate::NeedSet, &mut sent);
        maybe_send_prompt(&tx, "hi", true, ModelGate::AwaitingConfirm, &mut sent);
        assert!(!sent);
        assert!(rx.try_recv().is_err());

        // Not started yet → no prompt even when the gate is ready.
        let (tx, mut rx) = mpsc::unbounded_channel::<UiCommand>();
        let mut sent = false;
        maybe_send_prompt(&tx, "hi", false, ModelGate::Ready, &mut sent);
        assert!(!sent);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn truncate_adds_ellipsis_only_when_needed() {
        assert_eq!(truncate("short", 20), "short");
        let long = truncate("abcdefghij", 5);
        assert_eq!(long.chars().count(), 5);
        assert!(long.ends_with('…'));
    }

    #[test]
    fn state_starts_in_recruiting_with_no_winner() {
        let st = RagnarokState::new("build a thing");
        assert_eq!(st.phase, Phase::Recruiting);
        assert!(st.winner_index().is_none());
        assert!(!st.finished);
    }

    #[test]
    fn card_row_height_scales_with_count() {
        assert_eq!(card_row_height(1), 6);
        assert_eq!(card_row_height(5), 6);
        assert_eq!(card_row_height(6), 12);
        assert_eq!(card_row_height(10), 12);
    }
}
