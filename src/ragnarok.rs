//! `/ragnarok` — a competitive multi-agent implementation tournament,
//! launched by the `/ragnarok <task>` command (`submit_prompt` in
//! `ui.rs`). `Thor` (`thor` module) is spawned as a real ACP connection
//! with `mj mcp` wired in as its own MCP tool; it drives competitors,
//! actively gates their permission requests, assigns adversarial reviews,
//! and judges a winner — all through real tool calls this module observes
//! by watching Thor's own `UiEvent` stream, not through a progress
//! protocol Thor has to remember to speak.
//!
//! This file also owns the UI-side render state (`RagnarokState` et al.),
//! folded from `event::RagnarokEvent` by `AppState::apply_ragnarok_event`
//! in `app.rs`, and drawn by `draw_ragnarok_overlay` in `ui.rs`.

pub mod eligibility;
pub mod event;
pub mod thor;

use std::time::Instant;

use tokio::sync::mpsc;

pub use event::{RagnarokCommand, RagnarokEvent};
pub use thor::{RagnarokConfig, RagnarokTimeouts, spawn};

pub const MIN_COMPETITORS: usize = 2;
pub const MAX_COMPETITORS: usize = 10;
const FEED_CAP: usize = 200;

/// Tournament-wide phase. Drives the roster header/status line and which
/// combat-scene visual is picked. Each combatant's own `CombatantStatus`
/// (not this) drives its individual roster row — competitors progress
/// independently, not in lockstep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RagnarokPhase {
    Assessing,
    Implementing,
    Reviewing,
    Judging,
    AwaitingUserPick,
    Concluded,
    Cancelled,
}

impl RagnarokPhase {
    /// Short label for the overlay's title bar and status line.
    pub fn label(self) -> &'static str {
        match self {
            Self::Assessing => "thor is assessing the task",
            Self::Implementing => "competitors are implementing",
            Self::Reviewing => "competitors are reviewing each other",
            Self::Judging => "thor is judging the reviews",
            Self::AwaitingUserPick => "no clear winner \u{2014} your call",
            Self::Concluded => "tournament concluded",
            Self::Cancelled => "tournament cancelled",
        }
    }
}

/// Per-combatant lifecycle, independent of the tournament-wide phase.
/// Deliberately has no dedicated "reviewing" state: telemetry can tell us
/// *that* the review phase has started (`RagnarokEvent::ReviewPhaseStarted`,
/// tournament-wide) but not honestly attribute a specific review connection
/// to a specific reviewer/reviewee pair, so a combatant just stays
/// `AwaitingReviewAssignment` through that phase — the roster rendering
/// (not this state) shows "reviewing" for that combination by also reading
/// `RagnarokState.phase`, rather than this type overclaiming precision it
/// doesn't have.
#[derive(Debug, Clone)]
pub enum CombatantStatus {
    Spawning,
    Implementing,
    /// Finished implementing; waiting out the rest of the tournament
    /// (review, then judging) with nothing more precise to report — see the
    /// type doc for why this doesn't get split into finer states.
    Waiting,
    Judged {
        won: bool,
    },
    Failed {
        message: String,
    },
}

/// One roster row.
#[derive(Debug, Clone)]
pub struct Combatant {
    /// Stable id for the lifetime of a tournament, e.g. `"c1"`. Matches the
    /// branch/worktree naming Thor is instructed to use, so the roster row,
    /// the git branch, and Thor's own tool-call arguments are all the same
    /// string — no separate id-mapping layer to keep in sync.
    pub slot: String,
    pub agent_label: String,
    pub model_label: String,
    /// Elo at roster-selection time. Always present once a combatant
    /// exists — the eligibility scan (`eligibility::scan`) never lets an
    /// unscored model reach this far.
    pub elo: u32,
    pub status: CombatantStatus,
    pub started_at: Instant,
    /// The most recent real thing this connection did (a tool call title,
    /// or a snippet of message/thought text), from
    /// `RagnarokEvent::CompetitorActivity`. `None` until the first one
    /// arrives; genuinely absent rather than a placeholder, so the roster
    /// doesn't fabricate activity it hasn't observed yet.
    pub activity: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RagnarokFeedEntry {
    pub slot: Option<String>,
    pub text: String,
}

/// Full `/ragnarok` overlay state. One instance lives at
/// `AppState.ragnarok` for the duration of one tournament.
#[derive(Debug)]
pub struct RagnarokState {
    pub task: String,
    pub phase: RagnarokPhase,
    /// Roster, insertion order == the order Thor announced them in. Empty
    /// during `Assessing`.
    pub combatants: Vec<Combatant>,
    /// The tournament feed rendered at the bottom of the arena. Slot-tagged
    /// entries inherit that competitor's color.
    pub feed: Vec<RagnarokFeedEntry>,
    /// Set only in `AwaitingUserPick`: the two finalist slots plus Thor's
    /// own reasoning for why it couldn't pick outright.
    pub finalists: Option<(String, String, String)>,
    /// Set once `phase` reaches `Concluded`: Thor's outright pick, or the
    /// user's resolution of `finalists`.
    pub winner: Option<String>,
    pub verdict_summary: Option<String>,
    pub review_assessments: Vec<event::ReviewAssessment>,
    pub fell_back_reason: Option<String>,
    /// Keyboard cursor into `combatants` (roster view) or `finalists`
    /// (decision panel, 0 or 1).
    pub roster_cursor: usize,
    pub decision_cursor: usize,
    /// True once the user has requested cancellation but the backend
    /// hasn't confirmed teardown yet. Esc handling flips `phase` to
    /// `Cancelled` immediately regardless (see `ui.rs`'s key handler) —
    /// this only exists so a second Esc during that window is a no-op
    /// rather than sending `Cancel` twice.
    pub cancel_requested: bool,
    pub esc_armed: bool,
    pub started_at: Instant,
    /// Send `RagnarokCommand`s to the running orchestration task. `None`
    /// once the tournament has concluded/been cancelled and the task has
    /// exited (further sends would just fail silently anyway, but this
    /// makes "nothing to cancel" explicit rather than swallowed).
    pub cmd_tx: Option<mpsc::UnboundedSender<RagnarokCommand>>,
}

impl RagnarokState {
    pub fn new(task: String, cmd_tx: mpsc::UnboundedSender<RagnarokCommand>) -> Self {
        Self {
            task,
            phase: RagnarokPhase::Assessing,
            combatants: Vec::new(),
            feed: Vec::new(),
            finalists: None,
            winner: None,
            verdict_summary: None,
            review_assessments: Vec::new(),
            fell_back_reason: None,
            roster_cursor: 0,
            decision_cursor: 0,
            cancel_requested: false,
            esc_armed: false,
            started_at: Instant::now(),
            cmd_tx: Some(cmd_tx),
        }
    }

    /// Find a combatant by slot for in-place status updates.
    pub fn combatant_mut(&mut self, slot: &str) -> Option<&mut Combatant> {
        self.combatants.iter_mut().find(|c| c.slot == slot)
    }

    pub fn combatant_index(&self, slot: &str) -> Option<usize> {
        self.combatants.iter().position(|c| c.slot == slot)
    }

    pub fn combatant(&self, slot: &str) -> Option<&Combatant> {
        self.combatants.iter().find(|c| c.slot == slot)
    }

    pub fn push_feed(&mut self, slot: Option<String>, text: impl Into<String>) {
        self.feed.push(RagnarokFeedEntry {
            slot,
            text: text.into(),
        });
        if self.feed.len() > FEED_CAP {
            let remove = self.feed.len() - FEED_CAP;
            self.feed.drain(0..remove);
        }
    }
}
