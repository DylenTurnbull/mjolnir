//! Event vocabulary for a `/ragnarok` tournament, bridged into the main
//! `UiEvent` stream as `UiEvent::Ragnarok`. Kept separate from
//! `crate::event` because Thor's own connection — and every competitor/
//! reviewer connection it drives through `mj mcp` — speaks the *inner*
//! `UiEvent`/`UiCommand` vocabulary privately. None of that raw,
//! per-connection stream reaches the foreground UI directly; only this
//! curated, semantic summary does, derived by watching what Thor's own
//! tool calls actually did (see `super::thor`).

use std::time::Duration;

/// Outcome of one competitor's or reviewer's turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnOutcome {
    Completed,
    Failed { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JudgingVerdict {
    ClearWinner,
    TiedBestTwo,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct ReviewAssessment {
    pub reviewer_slot: String,
    pub reviewee_slot: String,
    pub credible: bool,
    #[serde(default)]
    pub note: String,
}

/// Thor's parsed closing verdict — deserialized directly from the fenced
/// JSON block in its final message (see `super::thor::validate_judgment`).
/// `#[serde(default)]` on everything but `verdict`/`reasoning` because which
/// fields Thor fills in depends on which verdict it reached (a
/// `ClearWinner` reply has no reason to include `finalist_slots`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct ThorJudgment {
    pub verdict: JudgingVerdict,
    #[serde(default)]
    pub winner_slot: Option<String>,
    #[serde(default)]
    pub finalist_slots: Vec<String>,
    #[serde(default)]
    pub review_assessments: Vec<ReviewAssessment>,
    #[serde(default)]
    pub reasoning: String,
}

/// Result of parsing+validating Thor's closing message. `FellBack` means
/// the reply was malformed or invalid and a deterministic top-2-by-Elo
/// substitute was used instead — never retried, always surfaced as such.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JudgingOutcome {
    ClearWinner {
        winner_slot: String,
        judgment: ThorJudgment,
    },
    TiedBestTwo {
        finalist_slots: [String; 2],
        judgment: Option<ThorJudgment>,
    },
    FellBack {
        reason: String,
        finalist_slots: [String; 2],
    },
}

/// Progress from an in-flight tournament, derived by watching Thor's own
/// tool-call stream — not a protocol Thor has to remember to speak. If
/// Thor calls the tool, the event follows; if it doesn't, the UI doesn't
/// pretend otherwise.
#[derive(Debug, Clone)]
pub enum RagnarokEvent {
    /// Eligibility scan finished; `eligible_count` (agent, model, Elo)
    /// pairs are usable. Sent once, before Thor is even spawned.
    EligibilityScanned { eligible_count: usize },
    /// Not enough eligible pairs to hold a tournament, or every competitor
    /// failed before producing a result — no verdict was possible.
    /// Terminal.
    Aborted { reason: String },
    /// Thor connected one more competitor (observed from a `connect` call
    /// naming an agent from the eligible pool).
    CompetitorStarted {
        slot: String,
        agent_label: String,
        model_name: String,
        elo: u32,
    },
    /// Thor selected a model for a connected competitor (observed from a
    /// `set_config_option` call on that connection) — separate from
    /// `CompetitorStarted` because `connect` alone doesn't carry the model;
    /// it's chosen in a follow-up call.
    CompetitorModelChosen {
        slot: String,
        model_name: String,
        elo: u32,
    },
    /// Thor answered a permission request raised by a competitor's turn.
    PermissionDecision {
        slot: String,
        approved: bool,
        summary: String,
    },
    /// The most recent real thing a competitor's connection did, observed
    /// from the latest item in a `poll_progress` result (a tool call title,
    /// or a snippet of agent message/thought text) — genuine telemetry, not
    /// a synthetic progress percentage. Only sent when it changes, so
    /// repeated polling of an unchanged state doesn't spam identical
    /// updates.
    CompetitorActivity { slot: String, summary: String },
    /// A competitor's turn finished (observed from a `get_result` call
    /// whose result shows a terminal `turn_status`).
    CompetitorFinished {
        slot: String,
        outcome: TurnOutcome,
        duration: Duration,
    },
    /// Thor started connecting reviewers (observed from the first `connect`
    /// call whose `cwd` doesn't match any known competitor worktree —
    /// reviewers connect against the project root, per the prompt's own
    /// instruction not to give them filesystem access to a reviewee's
    /// worktree). Deliberately coarse: telemetry alone can't honestly
    /// attribute a specific review connection to a specific reviewer/
    /// reviewee pair (that pairing exists only in the prose Thor sends via
    /// `submit_prompt`, which this module doesn't parse), so this is a
    /// tournament-wide phase signal, not a per-combatant one.
    ReviewPhaseStarted,
    /// Thor's turn ended with a parsed (or deterministically-fell-back)
    /// verdict. Terminal.
    TournamentDone { verdict: JudgingOutcome },
}

/// Commands flowing from the UI into a running tournament's orchestration
/// task. Deliberately not routed through `crate::event::UiCommand` — that
/// enum is scoped to driving one ACP connection, and this variant means
/// nothing to `acp::run`'s command loop. Resolving a
/// `JudgingOutcome::TiedBestTwo`/`FellBack` choice is handled entirely
/// client-side instead (see `ui.rs`'s `handle_ragnarok_key`): by the time a
/// pick is possible, Thor's turn has already ended and this task is on its
/// way out, so there's nothing left for the backend to do with the answer.
#[derive(Debug)]
pub enum RagnarokCommand {
    /// Tear down Thor's connection (and, transitively, every competitor/
    /// reviewer connection it holds open through its own `mj mcp`
    /// subprocess) without waiting for a verdict.
    Cancel,
}
