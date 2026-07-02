//! Deterministic Elo-based eligibility scan (`/ragnarok` Phase 0): which
//! already-configured, ready agents expose which models, and which of
//! those models have a real LMArena score. Unscored models never reach
//! Thor — code is more reliable than an LLM at "is this number present,"
//! and `scores.rs`/`model_resolve.rs` already treat a missing score as
//! disqualifying rather than something to guess at.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::stream::{self, StreamExt};

use crate::{config, probe, scores};

pub const ELIGIBILITY_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const ELIGIBILITY_PROBE_CONCURRENCY: usize = 5;

/// One already-configured agent to probe for readiness and models.
#[derive(Debug, Clone)]
pub struct CandidateAgent {
    pub source_id: String,
    pub label: String,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

/// One (agent, model) pair Thor is allowed to route work to. `elo` is
/// always present by construction — pairs without a score never make it
/// into `EligibilityScan::eligible`.
#[derive(Debug, Clone, PartialEq)]
pub struct EligibleCompetitor {
    pub agent_source_id: String,
    pub agent_label: String,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub model_value: String,
    pub model_name: String,
    pub elo: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedAgent {
    pub agent_label: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct EligibilityScan {
    /// Sorted descending by `elo`, ties broken by `(agent_source_id,
    /// model_value)` — fully deterministic.
    pub eligible: Vec<EligibleCompetitor>,
    pub skipped: Vec<SkippedAgent>,
}

/// Build the candidate pool from config: the one currently-selected agent
/// plus every saved custom agent. Deliberately not
/// `picker::spawn_startup_probes`'s cache, which is scoped to the picker's
/// curated/favorite/default view and isn't guaranteed to cover every custom
/// agent, and would also let Thor route to agents the user hasn't actually
/// configured — the "already configured and ready to use" requirement is
/// about what's really in `config.toml`, not what the picker curates.
pub fn candidates_from_config(cfg: &config::Config) -> Vec<CandidateAgent> {
    let mut out = Vec::new();
    if let Some(agent) = &cfg.agent {
        out.push(CandidateAgent {
            source_id: agent.source_id.clone(),
            label: agent.source_id.clone(),
            program: agent.program.clone(),
            args: agent.args.clone(),
            env: agent.env.clone(),
        });
    }
    for custom in &cfg.custom_agents {
        out.push(CandidateAgent {
            source_id: format!("{}{}", config::CUSTOM_AGENT_SOURCE_PREFIX, custom.name),
            label: custom.name.clone(),
            program: custom.program.clone(),
            args: custom.args.clone(),
            env: HashMap::new(),
        });
    }
    out
}

/// Probe every candidate concurrently, fetch each ready one's models, look
/// up real Elo scores, and rank the survivors. Never installs anything.
/// Candidates that aren't `ProbeStatus::Configured`, whose model list can't
/// be fetched, or whose every model lacks a score are recorded in
/// `skipped`, not silently dropped — so an empty roster is explainable.
pub async fn scan(
    candidates: Vec<CandidateAgent>,
    cwd: &Path,
    score_store: &scores::ScoreStore,
    probe_timeout: Duration,
) -> EligibilityScan {
    let cwd = cwd.to_path_buf();
    let results = stream::iter(candidates.into_iter().map(|candidate| {
        let cwd = cwd.clone();
        async move { probe_one(candidate, cwd, probe_timeout).await }
    }))
    .buffer_unordered(ELIGIBILITY_PROBE_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    let mut skipped = Vec::new();
    let mut scored = Vec::new();
    for result in results {
        match result {
            Err(s) => skipped.push(s),
            Ok((candidate, models)) => {
                let before = scored.len();
                score_models(&candidate, models, score_store, &mut scored);
                if scored.len() == before {
                    skipped.push(SkippedAgent {
                        agent_label: candidate.label,
                        reason: "no model has an Elo score on the leaderboard".to_string(),
                    });
                }
            }
        }
    }

    EligibilityScan {
        eligible: rank(scored),
        skipped,
    }
}

type ProbedModels = (CandidateAgent, Vec<probe::ModelOption>);

async fn probe_one(
    candidate: CandidateAgent,
    cwd: PathBuf,
    timeout: Duration,
) -> Result<ProbedModels, SkippedAgent> {
    let status = probe::probe_agent(
        candidate.program.clone(),
        candidate.args.clone(),
        candidate.env.clone(),
        cwd.clone(),
        timeout,
    )
    .await;
    if !matches!(status, probe::ProbeStatus::Configured) {
        return Err(SkippedAgent {
            agent_label: candidate.label.clone(),
            reason: format!("not ready ({status:?})"),
        });
    }
    match probe::session_models(
        candidate.program.clone(),
        candidate.args.clone(),
        candidate.env.clone(),
        cwd,
        timeout,
    )
    .await
    {
        Ok(models) => Ok((candidate, models)),
        Err(e) => Err(SkippedAgent {
            agent_label: candidate.label,
            reason: format!("couldn't read models: {e}"),
        }),
    }
}

fn score_models(
    candidate: &CandidateAgent,
    models: Vec<probe::ModelOption>,
    score_store: &scores::ScoreStore,
    out: &mut Vec<EligibleCompetitor>,
) {
    for model in models {
        let description = model.description.as_deref().unwrap_or_default();
        let Some(score) =
            score_store.lookup_score(&candidate.source_id, &model.value, &model.name, description)
        else {
            continue;
        };
        out.push(EligibleCompetitor {
            agent_source_id: candidate.source_id.clone(),
            agent_label: candidate.label.clone(),
            program: candidate.program.clone(),
            args: candidate.args.clone(),
            env: candidate.env.clone(),
            model_value: model.value,
            model_name: model.name,
            elo: score.elo,
        });
    }
}

/// Pure ranking: descending by Elo, ties broken by `(agent_source_id,
/// model_value)` for full determinism. No I/O — directly unit-testable.
fn rank(mut scored: Vec<EligibleCompetitor>) -> Vec<EligibleCompetitor> {
    scored.sort_by(|a, b| {
        b.elo
            .cmp(&a.elo)
            .then_with(|| a.agent_source_id.cmp(&b.agent_source_id))
            .then_with(|| a.model_value.cmp(&b.model_value))
    });
    scored
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn rank_sorts_descending_by_elo() {
        let ranked = rank(vec![
            competitor("a", "m1", 1500),
            competitor("b", "m2", 2000),
            competitor("c", "m3", 1800),
        ]);
        let elos: Vec<u32> = ranked.iter().map(|c| c.elo).collect();
        assert_eq!(elos, vec![2000, 1800, 1500]);
    }

    #[test]
    fn rank_breaks_ties_by_agent_then_model() {
        let ranked = rank(vec![
            competitor("z-agent", "m1", 1500),
            competitor("a-agent", "m2", 1500),
            competitor("a-agent", "m1", 1500),
        ]);
        let keys: Vec<(String, String)> = ranked
            .iter()
            .map(|c| (c.agent_source_id.clone(), c.model_value.clone()))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("a-agent".to_string(), "m1".to_string()),
                ("a-agent".to_string(), "m2".to_string()),
                ("z-agent".to_string(), "m1".to_string()),
            ]
        );
    }

    #[test]
    fn rank_is_deterministic_across_repeated_calls() {
        let input = vec![
            competitor("b", "m1", 1900),
            competitor("a", "m1", 1900),
            competitor("c", "m1", 2100),
        ];
        assert_eq!(rank(input.clone()), rank(input));
    }

    #[test]
    fn rank_keeps_multiple_agents_offering_the_same_model_name() {
        // Two different agents surfacing "the same" model are still two
        // distinct competitors, per constraint #3's own framing ("a
        // different ACP connection... ideally a different acp agent") —
        // not duplicates to collapse.
        let ranked = rank(vec![
            competitor("agent-a", "gpt-6", 2000),
            competitor("agent-b", "gpt-6", 2000),
        ]);
        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn candidates_from_config_includes_selected_and_custom_agents() {
        let mut cfg = config::Config {
            agent: Some(config::SelectedAgent {
                source_id: "claude-acp".to_string(),
                program: PathBuf::from("/usr/bin/claude"),
                args: Vec::new(),
                env: HashMap::new(),
            }),
            ..Default::default()
        };
        cfg.custom_agents.push(config::CustomAgent {
            name: "my-codex".to_string(),
            program: PathBuf::from("/usr/bin/codex"),
            args: Vec::new(),
            description: String::new(),
        });

        let candidates = candidates_from_config(&cfg);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].source_id, "claude-acp");
        assert_eq!(candidates[1].source_id, "custom:my-codex");
    }

    #[test]
    fn candidates_from_config_is_empty_when_nothing_configured() {
        let cfg = config::Config::default();
        assert!(candidates_from_config(&cfg).is_empty());
    }
}
