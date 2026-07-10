//! Model-first resolution for Thor (primary), Loki (reviewer), and Eitri
//! (builder). ACP adapters are an implementation detail selected from local
//! capabilities.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use futures::{StreamExt, stream};

use crate::config::Config;
use crate::deepswe::{self, Row};
use crate::{model_resolve, probe};

const PROBE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterKind {
    Codex,
    Claude,
    Anvil,
}

#[derive(Debug, Clone)]
pub struct AdapterLaunch {
    pub kind: AdapterKind,
    pub source_id: String,
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedRole {
    pub model: Row,
    pub model_value: String,
    pub launch: AdapterLaunch,
}

#[derive(Debug, Clone)]
pub struct ResolvedCouncil {
    pub thor: ResolvedRole,
    pub loki: Option<ResolvedRole>,
    pub eitri: ResolvedRole,
    pub available: Vec<ResolvedRole>,
    pub choices: Vec<ModelChoice>,
}

#[derive(Debug, Clone)]
pub struct ModelChoice {
    pub model: String,
    pub pass_at_1: f64,
    pub mean_cost_usd: f64,
    pub available: bool,
    pub disabled_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Availability {
    pub codex: Option<PathBuf>,
    pub claude: Option<PathBuf>,
    pub openrouter: bool,
}

impl Availability {
    pub fn detect() -> Self {
        Self {
            codex: find_on_path("codex"),
            claude: find_on_path("claude"),
            openrouter: std::env::var_os("OPENROUTER_API_KEY")
                .is_some_and(|value| !value.is_empty()),
        }
    }

    pub fn missing_reason(&self, model: &str) -> Option<&'static str> {
        match adapter_kind(model) {
            AdapterKind::Codex if self.codex.is_none() => {
                Some("codex executable not found on PATH")
            }
            AdapterKind::Claude if self.claude.is_none() => {
                Some("claude executable not found on PATH")
            }
            AdapterKind::Anvil if !self.openrouter => Some("OPENROUTER_API_KEY is not set"),
            _ => None,
        }
    }
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let exe = directory.join(format!("{name}.exe"));
            if exe.is_file() {
                return Some(exe);
            }
        }
    }
    None
}

fn adapter_kind(model: &str) -> AdapterKind {
    match deepswe::model_provider(model) {
        "openai" => AdapterKind::Codex,
        "anthropic" => AdapterKind::Claude,
        _ => AdapterKind::Anvil,
    }
}

fn launch_for(kind: AdapterKind) -> AdapterLaunch {
    match kind {
        AdapterKind::Codex => AdapterLaunch {
            kind,
            source_id: "codex-acp".to_string(),
            command: PathBuf::from("npx"),
            args: vec![
                "-y".to_string(),
                "@agentclientprotocol/codex-acp".to_string(),
            ],
            env: HashMap::new(),
        },
        AdapterKind::Claude => AdapterLaunch {
            kind,
            source_id: "claude-acp".to_string(),
            command: PathBuf::from("npx"),
            args: vec![
                "-y".to_string(),
                "@agentclientprotocol/claude-agent-acp".to_string(),
            ],
            env: HashMap::new(),
        },
        AdapterKind::Anvil => AdapterLaunch {
            kind,
            source_id: "anvil".to_string(),
            command: PathBuf::from("uvx"),
            args: vec!["brokk".to_string(), "acp".to_string()],
            env: HashMap::new(),
        },
    }
}

fn row_keys(row: &Row) -> HashSet<String> {
    model_resolve::lmarena_keys_ranked(&row.model, deepswe::model_provider(&row.model))
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

fn option_matches(launch: &AdapterLaunch, option: &probe::ModelOption, row: &Row) -> bool {
    let wanted = row_keys(row);
    let description = option.description.as_deref().unwrap_or_default();
    model_resolve::agent_keys(
        &launch.source_id,
        &option.value,
        &option.name,
        description,
        &HashMap::new(),
    )
    .into_iter()
    .any(|key| wanted.contains(&key))
}

async fn discover_available(
    rows: &[Row],
    availability: &Availability,
    cwd: &Path,
) -> Vec<ResolvedRole> {
    let mut kinds = Vec::new();
    if availability.codex.is_some() {
        kinds.push(AdapterKind::Codex);
    }
    if availability.claude.is_some() {
        kinds.push(AdapterKind::Claude);
    }
    if availability.openrouter {
        kinds.push(AdapterKind::Anvil);
    }

    let probes = stream::iter(kinds.into_iter().map(|kind| {
        let launch = launch_for(kind);
        let cwd = cwd.to_path_buf();
        async move {
            let options = probe::session_models(
                launch.command.clone(),
                launch.args.clone(),
                launch.env.clone(),
                cwd,
                PROBE_TIMEOUT,
            )
            .await;
            (launch, options)
        }
    }))
    .buffer_unordered(probe::PROBE_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    let mut resolved = Vec::new();
    for (launch, options) in probes {
        let Ok(options) = options else {
            tracing::warn!(adapter = %launch.source_id, "council adapter probe failed");
            continue;
        };
        for row in rows
            .iter()
            .filter(|row| adapter_kind(&row.model) == launch.kind)
        {
            if let Some(option) = options
                .iter()
                .find(|option| option_matches(&launch, option, row))
            {
                resolved.push(ResolvedRole {
                    model: row.clone(),
                    model_value: option.value.clone(),
                    launch: launch.clone(),
                });
            }
        }
    }
    resolved.sort_by(|a, b| {
        b.model
            .pass_at_1
            .total_cmp(&a.model.pass_at_1)
            .then_with(|| a.model.mean_cost_usd.total_cmp(&b.model.mean_cost_usd))
            .then_with(|| a.model.model.cmp(&b.model.model))
    });
    resolved
}

fn explicit<'a>(
    role: &str,
    selector: &str,
    rows: &[Row],
    available: &'a [ResolvedRole],
    availability: &Availability,
) -> Result<&'a ResolvedRole> {
    if !rows.iter().any(|row| row.model == selector) {
        bail!("{role} model '{selector}' is not an eligible DeepSWE High/default model");
    }
    if let Some(reason) = availability.missing_reason(selector) {
        bail!("{role} model '{selector}' is unavailable: {reason}");
    }
    available
        .iter()
        .find(|candidate| candidate.model.model == selector)
        .ok_or_else(|| {
            anyhow!(
                "{role} model '{selector}' was unlocked but its ACP adapter did not advertise it"
            )
        })
}

fn choose_eitri<'a>(rows: &[Row], available: &'a [ResolvedRole]) -> Option<&'a ResolvedRole> {
    let anchor = deepswe::sonnet_anchor(rows)?;
    let launchable_rows: Vec<Row> = available.iter().map(|role| role.model.clone()).collect();
    let frontier = deepswe::pareto_frontier(&launchable_rows);
    frontier
        .iter()
        .min_by(|a, b| {
            deepswe::normalized_distance(a, anchor, rows)
                .total_cmp(&deepswe::normalized_distance(b, anchor, rows))
                .then_with(|| a.mean_cost_usd.total_cmp(&b.mean_cost_usd))
                .then_with(|| b.pass_at_1.total_cmp(&a.pass_at_1))
                .then_with(|| a.model.cmp(&b.model))
        })
        .and_then(|row| {
            available
                .iter()
                .find(|candidate| candidate.model.model == row.model)
        })
}

pub async fn resolve(config: &Config, cwd: &Path) -> Result<ResolvedCouncil> {
    let leaderboard = deepswe::load(
        &deepswe::default_cache_path(),
        deepswe::CACHE_TTL,
        deepswe::DEFAULT_URL,
    )
    .await;
    let rows = deepswe::eligible_high(&leaderboard.rows);
    let availability = Availability::detect();
    let available = discover_available(&rows, &availability, cwd).await;
    let choices = rows
        .iter()
        .map(|row| {
            let launchable = available
                .iter()
                .any(|candidate| candidate.model.model == row.model);
            let disabled_reason = (!launchable).then(|| {
                availability
                    .missing_reason(&row.model)
                    .unwrap_or("ACP adapter did not advertise this model")
                    .to_string()
            });
            ModelChoice {
                model: row.model.clone(),
                pass_at_1: row.pass_at_1,
                mean_cost_usd: row.mean_cost_usd,
                available: launchable,
                disabled_reason,
            }
        })
        .collect();
    if available.is_empty() {
        bail!("no DeepSWE model is launchable: install codex or claude, or set OPENROUTER_API_KEY");
    }

    let thor = if config.models.thor == "auto" {
        available.first().expect("checked nonempty")
    } else {
        explicit(
            "Thor",
            &config.models.thor,
            &rows,
            &available,
            &availability,
        )?
    };
    let loki = if config.models.loki == "auto" {
        available
            .iter()
            .find(|candidate| candidate.model.model != thor.model.model)
    } else {
        Some(explicit(
            "Loki",
            &config.models.loki,
            &rows,
            &available,
            &availability,
        )?)
    };
    if loki.is_some_and(|candidate| candidate.model.model == thor.model.model) {
        bail!("Loki must use a model distinct from Thor");
    }
    let eitri = if config.models.eitri == "auto" {
        choose_eitri(&rows, &available)
            .ok_or_else(|| anyhow!("no launchable Eitri model lies on the DeepSWE frontier"))?
    } else {
        explicit(
            "Eitri",
            &config.models.eitri,
            &rows,
            &available,
            &availability,
        )?
    };

    Ok(ResolvedCouncil {
        thor: thor.clone(),
        loki: loki.cloned(),
        eitri: eitri.clone(),
        available,
        choices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_routes_are_model_first() {
        assert_eq!(adapter_kind("gpt-5-6-sol"), AdapterKind::Codex);
        assert_eq!(adapter_kind("claude-sonnet-5"), AdapterKind::Claude);
        assert_eq!(adapter_kind("gemini-3-5-flash"), AdapterKind::Anvil);
        assert_eq!(adapter_kind("glm-5-2"), AdapterKind::Anvil);
    }

    #[test]
    fn missing_reasons_never_include_secret_values() {
        let availability = Availability {
            codex: None,
            claude: None,
            openrouter: false,
        };
        assert_eq!(
            availability.missing_reason("gpt-5-6-sol"),
            Some("codex executable not found on PATH")
        );
        assert_eq!(
            availability.missing_reason("glm-5-2"),
            Some("OPENROUTER_API_KEY is not set")
        );
    }

    #[test]
    fn explicit_unavailable_model_has_actionable_provider_reason() {
        let rows = vec![Row {
            model: "gpt-5-6-sol".to_string(),
            reasoning_effort: Some("high".to_string()),
            pass_at_1: 0.7,
            mean_cost_usd: 3.0,
        }];
        let availability = Availability {
            codex: None,
            claude: None,
            openrouter: false,
        };
        let error = explicit("Thor", "gpt-5-6-sol", &rows, &[], &availability)
            .expect_err("must reject unavailable explicit model");
        assert!(
            error
                .to_string()
                .contains("codex executable not found on PATH")
        );
    }
}
