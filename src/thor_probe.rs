//! ACP validation probes and quota/capacity hints for Thor.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agent_client_protocol::schema::v1::UsageUpdate;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::acp::{self, AcpRuntimeConfig};
use crate::config::SelectedAgent;
use crate::event::{UiCommand, UiEvent};

const CLAUDE_RATE_LIMIT_META_KEY: &str = "_claude/rateLimit";
const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentValidation {
    pub source_id: String,
    pub usable: bool,
    pub agent_name: Option<String>,
    pub agent_version: Option<String>,
    pub session_started: bool,
    pub config_advertised: bool,
    pub prompt_images_supported: bool,
    pub session_fork_supported: bool,
    pub error: Option<String>,
    pub elapsed_ms: u64,
    pub checked_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct QuotaSnapshot {
    pub source_id: String,
    pub provider: QuotaProvider,
    #[serde(default)]
    pub probe_source: QuotaProbeSource,
    pub quota_known: bool,
    pub remaining_percent: Option<f64>,
    pub used_percent: Option<f64>,
    pub reset_at_unix: Option<u64>,
    pub window: Option<String>,
    pub available: Option<bool>,
    pub message: String,
    pub observed_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuotaProvider {
    Claude,
    Codex,
    Generic,
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuotaProbeSource {
    ClaudeSdk,
    CodexAppserver,
    AgentCommand,
    AcpUsageMetadata,
    #[default]
    Unknown,
}

pub async fn validate_agents(agents: &[SelectedAgent], cwd: PathBuf) -> Vec<AgentValidation> {
    let probes = agents
        .iter()
        .cloned()
        .map(|agent| validate_agent(agent, cwd.clone(), DEFAULT_PROBE_TIMEOUT));
    futures::future::join_all(probes).await
}

pub async fn refresh_configured_quota_snapshots(agents: &[SelectedAgent]) -> Vec<QuotaSnapshot> {
    let probes = agents.iter().map(refresh_configured_quota_snapshot);
    futures::future::join_all(probes)
        .await
        .into_iter()
        .flatten()
        .collect()
}

pub async fn refresh_configured_quota_snapshot(agent: &SelectedAgent) -> Option<QuotaSnapshot> {
    let (value, probe_source) = match quota_probe(agent) {
        Some(QuotaProbe::Command { command, source }) => {
            let output = Command::new(&command.program)
                .args(&command.args)
                .envs(&agent.env)
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .output()
                .await
                .ok()?;
            if !output.status.success() {
                return None;
            }
            (
                serde_json::from_slice::<Value>(&output.stdout).ok()?,
                source,
            )
        }
        Some(QuotaProbe::Http { url, token, source }) => {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .ok()?;
            let mut request = client.get(url);
            if let Some(token) = token {
                request = request.bearer_auth(token);
            }
            let response = request.send().await.ok()?;
            if !response.status().is_success() {
                return None;
            }
            (response.json::<Value>().await.ok()?, source)
        }
        None => return None,
    };
    let mut snapshot = snapshot_from_probe_json(&agent.source_id, &value)?;
    snapshot.probe_source = probe_source;
    snapshot.observed_at_unix = now_unix();
    let _ = save_quota_snapshot(&snapshot);
    Some(snapshot)
}

pub async fn validate_agent(
    agent: SelectedAgent,
    cwd: PathBuf,
    timeout: Duration,
) -> AgentValidation {
    let started_at = Instant::now();
    let source_id = agent.source_id.clone();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let runtime_cfg = AcpRuntimeConfig {
        command: agent.program,
        args: agent.args,
        cwd,
        additional_directories: Vec::new(),
        mcp_servers: Vec::new(),
        resume_session: None,
        env: agent.env,
        agent_stderr: None,
        fs_max_text_bytes: acp::DEFAULT_FS_TEXT_BYTES,
    };
    let runtime = tokio::spawn(acp::run(runtime_cfg, event_tx, cmd_rx));

    let mut validation = AgentValidation {
        source_id,
        usable: false,
        agent_name: None,
        agent_version: None,
        session_started: false,
        config_advertised: false,
        prompt_images_supported: false,
        session_fork_supported: false,
        error: None,
        elapsed_ms: 0,
        checked_at_unix: now_unix(),
    };

    let probe = async {
        while let Some(event) = event_rx.recv().await {
            match event {
                UiEvent::Connected {
                    agent_name,
                    agent_version,
                    prompt_images_supported,
                    session_fork_supported,
                } => {
                    validation.agent_name = agent_name;
                    validation.agent_version = agent_version;
                    validation.prompt_images_supported = prompt_images_supported;
                    validation.session_fork_supported = session_fork_supported;
                }
                UiEvent::SessionStarted { .. } => {
                    validation.session_started = true;
                    validation.usable = true;
                    break;
                }
                UiEvent::SessionConfigOptions => {
                    validation.config_advertised = true;
                }
                UiEvent::Fatal(message) | UiEvent::PromptFailed { message } => {
                    validation.error = Some(message);
                    break;
                }
                UiEvent::Warning(message) if validation.error.is_none() => {
                    validation.error = Some(message);
                }
                _ => {}
            }
        }
    };

    if tokio::time::timeout(timeout, probe).await.is_err() {
        validation.error = Some(format!(
            "ACP validation timed out after {}s",
            timeout.as_secs()
        ));
    }
    validation.elapsed_ms = started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;

    let _ = cmd_tx.send(UiCommand::Shutdown);
    let _ = tokio::time::timeout(Duration::from_secs(2), runtime).await;
    validation
}

pub fn quota_from_usage_update(source_id: &str, update: &UsageUpdate) -> Option<QuotaSnapshot> {
    let meta = update.meta.as_ref()?;
    if let Some(value) = meta.get(CLAUDE_RATE_LIMIT_META_KEY) {
        return claude_quota_snapshot(source_id, value);
    }

    find_quota_like_value(&Value::Object(meta.clone()))
        .and_then(|value| generic_quota_snapshot(source_id, value))
}

fn claude_quota_snapshot(source_id: &str, value: &Value) -> Option<QuotaSnapshot> {
    let object = value.as_object()?;
    let used_percent = number_field(object, "utilization", "utilization")
        .map(|used| used.round().clamp(0.0, 100.0));
    let remaining_percent = used_percent.map(|used| (100.0 - used).max(0.0));
    let reset_at_unix = number_field(object, "resetsAt", "resets_at")
        .or_else(|| number_field(object, "overageResetsAt", "overage_resets_at"))
        .and_then(epoch_to_unix);
    let window = string_field(object, "rateLimitType", "rate_limit_type")
        .map(rate_limit_window_label)
        .map(str::to_string);
    let available = remaining_percent.map(|remaining| remaining > 0.0);
    let message = quota_message(
        window.as_deref(),
        used_percent,
        remaining_percent,
        reset_at_unix,
    );

    Some(QuotaSnapshot {
        source_id: source_id.to_string(),
        provider: QuotaProvider::Claude,
        probe_source: QuotaProbeSource::AcpUsageMetadata,
        quota_known: used_percent.is_some() || reset_at_unix.is_some(),
        remaining_percent,
        used_percent,
        reset_at_unix,
        window,
        available,
        message,
        observed_at_unix: now_unix(),
    })
}

fn generic_quota_snapshot(source_id: &str, value: &Value) -> Option<QuotaSnapshot> {
    let object = value.as_object()?;
    let used_percent = number_field(object, "usedPercent", "used_percent")
        .or_else(|| number_field(object, "utilization", "utilization"))
        .map(|used| used.round().clamp(0.0, 100.0));
    let remaining_percent = number_field(object, "remainingPercent", "remaining_percent")
        .map(|remaining| remaining.round().clamp(0.0, 100.0))
        .or_else(|| used_percent.map(|used| (100.0 - used).max(0.0)));
    let reset_at_unix = number_field(object, "resetsAt", "resets_at")
        .or_else(|| number_field(object, "resetAt", "reset_at"))
        .and_then(epoch_to_unix);
    let available = remaining_percent.map(|remaining| remaining > 0.0);
    let provider = detect_quota_provider(source_id);
    let message = quota_message(None, used_percent, remaining_percent, reset_at_unix);

    Some(QuotaSnapshot {
        source_id: source_id.to_string(),
        provider,
        probe_source: QuotaProbeSource::AcpUsageMetadata,
        quota_known: used_percent.is_some()
            || remaining_percent.is_some()
            || reset_at_unix.is_some(),
        remaining_percent,
        used_percent,
        reset_at_unix,
        window: None,
        available,
        message,
        observed_at_unix: now_unix(),
    })
}

fn snapshot_from_probe_json(source_id: &str, value: &Value) -> Option<QuotaSnapshot> {
    let object = value.as_object()?;
    let provider = string_field(object, "provider", "provider")
        .map(provider_from_str)
        .unwrap_or_else(|| detect_quota_provider(source_id));
    let used_percent = number_field(object, "usedPercent", "used_percent")
        .or_else(|| number_field(object, "utilization", "utilization"))
        .map(|used| used.round().clamp(0.0, 100.0));
    let remaining_percent = number_field(object, "remainingPercent", "remaining_percent")
        .map(|remaining| remaining.round().clamp(0.0, 100.0))
        .or_else(|| used_percent.map(|used| (100.0 - used).max(0.0)));
    let reset_at_unix = number_field(object, "resetsAt", "resets_at")
        .or_else(|| number_field(object, "resetAt", "reset_at"))
        .and_then(epoch_to_unix);
    let window = string_field(object, "window", "window").map(str::to_string);
    let available = object
        .get("available")
        .and_then(Value::as_bool)
        .or_else(|| remaining_percent.map(|remaining| remaining > 0.0));
    let message = string_field(object, "message", "message")
        .map(str::to_string)
        .unwrap_or_else(|| {
            quota_message(
                window.as_deref(),
                used_percent,
                remaining_percent,
                reset_at_unix,
            )
        });

    Some(QuotaSnapshot {
        source_id: source_id.to_string(),
        provider,
        probe_source: QuotaProbeSource::Unknown,
        quota_known: true,
        remaining_percent,
        used_percent,
        reset_at_unix,
        window,
        available,
        message,
        observed_at_unix: now_unix(),
    })
}

#[derive(Debug)]
struct QuotaProbeCommand {
    program: String,
    args: Vec<String>,
}

#[derive(Debug)]
enum QuotaProbe {
    Command {
        command: QuotaProbeCommand,
        source: QuotaProbeSource,
    },
    Http {
        url: String,
        token: Option<String>,
        source: QuotaProbeSource,
    },
}

fn quota_probe(agent: &SelectedAgent) -> Option<QuotaProbe> {
    per_agent_quota_probe(agent)
        .or_else(|| claude_sdk_quota_probe(agent))
        .or_else(|| codex_appserver_quota_probe(agent))
}

fn per_agent_quota_probe(agent: &SelectedAgent) -> Option<QuotaProbe> {
    let env_key = quota_probe_env_key(&agent.source_id);
    let command = std::env::var(&env_key).ok()?;
    quota_command_from_string(&command).map(|command| QuotaProbe::Command {
        command,
        source: QuotaProbeSource::AgentCommand,
    })
}

fn claude_sdk_quota_probe(agent: &SelectedAgent) -> Option<QuotaProbe> {
    if detect_quota_provider(&agent.source_id) != QuotaProvider::Claude {
        return None;
    }
    let command = std::env::var("MJ_THOR_CLAUDE_SDK_QUOTA_CMD").ok()?;
    quota_command_from_string(&command).map(|command| QuotaProbe::Command {
        command,
        source: QuotaProbeSource::ClaudeSdk,
    })
}

fn codex_appserver_quota_probe(agent: &SelectedAgent) -> Option<QuotaProbe> {
    if detect_quota_provider(&agent.source_id) != QuotaProvider::Codex {
        return None;
    }
    if let Ok(command) = std::env::var("MJ_THOR_CODEX_APPSERVER_QUOTA_CMD") {
        return quota_command_from_string(&command).map(|command| QuotaProbe::Command {
            command,
            source: QuotaProbeSource::CodexAppserver,
        });
    }
    let url = std::env::var("MJ_THOR_CODEX_APPSERVER_QUOTA_URL")
        .ok()
        .or_else(|| {
            std::env::var("MJ_THOR_CODEX_APPSERVER_URL")
                .ok()
                .map(|base| format!("{}/quota", base.trim_end_matches('/')))
        })?;
    let token = std::env::var("MJ_THOR_CODEX_APPSERVER_TOKEN").ok();
    Some(QuotaProbe::Http {
        url,
        token,
        source: QuotaProbeSource::CodexAppserver,
    })
}

fn quota_command_from_string(command: &str) -> Option<QuotaProbeCommand> {
    let parts = shell_words::split(command).ok()?;
    let (program, args) = parts.split_first()?;
    Some(QuotaProbeCommand {
        program: program.clone(),
        args: args.to_vec(),
    })
}

fn quota_probe_env_key(source_id: &str) -> String {
    let suffix = source_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("MJ_THOR_QUOTA_PROBE_{suffix}")
}

fn provider_from_str(provider: &str) -> QuotaProvider {
    match provider.to_ascii_lowercase().as_str() {
        "claude" | "anthropic" | "claude_code" | "claude-code" => QuotaProvider::Claude,
        "codex" | "openai" | "gpt" => QuotaProvider::Codex,
        "generic" => QuotaProvider::Generic,
        _ => QuotaProvider::Unknown,
    }
}

fn find_quota_like_value(value: &Value) -> Option<&Value> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let lower = key.to_ascii_lowercase();
                if lower.contains("quota")
                    || lower.contains("ratelimit")
                    || lower.contains("rate_limit")
                {
                    return Some(value);
                }
                if let Some(found) = find_quota_like_value(value) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(values) => values.iter().find_map(find_quota_like_value),
        _ => None,
    }
}

fn detect_quota_provider(source_id: &str) -> QuotaProvider {
    let lower = source_id.to_ascii_lowercase();
    if lower.contains("claude") {
        QuotaProvider::Claude
    } else if lower.contains("codex") || lower.contains("openai") || lower.contains("gpt") {
        QuotaProvider::Codex
    } else {
        QuotaProvider::Generic
    }
}

fn quota_message(
    window: Option<&str>,
    used_percent: Option<f64>,
    remaining_percent: Option<f64>,
    reset_at_unix: Option<u64>,
) -> String {
    let mut parts = Vec::new();
    if let Some(window) = window {
        parts.push(window.to_string());
    }
    if let Some(used) = used_percent {
        parts.push(format!("{}% used", used.round() as u64));
    }
    if let Some(remaining) = remaining_percent {
        parts.push(format!("{}% remaining", remaining.round() as u64));
    }
    if let Some(reset) = reset_at_unix {
        parts.push(format!("resets at {reset}"));
    }
    if parts.is_empty() {
        "quota signal observed".to_string()
    } else {
        parts.join(" · ")
    }
}

fn rate_limit_window_label(kind: &str) -> &'static str {
    match kind {
        "five_hour" => "Current session",
        "seven_day" => "Current week (all models)",
        "seven_day_opus" => "Current week (Opus)",
        "seven_day_sonnet" => "Current week (Sonnet)",
        "overage" => "Extra usage",
        _ => "Usage limit",
    }
}

fn string_field<'a>(
    object: &'a serde_json::Map<String, Value>,
    camel: &str,
    snake: &str,
) -> Option<&'a str> {
    object
        .get(camel)
        .or_else(|| object.get(snake))
        .and_then(Value::as_str)
}

fn number_field(object: &serde_json::Map<String, Value>, camel: &str, snake: &str) -> Option<f64> {
    object
        .get(camel)
        .or_else(|| object.get(snake))
        .and_then(number_value)
}

fn number_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn epoch_to_unix(epoch: f64) -> Option<u64> {
    if !epoch.is_finite() || epoch < 0.0 {
        return None;
    }
    let seconds = if epoch >= 1_000_000_000_000.0 {
        epoch / 1000.0
    } else {
        epoch
    };
    Some(seconds.trunc() as u64)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn save_quota_snapshot(snapshot: &QuotaSnapshot) -> Result<()> {
    let path = quota_cache_path();
    let mut snapshots = load_quota_snapshots().unwrap_or_default();
    snapshots.retain(|existing| {
        !(existing.source_id == snapshot.source_id && existing.window == snapshot.window)
    });
    snapshots.push(snapshot.clone());
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create quota cache dir {}", parent.display()))?;
    }
    let body = serde_json::to_vec_pretty(&snapshots).context("serialize quota cache")?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, body).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

pub fn load_quota_snapshots() -> Result<Vec<QuotaSnapshot>> {
    let path = quota_cache_path();
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("parse {}", path.display()))
}

fn quota_cache_path() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("mj")
        .join("thor")
        .join("quota-snapshots.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::UsageUpdate;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn extracts_claude_quota_from_usage_meta() {
        let mut meta = serde_json::Map::new();
        meta.insert(
            CLAUDE_RATE_LIMIT_META_KEY.to_string(),
            json!({
                "rateLimitType": "five_hour",
                "utilization": 72.3,
                "resetsAt": 1_800_000_000
            }),
        );

        let snapshot =
            quota_from_usage_update("claude-code", &UsageUpdate::new(10, 100).meta(meta))
                .expect("quota");

        assert_eq!(snapshot.provider, QuotaProvider::Claude);
        assert_eq!(snapshot.window.as_deref(), Some("Current session"));
        assert_eq!(snapshot.used_percent, Some(72.0));
        assert_eq!(snapshot.remaining_percent, Some(28.0));
        assert_eq!(snapshot.reset_at_unix, Some(1_800_000_000));
        assert_eq!(snapshot.available, Some(true));
    }

    #[test]
    fn extracts_generic_nested_quota_signal() {
        let mut meta = serde_json::Map::new();
        meta.insert(
            "_codex/rateLimit".to_string(),
            json!({
                "usedPercent": 99,
                "resetAt": 1_800_000_001
            }),
        );

        let snapshot =
            quota_from_usage_update("codex", &UsageUpdate::new(10, 100).meta(meta)).expect("quota");

        assert_eq!(snapshot.provider, QuotaProvider::Codex);
        assert_eq!(snapshot.used_percent, Some(99.0));
        assert_eq!(snapshot.remaining_percent, Some(1.0));
        assert_eq!(snapshot.available, Some(true));
    }

    #[test]
    fn parses_provider_probe_json_for_codex_appserver() {
        let snapshot = snapshot_from_probe_json(
            "codex",
            &json!({
                "provider": "codex",
                "remainingPercent": 37,
                "resetAt": 1_800_000_002,
                "window": "daily",
                "available": true,
                "message": "daily: 37% remaining"
            }),
        )
        .expect("snapshot");

        assert_eq!(snapshot.provider, QuotaProvider::Codex);
        assert_eq!(snapshot.remaining_percent, Some(37.0));
        assert_eq!(snapshot.reset_at_unix, Some(1_800_000_002));
        assert_eq!(snapshot.window.as_deref(), Some("daily"));
        assert_eq!(snapshot.available, Some(true));
    }

    #[test]
    fn per_agent_quota_probe_env_key_is_stable() {
        assert_eq!(
            quota_probe_env_key("custom:claude-code"),
            "MJ_THOR_QUOTA_PROBE_CUSTOM_CLAUDE_CODE"
        );
    }

    #[tokio::test]
    async fn validation_fails_closed_for_missing_agent_command() {
        let agent = SelectedAgent {
            source_id: "missing-agent".to_string(),
            program: PathBuf::from("definitely-not-a-real-acp-agent-command"),
            args: Vec::new(),
            env: HashMap::new(),
        };

        let validation = validate_agent(
            agent,
            std::env::current_dir().expect("cwd"),
            Duration::from_secs(1),
        )
        .await;

        assert!(!validation.usable);
        assert!(!validation.session_started);
        assert!(validation.error.is_some());
    }
}
