//! ACP validation probes and direct quota/capacity reads for Thor.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Local, LocalResult, TimeZone};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::acp::{self, AcpRuntimeConfig};
use crate::config::SelectedAgent;
use crate::event::{UiCommand, UiEvent};

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
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuotaProbeSource {
    ClaudeUsageCommand,
    CodexAppserver,
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

pub async fn refresh_configured_quota_snapshot(agent: &SelectedAgent) -> Vec<QuotaSnapshot> {
    let snapshots = match detect_quota_provider(&agent.source_id) {
        QuotaProvider::Claude => refresh_claude_usage(agent).await,
        QuotaProvider::Codex => refresh_codex_appserver_usage(agent).await,
        QuotaProvider::Unknown => Vec::new(),
    };
    for snapshot in &snapshots {
        let _ = save_quota_snapshot(snapshot);
    }
    snapshots
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

async fn refresh_claude_usage(agent: &SelectedAgent) -> Vec<QuotaSnapshot> {
    let program = provider_program(agent, "claude");
    let command = async {
        let output = Command::new(program)
            .args(["-p", "/usage", "--output-format", "json"])
            .envs(&agent.env)
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let value = serde_json::from_slice::<Value>(&output.stdout).ok()?;
        let text = claude_usage_text(&value)?;
        Some(claude_usage_snapshots(&agent.source_id, &text))
    };
    tokio::time::timeout(DEFAULT_PROBE_TIMEOUT, command)
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
}

async fn refresh_codex_appserver_usage(agent: &SelectedAgent) -> Vec<QuotaSnapshot> {
    let program = provider_program(agent, "codex");
    let command = async {
        let mut child = Command::new(program)
            .arg("app-server")
            .envs(&agent.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let mut stdin = child.stdin.take()?;
        let stdout = child.stdout.take()?;
        write_json_line(
            &mut stdin,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "mj-thor-quota",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": {},
                    "protocolVersion": "0.1.0",
                }
            }),
        )
        .await
        .ok()?;
        write_json_line(
            &mut stdin,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "account/rateLimits/read"
            }),
        )
        .await
        .ok()?;
        let mut lines = BufReader::new(stdout).lines();
        while let Some(line) = lines.next_line().await.ok()? {
            let value = serde_json::from_str::<Value>(&line).ok()?;
            if value.get("id").and_then(Value::as_u64) != Some(2) {
                continue;
            }
            let result = value.get("result")?;
            let snapshots = codex_rate_limit_snapshots(&agent.source_id, result);
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Some(snapshots);
        }
        let _ = child.kill().await;
        let _ = child.wait().await;
        None
    };
    tokio::time::timeout(DEFAULT_PROBE_TIMEOUT, command)
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
}

async fn write_json_line(stdin: &mut tokio::process::ChildStdin, value: Value) -> Result<()> {
    stdin
        .write_all(serde_json::to_string(&value)?.as_bytes())
        .await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(())
}

fn provider_program(agent: &SelectedAgent, fallback: &'static str) -> PathBuf {
    let program_name = agent
        .program
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if program_name.contains(fallback) {
        agent.program.clone()
    } else {
        PathBuf::from(fallback)
    }
}

fn claude_usage_text(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => items
            .iter()
            .find(|item| item.get("type").and_then(Value::as_str) == Some("result"))
            .and_then(claude_usage_text)
            .or_else(|| items.iter().find_map(claude_usage_text)),
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some("result") {
                return object
                    .get("result")
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            object
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(Value::as_array)
                .and_then(|content| {
                    content.iter().find_map(|block| {
                        block
                            .get("text")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
                })
        }
        _ => None,
    }
}

fn claude_usage_snapshots(source_id: &str, text: &str) -> Vec<QuotaSnapshot> {
    text.lines()
        .filter_map(|line| claude_usage_snapshot(source_id, line))
        .collect()
}

fn claude_usage_snapshot(source_id: &str, line: &str) -> Option<QuotaSnapshot> {
    let (window, detail) = line.split_once(':')?;
    let window = window.trim();
    if !window.starts_with("Current ") {
        return None;
    }
    let used_percent = percent_before(detail, "% used")?;
    let remaining_percent = (100.0 - used_percent).max(0.0);
    let reset_at_unix = claude_reset_at_unix(detail);
    Some(QuotaSnapshot {
        source_id: source_id.to_string(),
        provider: QuotaProvider::Claude,
        probe_source: QuotaProbeSource::ClaudeUsageCommand,
        quota_known: true,
        remaining_percent: Some(remaining_percent),
        used_percent: Some(used_percent),
        reset_at_unix,
        window: Some(window.to_string()),
        available: Some(remaining_percent > 0.0),
        message: line.trim().to_string(),
        observed_at_unix: now_unix(),
    })
}

fn percent_before(text: &str, marker: &str) -> Option<f64> {
    let idx = text.find(marker)?;
    let prefix = &text[..idx];
    let number = prefix
        .split(|c: char| !c.is_ascii_digit() && c != '.')
        .rfind(|part| !part.is_empty())?;
    number
        .parse::<f64>()
        .ok()
        .map(|value| value.clamp(0.0, 100.0))
}

fn claude_reset_at_unix(text: &str) -> Option<u64> {
    parse_claude_reset_at_unix(text, Local::now())
}

fn parse_claude_reset_at_unix(text: &str, now: DateTime<Local>) -> Option<u64> {
    let reset = text.split_once("resets ")?.1;
    let reset = reset.split(" (").next().unwrap_or(reset).trim();
    let (date, time) = reset.split_once(" at ")?;
    let mut date_parts = date.split_whitespace();
    let month = month_number(date_parts.next()?)?;
    let day = date_parts.next()?.parse::<u32>().ok()?;
    let (hour, minute) = parse_clock_time(time)?;
    let current_year = now.year();

    [current_year, current_year + 1]
        .into_iter()
        .filter_map(|year| local_epoch_seconds(year, month, day, hour, minute))
        .find(|candidate| *candidate >= now.timestamp().max(0) as u64)
}

fn month_number(month: &str) -> Option<u32> {
    match month.to_ascii_lowercase().as_str() {
        "jan" | "january" => Some(1),
        "feb" | "february" => Some(2),
        "mar" | "march" => Some(3),
        "apr" | "april" => Some(4),
        "may" => Some(5),
        "jun" | "june" => Some(6),
        "jul" | "july" => Some(7),
        "aug" | "august" => Some(8),
        "sep" | "sept" | "september" => Some(9),
        "oct" | "october" => Some(10),
        "nov" | "november" => Some(11),
        "dec" | "december" => Some(12),
        _ => None,
    }
}

fn parse_clock_time(time: &str) -> Option<(u32, u32)> {
    let time = time.trim().to_ascii_lowercase();
    let (time, is_pm) = if let Some(time) = time.strip_suffix("am") {
        (time.trim(), false)
    } else if let Some(time) = time.strip_suffix("pm") {
        (time.trim(), true)
    } else {
        return None;
    };
    let (hour, minute) = match time.split_once(':') {
        Some((hour, minute)) => (hour.parse::<u32>().ok()?, minute.parse::<u32>().ok()?),
        None => (time.parse::<u32>().ok()?, 0),
    };
    if hour == 0 || hour > 12 || minute > 59 {
        return None;
    }
    let hour = match (hour, is_pm) {
        (12, false) => 0,
        (12, true) => 12,
        (hour, false) => hour,
        (hour, true) => hour + 12,
    };
    Some((hour, minute))
}

fn local_epoch_seconds(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> Option<u64> {
    let timestamp = match Local.with_ymd_and_hms(year, month, day, hour, minute, 0) {
        LocalResult::Single(value) => value.timestamp(),
        LocalResult::Ambiguous(a, b) => a.timestamp().min(b.timestamp()),
        LocalResult::None => return None,
    };
    u64::try_from(timestamp).ok()
}

fn codex_rate_limit_snapshots(source_id: &str, result: &Value) -> Vec<QuotaSnapshot> {
    let mut snapshots = Vec::new();
    if let Some(rate_limits) = result.get("rateLimits") {
        snapshots.extend(codex_snapshot_windows(source_id, rate_limits, None));
    }
    if let Some(by_limit) = result.get("rateLimitsByLimitId").and_then(Value::as_object) {
        for (limit_id, value) in by_limit {
            if limit_id == "codex" {
                continue;
            }
            snapshots.extend(codex_snapshot_windows(
                source_id,
                value,
                Some(limit_id.as_str()),
            ));
        }
    }
    snapshots
}

fn codex_snapshot_windows(
    source_id: &str,
    snapshot: &Value,
    limit_id: Option<&str>,
) -> Vec<QuotaSnapshot> {
    let mut snapshots = Vec::new();
    if let Some(primary) = snapshot.get("primary")
        && let Some(snapshot) =
            codex_window_snapshot(source_id, snapshot, primary, limit_id, "primary")
    {
        snapshots.push(snapshot);
    }
    if let Some(secondary) = snapshot.get("secondary")
        && let Some(snapshot) =
            codex_window_snapshot(source_id, snapshot, secondary, limit_id, "secondary")
    {
        snapshots.push(snapshot);
    }
    if let Some(individual) = snapshot.get("individualLimit")
        && let Some(snapshot) = codex_individual_limit_snapshot(source_id, snapshot, individual)
    {
        snapshots.push(snapshot);
    }
    snapshots
}

fn codex_window_snapshot(
    source_id: &str,
    root: &Value,
    window: &Value,
    explicit_limit_id: Option<&str>,
    window_kind: &str,
) -> Option<QuotaSnapshot> {
    let used_percent = window
        .get("usedPercent")
        .and_then(number_value)
        .map(|used| used.round().clamp(0.0, 100.0))?;
    let remaining_percent = (100.0 - used_percent).max(0.0);
    let reset_at_unix = window
        .get("resetsAt")
        .and_then(number_value)
        .and_then(epoch_to_unix);
    let duration = window.get("windowDurationMins").and_then(number_value);
    let limit_name = root
        .get("limitName")
        .and_then(Value::as_str)
        .or(explicit_limit_id);
    let window_label = codex_window_label(window_kind, duration, limit_name);
    let reached = root
        .get("rateLimitReachedType")
        .is_some_and(|value| !value.is_null());
    Some(QuotaSnapshot {
        source_id: source_id.to_string(),
        provider: QuotaProvider::Codex,
        probe_source: QuotaProbeSource::CodexAppserver,
        quota_known: true,
        remaining_percent: Some(remaining_percent),
        used_percent: Some(used_percent),
        reset_at_unix,
        window: Some(window_label.clone()),
        available: Some(remaining_percent > 0.0 && !reached),
        message: quota_message(
            Some(&window_label),
            Some(used_percent),
            Some(remaining_percent),
            reset_at_unix,
        ),
        observed_at_unix: now_unix(),
    })
}

fn codex_individual_limit_snapshot(
    source_id: &str,
    root: &Value,
    individual: &Value,
) -> Option<QuotaSnapshot> {
    let remaining_percent = individual
        .get("remainingPercent")
        .and_then(number_value)
        .map(|remaining| remaining.round().clamp(0.0, 100.0))?;
    let used_percent = Some((100.0 - remaining_percent).max(0.0));
    let reset_at_unix = individual
        .get("resetsAt")
        .and_then(number_value)
        .and_then(epoch_to_unix);
    let label = root
        .get("limitName")
        .and_then(Value::as_str)
        .unwrap_or("Individual limit")
        .to_string();
    Some(QuotaSnapshot {
        source_id: source_id.to_string(),
        provider: QuotaProvider::Codex,
        probe_source: QuotaProbeSource::CodexAppserver,
        quota_known: true,
        remaining_percent: Some(remaining_percent),
        used_percent,
        reset_at_unix,
        window: Some(label.clone()),
        available: Some(remaining_percent > 0.0),
        message: quota_message(
            Some(&label),
            used_percent,
            Some(remaining_percent),
            reset_at_unix,
        ),
        observed_at_unix: now_unix(),
    })
}

fn codex_window_label(kind: &str, duration_mins: Option<f64>, limit_name: Option<&str>) -> String {
    let base = match (kind, duration_mins.map(|duration| duration.round() as u64)) {
        ("primary", Some(300)) => "Current session".to_string(),
        ("secondary", Some(10_080)) => "Current week".to_string(),
        ("primary", Some(duration)) => format!("{duration}m window"),
        ("secondary", Some(duration)) => format!("{duration}m secondary window"),
        ("primary", None) => "Primary window".to_string(),
        ("secondary", None) => "Secondary window".to_string(),
        _ => "Usage window".to_string(),
    };
    match limit_name {
        Some(limit_name) if !limit_name.trim().is_empty() => format!("{base} ({limit_name})"),
        _ => base,
    }
}

fn detect_quota_provider(source_id: &str) -> QuotaProvider {
    let lower = source_id.to_ascii_lowercase();
    if lower.contains("claude") {
        QuotaProvider::Claude
    } else if lower.contains("codex") || lower.contains("openai") || lower.contains("gpt") {
        QuotaProvider::Codex
    } else {
        QuotaProvider::Unknown
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
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn parses_claude_usage_command_output() {
        let snapshots = claude_usage_snapshots(
            "claude-code",
            "You are currently using your subscription\n\nCurrent session: 3% used · resets Jun 26 at 3:50pm (Europe/Paris)\nCurrent week (all models): 6% used · resets Jul 2 at 1am (Europe/Paris)\nCurrent week (Sonnet only): 0% used",
        );

        assert_eq!(snapshots.len(), 3);
        let snapshot = &snapshots[0];
        assert_eq!(snapshot.provider, QuotaProvider::Claude);
        assert_eq!(snapshot.probe_source, QuotaProbeSource::ClaudeUsageCommand);
        assert_eq!(snapshot.window.as_deref(), Some("Current session"));
        assert_eq!(snapshot.used_percent, Some(3.0));
        assert_eq!(snapshot.remaining_percent, Some(97.0));
        assert!(snapshot.reset_at_unix.is_some());
        assert_eq!(snapshot.available, Some(true));
    }

    #[test]
    fn parses_claude_reset_times_against_local_year() {
        let now = match Local.with_ymd_and_hms(2026, 6, 26, 10, 0, 0) {
            LocalResult::Single(value) => value,
            LocalResult::Ambiguous(value, _) => value,
            LocalResult::None => panic!("valid local test time"),
        };
        let expected = local_epoch_seconds(2026, 6, 26, 15, 50).expect("expected timestamp");

        assert_eq!(
            parse_claude_reset_at_unix("3% used · resets Jun 26 at 3:50pm (Europe/Paris)", now),
            Some(expected)
        );
    }

    #[test]
    fn rolls_claude_reset_times_into_next_year_when_needed() {
        let now = match Local.with_ymd_and_hms(2026, 12, 31, 23, 0, 0) {
            LocalResult::Single(value) => value,
            LocalResult::Ambiguous(value, _) => value,
            LocalResult::None => panic!("valid local test time"),
        };
        let expected = local_epoch_seconds(2027, 1, 1, 1, 0).expect("expected timestamp");

        assert_eq!(
            parse_claude_reset_at_unix("6% used · resets Jan 1 at 1am (Europe/Paris)", now),
            Some(expected)
        );
    }

    #[test]
    fn extracts_result_text_from_claude_json_output() {
        let value = json!([
            {
                "type": "assistant",
                "message": {
                    "content": [
                        { "type": "text", "text": "fallback" }
                    ]
                }
            },
            {
                "type": "result",
                "result": "Current session: 4% used"
            }
        ]);

        assert_eq!(
            claude_usage_text(&value).as_deref(),
            Some("Current session: 4% used")
        );
    }

    #[test]
    fn parses_codex_appserver_rate_limit_response() {
        let snapshots = codex_rate_limit_snapshots(
            "codex",
            &json!({
                "rateLimits": {
                    "limitId": "codex",
                    "limitName": null,
                    "primary": {
                        "usedPercent": 4,
                        "windowDurationMins": 300,
                        "resetsAt": 1_800_000_002
                    },
                    "secondary": {
                        "usedPercent": 24,
                        "windowDurationMins": 10080,
                        "resetsAt": 1_800_000_003
                    },
                    "credits": {
                        "hasCredits": false,
                        "unlimited": false,
                        "balance": "0"
                    },
                    "individualLimit": null,
                    "planType": "pro",
                    "rateLimitReachedType": null
                },
                "rateLimitsByLimitId": {
                    "codex": {
                        "limitId": "codex",
                        "limitName": null,
                        "primary": {
                            "usedPercent": 4,
                            "windowDurationMins": 300,
                            "resetsAt": 1_800_000_002
                        },
                        "secondary": null,
                        "credits": null,
                        "individualLimit": null,
                        "planType": "pro",
                        "rateLimitReachedType": null
                    }
                },
                "rateLimitResetCredits": {
                    "availableCount": 2
                }
            }),
        );

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].provider, QuotaProvider::Codex);
        assert_eq!(snapshots[0].probe_source, QuotaProbeSource::CodexAppserver);
        assert_eq!(snapshots[0].window.as_deref(), Some("Current session"));
        assert_eq!(snapshots[0].used_percent, Some(4.0));
        assert_eq!(snapshots[0].remaining_percent, Some(96.0));
        assert_eq!(snapshots[0].reset_at_unix, Some(1_800_000_002));
        assert_eq!(snapshots[1].window.as_deref(), Some("Current week"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_codex_appserver_reads_rate_limits_over_stdio() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let codex = temp.path().join("codex");
        std::fs::write(
            &codex,
            r#"#!/bin/sh
printf '%s\n' '{"id":1,"result":{"ok":true}}'
printf '%s\n' '{"id":2,"result":{"rateLimits":{"limitId":"codex","limitName":null,"primary":{"usedPercent":5,"windowDurationMins":300,"resetsAt":1800000002},"secondary":{"usedPercent":25,"windowDurationMins":10080,"resetsAt":1800000003},"credits":null,"individualLimit":null,"planType":"pro","rateLimitReachedType":null},"rateLimitsByLimitId":null,"rateLimitResetCredits":null}}'
"#,
        )
        .expect("write fake codex");
        let mut permissions = std::fs::metadata(&codex).expect("metadata").permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&codex, permissions).expect("chmod fake codex");
        let agent = SelectedAgent {
            source_id: "custom:codex".to_string(),
            program: codex,
            args: Vec::new(),
            env: HashMap::new(),
        };

        let snapshots = refresh_codex_appserver_usage(&agent).await;

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].provider, QuotaProvider::Codex);
        assert_eq!(snapshots[0].probe_source, QuotaProbeSource::CodexAppserver);
        assert_eq!(snapshots[0].used_percent, Some(5.0));
        assert_eq!(snapshots[0].remaining_percent, Some(95.0));
        assert_eq!(snapshots[0].reset_at_unix, Some(1_800_000_002));
        assert_eq!(snapshots[1].window.as_deref(), Some("Current week"));
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
