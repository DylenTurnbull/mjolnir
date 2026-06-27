//! Non-interactive `mj --print` runner.
//!
//! This reuses the same ACP runtime as the TUI and swaps the terminal UI for a
//! small event collector. Thor-enabled runs default to the Anvil ACP backend
//! when `~/.config/mj/config.toml` has no configured backend yet.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_client_protocol::schema::v1::{
    PermissionOptionKind, SessionUpdate, StopReason, ToolCall, ToolCallStatus, ToolCallUpdate,
    ToolKind, Usage,
};
use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::acp::AcpRuntimeConfig;
use crate::config;
use crate::event::{PermissionDecision, UiCommand, UiEvent, content_block_text};
use crate::remote;
use crate::thor;

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Text,
    Json,
    StreamJson,
}

#[derive(Debug, Clone, Copy)]
pub enum PermissionMode {
    Default,
    AcceptEdits,
    BypassPermissions,
}

pub struct RunConfig {
    pub prompt: String,
    pub cwd: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub resume_session: Option<String>,
    pub agent_stderr: Option<PathBuf>,
    pub fs_max_text_bytes: u64,
    pub output_format: OutputFormat,
    pub permission_mode: PermissionMode,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamRecord<'a> {
    Connected {
        agent_name: Option<&'a str>,
        agent_version: Option<&'a str>,
    },
    SessionStarted {
        session_id: &'a str,
        resumed: bool,
    },
    AgentMessage {
        text: &'a str,
    },
    AgentThought {
        text: &'a str,
    },
    ToolCall {
        id: &'a str,
        title: &'a str,
        kind: String,
        status: String,
    },
    ToolCallUpdate {
        id: &'a str,
        title: Option<&'a str>,
        kind: Option<String>,
        status: Option<String>,
    },
    Permission {
        tool_call_id: &'a str,
        decision: &'a str,
    },
    Info {
        message: &'a str,
    },
    Warning {
        message: &'a str,
    },
    Error {
        message: &'a str,
    },
    Result {
        stop_reason: String,
        session_id: Option<&'a str>,
        resumed: bool,
        text: &'a str,
        usage: Option<&'a Usage>,
        error: Option<&'a str>,
    },
}

#[derive(Debug, Serialize)]
struct JsonResult<'a> {
    session_id: Option<&'a str>,
    resumed: bool,
    result: &'a str,
    stop_reason: String,
    usage: Option<&'a Usage>,
    error: Option<&'a str>,
}

#[derive(Debug, Default)]
struct HeadlessState {
    final_text: String,
    tool_calls: HashMap<String, ToolCall>,
}

pub async fn run(cfg: RunConfig) -> Result<()> {
    if cfg.prompt.trim().is_empty() {
        bail!("empty prompt");
    }

    let config_path = config::default_config_path();
    let mut app_config = config::Config::load(&config_path)
        .with_context(|| format!("load {}", config_path.display()))?;
    let agent = match app_config.agent.take() {
        Some(agent) => agent,
        None => {
            let agent = thor::default_anvil_agent();
            app_config.agent = Some(agent.clone());
            app_config
                .save(&config_path)
                .with_context(|| format!("save {}", config_path.display()))?;
            agent
        }
    };

    let project_label = crate::paths::project_label_from_cwd(&cfg.cwd);
    let thor_config = app_config.thor.clone();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let progress_event_tx = event_tx.clone();
    let timeout_event_tx = event_tx.clone();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let thor_progress_path = thor_progress_path();
    let _ = std::fs::remove_file(&thor_progress_path);
    let runtime_cfg = AcpRuntimeConfig {
        command: agent.program,
        args: agent.args,
        cwd: cfg.cwd,
        additional_directories: cfg.additional_directories,
        mcp_servers: crate::thor_mcp::mcp_servers_with_progress(
            config_path.clone(),
            Some(thor_progress_path.clone()),
        )?,
        resume_session: cfg.resume_session.clone(),
        env: agent.env,
        agent_stderr: cfg.agent_stderr,
        fs_max_text_bytes: cfg.fs_max_text_bytes,
    };

    let runtime = tokio::spawn(async move { crate::acp::run(runtime_cfg, event_tx, cmd_rx).await });
    let timeout_task = cfg.timeout.map(|timeout| {
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            let _ = timeout_event_tx.send(UiEvent::PromptFailed {
                message: format!("headless prompt timed out after {}s", timeout.as_secs()),
            });
        })
    });
    let progress_proxy = tokio::spawn(poll_thor_progress(
        thor_progress_path.clone(),
        progress_event_tx.clone(),
    ));
    let thor_turn_active = Arc::new(AtomicBool::new(false));
    let progress_heartbeat = tokio::spawn(thor_activity_heartbeat(
        thor_turn_active.clone(),
        progress_event_tx,
    ));
    // No UI event channel: headless answers permissions by policy, so
    // remote decisions have nothing to resolve.
    let remote_tracker = remote::RemoteSessionTracker::new(
        project_label,
        "Thor".to_string(),
        Some(cmd_tx.clone()),
        None,
    );

    let mut state = HeadlessState::default();
    let mut sent_prompt = false;
    let mut saw_terminal_event = false;
    let mut stop_reason = None;
    let mut usage = None;
    let mut session_id = None;
    let mut resumed = false;
    let mut terminal_error = None;
    let mut prompt_sent = false;
    let mut collecting_turn_output = false;

    while let Some(event) = event_rx.recv().await {
        let event = remote_tracker.intercept_event(event);
        remote_tracker.observe_event(&event);
        if matches!(cfg.output_format, OutputFormat::StreamJson) {
            emit_stream_event(&event, &state)?;
        }

        match event {
            UiEvent::Connected {
                agent_name,
                agent_version,
                ..
            } => {
                if matches!(cfg.output_format, OutputFormat::StreamJson) {
                    emit_json(&StreamRecord::Connected {
                        agent_name: agent_name.as_deref(),
                        agent_version: agent_version.as_deref(),
                    })?;
                }
            }
            UiEvent::SessionStarted {
                session_id: started_session_id,
                resumed: was_resumed,
            } => {
                session_id = Some(started_session_id.clone());
                resumed = was_resumed;
                if matches!(cfg.output_format, OutputFormat::StreamJson) {
                    emit_json(&StreamRecord::SessionStarted {
                        session_id: &started_session_id,
                        resumed: was_resumed,
                    })?;
                }
                if !sent_prompt {
                    sent_prompt = true;
                    prompt_sent = true;
                    thor_turn_active.store(true, Ordering::Relaxed);
                    let command = UiCommand::SendPrompt {
                        text: cfg.prompt.clone(),
                        images: Vec::new(),
                    };
                    remote_tracker.observe_command(&command);
                    let command = UiCommand::SendPrompt {
                        text: thor::host_prompt(&thor_config, &cfg.prompt),
                        images: Vec::new(),
                    };
                    cmd_tx.send(command).context("send prompt to ACP runtime")?;
                }
            }
            UiEvent::SessionUpdate(update) => {
                apply_session_update(&mut state, update, prompt_sent, &mut collecting_turn_output);
            }
            UiEvent::TerminalOutput(_) => {}
            UiEvent::SessionConfigOptions => {}
            UiEvent::PermissionRequest(prompt) => {
                let decision =
                    permission_decision(cfg.permission_mode, &prompt.tool_call, &prompt.options);
                let decision_label = match &decision {
                    Some(_) => "selected",
                    None => "cancelled",
                };
                if matches!(cfg.output_format, OutputFormat::StreamJson) {
                    emit_json(&StreamRecord::Permission {
                        tool_call_id: &prompt.tool_call.tool_call_id.to_string(),
                        decision: decision_label,
                    })?;
                }
                let _ = prompt.responder.send(match decision {
                    Some(option_id) => PermissionDecision::Selected(option_id),
                    None => PermissionDecision::Cancelled,
                });
            }
            UiEvent::PromptDone {
                stop_reason: reason,
                usage: prompt_usage,
            } => {
                thor_turn_active.store(false, Ordering::Relaxed);
                stop_reason = Some(reason);
                usage = prompt_usage;
                saw_terminal_event = true;
                let _ = cmd_tx.send(UiCommand::Shutdown);
                break;
            }
            UiEvent::PromptFailed { message }
            | UiEvent::SessionForkFailed { message }
            | UiEvent::Fatal(message) => {
                thor_turn_active.store(false, Ordering::Relaxed);
                if matches!(cfg.output_format, OutputFormat::StreamJson) {
                    emit_json(&StreamRecord::Error { message: &message })?;
                }
                terminal_error = Some(message);
                saw_terminal_event = true;
                let _ = cmd_tx.send(UiCommand::Shutdown);
                break;
            }
            UiEvent::Warning(message) => {
                if matches!(cfg.output_format, OutputFormat::StreamJson) {
                    emit_json(&StreamRecord::Warning { message: &message })?;
                } else {
                    eprintln!("warning: {message}");
                }
            }
            UiEvent::Info(_) => {}
            UiEvent::CancelPendingPermissions => {}
            // Headless runs never receive remote decisions (no UI event
            // channel is registered with the tracker).
            UiEvent::RemotePermissionDecision { .. } => {}
        }
    }

    if !saw_terminal_event {
        let _ = cmd_tx.send(UiCommand::Shutdown);
    }
    match tokio::time::timeout(std::time::Duration::from_secs(2), runtime).await {
        Ok(joined) => {
            joined.context("join ACP runtime")??;
        }
        Err(_) => {
            // The TUI path handles this same case by aborting; in headless mode
            // we keep that behavior local to the spawned task.
        }
    }
    progress_proxy.abort();
    progress_heartbeat.abort();
    if let Some(timeout_task) = timeout_task {
        timeout_task.abort();
        let _ = timeout_task.await;
    }
    let _ = progress_proxy.await;
    let _ = progress_heartbeat.await;
    let _ = std::fs::remove_file(thor_progress_path);
    remote_tracker.shutdown().await;

    let stop_reason_label = stop_reason.map(stop_reason_label).unwrap_or_else(|| {
        if terminal_error.is_some() {
            "error"
        } else {
            "cancelled"
        }
    });
    match cfg.output_format {
        OutputFormat::Text => {
            print!("{}", state.final_text);
            if !state.final_text.ends_with('\n') {
                println!();
            }
        }
        OutputFormat::Json => {
            emit_json(&JsonResult {
                session_id: session_id.as_deref(),
                resumed,
                result: &state.final_text,
                stop_reason: stop_reason_label.to_string(),
                usage: usage.as_ref(),
                error: terminal_error.as_deref(),
            })?;
        }
        OutputFormat::StreamJson => {
            emit_json(&StreamRecord::Result {
                stop_reason: stop_reason_label.to_string(),
                session_id: session_id.as_deref(),
                resumed,
                text: &state.final_text,
                usage: usage.as_ref(),
                error: terminal_error.as_deref(),
            })?;
        }
    }

    if let Some(message) = terminal_error {
        Err(anyhow!(message))
    } else if matches!(
        stop_reason.unwrap_or(StopReason::Cancelled),
        StopReason::EndTurn | StopReason::MaxTokens | StopReason::MaxTurnRequests
    ) {
        Ok(())
    } else {
        Err(anyhow!("prompt stopped with {}", stop_reason_label))
    }
}

fn thor_progress_path() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "mj-thor-headless-progress-{}-{now}.jsonl",
        std::process::id()
    ))
}

async fn poll_thor_progress(path: PathBuf, event_tx: mpsc::UnboundedSender<UiEvent>) {
    let mut offset = 0usize;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tick.tick().await;
        let Ok(body) = std::fs::read_to_string(&path) else {
            continue;
        };
        if body.len() < offset {
            offset = 0;
        }
        let Some(new_body) = body.get(offset..) else {
            offset = body.len();
            continue;
        };
        offset = body.len();
        for line in new_body.lines() {
            if let Some(message) = thor_progress_message(line) {
                let _ = event_tx.send(UiEvent::Info(message));
            }
        }
    }
}

async fn thor_activity_heartbeat(
    active: Arc<AtomicBool>,
    event_tx: mpsc::UnboundedSender<UiEvent>,
) {
    let mut elapsed_seconds = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(15)).await;
        if let Some(message) =
            thor_heartbeat_tick(active.load(Ordering::Relaxed), &mut elapsed_seconds, 15)
        {
            let _ = event_tx.send(UiEvent::Info(message));
        }
    }
}

fn thor_heartbeat_tick(
    active: bool,
    elapsed_seconds: &mut u64,
    step_seconds: u64,
) -> Option<String> {
    if !active {
        *elapsed_seconds = 0;
        return None;
    }
    *elapsed_seconds = elapsed_seconds.saturating_add(step_seconds);
    Some(format!(
        "Thor is still working... {elapsed_seconds}s elapsed"
    ))
}

fn thor_progress_message(line: &str) -> Option<String> {
    let record: serde_json::Value = serde_json::from_str(line).ok()?;
    let kind = record.get("kind").and_then(serde_json::Value::as_str)?;
    let detail = record.get("detail").and_then(serde_json::Value::as_str)?;
    Some(format!(
        "Thor worker {}: {}",
        thor_progress_kind_label(kind),
        detail
    ))
}

fn thor_progress_kind_label(kind: &str) -> &'static str {
    match kind {
        "session_started" => "started",
        "prompt_sent" => "prompt sent",
        "tool_call" => "tool",
        "tool_update" => "tool update",
        "permission" => "permission",
        "prompt_done" => "done",
        "timeout" => "timeout",
        "error" => "error",
        "worker_closed" => "closed",
        _ => "progress",
    }
}

fn apply_session_update(
    state: &mut HeadlessState,
    update: SessionUpdate,
    prompt_sent: bool,
    collecting_turn_output: &mut bool,
) {
    match update {
        SessionUpdate::UserMessageChunk(_) if prompt_sent => {
            *collecting_turn_output = true;
        }
        SessionUpdate::AgentThoughtChunk(_) if prompt_sent => {
            *collecting_turn_output = true;
        }
        SessionUpdate::AgentMessageChunk(chunk) if *collecting_turn_output => {
            state
                .final_text
                .push_str(&content_block_text(&chunk.content));
        }
        SessionUpdate::ToolCall(tool_call) => {
            if prompt_sent {
                *collecting_turn_output = true;
            }
            state
                .tool_calls
                .insert(tool_call.tool_call_id.to_string(), tool_call);
        }
        SessionUpdate::ToolCallUpdate(update) => {
            if prompt_sent {
                *collecting_turn_output = true;
            }
            let id = update.tool_call_id.to_string();
            if let Some(existing) = state.tool_calls.get_mut(&id) {
                existing.update(update.fields);
            } else if let Ok(tool_call) = ToolCall::try_from(update) {
                state.tool_calls.insert(id, tool_call);
            }
        }
        _ => {}
    }
}

fn emit_stream_event(event: &UiEvent, state: &HeadlessState) -> Result<()> {
    match event {
        UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(chunk)) => {
            let text = content_block_text(&chunk.content);
            emit_json(&StreamRecord::AgentMessage { text: &text })?;
        }
        UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(chunk)) => {
            let text = content_block_text(&chunk.content);
            emit_json(&StreamRecord::AgentThought { text: &text })?;
        }
        UiEvent::SessionUpdate(SessionUpdate::ToolCall(tool_call)) => {
            emit_json(&StreamRecord::ToolCall {
                id: &tool_call.tool_call_id.to_string(),
                title: &tool_call.title,
                kind: tool_kind_label(tool_call.kind).to_string(),
                status: tool_status_label(tool_call.status).to_string(),
            })?;
        }
        UiEvent::SessionUpdate(SessionUpdate::ToolCallUpdate(update)) => {
            let existing = state.tool_calls.get(&update.tool_call_id.to_string());
            emit_json(&StreamRecord::ToolCallUpdate {
                id: &update.tool_call_id.to_string(),
                title: update
                    .fields
                    .title
                    .as_deref()
                    .or_else(|| existing.map(|t| t.title.as_str())),
                kind: update.fields.kind.map(|k| tool_kind_label(k).to_string()),
                status: update
                    .fields
                    .status
                    .map(|s| tool_status_label(s).to_string()),
            })?;
        }
        UiEvent::Info(message) => {
            emit_json(&StreamRecord::Info { message })?;
        }
        _ => {}
    }
    Ok(())
}

fn permission_decision(
    mode: PermissionMode,
    tool_call: &ToolCallUpdate,
    options: &[agent_client_protocol::schema::v1::PermissionOption],
) -> Option<String> {
    let allow = match mode {
        PermissionMode::Default => false,
        PermissionMode::BypassPermissions => true,
        PermissionMode::AcceptEdits => matches!(
            tool_call.fields.kind,
            Some(ToolKind::Edit | ToolKind::Delete | ToolKind::Move)
        ),
    };
    if !allow {
        return None;
    }
    choose_allow_option(options)
}

fn choose_allow_option(
    options: &[agent_client_protocol::schema::v1::PermissionOption],
) -> Option<String> {
    options
        .iter()
        .find(|option| option.kind == PermissionOptionKind::AllowAlways)
        .or_else(|| {
            options
                .iter()
                .find(|option| option.kind == PermissionOptionKind::AllowOnce)
        })
        .map(|option| option.option_id.to_string())
}

fn emit_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

fn stop_reason_label(reason: StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn => "end_turn",
        StopReason::MaxTokens => "max_tokens",
        StopReason::MaxTurnRequests => "max_turn_requests",
        StopReason::Refusal => "refusal",
        StopReason::Cancelled => "cancelled",
        _ => "other",
    }
}

fn tool_kind_label(kind: ToolKind) -> &'static str {
    match kind {
        ToolKind::Read => "read",
        ToolKind::Edit => "edit",
        ToolKind::Delete => "delete",
        ToolKind::Move => "move",
        ToolKind::Search => "search",
        ToolKind::Execute => "execute",
        ToolKind::Think => "think",
        ToolKind::Fetch => "fetch",
        ToolKind::SwitchMode => "switch_mode",
        ToolKind::Other => "other",
        _ => "other",
    }
}

fn tool_status_label(status: ToolCallStatus) -> &'static str {
    match status {
        ToolCallStatus::Pending => "pending",
        ToolCallStatus::InProgress => "in_progress",
        ToolCallStatus::Completed => "completed",
        ToolCallStatus::Failed => "failed",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::ToolCallUpdateFields;

    fn permission_options() -> Vec<agent_client_protocol::schema::v1::PermissionOption> {
        vec![
            agent_client_protocol::schema::v1::PermissionOption::new(
                "approve",
                "Approve Thor plan",
                PermissionOptionKind::AllowOnce,
            ),
            agent_client_protocol::schema::v1::PermissionOption::new(
                "reject",
                "Reject",
                PermissionOptionKind::RejectOnce,
            ),
        ]
    }

    fn tool_call(id: &str, kind: ToolKind) -> ToolCallUpdate {
        let mut fields = ToolCallUpdateFields::default();
        fields.kind = Some(kind);
        ToolCallUpdate::new(id.to_string(), fields)
    }

    #[test]
    fn default_headless_mode_rejects_host_think_permissions() {
        let decision = permission_decision(
            PermissionMode::Default,
            &tool_call("host-think", ToolKind::Think),
            &permission_options(),
        );

        assert_eq!(decision, None);
    }

    #[test]
    fn headless_thor_progress_message_formats_worker_update() {
        let message =
            thor_progress_message(r#"{"kind":"tool_call","detail":"cargo test (execute)"}"#);

        assert_eq!(
            message.as_deref(),
            Some("Thor worker tool: cargo test (execute)")
        );
        assert_eq!(thor_progress_message("not json"), None);
    }

    #[test]
    fn headless_thor_heartbeat_tick_emits_distinct_elapsed_messages_and_resets() {
        let mut elapsed = 0;

        assert_eq!(
            thor_heartbeat_tick(true, &mut elapsed, 15).as_deref(),
            Some("Thor is still working... 15s elapsed")
        );
        assert_eq!(
            thor_heartbeat_tick(true, &mut elapsed, 15).as_deref(),
            Some("Thor is still working... 30s elapsed")
        );
        assert_eq!(thor_heartbeat_tick(false, &mut elapsed, 15), None);
        assert_eq!(elapsed, 0);
    }
}
