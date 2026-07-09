//! One-shot nested ACP agent orchestration for `_mj/codeAgent`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{SessionUpdate, StopReason};
use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::{Mutex, mpsc};

use crate::acp::{self, AcpRuntimeConfig, RuntimeAccessMode};
use crate::event::{CodeAgentEvent, CodeAgentOutcome, UiCommand, UiEvent, content_block_text};

pub const WIRE_METHOD: &str = "_mj/codeAgent";
pub const SDK_METHOD: &str = "mj/codeAgent";
pub const LABEL: &str = "codex";

#[derive(Debug, Clone)]
pub struct Config {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub agent_stderr: Option<PathBuf>,
}

impl Config {
    pub fn codex(agent_stderr: Option<PathBuf>, env: HashMap<String, String>) -> Self {
        Self {
            command: PathBuf::from("npx"),
            args: vec![
                "-y".to_string(),
                "@agentclientprotocol/codex-acp".to_string(),
            ],
            env,
            agent_stderr,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunContext {
    pub cwd: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub fs_max_text_bytes: u64,
    pub access_mode: RuntimeAccessMode,
}

#[derive(Debug, Default)]
enum ActiveRun {
    #[default]
    Idle,
    Starting {
        cancel_requested: bool,
        shutdown_requested: bool,
    },
    Running {
        commands: mpsc::UnboundedSender<UiCommand>,
    },
}

/// Coordinates the one allowed nested run and routes primary UI cancellation
/// without giving the nested runtime ownership of the primary command stream.
#[derive(Debug, Clone, Default)]
pub struct Controller {
    state: Arc<Mutex<ActiveRun>>,
}

impl Controller {
    pub async fn begin(&self) -> bool {
        let mut state = self.state.lock().await;
        if !matches!(*state, ActiveRun::Idle) {
            return false;
        }
        *state = ActiveRun::Starting {
            cancel_requested: false,
            shutdown_requested: false,
        };
        true
    }

    async fn attach(&self, commands: mpsc::UnboundedSender<UiCommand>) {
        let mut state = self.state.lock().await;
        let (cancel_requested, shutdown_requested) = match *state {
            ActiveRun::Starting {
                cancel_requested,
                shutdown_requested,
            } => (cancel_requested, shutdown_requested),
            _ => (false, false),
        };
        *state = ActiveRun::Running {
            commands: commands.clone(),
        };
        if shutdown_requested {
            let _ = commands.send(UiCommand::Shutdown);
        } else if cancel_requested {
            let _ = commands.send(UiCommand::CancelPrompt);
        }
    }

    pub async fn cancel(&self) -> bool {
        let mut state = self.state.lock().await;
        match &mut *state {
            ActiveRun::Idle => false,
            ActiveRun::Starting {
                cancel_requested, ..
            } => {
                *cancel_requested = true;
                true
            }
            ActiveRun::Running { commands } => {
                let _ = commands.send(UiCommand::CancelPrompt);
                true
            }
        }
    }

    pub async fn shutdown(&self) -> bool {
        let mut state = self.state.lock().await;
        match &mut *state {
            ActiveRun::Idle => false,
            ActiveRun::Starting {
                shutdown_requested, ..
            } => {
                *shutdown_requested = true;
                true
            }
            ActiveRun::Running { commands } => {
                let _ = commands.send(UiCommand::Shutdown);
                true
            }
        }
    }

    pub async fn finish(&self) {
        *self.state.lock().await = ActiveRun::Idle;
    }
}

struct AgentMessageCollector {
    last: String,
    message_open: bool,
}

impl AgentMessageCollector {
    fn new() -> Self {
        Self {
            last: String::new(),
            message_open: false,
        }
    }

    fn observe(&mut self, update: &SessionUpdate) {
        match update {
            SessionUpdate::AgentMessageChunk(chunk) => {
                if !self.message_open {
                    self.last.clear();
                    self.message_open = true;
                }
                self.last.push_str(&content_block_text(&chunk.content));
            }
            SessionUpdate::UserMessageChunk(_)
            | SessionUpdate::AgentThoughtChunk(_)
            | SessionUpdate::ToolCall(_)
            | SessionUpdate::Plan(_) => self.message_open = false,
            _ => {}
        }
    }

    fn finish(self) -> Result<String> {
        if self.last.trim().is_empty() {
            bail!("code agent completed without a final message");
        }
        Ok(self.last)
    }
}

pub fn run_boxed(
    config: Config,
    context: RunContext,
    instructions: String,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    controller: Controller,
) -> futures::future::BoxFuture<'static, Result<String>> {
    Box::pin(run(config, context, instructions, ui_tx, controller))
}

async fn run(
    config: Config,
    context: RunContext,
    instructions: String,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    controller: Controller,
) -> Result<String> {
    let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Started {
        label: LABEL.to_string(),
        instructions: instructions.clone(),
    }));

    let (nested_event_tx, mut nested_event_rx) = mpsc::unbounded_channel();
    let (nested_cmd_tx, nested_cmd_rx) = mpsc::unbounded_channel();
    controller.attach(nested_cmd_tx.clone()).await;

    let runtime_config = AcpRuntimeConfig {
        command: config.command,
        args: config.args,
        cwd: context.cwd,
        additional_directories: context.additional_directories,
        resume_session: None,
        env: config.env,
        agent_stderr: config.agent_stderr,
        fs_max_text_bytes: context.fs_max_text_bytes,
        access_mode: context.access_mode,
        agent_source_id: None,
        config_path: None,
        saved_session_config: HashMap::new(),
        code_agent: None,
    };
    let mut runtime = tokio::spawn(acp::run(runtime_config, nested_event_tx, nested_cmd_rx));

    let mut prompt_sent = false;
    let mut collector = AgentMessageCollector::new();
    let result = loop {
        tokio::select! {
            joined = &mut runtime => {
                break match joined {
                    Ok(Ok(())) => Err(anyhow!("code agent runtime closed before completing")),
                    Ok(Err(error)) => Err(error).context("code agent runtime"),
                    Err(error) => Err(anyhow!("code agent task failed: {error}")),
                };
            }
            event = nested_event_rx.recv() => {
                let Some(event) = event else {
                    break Err(anyhow!("code agent event stream closed before completing"));
                };
                match event {
                    UiEvent::Connected { .. } => {}
                    UiEvent::SessionStarted { .. } if !prompt_sent => {
                        prompt_sent = true;
                        if nested_cmd_tx
                            .send(UiCommand::SendPrompt {
                                text: instructions.clone(),
                                images: Vec::new(),
                            })
                            .is_err()
                        {
                            break Err(anyhow!("send instructions to code agent"));
                        }
                    }
                    UiEvent::SessionStarted { .. } | UiEvent::SessionConfigOptions { .. } => {}
                    UiEvent::SessionUpdate(update) => {
                        collector.observe(&update);
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::SessionUpdate(update)));
                    }
                    UiEvent::TerminalOutput(snapshot) => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::TerminalOutput(snapshot)));
                    }
                    UiEvent::PermissionRequest(prompt) => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::PermissionRequest(prompt)));
                    }
                    UiEvent::ElicitationRequest(prompt) => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::ElicitationRequest(prompt)));
                    }
                    UiEvent::CancelPendingPermissions => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::CancelPendingPermissions));
                    }
                    UiEvent::Info(message) | UiEvent::Warning(message) => {
                        let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Status(message)));
                    }
                    UiEvent::PromptDone { stop_reason, .. } => {
                        break if matches!(stop_reason, StopReason::Cancelled) {
                            Err(anyhow!("code agent cancelled"))
                        } else {
                            collector.finish()
                        };
                    }
                    UiEvent::PromptFailed { message }
                    | UiEvent::SessionForkFailed { message }
                    | UiEvent::Fatal(message) => break Err(anyhow!(message)),
                    UiEvent::ClaudeUsage(_) | UiEvent::RemotePermissionDecision { .. } => {}
                    UiEvent::CodeAgent(_) => {
                        break Err(anyhow!("nested code agent attempted recursive delegation"));
                    }
                }
            }
        }
    };

    let _ = nested_cmd_tx.send(UiCommand::Shutdown);
    if !runtime.is_finished()
        && tokio::time::timeout(Duration::from_secs(2), &mut runtime)
            .await
            .is_err()
    {
        runtime.abort();
    }
    controller.finish().await;

    let outcome = match &result {
        Ok(_) => CodeAgentOutcome::Completed,
        Err(error) if error.to_string().contains("cancelled") => CodeAgentOutcome::Cancelled,
        Err(error) => CodeAgentOutcome::Failed(error.to_string()),
    };
    let _ = ui_tx.send(UiEvent::CodeAgent(CodeAgentEvent::Finished { outcome }));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{ContentBlock, ContentChunk, TextContent};

    #[test]
    fn collector_returns_last_agent_message() {
        let mut collector = AgentMessageCollector::new();
        collector.observe(&SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("first")),
        )));
        collector.observe(&SessionUpdate::ToolCall(
            agent_client_protocol::schema::v1::ToolCall::new("tool", "work"),
        ));
        collector.observe(&SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("final")),
        )));
        assert_eq!(collector.finish().expect("message"), "final");
    }

    #[test]
    fn collector_rejects_message_less_completion() {
        let collector = AgentMessageCollector::new();
        assert!(collector.finish().is_err());
    }

    #[tokio::test]
    async fn controller_rejects_concurrent_runs_and_resets() {
        let controller = Controller::default();
        assert!(controller.begin().await);
        assert!(!controller.begin().await);
        assert!(controller.cancel().await);
        controller.finish().await;
        assert!(controller.begin().await);
    }

    #[tokio::test]
    async fn shutdown_requested_while_starting_reaches_nested_runtime() {
        let controller = Controller::default();
        assert!(controller.begin().await);
        assert!(controller.shutdown().await);
        let (commands, mut receiver) = mpsc::unbounded_channel();
        controller.attach(commands).await;
        assert!(matches!(receiver.recv().await, Some(UiCommand::Shutdown)));
    }
}
