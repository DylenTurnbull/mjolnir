//! Project structured `mj mcp` progress embedded in ACP tool results into
//! role- and model-attributed transcript activity.

use std::collections::{HashMap, HashSet};

use agent_client_protocol::schema::v1::{SessionUpdate, ToolCallContent};
use serde_json::Value;

use crate::event::{ActorActivity, ActorIdentity, content_block_text};
use crate::mcp::{POLL_PROGRESS_SCHEMA, PollResult, ProgressItem};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConnectionState {
    status: String,
    turn_id: u64,
    turn_status: String,
}

#[derive(Debug, Clone)]
struct ToolState {
    title: String,
    kind: Option<String>,
    status: Option<String>,
}

#[derive(Default)]
pub struct TranscriptBridge {
    seen_progress: HashSet<(String, u64, u64)>,
    seen_connections: HashSet<String>,
    actors: HashMap<String, ActorIdentity>,
    connection_states: HashMap<String, ConnectionState>,
    tool_states: HashMap<(String, u64, String), ToolState>,
}

impl TranscriptBridge {
    pub fn observe_session_update(&mut self, update: &SessionUpdate) -> Vec<ActorActivity> {
        let mut activities = Vec::new();
        for value in tool_result_values(update) {
            self.observe_value(&value, &mut activities);
        }
        activities
    }

    fn observe_value(&mut self, value: &Value, activities: &mut Vec<ActorActivity>) {
        if let Ok(poll) = serde_json::from_value::<PollResult>(value.clone())
            && poll.schema == POLL_PROGRESS_SCHEMA
        {
            self.project_poll(poll, activities);
        }
        for key in ["structuredContent", "structured_content", "result", "data"] {
            if let Some(nested) = value.get(key) {
                self.observe_value(nested, activities);
            }
        }
    }

    fn project_poll(&mut self, poll: PollResult, activities: &mut Vec<ActorActivity>) {
        let connection_id = poll.connection_id.clone();
        let actor = self.merge_actor(
            &connection_id,
            poll.source_id.as_deref(),
            poll.model_name.as_deref(),
            poll.model_value.as_deref(),
        );
        if self.seen_connections.insert(connection_id.clone()) {
            activities.push(ActorActivity::Connected {
                actor: actor.clone(),
            });
        }
        let state = ConnectionState {
            status: poll.connection_status.clone(),
            turn_id: poll.turn_id,
            turn_status: poll.turn_status.clone(),
        };
        if self
            .connection_states
            .insert(connection_id.clone(), state.clone())
            .as_ref()
            != Some(&state)
        {
            activities.push(ActorActivity::Status {
                actor: actor.clone(),
                connection_status: state.status,
                turn_id: state.turn_id,
                turn_status: state.turn_status,
            });
        }

        for entry in poll.items {
            if !self
                .seen_progress
                .insert((connection_id.clone(), entry.turn_id, entry.seq))
            {
                continue;
            }
            match entry.item {
                ProgressItem::AgentMessage { text } => activities.push(ActorActivity::Message {
                    actor: actor.clone(),
                    text,
                }),
                ProgressItem::AgentThought { text } => activities.push(ActorActivity::Thought {
                    actor: actor.clone(),
                    text,
                }),
                ProgressItem::ToolCall {
                    id,
                    title,
                    kind,
                    status,
                    ..
                } => {
                    let state = ToolState {
                        title,
                        kind: Some(kind),
                        status: Some(status),
                    };
                    self.tool_states.insert(
                        (connection_id.clone(), entry.turn_id, id.clone()),
                        state.clone(),
                    );
                    activities.push(tool_activity(&actor, entry.turn_id, &id, &state));
                }
                ProgressItem::ToolCallUpdate {
                    id,
                    title,
                    kind,
                    status,
                    ..
                } => {
                    let state = self
                        .tool_states
                        .entry((connection_id.clone(), entry.turn_id, id.clone()))
                        .or_insert_with(|| ToolState {
                            title: "tool".to_string(),
                            kind: None,
                            status: None,
                        });
                    if let Some(title) = title {
                        state.title = title;
                    }
                    if let Some(kind) = kind {
                        state.kind = Some(kind);
                    }
                    if let Some(status) = status {
                        state.status = Some(status);
                    }
                    activities.push(tool_activity(&actor, entry.turn_id, &id, state));
                }
                ProgressItem::PermissionRequested { title, .. } => {
                    activities.push(ActorActivity::PermissionRequested {
                        actor: actor.clone(),
                        title,
                    });
                }
                ProgressItem::Warning { message } => activities.push(ActorActivity::Warning {
                    actor: actor.clone(),
                    message,
                }),
                ProgressItem::Info { message } => activities.push(ActorActivity::Info {
                    actor: actor.clone(),
                    message,
                }),
            }
        }
    }

    fn merge_actor(
        &mut self,
        connection_id: &str,
        source_id: Option<&str>,
        model_name: Option<&str>,
        model_value: Option<&str>,
    ) -> ActorIdentity {
        let actor = self
            .actors
            .entry(connection_id.to_string())
            .or_insert_with(|| ActorIdentity {
                role: "nested".to_string(),
                connection_id: connection_id.to_string(),
                source_id: None,
                model_name: None,
                model_value: None,
            });
        remember(&mut actor.source_id, source_id);
        remember(&mut actor.model_name, model_name);
        remember(&mut actor.model_value, model_value);
        actor.clone()
    }
}

fn tool_activity(
    actor: &ActorIdentity,
    turn_id: u64,
    local_id: &str,
    state: &ToolState,
) -> ActorActivity {
    ActorActivity::Tool {
        actor: actor.clone(),
        tool_id: format!("{}:{turn_id}:{local_id}", actor.connection_id),
        title: state.title.clone(),
        kind: state.kind.clone(),
        status: state.status.clone(),
    }
}

fn remember(target: &mut Option<String>, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        *target = Some(value.to_string());
    }
}

fn tool_result_values(update: &SessionUpdate) -> Vec<Value> {
    let mut values = Vec::new();
    match update {
        SessionUpdate::ToolCall(call) => {
            if let Some(value) = call.raw_output.as_ref() {
                values.push(value.clone());
            }
            values.extend(content_json_values(&call.content));
        }
        SessionUpdate::ToolCallUpdate(update) => {
            if let Some(value) = update.fields.raw_output.as_ref() {
                values.push(value.clone());
            }
            if let Some(content) = update.fields.content.as_ref() {
                values.extend(content_json_values(content));
            }
        }
        _ => {}
    }
    values
}

fn content_json_values(content: &[ToolCallContent]) -> Vec<Value> {
    content
        .iter()
        .filter_map(|item| match item {
            ToolCallContent::Content(block) => {
                serde_json::from_str(&content_block_text(&block.content)).ok()
            }
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_poll_is_projected_once_and_sparse_tool_updates_merge() {
        let mut bridge = TranscriptBridge::default();
        let value = serde_json::json!({
            "schema": POLL_PROGRESS_SCHEMA,
            "connection_id": "conn-1",
            "source_id": "codex-acp",
            "model_value": "gpt",
            "model_name": "GPT",
            "connection_status": "ready",
            "turn_id": 2,
            "turn_status": "running",
            "items": [
                {"seq": 1, "turn_id": 2, "type": "tool_call", "id": "t", "title": "read", "kind": "read", "status": "pending", "content": [], "raw_input": null, "raw_output": null},
                {"seq": 2, "turn_id": 2, "type": "tool_call_update", "id": "t", "title": null, "kind": null, "status": "completed", "content": null, "raw_input": null, "raw_output": null}
            ],
            "next_seq": 2,
            "dropped_progress": 0,
            "final_text_so_far": "",
            "final_text_truncated": false,
            "stop_reason": null,
            "usage": null,
            "pending_permissions": [],
            "error": null
        });
        let mut first = Vec::new();
        bridge.observe_value(&value, &mut first);
        assert!(
            matches!(first.last(), Some(ActorActivity::Tool { title, status, .. }) if title == "read" && status.as_deref() == Some("completed"))
        );
        let mut repeated = Vec::new();
        bridge.observe_value(&value, &mut repeated);
        assert!(repeated.is_empty());
    }
}
