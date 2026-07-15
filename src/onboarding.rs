//! First-startup host for the shared settings editor.

use std::io::Stdout;
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{Event as CtEvent, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use ratatui::Terminal;

use crate::config::Config;
use crate::council::ResolvedCouncil;
use crate::settings::{SettingsAction, SettingsEditor, draw_settings_panel};
use crate::term::TrackedBackend;

#[derive(Debug)]
pub enum Outcome {
    Accept(Box<Config>),
    Cancel,
}

pub async fn run(
    terminal: &mut Terminal<TrackedBackend<Stdout>>,
    config: Config,
    council: Option<ResolvedCouncil>,
    notice: Option<String>,
) -> Result<Outcome> {
    let inventory = council
        .as_ref()
        .map(|council| council.inventory.clone())
        .unwrap_or_else(|| crate::council::discover_inventory(&config));
    let choices = council
        .as_ref()
        .map(|council| council.choices.clone())
        .unwrap_or_default();
    let mut editor = SettingsEditor::new(config, choices, notice).with_inventory(inventory);
    if let Some(council) = council {
        editor = editor.with_active_models(crate::config::ModelsConfig {
            thor: council.thor.model.model,
            eitri: council
                .eitri
                .map(|role| role.model.model)
                .unwrap_or_else(|| "off".to_string()),
            loki: council
                .loki
                .map(|role| role.model.model)
                .unwrap_or_else(|| "off".to_string()),
        });
    }
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    terminal
        .draw(|frame| draw_settings_panel(frame, frame.area(), &editor, "Welcome to Mjolnir"))?;
    loop {
        tokio::select! {
            biased;
            event = events.next() => {
                let Some(event) = event else {
                    return Ok(Outcome::Cancel);
                };
                if let Some(outcome) = handle_event(&mut editor, event.context("settings event")?) {
                    return Ok(outcome);
                }
            }
            _ = tick.tick() => editor.poll_background(),
        }
        terminal.draw(|frame| {
            draw_settings_panel(frame, frame.area(), &editor, "Welcome to Mjolnir")
        })?;
    }
}

fn handle_event(editor: &mut SettingsEditor, event: CtEvent) -> Option<Outcome> {
    let CtEvent::Key(key) = event else {
        return None;
    };
    if key.kind != KeyEventKind::Press {
        return None;
    }
    if key.modifiers == KeyModifiers::CONTROL && key.code == KeyCode::Char('c') {
        editor.cancel_background();
        return Some(Outcome::Cancel);
    }
    match editor.handle_key(key.code) {
        SettingsAction::Save => Some(Outcome::Accept(Box::new(editor.config.clone()))),
        SettingsAction::Cancel => {
            editor.cancel_background();
            Some(Outcome::Cancel)
        }
        SettingsAction::None | SettingsAction::Changed => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::council::{AdapterKind, AdapterLaunch, ModelChoice, ResolvedRole};
    use crate::deepswe::Row;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    fn role(model: &str, source_id: &str) -> ResolvedRole {
        ResolvedRole {
            model: Row {
                model: model.to_string(),
                reasoning_effort: None,
                pass_at_1: 0.5,
                mean_cost_usd: 1.0,
            },
            model_value: model.to_string(),
            launch: AdapterLaunch {
                kind: AdapterKind::Custom,
                source_id: source_id.to_string(),
                command: PathBuf::from(source_id),
                args: Vec::new(),
                env: Default::default(),
            },
            ranked: true,
        }
    }

    fn council() -> ResolvedCouncil {
        let thor = role("gpt-test", "codex-acp");
        ResolvedCouncil {
            thor: thor.clone(),
            loki: None,
            eitri: None,
            available: vec![thor],
            choices: vec![ModelChoice {
                model: "gpt-test".to_string(),
                pass_at_1: 0.5,
                mean_cost_usd: 1.0,
                available: true,
                disabled_reason: None,
                adapter: Some("codex-acp".to_string()),
                ranked: true,
            }],
            warnings: Vec::new(),
            inventory: crate::council::AcpInventory::default(),
        }
    }

    #[test]
    fn startup_uses_shared_settings_panel() {
        let editor = SettingsEditor::new(Config::default(), council().choices, None);
        let backend = TestBackend::new(90, 28);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| draw_settings_panel(frame, frame.area(), &editor, "Welcome to Mjolnir"))
            .expect("draw");
        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("Welcome to Mjolnir"));
        assert!(rendered.contains("Council"));
        assert!(rendered.contains("primary model; plans and reviews work"));
        assert!(rendered.contains("Enter save"));
    }
}
