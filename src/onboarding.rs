//! First-startup review of Council roles, detected ACP servers, and defaults.

use std::collections::HashSet;
use std::io::Stdout;
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{Event as CtEvent, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use ratatui::Terminal;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};

use crate::config::Config;
use crate::council::{Availability, ResolvedCouncil};
use crate::palette::TerminalTheme;
use crate::term::TrackedBackend;
use crate::version::mjolnir_version_label;

pub const ROLE_DESCRIPTIONS: [(&str, &str); 3] = [
    ("Thor", "primary model; plans and reviews work"),
    (
        "Eitri",
        "fast/cheap model; handles delegated implementation and exploration",
    ),
    ("Loki", "secondary model; advises Thor and Eitri"),
];

#[derive(Debug)]
pub enum Outcome {
    Accept(Box<Config>),
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Screen {
    Overview,
    Customize,
}

#[derive(Debug, Clone)]
struct AgentRow {
    label: String,
    enabled: bool,
    status: String,
}

struct State {
    config: Config,
    council: ResolvedCouncil,
    availability: Availability,
    screen: Screen,
    selected: usize,
    notice: Option<String>,
}

impl State {
    fn new(config: Config, council: ResolvedCouncil, notice: Option<String>) -> Self {
        Self {
            config,
            council,
            availability: Availability::detect(),
            screen: if notice.is_some() {
                Screen::Customize
            } else {
                Screen::Overview
            },
            selected: 0,
            notice,
        }
    }

    fn agent_count(&self) -> usize {
        self.config.acp_server_selections().len()
    }

    fn item_count(&self) -> usize {
        self.agent_count() + ROLE_DESCRIPTIONS.len() + 2
    }

    fn move_selection(&mut self, delta: i32) {
        let len = self.item_count();
        if len > 0 {
            self.selected = (self.selected as i32 + delta).rem_euclid(len as i32) as usize;
        }
    }

    fn toggle_selected_agent(&mut self) {
        if let Some(selection) = self.config.acp_server_selections().get(self.selected) {
            let id = selection.id.clone();
            let enabled = !selection.enabled;
            self.config.set_acp_server_enabled(&id, enabled);
        }
        self.notice = None;
    }

    fn cycle_selected_role(&mut self, delta: i32) {
        let Some(role_index) = self.selected.checked_sub(self.agent_count()) else {
            return;
        };
        if role_index >= ROLE_DESCRIPTIONS.len() {
            return;
        }
        let choices = self.model_choices();
        let current = match role_index {
            0 => &self.config.thor.model,
            1 => &self.config.eitri.model,
            2 => &self.config.loki.model,
            _ => return,
        };
        let current_index = choices
            .iter()
            .position(|choice| choice == current)
            .unwrap_or(0);
        let next = (current_index as i32 + delta).rem_euclid(choices.len() as i32) as usize;
        match role_index {
            0 => self.config.thor.model.clone_from(&choices[next]),
            1 => self.config.eitri.model.clone_from(&choices[next]),
            2 => self.config.loki.model.clone_from(&choices[next]),
            _ => {}
        }
        self.notice = None;
    }

    fn toggle_selected_review(&mut self) {
        let review_index = self
            .selected
            .saturating_sub(self.agent_count() + ROLE_DESCRIPTIONS.len());
        match review_index {
            0 => self.config.thor.discrete_review = !self.config.thor.discrete_review,
            1 => self.config.loki.streaming_review = !self.config.loki.streaming_review,
            _ => return,
        }
        self.notice = None;
    }

    fn model_choices(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut choices = vec!["auto".to_string()];
        seen.insert("auto".to_string());
        for choice in self
            .council
            .choices
            .iter()
            .filter(|choice| choice.available)
        {
            if seen.insert(choice.model.clone()) {
                choices.push(choice.model.clone());
            }
        }
        choices
    }

    fn agent_rows(&self) -> Vec<AgentRow> {
        self.config
            .acp_server_selections()
            .into_iter()
            .map(|selection| AgentRow {
                status: self.agent_status(&selection.id, selection.enabled),
                label: selection.label,
                enabled: selection.enabled,
            })
            .collect()
    }

    fn agent_status(&self, id: &str, enabled: bool) -> String {
        if !enabled {
            return "disabled".to_string();
        }
        let model_count = self
            .council
            .available
            .iter()
            .filter(|role| role.launch.source_id == id)
            .map(|role| role.model.model.as_str())
            .collect::<HashSet<_>>()
            .len();
        if model_count > 0 {
            return format!(
                "ready; {model_count} model{}",
                if model_count == 1 { "" } else { "s" }
            );
        }
        match id {
            "codex-acp" if self.availability.codex.is_none() => "not detected".to_string(),
            "claude-acp" if self.availability.claude.is_none() => "not detected".to_string(),
            "opencode-acp" if self.availability.opencode.is_none() => "not detected".to_string(),
            _ => "detected but unavailable".to_string(),
        }
    }
}

pub async fn run(
    terminal: &mut Terminal<TrackedBackend<Stdout>>,
    theme: TerminalTheme,
    config: Config,
    council: ResolvedCouncil,
    notice: Option<String>,
) -> Result<Outcome> {
    let mut state = State::new(config, council, notice);
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    terminal.draw(|frame| draw(frame, &state, theme))?;
    loop {
        tokio::select! {
            biased;
            event = events.next() => {
                let Some(event) = event else {
                    return Ok(Outcome::Cancel);
                };
                if let Some(outcome) = handle_event(&mut state, event.context("onboarding event")?) {
                    return Ok(outcome);
                }
            }
            _ = tick.tick() => {}
        }
        terminal.draw(|frame| draw(frame, &state, theme))?;
    }
}

fn handle_event(state: &mut State, event: CtEvent) -> Option<Outcome> {
    let CtEvent::Key(key) = event else {
        return None;
    };
    if key.kind != KeyEventKind::Press {
        return None;
    }
    if key.modifiers == KeyModifiers::CONTROL && key.code == KeyCode::Char('c') {
        return Some(Outcome::Cancel);
    }
    match state.screen {
        Screen::Overview => match key.code {
            KeyCode::Enter => Some(Outcome::Accept(Box::new(state.config.clone()))),
            KeyCode::Char('c' | 'C') => {
                state.screen = Screen::Customize;
                None
            }
            KeyCode::Esc => Some(Outcome::Cancel),
            _ => None,
        },
        Screen::Customize => match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                state.move_selection(-1);
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                state.move_selection(1);
                None
            }
            KeyCode::Char(' ') if state.selected < state.agent_count() => {
                state.toggle_selected_agent();
                None
            }
            KeyCode::Char(' ')
                if state.selected >= state.agent_count() + ROLE_DESCRIPTIONS.len() =>
            {
                state.toggle_selected_review();
                None
            }
            KeyCode::Left | KeyCode::Char('h')
                if state.selected >= state.agent_count()
                    && state.selected < state.agent_count() + ROLE_DESCRIPTIONS.len() =>
            {
                state.cycle_selected_role(-1);
                None
            }
            KeyCode::Right | KeyCode::Char('l')
                if state.selected >= state.agent_count()
                    && state.selected < state.agent_count() + ROLE_DESCRIPTIONS.len() =>
            {
                state.cycle_selected_role(1);
                None
            }
            KeyCode::Enter => Some(Outcome::Accept(Box::new(state.config.clone()))),
            KeyCode::Esc => {
                state.screen = Screen::Overview;
                state.notice = None;
                None
            }
            _ => None,
        },
    }
}

fn draw(frame: &mut ratatui::Frame, state: &State, theme: TerminalTheme) {
    let notice_height = if state.notice.is_some() { 3 } else { 0 };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(notice_height),
            Constraint::Length(1),
        ])
        .split(frame.area());
    let header = Paragraph::new(format!(" {} | first startup ", mjolnir_version_label()))
        .style(Style::default().add_modifier(Modifier::REVERSED));
    frame.render_widget(header, rows[0]);

    match state.screen {
        Screen::Overview => draw_overview(frame, rows[1], state, theme),
        Screen::Customize => draw_customize(frame, rows[1], state, theme),
    }

    if let Some(notice) = &state.notice {
        frame.render_widget(
            Paragraph::new(notice.as_str())
                .style(Style::default().fg(theme.error))
                .wrap(Wrap { trim: false }),
            rows[2],
        );
    }
    let footer = match state.screen {
        Screen::Overview => "Enter use this configuration | C customize | Esc exit",
        Screen::Customize => {
            "Up/Down select | Space toggle | Left/Right choose model | Enter save | Esc back"
        }
    };
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(theme.muted)),
        rows[3],
    );
}

fn draw_overview(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &State,
    theme: TerminalTheme,
) {
    let agent_height = state.agent_count().saturating_add(2) as u16;
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(agent_height.min(area.height.saturating_sub(8))),
            Constraint::Min(5),
        ])
        .split(area);

    let role_lines = ROLE_DESCRIPTIONS
        .iter()
        .map(|(role, description)| {
            Line::from(vec![
                Span::styled(
                    format!("{role:<6}"),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(*description),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(role_lines)
            .block(Block::default().borders(Borders::ALL).title(" roles "))
            .wrap(Wrap { trim: false }),
        sections[0],
    );

    let agents = state
        .agent_rows()
        .into_iter()
        .map(|agent| {
            let color = if agent.status.starts_with("ready") {
                theme.success
            } else {
                theme.muted
            };
            Line::from(vec![
                Span::styled(
                    format!("{:<14}", agent.label),
                    Style::default().fg(theme.text),
                ),
                Span::styled(agent.status, Style::default().fg(color)),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(agents)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" detected ACP servers "),
            )
            .wrap(Wrap { trim: false }),
        sections[1],
    );

    let configured = vec![
        configured_role_line("Thor", Some(&state.council.thor), theme),
        configured_role_line("Eitri", state.council.eitri.as_ref(), theme),
        configured_role_line("Loki", state.council.loki.as_ref(), theme),
        Line::from(vec![
            Span::styled("Reviews  ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(
                "Thor {} | Loki {}",
                on_off(state.config.thor.discrete_review),
                on_off(state.config.loki.streaming_review)
            )),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(configured)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" automatic configuration "),
            )
            .wrap(Wrap { trim: false }),
        sections[2],
    );
}

fn configured_role_line(
    label: &str,
    role: Option<&crate::council::ResolvedRole>,
    theme: TerminalTheme,
) -> Line<'static> {
    let value = role.map_or_else(
        || "off".to_string(),
        |role| format!("{} via {}", role.model.model, role.launch.source_id),
    );
    Line::from(vec![
        Span::styled(
            format!("{label:<9}"),
            Style::default().fg(theme.text).add_modifier(Modifier::BOLD),
        ),
        Span::raw(value),
    ])
}

fn draw_customize(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &State,
    theme: TerminalTheme,
) {
    let agents = state.agent_rows();
    let mut items = agents
        .iter()
        .enumerate()
        .map(|(index, agent)| {
            let marker = if agent.enabled { "[on] " } else { "[off]" };
            selectable_item(
                format!("{marker} {:<14} {}", agent.label, agent.status),
                state.selected == index,
                theme,
            )
        })
        .collect::<Vec<_>>();
    for (offset, (label, _)) in ROLE_DESCRIPTIONS.iter().enumerate() {
        let index = agents.len() + offset;
        let model = match offset {
            0 => &state.config.thor.model,
            1 => &state.config.eitri.model,
            _ => &state.config.loki.model,
        };
        items.push(selectable_item(
            format!("{label:<6} model  < {model} >"),
            state.selected == index,
            theme,
        ));
    }
    for (offset, (label, enabled)) in [
        ("Thor review", state.config.thor.discrete_review),
        ("Loki advice", state.config.loki.streaming_review),
    ]
    .into_iter()
    .enumerate()
    {
        let index = agents.len() + ROLE_DESCRIPTIONS.len() + offset;
        items.push(selectable_item(
            format!("{label:<12} [{}]", on_off(enabled)),
            state.selected == index,
            theme,
        ));
    }
    let visible = area.height.saturating_sub(2) as usize;
    let start = if items.len() <= visible {
        0
    } else {
        state
            .selected
            .saturating_sub(visible / 2)
            .min(items.len() - visible)
    };
    let end = (start + visible).min(items.len());
    frame.render_widget(
        List::new(items.drain(start..end).collect::<Vec<_>>()).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" customize Council "),
        ),
        area,
    );
}

fn selectable_item(text: String, selected: bool, theme: TerminalTheme) -> ListItem<'static> {
    let style = if selected {
        Style::default()
            .fg(theme.selection_fg)
            .bg(theme.selection_bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(theme.text)
    };
    ListItem::new(format!("{} {text}", if selected { ">" } else { " " })).style(style)
}

fn on_off(enabled: bool) -> &'static str {
    if enabled { "on" } else { "off" }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::council::{AdapterKind, AdapterLaunch, ModelChoice, ResolvedRole};
    use crate::deepswe::Row;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

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
        let eitri = role("qwen-test", "anvil");
        ResolvedCouncil {
            thor: thor.clone(),
            loki: None,
            eitri: Some(eitri.clone()),
            available: vec![thor, eitri],
            choices: vec![
                ModelChoice {
                    model: "gpt-test".to_string(),
                    pass_at_1: 0.5,
                    mean_cost_usd: 1.0,
                    available: true,
                    disabled_reason: None,
                    adapter: Some("codex-acp".to_string()),
                    ranked: true,
                },
                ModelChoice {
                    model: "qwen-test".to_string(),
                    pass_at_1: 0.4,
                    mean_cost_usd: 0.2,
                    available: true,
                    disabled_reason: None,
                    adapter: Some("anvil".to_string()),
                    ranked: true,
                },
            ],
            warnings: Vec::new(),
        }
    }

    #[test]
    fn role_descriptions_match_product_language() {
        assert_eq!(
            ROLE_DESCRIPTIONS[0].1,
            "primary model; plans and reviews work"
        );
        assert_eq!(
            ROLE_DESCRIPTIONS[1].1,
            "fast/cheap model; handles delegated implementation and exploration"
        );
        assert_eq!(
            ROLE_DESCRIPTIONS[2].1,
            "secondary model; advises Thor and Eitri"
        );
    }

    #[test]
    fn customize_toggles_agents_and_cycles_role_models() {
        let mut state = State::new(Config::default(), council(), None);
        state.screen = Screen::Customize;
        state.selected = 0;
        state.toggle_selected_agent();
        assert!(!state.config.acp.codex);

        state.selected = state.agent_count();
        state.cycle_selected_role(1);
        assert_eq!(state.config.thor.model, "gpt-test");

        state.selected = state.agent_count() + ROLE_DESCRIPTIONS.len();
        state.toggle_selected_review();
        assert!(!state.config.thor.discrete_review);
    }

    #[test]
    fn validation_notice_reopens_customize_screen() {
        let state = State::new(
            Config::default(),
            council(),
            Some("not launchable".to_string()),
        );
        assert_eq!(state.screen, Screen::Customize);
    }

    #[test]
    fn overview_renders_roles_detection_and_automatic_configuration() {
        let state = State::new(Config::default(), council(), None);
        let backend = TestBackend::new(90, 28);
        let mut terminal = Terminal::new(backend).expect("terminal");

        terminal
            .draw(|frame| draw(frame, &state, state.config.theme.palette()))
            .expect("draw");

        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("Thor  primary model; plans and reviews work"));
        assert!(rendered.contains("detected ACP servers"));
        assert!(rendered.contains("automatic configuration"));
        assert!(rendered.contains("gpt-test via codex-acp"));
        assert!(rendered.contains("Enter use this configuration"));
    }
}
