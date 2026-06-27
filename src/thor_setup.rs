//! First-run Thor setup.

use std::io::Stdout;
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{Event as CtEvent, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use ratatui::Terminal;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};

use crate::config::{SelectedAgent, ThorQuotaBackend};
use crate::palette::TerminalTheme;
use crate::term::TrackedBackend;
use crate::thor::{ThorConfig, ThorOptimizationMode, ThorReasoning};
use crate::thor_probe::AgentValidation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThorSetupAgent {
    pub agent: SelectedAgent,
    pub name: String,
    pub description: String,
    pub setup_url: String,
    pub quota_backend: ThorQuotaBackend,
    pub validation: Option<AgentValidation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThorSetupRegistryAgent {
    pub source_id: String,
    pub name: String,
    pub description: String,
    pub setup_url: String,
    pub command: String,
    pub setup_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThorSetupSelection {
    pub enabled_worker_source_ids: Vec<String>,
    pub host_source_id: String,
    pub optimization_mode: ThorOptimizationMode,
    pub coordinator_model: String,
    pub coordinator_reasoning: ThorReasoning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThorSetupCustomAgent {
    pub name: String,
    pub command: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThorSetupOutcome {
    Selection(ThorSetupSelection),
    AddCustom(ThorSetupCustomAgent),
    AddRegistry(String),
    RetryValidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupStep {
    Persona,
    Host,
    Registry,
    CustomName,
    CustomCommand,
    Confirm,
}

impl SetupStep {
    fn title(self) -> &'static str {
        match self {
            Self::Persona => "work style",
            Self::Host => "agents",
            Self::Registry => "known agent",
            Self::CustomName => "name",
            Self::CustomCommand => "command",
            Self::Confirm => "start",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Persona => 0,
            Self::Host => 1,
            Self::Registry => 2,
            Self::CustomName => 3,
            Self::CustomCommand => 4,
            Self::Confirm => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HostChoice {
    Agent(String),
    AddRegistry,
    RetryValidation,
    AddCustom,
}

#[derive(Debug, Clone)]
struct ThorSetupState {
    agents: Vec<ThorSetupAgent>,
    registry_agents: Vec<ThorSetupRegistryAgent>,
    step: SetupStep,
    cursor: usize,
    selected_workers: Vec<bool>,
    optimization_mode: ThorOptimizationMode,
    host_source_id: String,
    coordinator_model: String,
    coordinator_reasoning: ThorReasoning,
    custom_name: String,
    custom_command: String,
    notice: Option<String>,
}

impl ThorSetupState {
    fn new(
        thor_config: &ThorConfig,
        agents: &[ThorSetupAgent],
        registry_agents: &[ThorSetupRegistryAgent],
        initial_host: &SelectedAgent,
    ) -> Self {
        let agents = if agents.is_empty() {
            vec![ThorSetupAgent {
                agent: crate::thor::default_anvil_agent(),
                name: "Anvil".to_string(),
                description: "Brokk agent via uv".to_string(),
                setup_url: "https://github.com/BrokkAi/brokk".to_string(),
                quota_backend: ThorQuotaBackend::None,
                validation: None,
            }]
        } else {
            agents.to_vec()
        };
        let selected_workers = agents.iter().map(setup_agent_is_usable).collect::<Vec<_>>();

        let host_source_id = if agents.iter().any(|setup_agent| {
            setup_agent.agent.source_id == initial_host.source_id
                && setup_agent_is_usable(setup_agent)
        }) {
            initial_host.source_id.clone()
        } else {
            agents
                .iter()
                .find(|setup_agent| setup_agent_is_usable(setup_agent))
                .map(|setup_agent| setup_agent.agent.source_id.clone())
                .unwrap_or_default()
        };
        let optimization_mode = match thor_config.optimization_mode {
            ThorOptimizationMode::Cost => ThorOptimizationMode::Cost,
            _ => ThorOptimizationMode::BestSolution,
        };

        let mut state = Self {
            agents,
            registry_agents: registry_agents.to_vec(),
            step: SetupStep::Persona,
            cursor: 0,
            selected_workers,
            optimization_mode,
            host_source_id,
            coordinator_model: thor_config.coordinator_model.clone(),
            coordinator_reasoning: thor_config.coordinator_reasoning,
            custom_name: String::new(),
            custom_command: String::new(),
            notice: None,
        };
        state.cursor = state.default_cursor_for(SetupStep::Persona);
        state.ensure_host_is_enabled();
        state
    }

    fn move_selection(&mut self, delta: i32) {
        let len = self.current_len();
        if len == 0 {
            self.cursor = 0;
            return;
        }
        self.cursor = (self.cursor as i32 + delta).rem_euclid(len as i32) as usize;
    }

    fn advance(&mut self) -> Option<ThorSetupOutcome> {
        match self.step {
            SetupStep::Persona => {
                self.optimization_mode = if self.cursor == 1 {
                    ThorOptimizationMode::Cost
                } else {
                    ThorOptimizationMode::BestSolution
                };
                self.set_step(SetupStep::Host);
            }
            SetupStep::Host => match self.host_choices().get(self.cursor) {
                Some(HostChoice::Agent(source_id)) => {
                    self.host_source_id = source_id.clone();
                    self.enable_agent(source_id);
                    self.set_step(SetupStep::Confirm);
                }
                Some(HostChoice::AddRegistry) => {
                    self.notice = None;
                    self.set_step(SetupStep::Registry);
                }
                Some(HostChoice::RetryValidation) => {
                    return Some(ThorSetupOutcome::RetryValidation);
                }
                Some(HostChoice::AddCustom) => {
                    self.notice = None;
                    if self.custom_name.trim().is_empty() {
                        self.custom_name = default_custom_name(&self.agents);
                    }
                    self.set_step(SetupStep::CustomName);
                }
                None => {}
            },
            SetupStep::Registry => {
                if let Some(registry_agent) = self.registry_agents.get(self.cursor) {
                    return Some(ThorSetupOutcome::AddRegistry(
                        registry_agent.source_id.clone(),
                    ));
                }
            }
            SetupStep::CustomName => {
                if self.custom_name.trim().is_empty() {
                    self.notice = Some("Enter a short name for this agent.".to_string());
                } else {
                    self.notice = None;
                    self.set_step(SetupStep::CustomCommand);
                }
            }
            SetupStep::CustomCommand => {
                if self.custom_command.trim().is_empty() {
                    self.notice = Some("Enter the command that starts this agent.".to_string());
                } else {
                    return Some(ThorSetupOutcome::AddCustom(ThorSetupCustomAgent {
                        name: self.custom_name.trim().to_string(),
                        command: self.custom_command.trim().to_string(),
                    }));
                }
            }
            SetupStep::Confirm => return Some(ThorSetupOutcome::Selection(self.selection())),
        }
        None
    }

    fn back(&mut self) {
        match self.step {
            SetupStep::Persona => {}
            SetupStep::Host => self.set_step(SetupStep::Persona),
            SetupStep::Registry => self.set_step(SetupStep::Host),
            SetupStep::CustomName => self.set_step(SetupStep::Host),
            SetupStep::CustomCommand => self.set_step(SetupStep::CustomName),
            SetupStep::Confirm => self.set_step(SetupStep::Host),
        }
    }

    fn edit_text(&mut self, ch: char) {
        match self.step {
            SetupStep::CustomName => {
                self.custom_name.push(ch);
                self.notice = None;
            }
            SetupStep::CustomCommand => {
                self.custom_command.push(ch);
                self.notice = None;
            }
            SetupStep::Persona | SetupStep::Host | SetupStep::Registry | SetupStep::Confirm => {}
        }
    }

    fn delete_text(&mut self) -> bool {
        match self.step {
            SetupStep::CustomName => {
                let deleted = self.custom_name.pop().is_some();
                if deleted {
                    self.notice = None;
                }
                deleted
            }
            SetupStep::CustomCommand => {
                let deleted = self.custom_command.pop().is_some();
                if deleted {
                    self.notice = None;
                }
                deleted
            }
            SetupStep::Persona | SetupStep::Host | SetupStep::Registry | SetupStep::Confirm => {
                false
            }
        }
    }

    fn is_text_step(&self) -> bool {
        matches!(self.step, SetupStep::CustomName | SetupStep::CustomCommand)
    }

    fn toggle_current_worker(&mut self) {
        if self.step != SetupStep::Host {
            return;
        }
        let choices = self.host_choices();
        let Some(HostChoice::Agent(source_id)) = choices.get(self.cursor) else {
            return;
        };
        let Some(idx) = self
            .agents
            .iter()
            .position(|setup_agent| setup_agent.agent.source_id == *source_id)
        else {
            return;
        };
        if !setup_agent_is_usable(&self.agents[idx]) {
            return;
        }
        let selected_count = self
            .selected_workers
            .iter()
            .filter(|selected| **selected)
            .count();
        if self.selected_workers[idx] && selected_count <= 1 {
            self.notice = Some("Thor needs at least one ready agent.".to_string());
            return;
        }
        self.notice = None;
        self.selected_workers[idx] = !self.selected_workers[idx];
        self.ensure_host_is_enabled();
    }

    fn selection(&self) -> ThorSetupSelection {
        ThorSetupSelection {
            enabled_worker_source_ids: self.enabled_source_ids(),
            host_source_id: self.host_source_id.clone(),
            optimization_mode: self.optimization_mode,
            coordinator_model: self.coordinator_model.clone(),
            coordinator_reasoning: self.coordinator_reasoning,
        }
    }

    fn set_step(&mut self, step: SetupStep) {
        self.step = step;
        self.cursor = self.default_cursor_for(step);
    }

    fn default_cursor_for(&self, step: SetupStep) -> usize {
        match step {
            SetupStep::Host => self
                .host_choices()
                .iter()
                .position(|choice| match choice {
                    HostChoice::Agent(source_id) => source_id == &self.host_source_id,
                    HostChoice::AddRegistry => false,
                    HostChoice::RetryValidation => false,
                    HostChoice::AddCustom => false,
                })
                .unwrap_or(0),
            SetupStep::Persona => match self.optimization_mode {
                ThorOptimizationMode::Cost => 1,
                ThorOptimizationMode::BestSolution | ThorOptimizationMode::Balanced => 0,
            },
            SetupStep::Registry => 0,
            SetupStep::CustomName | SetupStep::CustomCommand => 0,
            SetupStep::Confirm => 0,
        }
    }

    fn current_len(&self) -> usize {
        match self.step {
            SetupStep::Persona => 2,
            SetupStep::Host => self.host_choices().len(),
            SetupStep::Registry => self.registry_agents.len(),
            SetupStep::CustomName | SetupStep::CustomCommand => 1,
            SetupStep::Confirm => 1,
        }
    }

    fn host_choices(&self) -> Vec<HostChoice> {
        let mut choices = self
            .agents
            .iter()
            .filter(|setup_agent| setup_agent_is_usable(setup_agent))
            .map(|setup_agent| HostChoice::Agent(setup_agent.agent.source_id.clone()))
            .collect::<Vec<_>>();
        if !self.registry_agents.is_empty() {
            choices.push(HostChoice::AddRegistry);
        }
        choices.push(HostChoice::AddCustom);
        if self
            .agents
            .iter()
            .any(|agent| !setup_agent_is_usable(agent))
        {
            choices.push(HostChoice::RetryValidation);
        }
        choices
    }

    fn enabled_source_ids(&self) -> Vec<String> {
        self.agents
            .iter()
            .zip(self.selected_workers.iter())
            .filter(|(_, selected)| **selected)
            .map(|(setup_agent, _)| setup_agent.agent.source_id.clone())
            .collect()
    }

    fn ensure_host_is_enabled(&mut self) {
        let enabled = self.enabled_source_ids();
        if enabled
            .iter()
            .any(|source_id| source_id == &self.host_source_id)
        {
            return;
        }
        if let Some(source_id) = enabled.first() {
            self.host_source_id = source_id.clone();
        }
    }

    fn enable_agent(&mut self, source_id: &str) {
        if let Some(idx) = self
            .agents
            .iter()
            .position(|setup_agent| setup_agent.agent.source_id == source_id)
        {
            self.selected_workers[idx] = true;
        }
    }
}

/// Run Thor setup until the user confirms or cancels with Esc/Ctrl-C.
pub async fn run_thor_setup(
    terminal: &mut Terminal<TrackedBackend<Stdout>>,
    theme: TerminalTheme,
    thor_config: &ThorConfig,
    agents: &[ThorSetupAgent],
    registry_agents: &[ThorSetupRegistryAgent],
    initial_host: &SelectedAgent,
) -> Result<Option<ThorSetupOutcome>> {
    let mut state = ThorSetupState::new(thor_config, agents, registry_agents, initial_host);
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));

    terminal.draw(|f| draw(f, &state, theme))?;

    loop {
        tokio::select! {
            biased;
            maybe_ev = events.next() => {
                let Some(ev) = maybe_ev else {
                    return Ok(None);
                };
                let ev = ev.context("crossterm event stream")?;
                if let Some(outcome) = handle_event(&mut state, ev) {
                    return Ok(outcome);
                }
            }
            _ = tick.tick() => {}
        }
        terminal.draw(|f| draw(f, &state, theme))?;
    }
}

fn handle_event(state: &mut ThorSetupState, ev: CtEvent) -> Option<Option<ThorSetupOutcome>> {
    let CtEvent::Key(key) = ev else {
        return None;
    };
    if key.kind != KeyEventKind::Press {
        return None;
    }

    match key.code {
        KeyCode::Esc => Some(None),
        KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => Some(None),
        KeyCode::Up => {
            state.move_selection(-1);
            None
        }
        KeyCode::Down => {
            state.move_selection(1);
            None
        }
        KeyCode::Backspace | KeyCode::Left => {
            if !state.delete_text() {
                state.back();
            }
            None
        }
        KeyCode::Char(' ') => {
            if state.is_text_step() {
                state.edit_text(' ');
            } else {
                state.toggle_current_worker();
            }
            None
        }
        KeyCode::Char(ch) => {
            state.edit_text(ch);
            None
        }
        KeyCode::Enter | KeyCode::Right => state.advance().map(Some),
        _ => None,
    }
}

fn draw(f: &mut ratatui::Frame, state: &ThorSetupState, theme: TerminalTheme) {
    let area = setup_rect(f.area());
    let block = Block::default()
        .title(" Set up Thor ")
        .borders(Borders::ALL)
        .style(Style::default().fg(theme.text));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(2),
            Constraint::Min(10),
            Constraint::Length(4),
            Constraint::Length(1),
        ])
        .split(inner);

    let title = Paragraph::new(intro_lines(state, theme)).style(Style::default().fg(theme.text));
    f.render_widget(title, layout[0]);

    f.render_widget(progress_line(state, theme), layout[1]);

    let content = match state.step {
        SetupStep::Persona => persona_rows(state, theme),
        SetupStep::Host => host_rows(state, theme),
        SetupStep::Registry => registry_rows(state, theme),
        SetupStep::CustomName => custom_name_rows(state, theme),
        SetupStep::CustomCommand => custom_command_rows(state, theme),
        SetupStep::Confirm => confirm_rows(state, theme),
    };
    let content_cursor = match state.step {
        SetupStep::Persona => state.cursor,
        SetupStep::Host => host_selected_row_index(state),
        SetupStep::Registry
        | SetupStep::CustomName
        | SetupStep::CustomCommand
        | SetupStep::Confirm => state.cursor,
    };
    let content = visible_rows(content, content_cursor, layout[2].height as usize);
    f.render_widget(List::new(content), layout[2]);

    let summary = Paragraph::new(summary_lines(state, theme))
        .style(Style::default().fg(theme.text))
        .wrap(Wrap { trim: true });
    f.render_widget(summary, layout[3]);

    let footer_text = match state.step {
        SetupStep::Confirm => "Enter starts Thor  |  Backspace edits  |  Esc quits",
        SetupStep::CustomName | SetupStep::CustomCommand => {
            "Type to edit  |  Enter continues  |  Backspace deletes  |  Esc quits"
        }
        SetupStep::Persona => "Enter chooses work style  |  Esc quits",
        SetupStep::Host => {
            "Space includes agent  |  Enter chooses Thor host  |  Backspace returns  |  Esc quits"
        }
        SetupStep::Registry => "Enter adds and checks  |  Backspace returns  |  Esc quits",
    };
    let footer = Paragraph::new(footer_text).style(Style::default().fg(theme.muted));
    f.render_widget(footer, layout[4]);
}

fn intro_lines(state: &ThorSetupState, theme: TerminalTheme) -> Vec<Line<'static>> {
    let ready_count = state
        .agents
        .iter()
        .filter(|agent| setup_agent_is_usable(agent))
        .count();
    let failed_count = state.agents.len().saturating_sub(ready_count);
    let status = if ready_count == 0 {
        Span::styled(
            "No agent is ready. Add one, fix setup, then retry checks.",
            Style::default().fg(theme.warning),
        )
    } else if failed_count == 0 {
        Span::styled(
            format!("{ready_count} ready. Choose where Thor runs, then start."),
            Style::default().fg(theme.text),
        )
    } else {
        Span::styled(
            format!("{ready_count} ready, {failed_count} need setup. Choose Thor or fix another."),
            Style::default().fg(theme.text),
        )
    };
    vec![
        Line::from(vec![Span::styled(
            "Thor coordinates your coding agents.",
            Style::default()
                .fg(theme.primary)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(status),
        Line::from("Add a known agent, or paste the command for an installed agent."),
        Line::from("Choose a work style, choose the ready agents Thor may use, then start."),
    ]
}

fn progress_line(state: &ThorSetupState, theme: TerminalTheme) -> Paragraph<'static> {
    let steps = match state.step {
        SetupStep::CustomName | SetupStep::CustomCommand => vec![
            SetupStep::Persona,
            SetupStep::Host,
            SetupStep::CustomName,
            SetupStep::CustomCommand,
            SetupStep::Confirm,
        ],
        SetupStep::Registry => vec![
            SetupStep::Persona,
            SetupStep::Host,
            SetupStep::Registry,
            SetupStep::Confirm,
        ],
        SetupStep::Persona | SetupStep::Host | SetupStep::Confirm => {
            vec![SetupStep::Persona, SetupStep::Host, SetupStep::Confirm]
        }
    };
    let spans = steps
        .iter()
        .enumerate()
        .flat_map(|(idx, step)| {
            let style = if *step == state.step {
                Style::default()
                    .fg(theme.selection_fg)
                    .bg(theme.selection_bg)
                    .add_modifier(Modifier::BOLD)
            } else if step.index() < state.step.index() {
                Style::default().fg(theme.primary)
            } else {
                Style::default().fg(theme.muted)
            };
            let mut spans = vec![Span::styled(
                format!(" {}. {} ", idx + 1, step.title()),
                style,
            )];
            if idx + 1 < steps.len() {
                spans.push(Span::styled(" ", Style::default().fg(theme.muted)));
            }
            spans
        })
        .collect::<Vec<_>>();
    Paragraph::new(Line::from(spans))
}

fn setup_rect(area: Rect) -> Rect {
    let width = area
        .width
        .saturating_mul(9)
        .saturating_div(10)
        .clamp(72, 112);
    let height = area
        .height
        .saturating_mul(9)
        .saturating_div(10)
        .clamp(24, 36);
    crate::term::centered_rect(area, width, height)
}

fn visible_rows(
    rows: Vec<ListItem<'static>>,
    cursor: usize,
    viewport_height: usize,
) -> Vec<ListItem<'static>> {
    if rows.is_empty() || viewport_height == 0 || rows.len() <= viewport_height {
        return rows;
    }
    let half = viewport_height / 2;
    let max_start = rows.len().saturating_sub(viewport_height);
    let start = cursor.saturating_sub(half).min(max_start);
    rows.into_iter().skip(start).take(viewport_height).collect()
}

fn persona_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    [
        (
            ThorOptimizationMode::BestSolution,
            "Architect",
            "best answer; Thor may compare results",
        ),
        (
            ThorOptimizationMode::Cost,
            "Accountant",
            "lower spend; cheaper agents for simple work",
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(idx, (mode, label, description))| {
        let active = state.optimization_mode == mode;
        selectable_row(
            state.cursor == idx,
            vec![
                Span::styled(
                    format!("{} {label}", if active { "[x]" } else { "[ ]" }),
                    Style::default().fg(theme.text),
                ),
                Span::styled(format!("  {description}"), Style::default().fg(theme.muted)),
            ],
            theme,
        )
    })
    .collect()
}

fn host_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    let choices = state.host_choices();
    let mut choice_idx = 0usize;
    let mut rows = Vec::new();
    for (agent_idx, setup_agent) in state.agents.iter().enumerate() {
        if setup_agent_is_usable(setup_agent) {
            let selected = matches!(
                choices.get(choice_idx),
                Some(HostChoice::Agent(source_id)) if source_id == &setup_agent.agent.source_id
            ) && choice_idx == state.cursor;
            choice_idx += 1;
            let enabled = state
                .selected_workers
                .get(agent_idx)
                .copied()
                .unwrap_or(false);
            let host_marker = if setup_agent.agent.source_id == state.host_source_id {
                "  Thor host"
            } else {
                ""
            };
            rows.push(selectable_row(
                selected,
                vec![
                    Span::styled(
                        format!(
                            "{} {}",
                            if enabled { "[x]" } else { "[ ]" },
                            setup_agent_label(setup_agent)
                        ),
                        Style::default().fg(theme.text),
                    ),
                    Span::styled(
                        format!("  {}", host_status_label(setup_agent)),
                        if setup_agent_is_usable(setup_agent) {
                            Style::default().fg(theme.primary)
                        } else {
                            Style::default().fg(theme.warning)
                        },
                    ),
                    Span::styled(
                        format!("  {}", setup_agent_description(setup_agent)),
                        Style::default().fg(theme.muted),
                    ),
                    Span::styled(host_marker.to_string(), Style::default().fg(theme.primary)),
                ],
                theme,
            ));
        } else {
            rows.push(disabled_row(
                vec![
                    Span::styled(
                        setup_agent_label(setup_agent),
                        Style::default().fg(theme.muted),
                    ),
                    Span::styled(
                        format!("  {}", host_status_label(setup_agent)),
                        Style::default().fg(theme.warning),
                    ),
                    Span::styled(
                        format!("  {}", setup_agent_description(setup_agent)),
                        Style::default().fg(theme.muted),
                    ),
                ],
                theme,
            ));
        }
    }
    if state.registry_agents.is_empty() {
        rows.push(disabled_row(
            vec![
                Span::styled("Known agents".to_string(), Style::default().fg(theme.muted)),
                Span::styled(
                    "  unavailable; add an installed agent instead".to_string(),
                    Style::default().fg(theme.muted),
                ),
            ],
            theme,
        ));
    } else {
        let registry_choice_idx = choices
            .iter()
            .position(|choice| matches!(choice, HostChoice::AddRegistry));
        rows.push(selectable_row(
            registry_choice_idx == Some(state.cursor),
            vec![
                Span::styled(
                    "Add known agent".to_string(),
                    Style::default().fg(theme.text),
                ),
                Span::styled(
                    format!("  {} setup choices", state.registry_agents.len()),
                    Style::default().fg(theme.muted),
                ),
            ],
            theme,
        ));
    }
    rows.push(selectable_row(
        choices
            .iter()
            .position(|choice| matches!(choice, HostChoice::AddCustom))
            == Some(state.cursor),
        vec![
            Span::styled(
                "Add installed agent".to_string(),
                Style::default().fg(theme.text),
            ),
            Span::styled(
                "  paste the command from that agent's docs".to_string(),
                Style::default().fg(theme.muted),
            ),
        ],
        theme,
    ));
    if state
        .agents
        .iter()
        .any(|agent| !setup_agent_is_usable(agent))
    {
        let retry_choice_idx = choices
            .iter()
            .position(|choice| matches!(choice, HostChoice::RetryValidation));
        rows.push(selectable_row(
            retry_choice_idx == Some(state.cursor),
            vec![
                Span::styled("Retry checks".to_string(), Style::default().fg(theme.text)),
                Span::styled(
                    "  after installing or signing in".to_string(),
                    Style::default().fg(theme.muted),
                ),
            ],
            theme,
        ));
    }
    rows
}

fn registry_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    state
        .registry_agents
        .iter()
        .enumerate()
        .map(|(idx, registry_agent)| {
            selectable_row(
                idx == state.cursor,
                vec![
                    Span::styled(registry_agent.name.clone(), Style::default().fg(theme.text)),
                    Span::styled(
                        format!("  {}", registry_agent_summary(registry_agent)),
                        Style::default().fg(theme.muted),
                    ),
                ],
                theme,
            )
        })
        .collect()
}

fn host_selected_row_index(state: &ThorSetupState) -> usize {
    let choices = state.host_choices();
    let Some(choice) = choices.get(state.cursor) else {
        return 0;
    };
    match choice {
        HostChoice::Agent(source_id) => state
            .agents
            .iter()
            .position(|setup_agent| setup_agent.agent.source_id == *source_id)
            .unwrap_or(0),
        HostChoice::AddRegistry => state.agents.len(),
        HostChoice::AddCustom => state.agents.len() + 1,
        HostChoice::RetryValidation => state.agents.len() + 2,
    }
}

fn registry_agent_summary(registry_agent: &ThorSetupRegistryAgent) -> String {
    let mut parts = Vec::new();
    if !registry_agent.description.trim().is_empty() {
        parts.push(registry_agent.description.clone());
    }
    if !registry_agent.command.trim().is_empty() {
        parts.push(format!("runs `{}`", registry_agent.command));
    }
    if !registry_agent.setup_hint.trim().is_empty() {
        parts.push(registry_agent.setup_hint.clone());
    }
    if parts.is_empty() && !registry_agent.setup_url.trim().is_empty() {
        parts.push(format!("docs: {}", registry_agent.setup_url));
    }
    if parts.is_empty() {
        registry_agent.source_id.clone()
    } else if !registry_agent.setup_url.trim().is_empty() {
        parts.push(format!("docs: {}", registry_agent.setup_url));
        truncate_label(&parts.join("; "), 96)
    } else {
        truncate_label(&parts.join("; "), 96)
    }
}

fn custom_name_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    text_input_rows(
        "Agent name",
        &state.custom_name,
        "Example: Claude Code, Codex, Goose",
        state.notice.as_deref(),
        theme,
    )
}

fn custom_command_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    text_input_rows(
        "Command",
        &state.custom_command,
        "Example: npx -y @agentclientprotocol/claude-agent-acp",
        state.notice.as_deref(),
        theme,
    )
}

fn text_input_rows(
    label: &str,
    value: &str,
    hint: &str,
    notice: Option<&str>,
    theme: TerminalTheme,
) -> Vec<ListItem<'static>> {
    let mut rows = vec![
        ListItem::new(Line::from(vec![
            Span::styled(format!("{label}: "), Style::default().fg(theme.muted)),
            Span::styled(format!("{value}_"), Style::default().fg(theme.text)),
        ])),
        ListItem::new(Line::from(Span::styled(
            hint.to_string(),
            Style::default().fg(theme.muted),
        ))),
    ];
    if let Some(notice) = notice {
        rows.push(ListItem::new(Line::from(Span::styled(
            notice.to_string(),
            Style::default().fg(theme.warning),
        ))));
    }
    rows
}

fn confirm_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    vec![selectable_row(
        state.cursor == 0,
        vec![
            Span::styled("Start Thor".to_string(), Style::default().fg(theme.text)),
            Span::styled(
                "  save this setup and open the prompt".to_string(),
                Style::default().fg(theme.muted),
            ),
        ],
        theme,
    )]
}

fn selectable_row(
    selected: bool,
    spans: Vec<Span<'static>>,
    theme: TerminalTheme,
) -> ListItem<'static> {
    let row_style = if selected {
        Style::default()
            .fg(theme.selection_fg)
            .bg(theme.selection_bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(theme.text)
    };
    let mut row = vec![Span::raw(if selected { "> " } else { "  " })];
    row.extend(spans);
    ListItem::new(Line::from(row)).style(row_style)
}

fn disabled_row(spans: Vec<Span<'static>>, theme: TerminalTheme) -> ListItem<'static> {
    let mut row = vec![Span::raw("  ")];
    row.extend(spans);
    ListItem::new(Line::from(row)).style(Style::default().fg(theme.muted))
}

fn summary_lines(state: &ThorSetupState, theme: TerminalTheme) -> Vec<Line<'static>> {
    match state.step {
        SetupStep::Registry => registry_summary_lines(state, theme),
        SetupStep::CustomName | SetupStep::CustomCommand => custom_summary_lines(state, theme),
        SetupStep::Persona | SetupStep::Host | SetupStep::Confirm => {
            default_summary_lines(state, theme)
        }
    }
}

fn default_summary_lines(state: &ThorSetupState, theme: TerminalTheme) -> Vec<Line<'static>> {
    let mut lines = vec![
        detail_line(
            "Work style",
            persona_summary(state.optimization_mode),
            theme,
        ),
        detail_line("Agents Thor may use", worker_summary(state), theme),
        detail_line("Run Thor in", host_summary(state), theme),
    ];
    if let Some(notice) = state.notice.as_ref() {
        lines.push(Line::from(Span::styled(
            notice.clone(),
            Style::default().fg(theme.warning),
        )));
    }
    lines
}

fn registry_summary_lines(state: &ThorSetupState, theme: TerminalTheme) -> Vec<Line<'static>> {
    let Some(registry_agent) = state.registry_agents.get(state.cursor) else {
        return vec![detail_line(
            "Known agents",
            "no entries loaded; go back and add an installed agent".to_string(),
            theme,
        )];
    };
    let setup = if registry_agent.setup_hint.trim().is_empty() {
        "open provider docs if setup is required".to_string()
    } else {
        registry_agent.setup_hint.clone()
    };
    vec![
        detail_line("Will add", registry_agent.name.clone(), theme),
        detail_line("Runs", truncate_label(&registry_agent.command, 72), theme),
        detail_line("Setup", truncate_label(&setup, 72), theme),
    ]
}

fn custom_summary_lines(state: &ThorSetupState, theme: TerminalTheme) -> Vec<Line<'static>> {
    vec![
        detail_line("Installed agent", custom_name_summary(state), theme),
        detail_line("Runs", custom_command_summary(state), theme),
        detail_line(
            "After add",
            "mj checks it before Thor uses it".to_string(),
            theme,
        ),
    ]
}

fn custom_name_summary(state: &ThorSetupState) -> String {
    if state.custom_name.trim().is_empty() {
        "choose a short name".to_string()
    } else {
        state.custom_name.trim().to_string()
    }
}

fn custom_command_summary(state: &ThorSetupState) -> String {
    if state.custom_command.trim().is_empty() {
        "enter the command that starts this agent".to_string()
    } else {
        truncate_label(state.custom_command.trim(), 72)
    }
}

fn worker_summary(state: &ThorSetupState) -> String {
    let selected = state
        .agents
        .iter()
        .zip(state.selected_workers.iter())
        .filter(|(setup_agent, selected)| **selected && setup_agent_is_usable(setup_agent))
        .map(|(setup_agent, _)| setup_agent_label(setup_agent))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        "choose at least one ready agent".to_string()
    } else {
        selected.join(", ")
    }
}

fn host_summary(state: &ThorSetupState) -> String {
    state
        .agents
        .iter()
        .find(|setup_agent| {
            setup_agent.agent.source_id == state.host_source_id
                && setup_agent_is_usable(setup_agent)
        })
        .map(setup_agent_label)
        .unwrap_or_else(|| "choose a ready agent".to_string())
}

fn detail_line(label: &str, value: String, theme: TerminalTheme) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label}: "), Style::default().fg(theme.muted)),
        Span::styled(value, Style::default().fg(theme.text)),
    ])
}

fn persona_summary(mode: ThorOptimizationMode) -> String {
    match mode {
        ThorOptimizationMode::Cost => "accountant; control spend on simple work".to_string(),
        ThorOptimizationMode::BestSolution | ThorOptimizationMode::Balanced => {
            "architect; prioritize the strongest answer".to_string()
        }
    }
}

fn host_agent_label(agent: &SelectedAgent) -> String {
    if agent.source_id == "anvil" {
        "Anvil".to_string()
    } else if let Some(custom) = agent.source_id.strip_prefix("custom:") {
        custom.to_string()
    } else {
        agent.source_id.clone()
    }
}

fn setup_agent_label(setup_agent: &ThorSetupAgent) -> String {
    if setup_agent.name.trim().is_empty() {
        host_agent_label(&setup_agent.agent)
    } else {
        setup_agent.name.clone()
    }
}

fn command_label(agent: &SelectedAgent) -> String {
    let mut parts = vec![agent.program.to_string_lossy().into_owned()];
    parts.extend(agent.args.iter().cloned());
    parts.join(" ")
}

fn setup_agent_description(setup_agent: &ThorSetupAgent) -> String {
    if let Some(validation) = setup_agent
        .validation
        .as_ref()
        .filter(|validation| !validation.usable)
    {
        return validation_detail_label(setup_agent, validation);
    }
    if !setup_agent.description.trim().is_empty() {
        truncate_label(&setup_agent.description, 72)
    } else {
        command_label(&setup_agent.agent)
    }
}

fn compact_setup_url_label(setup_agent: &ThorSetupAgent) -> Option<String> {
    let url = setup_agent.setup_url.trim();
    if url.is_empty() {
        return None;
    }
    let compact = url
        .strip_prefix("https://github.com/")
        .or_else(|| url.strip_prefix("http://github.com/"))
        .or_else(|| url.strip_prefix("https://"))
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url)
        .trim_end_matches('/');
    Some(truncate_label(compact, 28))
}

fn setup_agent_is_usable(setup_agent: &ThorSetupAgent) -> bool {
    setup_agent
        .validation
        .as_ref()
        .map(|validation| validation.usable)
        .unwrap_or(true)
}

fn host_status_label(setup_agent: &ThorSetupAgent) -> String {
    match &setup_agent.validation {
        Some(validation) if validation.usable => "ready".to_string(),
        Some(validation) => validation_action_label(setup_agent, validation),
        None => "available".to_string(),
    }
}

fn validation_action_label(setup_agent: &ThorSetupAgent, validation: &AgentValidation) -> String {
    let Some(error) = validation.error.as_deref() else {
        return "setup needed".to_string();
    };
    let lower = error.to_ascii_lowercase();
    if lower.contains("not found")
        || lower.contains("no such file")
        || lower.contains("missing command")
        || lower.contains("permission denied")
    {
        return install_action_label(setup_agent);
    }
    if lower.contains("auth")
        || lower.contains("login")
        || lower.contains("api key")
        || lower.contains("missing key")
        || lower.contains("token")
        || lower.contains("credential")
    {
        return auth_action_label(setup_agent);
    }
    if lower.contains("exited unexpectedly") || lower.contains("exit status") {
        generic_failure_action_label(setup_agent)
    } else if lower.contains("timed out") || lower.contains("timeout") {
        generic_timeout_action_label(setup_agent)
    } else {
        "setup needed".to_string()
    }
}

fn validation_detail_label(setup_agent: &ThorSetupAgent, validation: &AgentValidation) -> String {
    let Some(error) = validation.error.as_deref() else {
        return format!(
            "Command: {}",
            truncate_label(&command_label(&setup_agent.agent), 72)
        );
    };
    let lower = error.to_ascii_lowercase();
    if lower.contains("not found")
        || lower.contains("no such file")
        || lower.contains("missing command")
        || lower.contains("permission denied")
    {
        return with_setup_url(install_detail_label(setup_agent), setup_agent);
    }
    if lower.contains("auth")
        || lower.contains("login")
        || lower.contains("api key")
        || lower.contains("missing key")
        || lower.contains("token")
        || lower.contains("credential")
    {
        return with_setup_url(auth_detail_label(setup_agent), setup_agent);
    }
    if lower.contains("exited unexpectedly") || lower.contains("exit status") {
        return with_setup_url(generic_failure_detail_label(setup_agent), setup_agent);
    }
    if lower.contains("timed out") || lower.contains("timeout") {
        return with_setup_url(generic_timeout_detail_label(setup_agent), setup_agent);
    }
    format!("Last error: {}", truncate_label(error, 56))
}

fn with_setup_url(detail: String, setup_agent: &ThorSetupAgent) -> String {
    let Some(url) = compact_setup_url_label(setup_agent) else {
        return detail;
    };
    truncate_label(&format!("{detail}; docs: {url}"), 64)
}

fn install_action_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "anvil" => "install uv".to_string(),
        "claude-acp" => "install Node.js and Claude Code".to_string(),
        "codex-acp" => "install Node.js and Codex".to_string(),
        "opencode" => "install OpenCode CLI".to_string(),
        "goose" => "install Goose".to_string(),
        "cursor" => "install Cursor Agent".to_string(),
        "github-copilot-cli" => "install GitHub Copilot CLI".to_string(),
        _ => match setup_agent.agent.program.to_string_lossy().as_ref() {
            "npx" => "install Node.js/npm".to_string(),
            "uvx" => "install uv".to_string(),
            program => format!("install {program}"),
        },
    }
}

fn install_detail_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "anvil" => "Install uv; `uvx brokk acp`".to_string(),
        "claude-acp" => {
            "Install Node.js/npm and Claude Code; runs `npx ... claude-agent-acp`".to_string()
        }
        "codex-acp" => "Install Node.js/npm and Codex; runs `npx ... codex-acp`".to_string(),
        "opencode" => "Install OpenCode CLI, then configure provider credentials".to_string(),
        "goose" => "Install Goose, then configure a Goose provider".to_string(),
        "cursor" => "Install Cursor Agent, then sign in to Cursor".to_string(),
        "github-copilot-cli" => {
            "Install GitHub Copilot CLI, then sign in to GitHub Copilot".to_string()
        }
        _ => match setup_agent.agent.program.to_string_lossy().as_ref() {
            "npx" => "Install Node.js/npm, then retry this agent".to_string(),
            "uvx" => "Install uv, then retry this agent".to_string(),
            _ => format!(
                "Command: {}",
                truncate_label(&command_label(&setup_agent.agent), 72)
            ),
        },
    }
}

fn auth_action_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "claude-acp" => "sign in with Claude Code".to_string(),
        "codex-acp" => "sign in with Codex".to_string(),
        "opencode" => "set OpenCode provider".to_string(),
        "goose" => "set Goose provider".to_string(),
        "cursor" => "sign in to Cursor".to_string(),
        "github-copilot-cli" => "sign in to GitHub Copilot".to_string(),
        _ => "sign in or add key".to_string(),
    }
}

fn auth_detail_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "claude-acp" => "Run Claude Code sign-in, then retry Thor setup".to_string(),
        "codex-acp" => "Run Codex sign-in, then retry Thor setup".to_string(),
        "opencode" => "Set provider, retry".to_string(),
        "goose" => "Set provider, retry".to_string(),
        "cursor" => "Sign in, retry".to_string(),
        "github-copilot-cli" => "Sign in, retry".to_string(),
        _ => format!(
            "Command: {}",
            truncate_label(&command_label(&setup_agent.agent), 72)
        ),
    }
}

fn generic_failure_action_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "opencode" | "goose" | "cursor" | "github-copilot-cli" => auth_action_label(setup_agent),
        _ => "agent exited".to_string(),
    }
}

fn generic_failure_detail_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "opencode" | "goose" | "cursor" | "github-copilot-cli" => auth_detail_label(setup_agent),
        _ => "Check auth/config, then retry".to_string(),
    }
}

fn generic_timeout_action_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "opencode" | "goose" | "cursor" | "github-copilot-cli" => auth_action_label(setup_agent),
        _ => "timeout".to_string(),
    }
}

fn generic_timeout_detail_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "opencode" | "goose" | "cursor" | "github-copilot-cli" => auth_detail_label(setup_agent),
        _ => "Retry after install/auth is ready".to_string(),
    }
}

fn default_custom_name(agents: &[ThorSetupAgent]) -> String {
    let base = "Installed agent";
    if !agents.iter().any(|agent| setup_agent_label(agent) == base) {
        return base.to_string();
    }
    for idx in 2..100 {
        let name = format!("{base} {idx}");
        if !agents.iter().any(|agent| setup_agent_label(agent) == name) {
            return name;
        }
    }
    base.to_string()
}

fn truncate_label(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyEvent;
    use ratatui::backend::TestBackend;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn agent(source_id: &str) -> SelectedAgent {
        SelectedAgent {
            source_id: source_id.to_string(),
            program: PathBuf::from(source_id),
            args: Vec::new(),
            env: HashMap::new(),
        }
    }

    fn setup_agents(agents: &[SelectedAgent]) -> Vec<ThorSetupAgent> {
        agents
            .iter()
            .cloned()
            .map(|agent| ThorSetupAgent {
                agent,
                name: String::new(),
                description: String::new(),
                setup_url: String::new(),
                quota_backend: ThorQuotaBackend::None,
                validation: None,
            })
            .collect()
    }

    fn setup_agent_with_validation(source_id: &str, usable: bool) -> ThorSetupAgent {
        setup_agent_with_error(source_id, (!usable).then(|| "missing key".to_string()))
    }

    fn setup_agent_with_error(source_id: &str, error: Option<String>) -> ThorSetupAgent {
        ThorSetupAgent {
            agent: agent(source_id),
            name: source_id.to_string(),
            description: String::new(),
            setup_url: String::new(),
            quota_backend: ThorQuotaBackend::None,
            validation: Some(AgentValidation {
                source_id: source_id.to_string(),
                usable: error.is_none(),
                agent_name: None,
                agent_version: None,
                session_started: error.is_none(),
                config_advertised: false,
                prompt_images_supported: false,
                session_fork_supported: false,
                error,
                elapsed_ms: 10,
                checked_at_unix: 1,
            }),
        }
    }

    fn registry_agent(source_id: &str) -> ThorSetupRegistryAgent {
        ThorSetupRegistryAgent {
            source_id: source_id.to_string(),
            name: source_id.to_string(),
            description: format!("{source_id} from registry"),
            setup_url: format!("https://example.com/{source_id}"),
            command: format!("npx -y {source_id}"),
            setup_hint: "requires Node.js/npm".to_string(),
        }
    }

    #[test]
    fn available_agents_are_enabled_without_worker_step() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);

        assert_eq!(state.step, SetupStep::Persona);
        assert_eq!(state.enabled_source_ids(), vec!["claude", "codex"]);
    }

    #[test]
    fn persona_selection_advances_to_agent_selection() {
        let raw_agents = vec![agent("claude")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);

        state.cursor = 1;
        state.advance();

        assert_eq!(state.step, SetupStep::Host);
        assert_eq!(state.optimization_mode, ThorOptimizationMode::Cost);
    }

    #[test]
    fn host_selection_advances_directly_to_confirm() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);

        state.set_step(SetupStep::Host);
        state.cursor = 1;
        state.advance();

        assert_eq!(state.step, SetupStep::Confirm);
        assert_eq!(state.host_source_id, "codex");
    }

    #[test]
    fn back_from_confirm_returns_to_host_selection() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);

        state.set_step(SetupStep::Confirm);
        state.back();

        assert_eq!(state.step, SetupStep::Host);
    }

    #[test]
    fn confirm_returns_full_selection() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let mut cfg = ThorConfig {
            enabled_worker_source_ids: vec!["codex".to_string()],
            coordinator_model: "gpt-strong".to_string(),
            coordinator_reasoning: ThorReasoning::Medium,
            ..ThorConfig::default()
        };
        cfg.optimization_mode = ThorOptimizationMode::Cost;
        let mut state = ThorSetupState::new(&cfg, &agents, &[], &raw_agents[1]);
        state.set_step(SetupStep::Confirm);

        let ThorSetupOutcome::Selection(selection) = state.advance().expect("selection") else {
            panic!("expected selection");
        };
        assert_eq!(selection.enabled_worker_source_ids, vec!["claude", "codex"]);
        assert_eq!(selection.host_source_id, "codex");
        assert_eq!(selection.optimization_mode, ThorOptimizationMode::Cost);
        assert_eq!(selection.coordinator_model, "gpt-strong");
        assert_eq!(selection.coordinator_reasoning, ThorReasoning::Medium);
    }

    #[test]
    fn enter_on_confirm_finishes() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);
        state.set_step(SetupStep::Confirm);
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);

        assert!(
            handle_event(&mut state, CtEvent::Key(key))
                .expect("handled")
                .is_some()
        );
    }

    #[test]
    fn escape_cancels() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);
        let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);

        assert_eq!(handle_event(&mut state, CtEvent::Key(key)), Some(None));
    }

    #[test]
    fn unusable_workers_are_not_preselected_when_usable_worker_exists() {
        let agents = vec![
            setup_agent_with_validation("claude", false),
            setup_agent_with_validation("codex", true),
        ];
        let initial_host = agents[0].agent.clone();
        let state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &initial_host);

        assert_eq!(state.enabled_source_ids(), vec!["codex"]);
        assert_eq!(state.host_source_id, "codex");
    }

    #[test]
    fn no_usable_workers_are_not_treated_as_available() {
        let agents = vec![
            setup_agent_with_validation("claude", false),
            setup_agent_with_validation("codex", false),
        ];
        let initial_host = agents[0].agent.clone();
        let state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &initial_host);

        assert!(state.enabled_source_ids().is_empty());
        assert_eq!(host_summary(&state), "choose a ready agent");
        assert!(matches!(
            state.host_choices().first(),
            Some(HostChoice::AddCustom)
        ));
    }

    #[test]
    fn summary_uses_display_names_instead_of_source_ids() {
        let mut agents = vec![setup_agent_with_validation("custom:claude-code", true)];
        agents[0].name = "Claude Code".to_string();
        let initial_host = agents[0].agent.clone();
        let state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &initial_host);

        assert_eq!(worker_summary(&state), "Claude Code");
        assert_eq!(host_summary(&state), "Claude Code");
        let summary = summary_lines(&state, crate::theme::TerminalThemeKind::Dark.palette());
        let rendered = summary
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("Work style: architect; prioritize the strongest answer"));
        assert!(rendered.contains("Agents Thor may use: Claude Code"));
        assert!(!rendered.contains("auto-strong"));
        assert!(!rendered.contains("custom:claude-code"));
    }

    #[test]
    fn registry_summary_shows_command_and_setup_before_add() {
        let agents = vec![setup_agent_with_validation("anvil", false)];
        let registry_agents = vec![registry_agent("gemini")];
        let initial_host = agents[0].agent.clone();
        let mut state = ThorSetupState::new(
            &ThorConfig::default(),
            &agents,
            &registry_agents,
            &initial_host,
        );
        state.set_step(SetupStep::Registry);

        let summary = summary_lines(&state, crate::theme::TerminalThemeKind::Dark.palette());
        let rendered = summary
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Will add: gemini"));
        assert!(rendered.contains("Runs: npx -y gemini"));
        assert!(rendered.contains("Setup: requires Node.js/npm"));
    }

    #[test]
    fn custom_summary_says_validation_happens_after_add() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);
        state.set_step(SetupStep::CustomCommand);
        state.custom_name = "Local Agent".to_string();
        state.custom_command = "local-agent acp".to_string();

        let summary = summary_lines(&state, crate::theme::TerminalThemeKind::Dark.palette());
        let rendered = summary
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Installed agent: Local Agent"));
        assert!(rendered.contains("Runs: local-agent acp"));
        assert!(rendered.contains("After add: mj checks it before Thor uses it"));
    }

    #[test]
    fn retry_validation_choice_returns_retry_outcome() {
        let agents = vec![setup_agent_with_validation("claude", false)];
        let initial_host = agents[0].agent.clone();
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &initial_host);

        state.set_step(SetupStep::Host);
        state.cursor = state
            .host_choices()
            .iter()
            .position(|choice| matches!(choice, HostChoice::RetryValidation))
            .expect("retry choice");

        assert_eq!(state.advance(), Some(ThorSetupOutcome::RetryValidation));
    }

    #[test]
    fn validation_error_gives_actionable_sign_in_guidance() {
        let setup_agent = setup_agent_with_validation("claude", false);

        assert_eq!(host_status_label(&setup_agent), "sign in or add key");
    }

    #[test]
    fn validation_error_gives_provider_specific_auth_guidance() {
        let setup_agent = setup_agent_with_error("claude-acp", Some("auth required".to_string()));

        assert_eq!(host_status_label(&setup_agent), "sign in with Claude Code");
        assert_eq!(
            setup_agent_description(&setup_agent),
            "Run Claude Code sign-in, then retry Thor setup"
        );
    }

    #[test]
    fn validation_error_gives_provider_specific_install_guidance() {
        let mut setup_agent =
            setup_agent_with_error("codex-acp", Some("npx not found".to_string()));
        setup_agent.agent.program = PathBuf::from("npx");

        assert_eq!(host_status_label(&setup_agent), "install Node.js and Codex");
        assert_eq!(
            setup_agent_description(&setup_agent),
            "Install Node.js/npm and Codex; runs `npx ... codex-acp`"
        );
    }

    #[test]
    fn validation_error_gives_anvil_install_guidance() {
        let mut setup_agent = setup_agent_with_error("anvil", Some("uvx not found".to_string()));
        setup_agent.agent.program = PathBuf::from("uvx");

        assert_eq!(host_status_label(&setup_agent), "install uv");
        assert_eq!(
            setup_agent_description(&setup_agent),
            "Install uv; `uvx brokk acp`"
        );
    }

    #[test]
    fn setup_docs_urls_are_compacted_for_rows() {
        let mut setup_agent = setup_agent_with_validation("anvil", false);
        setup_agent.setup_url = "https://github.com/BrokkAi/brokk".to_string();

        assert_eq!(
            compact_setup_url_label(&setup_agent).as_deref(),
            Some("BrokkAi/brokk")
        );
    }

    #[test]
    fn validation_error_compacts_unexpected_exit_guidance() {
        let setup_agent = setup_agent_with_error(
            "unknown-agent",
            Some("agent process exited unexpectedly: exit status 1".to_string()),
        );

        assert_eq!(host_status_label(&setup_agent), "agent exited");
        assert_eq!(
            setup_agent_description(&setup_agent),
            "Check auth/config, then retry"
        );
    }

    #[test]
    fn validation_error_uses_provider_guidance_for_known_binary_agents() {
        let setup_agent = setup_agent_with_error(
            "opencode",
            Some("agent process exited unexpectedly: exit status 1".to_string()),
        );

        assert_eq!(host_status_label(&setup_agent), "set OpenCode provider");
        assert_eq!(setup_agent_description(&setup_agent), "Set provider, retry");
    }

    #[test]
    fn validation_error_compacts_timeout_guidance() {
        let setup_agent = setup_agent_with_error(
            "anvil",
            Some("ACP validation timed out after 8s".to_string()),
        );

        assert_eq!(host_status_label(&setup_agent), "timeout");
        assert_eq!(
            setup_agent_description(&setup_agent),
            "Retry after install/auth is ready"
        );
    }

    #[test]
    fn add_custom_choice_collects_name_and_command() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);

        state.set_step(SetupStep::Host);
        state.cursor = state.host_choices().len() - 1;
        state.advance();
        assert_eq!(state.step, SetupStep::CustomName);

        state.custom_name.clear();
        for ch in "Claude Code".chars() {
            state.edit_text(ch);
        }
        state.advance();
        assert_eq!(state.step, SetupStep::CustomCommand);

        for ch in "npx -y @agentclientprotocol/claude-agent-acp".chars() {
            state.edit_text(ch);
        }
        let ThorSetupOutcome::AddCustom(custom) = state.advance().expect("custom agent") else {
            panic!("expected custom agent");
        };
        assert_eq!(custom.name, "Claude Code");
        assert_eq!(
            custom.command,
            "npx -y @agentclientprotocol/claude-agent-acp"
        );
    }

    #[test]
    fn registry_choice_returns_selected_registry_source() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let registry_agents = vec![registry_agent("gemini"), registry_agent("goose")];
        let mut state = ThorSetupState::new(
            &ThorConfig::default(),
            &agents,
            &registry_agents,
            &raw_agents[0],
        );

        state.set_step(SetupStep::Host);
        let registry_choice_idx = state
            .host_choices()
            .iter()
            .position(|choice| matches!(choice, HostChoice::AddRegistry))
            .expect("registry choice");
        state.cursor = registry_choice_idx;
        state.advance();
        assert_eq!(state.step, SetupStep::Registry);

        state.cursor = 1;
        let ThorSetupOutcome::AddRegistry(source_id) = state.advance().expect("registry") else {
            panic!("expected registry selection");
        };
        assert_eq!(source_id, "goose");
    }

    #[test]
    fn space_toggles_agents_available_to_thor() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);
        state.set_step(SetupStep::Host);

        state.cursor = 1;
        handle_event(
            &mut state,
            CtEvent::Key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE)),
        );

        assert_eq!(state.enabled_source_ids(), vec!["claude"]);
        assert_eq!(worker_summary(&state), "claude");
    }

    #[test]
    fn last_ready_agent_notice_is_visible() {
        let raw_agents = vec![agent("claude")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);
        state.set_step(SetupStep::Host);

        handle_event(
            &mut state,
            CtEvent::Key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE)),
        );

        let summary = summary_lines(&state, crate::theme::TerminalThemeKind::Dark.palette());
        let rendered = summary
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("Thor needs at least one ready agent."));
    }

    #[test]
    fn registry_summary_includes_command() {
        let registry_agent = registry_agent("gemini");

        assert!(registry_agent_summary(&registry_agent).contains("runs `npx -y gemini`"));
        assert!(registry_agent_summary(&registry_agent).contains("requires Node.js/npm"));
    }

    #[test]
    fn host_selected_row_index_tracks_registry_custom_and_retry_actions() {
        let agents = vec![setup_agent_with_validation("anvil", false)];
        let raw_agent = agents[0].agent.clone();
        let registry_agents = vec![registry_agent("gemini")];
        let mut state = ThorSetupState::new(
            &ThorConfig::default(),
            &agents,
            &registry_agents,
            &raw_agent,
        );

        state.cursor = state
            .host_choices()
            .iter()
            .position(|choice| matches!(choice, HostChoice::AddRegistry))
            .expect("registry choice");
        assert_eq!(host_selected_row_index(&state), 1);

        state.cursor = state.host_choices().len() - 1;
        assert_eq!(host_selected_row_index(&state), 3);

        state.cursor = state
            .host_choices()
            .iter()
            .position(|choice| matches!(choice, HostChoice::AddCustom))
            .expect("custom choice");
        assert_eq!(host_selected_row_index(&state), 2);
    }

    #[test]
    fn space_key_edits_custom_command() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);
        state.set_step(SetupStep::CustomCommand);

        handle_event(
            &mut state,
            CtEvent::Key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE)),
        );
        handle_event(
            &mut state,
            CtEvent::Key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE)),
        );
        handle_event(
            &mut state,
            CtEvent::Key(KeyEvent::new(KeyCode::Char('-'), KeyModifiers::NONE)),
        );

        assert_eq!(state.custom_command, "n -");
    }

    #[test]
    fn visible_rows_follow_cursor_for_long_lists() {
        let rows = (0..10)
            .map(|idx| ListItem::new(Line::from(format!("row {idx}"))))
            .collect::<Vec<_>>();

        let window = visible_rows(rows, 8, 4);

        assert_eq!(window.len(), 4);
    }

    #[test]
    fn host_selected_row_index_tracks_add_command_after_failed_rows() {
        let agents = vec![
            setup_agent_with_validation("claude", false),
            setup_agent_with_validation("codex", false),
        ];
        let initial_host = agents[0].agent.clone();
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &initial_host);
        state.cursor = state
            .host_choices()
            .iter()
            .position(|choice| matches!(choice, HostChoice::AddCustom))
            .expect("custom choice");

        assert_eq!(host_selected_row_index(&state), 3);

        state.cursor = state
            .host_choices()
            .iter()
            .position(|choice| matches!(choice, HostChoice::RetryValidation))
            .expect("retry choice");

        assert_eq!(host_selected_row_index(&state), 4);
    }

    #[test]
    fn setup_screen_renders_recovery_paths_across_terminal_sizes() {
        let agents = vec![
            setup_agent_with_validation("claude", false),
            setup_agent_with_validation("codex", false),
        ];
        let registry_agents = vec![registry_agent("gemini")];
        let initial_host = agents[0].agent.clone();
        let mut state = ThorSetupState::new(
            &ThorConfig::default(),
            &agents,
            &registry_agents,
            &initial_host,
        );

        for (width, height) in [(72, 24), (120, 36)] {
            let backend = TestBackend::new(width, height);
            let mut terminal = Terminal::new(backend).expect("terminal");
            terminal
                .draw(|f| draw(f, &state, crate::theme::TerminalThemeKind::Dark.palette()))
                .expect("draw setup");
            let rendered = buffer_lines(terminal.backend().buffer()).join("\n");

            assert!(rendered.contains("Set up Thor"));
            assert!(rendered.contains("Architect"));
            assert!(rendered.contains("Accountant"));
            assert!(!rendered.contains("registry"));
            assert!(!rendered.contains("custom command"));
            assert!(!rendered.contains("ACP"));
        }

        state.set_step(SetupStep::Host);
        for (width, height) in [(72, 24), (120, 36)] {
            let backend = TestBackend::new(width, height);
            let mut terminal = Terminal::new(backend).expect("terminal");
            terminal
                .draw(|f| draw(f, &state, crate::theme::TerminalThemeKind::Dark.palette()))
                .expect("draw setup");
            let rendered = buffer_lines(terminal.backend().buffer()).join("\n");

            assert!(rendered.contains("Add known agent"));
            assert!(rendered.contains("Add installed agent"));
            assert!(rendered.contains("No agent is ready"));
            assert!(!rendered.contains("Add agent from registry"));
            assert!(!rendered.contains("Add custom command"));
        }
    }

    fn buffer_lines(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
        (0..buffer.area().height)
            .map(|y| {
                (0..buffer.area().width)
                    .map(|x| buffer.cell((x, y)).expect("cell").symbol())
                    .collect()
            })
            .collect()
    }
}
