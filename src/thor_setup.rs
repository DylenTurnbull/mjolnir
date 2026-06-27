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
    Host,
    Registry,
    CustomName,
    CustomCommand,
    Confirm,
}

impl SetupStep {
    fn title(self) -> &'static str {
        match self {
            Self::Host => "choose Thor",
            Self::Registry => "registry",
            Self::CustomName => "name",
            Self::CustomCommand => "command",
            Self::Confirm => "start",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Host => 0,
            Self::Registry => 1,
            Self::CustomName => 2,
            Self::CustomCommand => 3,
            Self::Confirm => 4,
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
                description: "Brokk ACP server via uvx".to_string(),
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
            step: SetupStep::Host,
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
        state.cursor = state.default_cursor_for(SetupStep::Host);
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
            SetupStep::Host => match self.host_choices().get(self.cursor) {
                Some(HostChoice::Agent(source_id)) => {
                    self.host_source_id = source_id.clone();
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
                    self.notice = Some("Enter a short name for this ACP agent.".to_string());
                } else {
                    self.notice = None;
                    self.set_step(SetupStep::CustomCommand);
                }
            }
            SetupStep::CustomCommand => {
                if self.custom_command.trim().is_empty() {
                    self.notice = Some("Enter the command that starts the ACP server.".to_string());
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
            SetupStep::Host => {}
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
            SetupStep::Host | SetupStep::Registry | SetupStep::Confirm => {}
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
            SetupStep::Host | SetupStep::Registry | SetupStep::Confirm => false,
        }
    }

    fn is_text_step(&self) -> bool {
        matches!(self.step, SetupStep::CustomName | SetupStep::CustomCommand)
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
            SetupStep::Registry => 0,
            SetupStep::CustomName | SetupStep::CustomCommand => 0,
            SetupStep::Confirm => 0,
        }
    }

    fn current_len(&self) -> usize {
        match self.step {
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
        SetupStep::Host => host_rows(state, theme),
        SetupStep::Registry => registry_rows(state, theme),
        SetupStep::CustomName => custom_name_rows(state, theme),
        SetupStep::CustomCommand => custom_command_rows(state, theme),
        SetupStep::Confirm => confirm_rows(state, theme),
    };
    let content_cursor = match state.step {
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
        SetupStep::Host => "Enter selects  |  Retry after install/sign-in  |  Esc quits",
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
            "No agent is ready yet. Add one, fix install/sign-in, then retry checks.",
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
        Line::from("Add from the registry for known agents, or add a custom ACP command."),
        Line::from("Model and reasoning defaults are already set and can be changed later."),
    ]
}

fn progress_line(state: &ThorSetupState, theme: TerminalTheme) -> Paragraph<'static> {
    let steps = match state.step {
        SetupStep::CustomName | SetupStep::CustomCommand => vec![
            SetupStep::Host,
            SetupStep::CustomName,
            SetupStep::CustomCommand,
            SetupStep::Confirm,
        ],
        SetupStep::Registry => vec![SetupStep::Host, SetupStep::Registry, SetupStep::Confirm],
        SetupStep::Host | SetupStep::Confirm => vec![SetupStep::Host, SetupStep::Confirm],
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

fn host_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    let choices = state.host_choices();
    let mut choice_idx = 0usize;
    let mut rows = Vec::new();
    for setup_agent in &state.agents {
        if setup_agent_is_usable(setup_agent) {
            let selected = matches!(
                choices.get(choice_idx),
                Some(HostChoice::Agent(source_id)) if source_id == &setup_agent.agent.source_id
            ) && choice_idx == state.cursor;
            choice_idx += 1;
            rows.push(selectable_row(
                selected,
                vec![
                    Span::styled(
                        setup_agent_label(setup_agent),
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
                Span::styled("ACP registry".to_string(), Style::default().fg(theme.muted)),
                Span::styled(
                    "  unavailable; add a custom ACP command instead".to_string(),
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
                    "Add from ACP registry".to_string(),
                    Style::default().fg(theme.text),
                ),
                Span::styled(
                    format!("  {} available server types", state.registry_agents.len()),
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
                "Add ACP command".to_string(),
                Style::default().fg(theme.text),
            ),
            Span::styled(
                "  configure another agent for Thor".to_string(),
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
        "ACP command",
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
    vec![
        detail_line("Available agents", enabled_summary(state), theme),
        detail_line("Thor runs in", host_summary(state), theme),
        detail_line(
            "Defaults",
            format!(
                "{}, {}, {} reasoning",
                persona_label(state.optimization_mode),
                state.coordinator_model,
                reasoning_label(state.coordinator_reasoning)
            ),
            theme,
        ),
    ]
}

fn enabled_summary(state: &ThorSetupState) -> String {
    let enabled = state.enabled_source_ids();
    if enabled.is_empty() {
        "none ready; add or fix an ACP command".to_string()
    } else {
        enabled.join(", ")
    }
}

fn host_summary(state: &ThorSetupState) -> String {
    if state
        .host_choices()
        .iter()
        .any(|choice| matches!(choice, HostChoice::Agent(source_id) if source_id == &state.host_source_id))
    {
        state.host_source_id.clone()
    } else {
        "none ready yet".to_string()
    }
}

fn detail_line(label: &str, value: String, theme: TerminalTheme) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label}: "), Style::default().fg(theme.muted)),
        Span::styled(value, Style::default().fg(theme.text)),
    ])
}

fn persona_label(mode: ThorOptimizationMode) -> String {
    match mode {
        ThorOptimizationMode::Cost => "accountant".to_string(),
        ThorOptimizationMode::BestSolution => "architect".to_string(),
        ThorOptimizationMode::Balanced => "architect".to_string(),
    }
}

fn reasoning_label(reasoning: ThorReasoning) -> String {
    match reasoning {
        ThorReasoning::Low => "low".to_string(),
        ThorReasoning::Medium => "medium".to_string(),
        ThorReasoning::High => "high".to_string(),
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

fn setup_url_label(setup_agent: &ThorSetupAgent) -> Option<String> {
    let url = setup_agent.setup_url.trim();
    (!url.is_empty()).then(|| url.to_string())
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
    format!("setup needed: {}", truncate_label(error, 48))
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
    format!("Last error: {}", truncate_label(error, 72))
}

fn with_setup_url(detail: String, setup_agent: &ThorSetupAgent) -> String {
    let Some(url) = setup_url_label(setup_agent) else {
        return detail;
    };
    truncate_label(&format!("{detail}; docs: {url}"), 96)
}

fn install_action_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "anvil" => "install uv".to_string(),
        "claude-acp" => "install Node.js and Claude Code".to_string(),
        "codex-acp" => "install Node.js and Codex".to_string(),
        _ => match setup_agent.agent.program.to_string_lossy().as_ref() {
            "npx" => "install Node.js/npm".to_string(),
            "uvx" => "install uv".to_string(),
            program => format!("install {program}"),
        },
    }
}

fn install_detail_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "anvil" => "Install uv; Thor starts Anvil with `uvx brokk acp`".to_string(),
        "claude-acp" => {
            "Install Node.js/npm and Claude Code; setup starts `npx ... claude-agent-acp`"
                .to_string()
        }
        "codex-acp" => {
            "Install Node.js/npm and Codex; setup starts `npx ... codex-acp`".to_string()
        }
        _ => match setup_agent.agent.program.to_string_lossy().as_ref() {
            "npx" => "Install Node.js/npm, then retry this ACP server".to_string(),
            "uvx" => "Install uv, then retry this ACP server".to_string(),
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
        _ => "sign in or add key".to_string(),
    }
}

fn auth_detail_label(setup_agent: &ThorSetupAgent) -> String {
    match setup_agent.agent.source_id.as_str() {
        "claude-acp" => "Run Claude Code sign-in, then retry Thor setup".to_string(),
        "codex-acp" => "Run Codex sign-in, then retry Thor setup".to_string(),
        _ => format!(
            "Command: {}",
            truncate_label(&command_label(&setup_agent.agent), 72)
        ),
    }
}

fn default_custom_name(agents: &[ThorSetupAgent]) -> String {
    let base = "Custom ACP";
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
        }
    }

    #[test]
    fn available_agents_are_enabled_without_worker_step() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);

        assert_eq!(state.step, SetupStep::Host);
        assert_eq!(state.enabled_source_ids(), vec!["claude", "codex"]);
    }

    #[test]
    fn host_selection_advances_directly_to_confirm() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);

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
        assert_eq!(host_summary(&state), "none ready yet");
        assert!(matches!(
            state.host_choices().first(),
            Some(HostChoice::AddCustom)
        ));
    }

    #[test]
    fn retry_validation_choice_returns_retry_outcome() {
        let agents = vec![setup_agent_with_validation("claude", false)];
        let initial_host = agents[0].agent.clone();
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &initial_host);

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
            "Install Node.js/npm and Codex; setup starts `npx ... codex-acp`"
        );
    }

    #[test]
    fn validation_error_gives_anvil_install_guidance() {
        let mut setup_agent = setup_agent_with_error("anvil", Some("uvx not found".to_string()));
        setup_agent.agent.program = PathBuf::from("uvx");

        assert_eq!(host_status_label(&setup_agent), "install uv");
        assert_eq!(
            setup_agent_description(&setup_agent),
            "Install uv; Thor starts Anvil with `uvx brokk acp`"
        );
    }

    #[test]
    fn add_custom_choice_collects_name_and_command() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &[], &raw_agents[0]);

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
    fn registry_summary_includes_command() {
        let registry_agent = registry_agent("gemini");

        assert!(registry_agent_summary(&registry_agent).contains("runs `npx -y gemini`"));
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
        let state = ThorSetupState::new(
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
            assert!(rendered.contains("Add from ACP registry"));
            assert!(rendered.contains("Add ACP command"));
            assert!(rendered.contains("none ready"));
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
