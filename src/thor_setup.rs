//! First-run Thor setup.

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

use crate::config::SelectedAgent;
use crate::palette::TerminalTheme;
use crate::term::TrackedBackend;
use crate::thor::{ThorConfig, ThorOptimizationMode, ThorReasoning};
use crate::thor_probe::AgentValidation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThorSetupAgent {
    pub agent: SelectedAgent,
    pub validation: Option<AgentValidation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThorSetupSelection {
    pub enabled_worker_source_ids: Vec<String>,
    pub host_source_id: String,
    pub optimization_mode: ThorOptimizationMode,
    pub coordinator_model: String,
    pub coordinator_reasoning: ThorReasoning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupStep {
    Workers,
    Persona,
    Host,
    Model,
    Reasoning,
    Confirm,
}

impl SetupStep {
    fn title(self) -> &'static str {
        match self {
            Self::Workers => "select workers",
            Self::Persona => "pick persona",
            Self::Host => "pick Thor host",
            Self::Model => "pick Thor model",
            Self::Reasoning => "pick reasoning",
            Self::Confirm => "confirm",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Workers => 0,
            Self::Persona => 1,
            Self::Host => 2,
            Self::Model => 3,
            Self::Reasoning => 4,
            Self::Confirm => 5,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ModelOption {
    value: &'static str,
    label: &'static str,
    description: &'static str,
}

const MODEL_OPTIONS: [ModelOption; 4] = [
    ModelOption {
        value: "auto-strong",
        label: "Auto strong",
        description: "let Thor choose the strongest configured coordinator model",
    },
    ModelOption {
        value: "claude-strong",
        label: "Claude strong",
        description: "prefer a strong Claude-family coordinator",
    },
    ModelOption {
        value: "gpt-strong",
        label: "GPT strong",
        description: "prefer a strong GPT-family coordinator",
    },
    ModelOption {
        value: "openrouter-strong",
        label: "OpenRouter strong",
        description: "prefer the strongest OpenRouter route",
    },
];

const REASONING_OPTIONS: [(ThorReasoning, &str, &str); 3] = [
    (
        ThorReasoning::High,
        "High",
        "best default for Thor planning, routing, and review",
    ),
    (
        ThorReasoning::Medium,
        "Medium",
        "less planning overhead for routine work",
    ),
    (
        ThorReasoning::Low,
        "Low",
        "fastest coordinator behavior for simple requests",
    ),
];

#[derive(Debug, Clone)]
struct ThorSetupState {
    agents: Vec<ThorSetupAgent>,
    step: SetupStep,
    cursor: usize,
    selected_workers: Vec<bool>,
    optimization_mode: ThorOptimizationMode,
    host_source_id: String,
    coordinator_model: String,
    coordinator_reasoning: ThorReasoning,
}

impl ThorSetupState {
    fn new(
        thor_config: &ThorConfig,
        agents: &[ThorSetupAgent],
        initial_host: &SelectedAgent,
    ) -> Self {
        let agents = if agents.is_empty() {
            vec![ThorSetupAgent {
                agent: crate::thor::default_anvil_agent(),
                validation: None,
            }]
        } else {
            agents.to_vec()
        };
        let has_usable_agent = agents.iter().any(setup_agent_is_usable);
        let mut selected_workers = agents
            .iter()
            .map(|setup_agent| {
                let selected_by_config = thor_config.enabled_worker_source_ids.is_empty()
                    || thor_config
                        .enabled_worker_source_ids
                        .iter()
                        .any(|source_id| source_id == &setup_agent.agent.source_id);
                selected_by_config && (!has_usable_agent || setup_agent_is_usable(setup_agent))
            })
            .collect::<Vec<_>>();
        if !selected_workers.iter().any(|selected| *selected) {
            let idx = agents.iter().position(setup_agent_is_usable).unwrap_or(0);
            selected_workers[idx] = true;
        }

        let host_source_id = if agents
            .iter()
            .any(|setup_agent| setup_agent.agent.source_id == initial_host.source_id)
        {
            initial_host.source_id.clone()
        } else {
            agents[0].agent.source_id.clone()
        };
        let optimization_mode = match thor_config.optimization_mode {
            ThorOptimizationMode::Cost => ThorOptimizationMode::Cost,
            _ => ThorOptimizationMode::BestSolution,
        };

        let mut state = Self {
            agents,
            step: SetupStep::Workers,
            cursor: 0,
            selected_workers,
            optimization_mode,
            host_source_id,
            coordinator_model: thor_config.coordinator_model.clone(),
            coordinator_reasoning: thor_config.coordinator_reasoning,
        };
        if let Some(idx) = state.selected_workers.iter().position(|selected| *selected) {
            state.cursor = idx;
        }
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

    fn toggle_current_worker(&mut self) {
        if self.step != SetupStep::Workers || self.agents.is_empty() {
            return;
        }
        if self.has_usable_agent() && !self.current_worker_is_usable() {
            return;
        }
        self.selected_workers[self.cursor] = !self.selected_workers[self.cursor];
        if !self.selected_workers.iter().any(|selected| *selected) {
            self.selected_workers[self.cursor] = true;
        }
        self.ensure_host_is_enabled();
    }

    fn advance(&mut self) -> Option<ThorSetupSelection> {
        match self.step {
            SetupStep::Workers => self.set_step(SetupStep::Persona),
            SetupStep::Persona => {
                self.optimization_mode = persona_mode_for_cursor(self.cursor);
                self.set_step(SetupStep::Host);
            }
            SetupStep::Host => {
                if let Some(source_id) = self.enabled_source_ids().get(self.cursor) {
                    self.host_source_id = source_id.clone();
                }
                self.set_step(SetupStep::Model);
            }
            SetupStep::Model => {
                self.coordinator_model = MODEL_OPTIONS[self.cursor].value.to_string();
                self.set_step(SetupStep::Reasoning);
            }
            SetupStep::Reasoning => {
                self.coordinator_reasoning = REASONING_OPTIONS[self.cursor].0;
                self.set_step(SetupStep::Confirm);
            }
            SetupStep::Confirm => return Some(self.selection()),
        }
        None
    }

    fn back(&mut self) {
        match self.step {
            SetupStep::Workers => {}
            SetupStep::Persona => self.set_step(SetupStep::Workers),
            SetupStep::Host => self.set_step(SetupStep::Persona),
            SetupStep::Model => self.set_step(SetupStep::Host),
            SetupStep::Reasoning => self.set_step(SetupStep::Model),
            SetupStep::Confirm => self.set_step(SetupStep::Reasoning),
        }
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
            SetupStep::Workers => self
                .selected_workers
                .iter()
                .position(|selected| *selected)
                .unwrap_or(0),
            SetupStep::Persona => {
                if self.optimization_mode == ThorOptimizationMode::Cost {
                    1
                } else {
                    0
                }
            }
            SetupStep::Host => self
                .enabled_source_ids()
                .iter()
                .position(|source_id| source_id == &self.host_source_id)
                .unwrap_or(0),
            SetupStep::Model => MODEL_OPTIONS
                .iter()
                .position(|option| option.value == self.coordinator_model)
                .unwrap_or(0),
            SetupStep::Reasoning => REASONING_OPTIONS
                .iter()
                .position(|(reasoning, _, _)| *reasoning == self.coordinator_reasoning)
                .unwrap_or(0),
            SetupStep::Confirm => 0,
        }
    }

    fn current_len(&self) -> usize {
        match self.step {
            SetupStep::Workers => self.agents.len(),
            SetupStep::Persona => 2,
            SetupStep::Host => self.enabled_source_ids().len(),
            SetupStep::Model => MODEL_OPTIONS.len(),
            SetupStep::Reasoning => REASONING_OPTIONS.len(),
            SetupStep::Confirm => 1,
        }
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

    fn has_usable_agent(&self) -> bool {
        self.agents.iter().any(setup_agent_is_usable)
    }

    fn current_worker_is_usable(&self) -> bool {
        self.agents
            .get(self.cursor)
            .map(setup_agent_is_usable)
            .unwrap_or(false)
    }
}

/// Run Thor setup until the user confirms or cancels with Esc/Ctrl-C.
pub async fn run_thor_setup(
    terminal: &mut Terminal<TrackedBackend<Stdout>>,
    theme: TerminalTheme,
    thor_config: &ThorConfig,
    agents: &[ThorSetupAgent],
    initial_host: &SelectedAgent,
) -> Result<Option<ThorSetupSelection>> {
    let mut state = ThorSetupState::new(thor_config, agents, initial_host);
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

fn handle_event(state: &mut ThorSetupState, ev: CtEvent) -> Option<Option<ThorSetupSelection>> {
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
            state.back();
            None
        }
        KeyCode::Char(' ') => {
            state.toggle_current_worker();
            None
        }
        KeyCode::Enter | KeyCode::Right => state.advance().map(Some),
        _ => None,
    }
}

fn draw(f: &mut ratatui::Frame, state: &ThorSetupState, theme: TerminalTheme) {
    let area = crate::term::centered_rect(f.area(), 80, 24);
    let block = Block::default()
        .title(" Thor setup ")
        .borders(Borders::ALL)
        .style(Style::default().fg(theme.text));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(2),
            Constraint::Min(10),
            Constraint::Length(4),
            Constraint::Length(1),
        ])
        .split(inner);

    let title = Paragraph::new(vec![
        Line::from(vec![Span::styled(
            "Thor is the only prompt path.",
            Style::default()
                .fg(theme.primary)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from("Set who Thor can use, how Thor thinks, and where Thor runs."),
    ])
    .style(Style::default().fg(theme.text));
    f.render_widget(title, layout[0]);

    f.render_widget(progress_line(state, theme), layout[1]);

    let content = match state.step {
        SetupStep::Workers => worker_rows(state, theme),
        SetupStep::Persona => persona_rows(state, theme),
        SetupStep::Host => host_rows(state, theme),
        SetupStep::Model => model_rows(state, theme),
        SetupStep::Reasoning => reasoning_rows(state, theme),
        SetupStep::Confirm => confirm_rows(state, theme),
    };
    f.render_widget(List::new(content), layout[2]);

    let summary = Paragraph::new(summary_lines(state, theme))
        .style(Style::default().fg(theme.text))
        .wrap(Wrap { trim: true });
    f.render_widget(summary, layout[3]);

    let footer_text = match state.step {
        SetupStep::Workers => "Space toggles  |  Enter continues  |  Esc quits",
        SetupStep::Confirm => "Enter starts Thor  |  Backspace edits  |  Esc quits",
        _ => "Enter selects  |  Backspace edits  |  Esc quits",
    };
    let footer = Paragraph::new(footer_text).style(Style::default().fg(theme.muted));
    f.render_widget(footer, layout[4]);
}

fn progress_line(state: &ThorSetupState, theme: TerminalTheme) -> Paragraph<'static> {
    let steps = [
        SetupStep::Workers,
        SetupStep::Persona,
        SetupStep::Host,
        SetupStep::Model,
        SetupStep::Reasoning,
        SetupStep::Confirm,
    ];
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

fn worker_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    state
        .agents
        .iter()
        .enumerate()
        .map(|(idx, setup_agent)| {
            let agent = &setup_agent.agent;
            let checked = if state.selected_workers[idx] {
                "[x]"
            } else {
                "[ ]"
            };
            let status_style = if setup_agent_is_usable(setup_agent) {
                Style::default().fg(theme.primary)
            } else {
                Style::default().fg(theme.warning)
            };
            selectable_row(
                idx == state.cursor,
                vec![
                    Span::raw(format!("{checked} ")),
                    Span::styled(host_agent_label(agent), Style::default().fg(theme.text)),
                    Span::styled(format!("  {}", validation_label(setup_agent)), status_style),
                    Span::styled(
                        format!("  {}", command_label(agent)),
                        Style::default().fg(theme.muted),
                    ),
                ],
                theme,
            )
        })
        .collect()
}

fn persona_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    [
        (
            "Architect",
            "optimize for the best solution; compare two versions on complex work",
        ),
        (
            "Accountant",
            "optimize for cost; use cheaper models when the task is simple enough",
        ),
    ]
    .iter()
    .enumerate()
    .map(|(idx, (label, description))| {
        selectable_row(
            idx == state.cursor,
            vec![
                Span::styled((*label).to_string(), Style::default().fg(theme.text)),
                Span::styled(format!("  {description}"), Style::default().fg(theme.muted)),
            ],
            theme,
        )
    })
    .collect()
}

fn host_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    let enabled = state.enabled_source_ids();
    enabled
        .iter()
        .enumerate()
        .filter_map(|(idx, source_id)| {
            let agent = state
                .agents
                .iter()
                .find(|setup_agent| &setup_agent.agent.source_id == source_id)?
                .agent
                .clone();
            Some(selectable_row(
                idx == state.cursor,
                vec![
                    Span::styled(host_agent_label(&agent), Style::default().fg(theme.text)),
                    Span::styled(
                        format!("  {}", command_label(&agent)),
                        Style::default().fg(theme.muted),
                    ),
                ],
                theme,
            ))
        })
        .collect()
}

fn model_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    MODEL_OPTIONS
        .iter()
        .enumerate()
        .map(|(idx, option)| {
            selectable_row(
                idx == state.cursor,
                vec![
                    Span::styled(option.label.to_string(), Style::default().fg(theme.text)),
                    Span::styled(
                        format!("  {}", option.description),
                        Style::default().fg(theme.muted),
                    ),
                ],
                theme,
            )
        })
        .collect()
}

fn reasoning_rows(state: &ThorSetupState, theme: TerminalTheme) -> Vec<ListItem<'static>> {
    REASONING_OPTIONS
        .iter()
        .enumerate()
        .map(|(idx, (_, label, description))| {
            selectable_row(
                idx == state.cursor,
                vec![
                    Span::styled((*label).to_string(), Style::default().fg(theme.text)),
                    Span::styled(format!("  {description}"), Style::default().fg(theme.muted)),
                ],
                theme,
            )
        })
        .collect()
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

fn summary_lines(state: &ThorSetupState, theme: TerminalTheme) -> Vec<Line<'static>> {
    vec![
        detail_line("Workers", state.enabled_source_ids().join(", "), theme),
        detail_line("Persona", persona_label(state.optimization_mode), theme),
        detail_line("Thor host", state.host_source_id.clone(), theme),
        detail_line("Thor model", state.coordinator_model.clone(), theme),
        detail_line(
            "Reasoning",
            reasoning_label(state.coordinator_reasoning),
            theme,
        ),
    ]
}

fn detail_line(label: &str, value: String, theme: TerminalTheme) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label}: "), Style::default().fg(theme.muted)),
        Span::styled(value, Style::default().fg(theme.text)),
    ])
}

fn persona_mode_for_cursor(cursor: usize) -> ThorOptimizationMode {
    if cursor == 1 {
        ThorOptimizationMode::Cost
    } else {
        ThorOptimizationMode::BestSolution
    }
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

fn command_label(agent: &SelectedAgent) -> String {
    let mut parts = vec![agent.program.to_string_lossy().into_owned()];
    parts.extend(agent.args.iter().cloned());
    parts.join(" ")
}

fn setup_agent_is_usable(setup_agent: &ThorSetupAgent) -> bool {
    setup_agent
        .validation
        .as_ref()
        .map(|validation| validation.usable)
        .unwrap_or(true)
}

fn validation_label(setup_agent: &ThorSetupAgent) -> String {
    match &setup_agent.validation {
        Some(validation) if validation.usable => match (
            validation.agent_name.as_deref(),
            validation.agent_version.as_deref(),
        ) {
            (Some(name), Some(version)) => format!("ready: {name} {version}"),
            (Some(name), None) => format!("ready: {name}"),
            _ => "ready".to_string(),
        },
        Some(validation) => {
            let reason = validation
                .error
                .as_deref()
                .filter(|message| !message.trim().is_empty())
                .unwrap_or("ACP session did not start");
            format!("unavailable: {}", truncate_label(reason, 64))
        }
        None => "not checked".to_string(),
    }
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
                validation: None,
            })
            .collect()
    }

    fn setup_agent_with_validation(source_id: &str, usable: bool) -> ThorSetupAgent {
        ThorSetupAgent {
            agent: agent(source_id),
            validation: Some(AgentValidation {
                source_id: source_id.to_string(),
                usable,
                agent_name: None,
                agent_version: None,
                session_started: usable,
                config_advertised: false,
                prompt_images_supported: false,
                session_fork_supported: false,
                error: (!usable).then(|| "missing key".to_string()),
                elapsed_ms: 10,
                checked_at_unix: 1,
            }),
        }
    }

    #[test]
    fn worker_step_toggles_available_workers_but_keeps_one() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &raw_agents[0]);

        state.toggle_current_worker();
        assert_eq!(state.enabled_source_ids(), vec!["codex"]);

        state.move_selection(1);
        state.toggle_current_worker();
        assert_eq!(state.enabled_source_ids(), vec!["codex"]);
    }

    #[test]
    fn persona_step_maps_architect_and_accountant() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &raw_agents[0]);

        state.set_step(SetupStep::Persona);
        state.cursor = 0;
        state.advance();
        assert_eq!(state.optimization_mode, ThorOptimizationMode::BestSolution);

        state.set_step(SetupStep::Persona);
        state.cursor = 1;
        state.advance();
        assert_eq!(state.optimization_mode, ThorOptimizationMode::Cost);
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
        let mut state = ThorSetupState::new(&cfg, &agents, &raw_agents[1]);
        state.set_step(SetupStep::Confirm);

        let selection = state.advance().expect("selection");
        assert_eq!(selection.enabled_worker_source_ids, vec!["codex"]);
        assert_eq!(selection.host_source_id, "codex");
        assert_eq!(selection.optimization_mode, ThorOptimizationMode::Cost);
        assert_eq!(selection.coordinator_model, "gpt-strong");
        assert_eq!(selection.coordinator_reasoning, ThorReasoning::Medium);
    }

    #[test]
    fn enter_on_confirm_finishes() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &raw_agents[0]);
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
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &raw_agents[0]);
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
        let state = ThorSetupState::new(&ThorConfig::default(), &agents, &initial_host);

        assert_eq!(state.enabled_source_ids(), vec!["codex"]);
        assert_eq!(state.host_source_id, "codex");
    }

    #[test]
    fn toggling_unusable_worker_is_ignored_when_usable_worker_exists() {
        let agents = vec![
            setup_agent_with_validation("claude", false),
            setup_agent_with_validation("codex", true),
        ];
        let initial_host = agents[1].agent.clone();
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &initial_host);

        state.cursor = 0;
        state.toggle_current_worker();

        assert_eq!(state.enabled_source_ids(), vec!["codex"]);
    }
}
