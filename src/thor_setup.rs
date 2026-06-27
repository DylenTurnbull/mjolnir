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
    pub quota_backend: ThorQuotaBackend,
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
    Host,
    Confirm,
}

impl SetupStep {
    fn title(self) -> &'static str {
        match self {
            Self::Host => "choose Thor",
            Self::Confirm => "start",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Host => 0,
            Self::Confirm => 1,
        }
    }
}

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
                name: "Anvil".to_string(),
                description: "Brokk ACP server via uvx".to_string(),
                quota_backend: ThorQuotaBackend::None,
                validation: None,
            }]
        } else {
            agents.to_vec()
        };
        let has_usable_agent = agents.iter().any(setup_agent_is_usable);
        let selected_workers = agents
            .iter()
            .map(|setup_agent| !has_usable_agent || setup_agent_is_usable(setup_agent))
            .collect::<Vec<_>>();

        let host_source_id = if agents.iter().any(|setup_agent| {
            setup_agent.agent.source_id == initial_host.source_id
                && (!has_usable_agent || setup_agent_is_usable(setup_agent))
        }) {
            initial_host.source_id.clone()
        } else {
            agents
                .iter()
                .find(|setup_agent| !has_usable_agent || setup_agent_is_usable(setup_agent))
                .map(|setup_agent| setup_agent.agent.source_id.clone())
                .unwrap_or_else(|| agents[0].agent.source_id.clone())
        };
        let optimization_mode = match thor_config.optimization_mode {
            ThorOptimizationMode::Cost => ThorOptimizationMode::Cost,
            _ => ThorOptimizationMode::BestSolution,
        };

        let mut state = Self {
            agents,
            step: SetupStep::Host,
            cursor: 0,
            selected_workers,
            optimization_mode,
            host_source_id,
            coordinator_model: thor_config.coordinator_model.clone(),
            coordinator_reasoning: thor_config.coordinator_reasoning,
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

    fn advance(&mut self) -> Option<ThorSetupSelection> {
        match self.step {
            SetupStep::Host => {
                if let Some(source_id) = self.enabled_source_ids().get(self.cursor) {
                    self.host_source_id = source_id.clone();
                }
                self.set_step(SetupStep::Confirm);
            }
            SetupStep::Confirm => return Some(self.selection()),
        }
        None
    }

    fn back(&mut self) {
        match self.step {
            SetupStep::Host => {}
            SetupStep::Confirm => self.set_step(SetupStep::Host),
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
            SetupStep::Host => self
                .enabled_source_ids()
                .iter()
                .position(|source_id| source_id == &self.host_source_id)
                .unwrap_or(0),
            SetupStep::Confirm => 0,
        }
    }

    fn current_len(&self) -> usize {
        match self.step {
            SetupStep::Host => self.enabled_source_ids().len(),
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
        KeyCode::Char(' ') => None,
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
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Min(10),
            Constraint::Length(4),
            Constraint::Length(1),
        ])
        .split(inner);

    let title = Paragraph::new(vec![
        Line::from(vec![Span::styled(
            "Thor coordinates your coding agents.",
            Style::default()
                .fg(theme.primary)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from("Choose where Thor runs. The other defaults are ready to change later."),
        Line::from("Configured agents that work are available for Thor to delegate to."),
    ])
    .style(Style::default().fg(theme.text));
    f.render_widget(title, layout[0]);

    f.render_widget(progress_line(state, theme), layout[1]);

    let content = match state.step {
        SetupStep::Host => host_rows(state, theme),
        SetupStep::Confirm => confirm_rows(state, theme),
    };
    let content = visible_rows(content, state.cursor, layout[2].height as usize);
    f.render_widget(List::new(content), layout[2]);

    let summary = Paragraph::new(summary_lines(state, theme))
        .style(Style::default().fg(theme.text))
        .wrap(Wrap { trim: true });
    f.render_widget(summary, layout[3]);

    let footer_text = match state.step {
        SetupStep::Confirm => "Enter starts Thor  |  Backspace edits  |  Esc quits",
        _ => "Enter selects  |  Backspace edits  |  Esc quits",
    };
    let footer = Paragraph::new(footer_text).style(Style::default().fg(theme.muted));
    f.render_widget(footer, layout[4]);
}

fn progress_line(state: &ThorSetupState, theme: TerminalTheme) -> Paragraph<'static> {
    let steps = [SetupStep::Host, SetupStep::Confirm];
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
    let enabled = state.enabled_source_ids();
    enabled
        .iter()
        .enumerate()
        .filter_map(|(idx, source_id)| {
            let setup_agent = state
                .agents
                .iter()
                .find(|setup_agent| &setup_agent.agent.source_id == source_id)?;
            Some(selectable_row(
                idx == state.cursor,
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
            ))
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
        detail_line(
            "Available agents",
            state.enabled_source_ids().join(", "),
            theme,
        ),
        detail_line("Thor runs in", state.host_source_id.clone(), theme),
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
    if !setup_agent.description.trim().is_empty() {
        truncate_label(&setup_agent.description, 72)
    } else {
        command_label(&setup_agent.agent)
    }
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
        return format!("install {}", setup_agent.agent.program.to_string_lossy());
    }
    if lower.contains("auth")
        || lower.contains("login")
        || lower.contains("api key")
        || lower.contains("missing key")
        || lower.contains("token")
        || lower.contains("credential")
    {
        return "sign in or add key".to_string();
    }
    format!("setup needed: {}", truncate_label(error, 48))
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
                name: String::new(),
                description: String::new(),
                quota_backend: ThorQuotaBackend::None,
                validation: None,
            })
            .collect()
    }

    fn setup_agent_with_validation(source_id: &str, usable: bool) -> ThorSetupAgent {
        ThorSetupAgent {
            agent: agent(source_id),
            name: source_id.to_string(),
            description: String::new(),
            quota_backend: ThorQuotaBackend::None,
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
    fn available_agents_are_enabled_without_worker_step() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let state = ThorSetupState::new(&ThorConfig::default(), &agents, &raw_agents[0]);

        assert_eq!(state.step, SetupStep::Host);
        assert_eq!(state.enabled_source_ids(), vec!["claude", "codex"]);
    }

    #[test]
    fn host_selection_advances_directly_to_confirm() {
        let raw_agents = vec![agent("claude"), agent("codex")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &raw_agents[0]);

        state.cursor = 1;
        state.advance();

        assert_eq!(state.step, SetupStep::Confirm);
        assert_eq!(state.host_source_id, "codex");
    }

    #[test]
    fn back_from_confirm_returns_to_host_selection() {
        let raw_agents = vec![agent("anvil")];
        let agents = setup_agents(&raw_agents);
        let mut state = ThorSetupState::new(&ThorConfig::default(), &agents, &raw_agents[0]);

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
        let mut state = ThorSetupState::new(&cfg, &agents, &raw_agents[1]);
        state.set_step(SetupStep::Confirm);

        let selection = state.advance().expect("selection");
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
    fn validation_error_gives_actionable_sign_in_guidance() {
        let setup_agent = setup_agent_with_validation("claude", false);

        assert_eq!(host_status_label(&setup_agent), "sign in or add key");
    }

    #[test]
    fn visible_rows_follow_cursor_for_long_lists() {
        let rows = (0..10)
            .map(|idx| ListItem::new(Line::from(format!("row {idx}"))))
            .collect::<Vec<_>>();

        let window = visible_rows(rows, 8, 4);

        assert_eq!(window.len(), 4);
    }
}
