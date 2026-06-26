//! First-run Thor setup introduction.

use std::io::Stdout;
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{Event as CtEvent, EventStream, KeyCode, KeyEventKind};
use futures::StreamExt;
use ratatui::Terminal;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::config::SelectedAgent;
use crate::palette::TerminalTheme;
use crate::term::TrackedBackend;
use crate::thor::{self, ThorConfig, ThorOptimizationMode, ThorPlanApproval};

/// Run the Thor intro until the user continues or cancels with Esc/Ctrl-C.
pub async fn run_thor_setup(
    terminal: &mut Terminal<TrackedBackend<Stdout>>,
    theme: TerminalTheme,
    thor_config: &ThorConfig,
    host_agent: &SelectedAgent,
) -> Result<Option<()>> {
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));

    terminal.draw(|f| draw(f, theme, thor_config, host_agent))?;

    loop {
        tokio::select! {
            biased;
            maybe_ev = events.next() => {
                let Some(ev) = maybe_ev else {
                    return Ok(None);
                };
                let ev = ev.context("crossterm event stream")?;
                if let Some(outcome) = handle_event(ev) {
                    return Ok(outcome);
                }
            }
            _ = tick.tick() => {}
        }
        terminal.draw(|f| draw(f, theme, thor_config, host_agent))?;
    }
}

fn handle_event(ev: CtEvent) -> Option<Option<()>> {
    let CtEvent::Key(key) = ev else {
        return None;
    };
    if key.kind != KeyEventKind::Press {
        return None;
    }

    match key.code {
        KeyCode::Esc => Some(None),
        KeyCode::Char('c') if key.modifiers == crossterm::event::KeyModifiers::CONTROL => {
            Some(None)
        }
        KeyCode::Enter => Some(Some(())),
        _ => None,
    }
}

fn draw(
    f: &mut ratatui::Frame,
    theme: TerminalTheme,
    thor_config: &ThorConfig,
    host_agent: &SelectedAgent,
) {
    let area = crate::term::centered_rect(f.area(), 76, 16);
    let block = Block::default()
        .title(" First-run setup: Thor ")
        .borders(Borders::ALL)
        .style(Style::default().fg(theme.text));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(7),
            Constraint::Length(1),
        ])
        .split(inner);

    let intro = Paragraph::new(vec![
        Line::from(vec![Span::styled(
            "Thor is the only mj path.",
            Style::default()
                .fg(theme.primary)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from("Your prompt goes to a Thor coordinator running inside an ACP host agent."),
        Line::from("Thor uses the MCP bridge to delegate work to configured ACP workers."),
    ])
    .style(Style::default().fg(theme.text));
    f.render_widget(intro, layout[0]);

    let details = Paragraph::new(vec![
        detail_line("Host backend", host_agent_label(host_agent), theme),
        detail_line(
            "Coordinator model",
            thor_config.coordinator_model.clone(),
            theme,
        ),
        detail_line(
            "Optimization",
            optimization_label(thor_config.optimization_mode).to_string(),
            theme,
        ),
        detail_line(
            "Plan approval",
            plan_approval_label(thor_config.plan_approval).to_string(),
            theme,
        ),
        detail_line(
            "Worker bridge",
            format!(
                "stdio MCP server `{}` via `mj thor-mcp`",
                thor::THOR_MCP_SERVER_NAME
            ),
            theme,
        ),
    ]);
    f.render_widget(details, layout[1]);

    let footer = Paragraph::new("Enter to continue  |  Esc to cancel")
        .style(Style::default().fg(theme.muted));
    f.render_widget(footer, layout[2]);
}

fn detail_line(label: &str, value: String, theme: TerminalTheme) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label}: "), Style::default().fg(theme.muted)),
        Span::styled(value, Style::default().fg(theme.text)),
    ])
}

fn host_agent_label(agent: &SelectedAgent) -> String {
    if agent.args.is_empty() {
        format!("{} ({})", agent.source_id, agent.program.display())
    } else {
        format!(
            "{} ({} {})",
            agent.source_id,
            agent.program.display(),
            agent.args.join(" ")
        )
    }
}

fn optimization_label(mode: ThorOptimizationMode) -> &'static str {
    match mode {
        ThorOptimizationMode::Balanced => "balanced",
        ThorOptimizationMode::Cost => "cost/accountant",
        ThorOptimizationMode::BestSolution => "best solution/architect",
    }
}

fn plan_approval_label(mode: ThorPlanApproval) -> &'static str {
    match mode {
        ThorPlanApproval::Always => "show plan before execution",
        ThorPlanApproval::AskToSkip => "ask before skipping plan display",
        ThorPlanApproval::Never => "execute without plan approval",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyEvent, KeyModifiers};

    #[test]
    fn enter_continues() {
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
        assert_eq!(handle_event(CtEvent::Key(key)), Some(Some(())));
    }

    #[test]
    fn escape_cancels() {
        let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        assert_eq!(handle_event(CtEvent::Key(key)), Some(None));
    }

    #[test]
    fn host_agent_label_includes_program_and_args() {
        let agent = SelectedAgent {
            source_id: "anvil".to_string(),
            program: "uvx".into(),
            args: vec!["brokk".to_string(), "acp".to_string()],
            env: Default::default(),
        };

        assert_eq!(host_agent_label(&agent), "anvil (uvx brokk acp)");
    }
}
