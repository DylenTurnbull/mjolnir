//! Shared first-startup and in-session settings editor.

use std::collections::HashSet;

use crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::config::Config;
use crate::config::ModelsConfig;
use crate::council::ModelChoice;
use crate::palette::TerminalTheme;
use crate::spinner::SpinnerStyle;
use crate::theme::TerminalThemeKind;

pub const ROLE_DESCRIPTIONS: [(&str, &str); 3] = [
    ("Thor", "primary model; plans and reviews work"),
    (
        "Eitri",
        "fast/cheap model; handles delegated implementation and exploration",
    ),
    ("Loki", "secondary model; advises Thor and Eitri"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsTab {
    Council,
    AcpServers,
    Appearance,
}

impl SettingsTab {
    const ALL: [Self; 3] = [Self::Council, Self::AcpServers, Self::Appearance];

    fn label(self) -> &'static str {
        match self {
            Self::Council => "Council",
            Self::AcpServers => "ACP Servers",
            Self::Appearance => "Appearance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsAction {
    None,
    Changed,
    Save,
    Cancel,
}

#[derive(Debug, Clone)]
pub struct SettingsEditor {
    pub config: Config,
    pub tab: SettingsTab,
    pub selected: usize,
    pub notice: Option<String>,
    choices: Vec<ModelChoice>,
    active_models: Option<ModelsConfig>,
}

impl SettingsEditor {
    pub fn new(config: Config, choices: Vec<ModelChoice>, notice: Option<String>) -> Self {
        Self {
            config,
            tab: SettingsTab::Council,
            selected: 0,
            notice,
            choices,
            active_models: None,
        }
    }

    pub fn with_active_models(mut self, active_models: ModelsConfig) -> Self {
        self.active_models = Some(active_models);
        self
    }

    pub fn handle_key(&mut self, code: KeyCode) -> SettingsAction {
        match code {
            KeyCode::Esc => SettingsAction::Cancel,
            KeyCode::Enter => SettingsAction::Save,
            KeyCode::Tab => {
                self.change_tab(1);
                SettingsAction::None
            }
            KeyCode::BackTab => {
                self.change_tab(-1);
                SettingsAction::None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_selection(-1);
                SettingsAction::None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_selection(1);
                SettingsAction::None
            }
            KeyCode::Left | KeyCode::Char('h') => self.change_selected(-1),
            KeyCode::Right | KeyCode::Char('l') => self.change_selected(1),
            KeyCode::Char(' ') => self.toggle_selected(),
            _ => SettingsAction::None,
        }
    }

    fn change_tab(&mut self, delta: i32) {
        let current = SettingsTab::ALL
            .iter()
            .position(|tab| *tab == self.tab)
            .unwrap_or(0);
        let next = (current as i32 + delta).rem_euclid(SettingsTab::ALL.len() as i32) as usize;
        self.tab = SettingsTab::ALL[next];
        self.selected = 0;
        self.notice = None;
    }

    fn row_count(&self) -> usize {
        match self.tab {
            SettingsTab::Council => 4,
            SettingsTab::AcpServers => self.config.acp_server_selections().len(),
            SettingsTab::Appearance => 2,
        }
    }

    fn move_selection(&mut self, delta: i32) {
        let len = self.row_count();
        if len > 0 {
            self.selected = (self.selected as i32 + delta).rem_euclid(len as i32) as usize;
        }
    }

    fn change_selected(&mut self, delta: i32) -> SettingsAction {
        match self.tab {
            SettingsTab::Council if self.selected < 3 => self.cycle_model(self.selected, delta),
            SettingsTab::Appearance if self.selected == 0 => {
                let current = TerminalThemeKind::ALL
                    .iter()
                    .position(|kind| *kind == self.config.theme)
                    .unwrap_or(0);
                let next = (current as i32 + delta).rem_euclid(TerminalThemeKind::ALL.len() as i32)
                    as usize;
                self.config.theme = TerminalThemeKind::ALL[next];
            }
            SettingsTab::Appearance if self.selected == 1 => {
                let current = SpinnerStyle::ALL
                    .iter()
                    .position(|style| *style == self.config.spinner)
                    .unwrap_or(0);
                let next =
                    (current as i32 + delta).rem_euclid(SpinnerStyle::ALL.len() as i32) as usize;
                self.config.spinner = SpinnerStyle::ALL[next];
            }
            _ => return SettingsAction::None,
        }
        self.notice = None;
        SettingsAction::Changed
    }

    fn toggle_selected(&mut self) -> SettingsAction {
        match self.tab {
            SettingsTab::Council if self.selected == 3 => {
                self.config.thor.discrete_review = !self.config.thor.discrete_review;
            }
            SettingsTab::AcpServers => {
                let servers = self.config.acp_server_selections();
                let Some(server) = servers.get(self.selected) else {
                    return SettingsAction::None;
                };
                let id = server.id.clone();
                let enabled = !server.enabled;
                self.config.set_acp_server_enabled(&id, enabled);
            }
            _ => return SettingsAction::None,
        }
        self.notice = None;
        SettingsAction::Changed
    }

    fn cycle_model(&mut self, role: usize, delta: i32) {
        let choices = self.model_choices(role);
        let current = match role {
            0 => &self.config.thor.model,
            1 => &self.config.eitri.model,
            2 => &self.config.loki.model,
            _ => return,
        };
        let index = choices
            .iter()
            .position(|choice| choice == current)
            .unwrap_or(0);
        let next = (index as i32 + delta).rem_euclid(choices.len() as i32) as usize;
        match role {
            0 => self.config.thor.model.clone_from(&choices[next]),
            1 => self.config.eitri.model.clone_from(&choices[next]),
            2 => self.config.loki.model.clone_from(&choices[next]),
            _ => {}
        }
    }

    fn model_choices(&self, role: usize) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut choices = vec!["auto".to_string()];
        seen.insert("auto".to_string());
        if role != 0 {
            choices.push(crate::config::DISABLED_MODEL.to_string());
            seen.insert(crate::config::DISABLED_MODEL.to_string());
        }
        for choice in self.choices.iter().filter(|choice| choice.available) {
            if seen.insert(choice.model.clone()) {
                choices.push(choice.model.clone());
            }
        }
        choices
    }

    fn staged_model_detail(&self, model: &str) -> String {
        if model == "auto" {
            return "automatic selection".to_string();
        }
        if model == crate::config::DISABLED_MODEL {
            return "role disabled".to_string();
        }
        let Some(choice) = self.choices.iter().find(|choice| choice.model == model) else {
            return "saved model; not reported this session".to_string();
        };
        if !choice.available {
            return format!(
                "unavailable: {}",
                choice
                    .disabled_reason
                    .as_deref()
                    .unwrap_or("no launchable ACP route")
            );
        }
        let adapter = choice.adapter.as_deref().unwrap_or("adapter unknown");
        if choice.ranked {
            format!(
                "{adapter}; Pass@1 {:.1}%; ${:.2}",
                choice.pass_at_1 * 100.0,
                choice.mean_cost_usd
            )
        } else {
            format!("{adapter}; unranked")
        }
    }

    fn active_model_detail(&self, role: usize) -> String {
        let Some(models) = self.active_models.as_ref() else {
            return "not running".to_string();
        };
        let model = match role {
            0 => &models.thor,
            1 => &models.eitri,
            _ => &models.loki,
        };
        let adapter = self
            .choices
            .iter()
            .find(|choice| choice.available && choice.model == *model)
            .and_then(|choice| choice.adapter.as_deref());
        adapter.map_or_else(|| model.clone(), |adapter| format!("{model} via {adapter}"))
    }

    fn server_status(&self, id: &str, enabled: bool) -> String {
        if !enabled {
            return "disabled".to_string();
        }
        let available = self
            .choices
            .iter()
            .filter(|choice| choice.available && choice.adapter.as_deref() == Some(id))
            .count();
        if available > 0 {
            return format!(
                "ready; {available} model{}",
                if available == 1 { "" } else { "s" }
            );
        }
        "not detected or no models reported".to_string()
    }
}

pub fn draw_settings_panel(
    frame: &mut ratatui::Frame,
    area: Rect,
    editor: &SettingsEditor,
    title: &str,
) {
    if area.width < 28 || area.height < 12 {
        return;
    }
    let theme = editor.config.theme.palette();
    let rect = crate::term::centered_rect(area, 90, 24);
    frame.render_widget(Clear, rect);
    let block = Block::default()
        .title(format!(" {title} "))
        .borders(Borders::ALL)
        .style(Style::default().fg(theme.text));
    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(6),
            Constraint::Length(if editor.notice.is_some() { 2 } else { 0 }),
            Constraint::Length(1),
        ])
        .split(inner);
    draw_tabs(frame, rows[0], editor, theme);
    match editor.tab {
        SettingsTab::Council => draw_council(frame, rows[1], editor, theme),
        SettingsTab::AcpServers => draw_servers(frame, rows[1], editor, theme),
        SettingsTab::Appearance => draw_appearance(frame, rows[1], editor, theme),
    }
    if let Some(notice) = &editor.notice {
        frame.render_widget(
            Paragraph::new(notice.as_str())
                .style(Style::default().fg(theme.error))
                .wrap(Wrap { trim: false }),
            rows[2],
        );
    }
    frame.render_widget(
        Paragraph::new(
            "Tab view · ↑/↓ select · ←/→ change · Space toggle · Enter save · Esc cancel",
        )
        .style(Style::default().fg(theme.muted)),
        rows[3],
    );
}

fn draw_tabs(
    frame: &mut ratatui::Frame,
    area: Rect,
    editor: &SettingsEditor,
    theme: TerminalTheme,
) {
    let tabs = SettingsTab::ALL.into_iter().flat_map(|tab| {
        let active = tab == editor.tab;
        let style = if active {
            Style::default()
                .fg(theme.selection_fg)
                .bg(theme.selection_bg)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.muted)
        };
        [
            Span::styled(format!(" {} ", tab.label()), style),
            Span::raw("  "),
        ]
    });
    frame.render_widget(Paragraph::new(Line::from(tabs.collect::<Vec<_>>())), area);
}

fn draw_council(
    frame: &mut ratatui::Frame,
    area: Rect,
    editor: &SettingsEditor,
    theme: TerminalTheme,
) {
    let mut lines = Vec::new();
    for (index, (role, description)) in ROLE_DESCRIPTIONS.iter().enumerate() {
        let model = match index {
            0 => &editor.config.thor.model,
            1 => &editor.config.eitri.model,
            _ => &editor.config.loki.model,
        };
        lines.push(selected_line(
            editor.selected == index,
            format!("{role:<6} < {model} >"),
            theme,
        ));
        lines.push(Line::from(vec![
            Span::raw("         "),
            Span::styled(*description, Style::default().fg(theme.muted)),
        ]));
        lines.push(Line::from(Span::styled(
            format!(
                "         saved: {} · active: {}",
                editor.staged_model_detail(model),
                editor.active_model_detail(index)
            ),
            Style::default().fg(theme.muted),
        )));
    }
    lines.push(Line::raw(""));
    lines.push(selected_line(
        editor.selected == 3,
        format!(
            "Thor review     [{}]",
            on_off(editor.config.thor.discrete_review)
        ),
        theme,
    ));
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
}

fn draw_servers(
    frame: &mut ratatui::Frame,
    area: Rect,
    editor: &SettingsEditor,
    theme: TerminalTheme,
) {
    let mut lines = vec![
        Line::styled(
            "Detected and configured ACP servers. Changes apply next session.",
            Style::default().fg(theme.muted),
        ),
        Line::raw(""),
    ];
    for (index, server) in editor.config.acp_server_selections().iter().enumerate() {
        lines.push(selected_line(
            editor.selected == index,
            format!(
                "[{}] {:<16} {}",
                on_off(server.enabled),
                server.label,
                editor.server_status(&server.id, server.enabled)
            ),
            theme,
        ));
    }
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
}

fn draw_appearance(
    frame: &mut ratatui::Frame,
    area: Rect,
    editor: &SettingsEditor,
    theme: TerminalTheme,
) {
    let lines = vec![
        Line::styled(
            "Appearance changes preview immediately.",
            Style::default().fg(theme.muted),
        ),
        Line::raw(""),
        selected_line(
            editor.selected == 0,
            format!("Theme       < {} >", editor.config.theme),
            theme,
        ),
        selected_line(
            editor.selected == 1,
            format!(
                "Spinner     < {} {} >",
                editor.config.spinner,
                editor.config.spinner.current_frame()
            ),
            theme,
        ),
    ];
    frame.render_widget(Paragraph::new(lines), area);
}

fn selected_line(selected: bool, text: String, theme: TerminalTheme) -> Line<'static> {
    let style = if selected {
        Style::default()
            .fg(theme.selection_fg)
            .bg(theme.selection_bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(theme.text)
    };
    Line::from(Span::styled(
        format!("{} {text}", if selected { ">" } else { " " }),
        style,
    ))
}

fn on_off(enabled: bool) -> &'static str {
    if enabled { "on" } else { "off" }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn tabs_share_one_editable_config() {
        let mut editor = SettingsEditor::new(Config::default(), Vec::new(), None);
        editor.selected = 3;
        assert_eq!(
            editor.handle_key(KeyCode::Char(' ')),
            SettingsAction::Changed
        );
        assert!(!editor.config.thor.discrete_review);
        editor.handle_key(KeyCode::Tab);
        assert_eq!(editor.tab, SettingsTab::AcpServers);
        assert_eq!(
            editor.handle_key(KeyCode::Char(' ')),
            SettingsAction::Changed
        );
        assert!(!editor.config.acp.codex);
    }

    #[test]
    fn disabled_is_only_available_for_optional_roles() {
        let editor = SettingsEditor::new(Config::default(), Vec::new(), None);
        assert!(
            !editor
                .model_choices(0)
                .iter()
                .any(|choice| choice == "disabled")
        );
        assert!(
            editor
                .model_choices(1)
                .iter()
                .any(|choice| choice == "disabled")
        );
        assert!(
            editor
                .model_choices(2)
                .iter()
                .any(|choice| choice == "disabled")
        );
    }

    #[test]
    fn optional_role_model_selection_can_disable_both_roles() {
        let mut editor = SettingsEditor::new(Config::default(), Vec::new(), None);
        editor.selected = 1;
        assert_eq!(editor.handle_key(KeyCode::Right), SettingsAction::Changed);
        assert_eq!(editor.config.eitri.model, crate::config::DISABLED_MODEL);

        editor.selected = 2;
        assert_eq!(editor.handle_key(KeyCode::Right), SettingsAction::Changed);
        assert_eq!(editor.config.loki.model, crate::config::DISABLED_MODEL);
    }
}
