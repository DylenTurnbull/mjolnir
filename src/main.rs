//! mjolnir: an interactive terminal client for any ACP-speaking agent.
//!
//! Starts Thor-backed sessions with the configured ACP backend, defaulting to
//! Anvil when no backend exists yet. It persists global preferences to
//! `~/.config/mj/config.toml`, then spawns the backend as a child process and
//! renders the session in a ratatui chat UI.

mod acp;
mod app;
mod clipboard;
mod config;
mod event;
mod headless;
mod install;
mod notifications;
mod palette;
mod paths;
mod registry;
mod remote;
mod self_update;
mod session;
mod speech;
mod spinner;
mod spinner_picker;
mod term;
mod text;
mod theme;
mod theme_picker;
mod thor;
mod thor_catalog;
mod thor_mcp;
mod thor_probe;
mod thor_setup;
mod ui;
mod version;
mod worktree;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::app::UiExitReason;
use crate::config::{
    Config, ConfiguredAcpServer, SelectedAgent, ThorQuotaBackend, history_path,
    transcript_export_dir,
};
use crate::event::{LoadSessionResult, UiCommand};
use crate::session::SessionEntryJson;
use crate::ui::{HeaderLabels, UiMode};
use crate::worktree::CreatedWorktree;

#[derive(Debug, Parser)]
#[command(name = "mj", version, about = "Interactive ACP chat TUI")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Run one prompt non-interactively and print the result.
    ///
    /// Matches Claude Code's `--print`/`-p` shape where practical: provide
    /// the prompt as the optional value, or omit the value/read `-` to read
    /// stdin. Headless mode uses the configured agent from
    /// `~/.config/mj/config.toml`; it does not open the interactive picker.
    #[arg(short = 'p', long = "print", value_name = "PROMPT", num_args = 0..=1, default_missing_value = "-")]
    print: Option<String>,

    /// Output format for `--print`.
    #[arg(long, value_enum, default_value_t = HeadlessOutputFormat::Text)]
    output_format: HeadlessOutputFormat,

    /// Permission handling for `--print`.
    ///
    /// `default` rejects permission prompts so headless runs never hang.
    /// `acceptEdits` accepts edit/delete/move prompts but rejects shell
    /// execution. `bypassPermissions` accepts every permission prompt.
    #[arg(long, value_enum, default_value_t = HeadlessPermissionMode::Default)]
    permission_mode: HeadlessPermissionMode,

    /// Working directory used when opening a new session. Defaults to
    /// the current directory.
    #[arg(long)]
    cwd: Option<PathBuf>,

    /// Additional absolute workspace directory to expose to the agent.
    ///
    /// Repeat to pass multiple directories. These expand workspace scope
    /// for ACP file and terminal requests but do not imply trust.
    #[arg(
        long = "additional-directory",
        visible_alias = "add-dir",
        value_name = "PATH"
    )]
    additional_directories: Vec<PathBuf>,

    /// Use the legacy alternate-screen full-screen chat TUI.
    #[arg(long)]
    fullscreen_tui: bool,

    /// Resume an existing ACP session in headless mode instead of
    /// opening a new one.
    #[arg(long, hide = true)]
    resume_session: Option<String>,

    /// Path to a log file. When unset, logging is disabled because the
    /// TUI owns the terminal and stderr would corrupt the screen.
    #[arg(long = "debug-file", visible_alias = "log-file", env = "BROKK_TUI_LOG")]
    log_file: Option<PathBuf>,

    /// Run the ACP session in a Git worktree.
    ///
    /// With no value, creates a new linked worktree under
    /// <project>/.mjolnir/worktrees/ with a random adjective-noun name
    /// (e.g. `bold-robin`). With a value, reuses an existing worktree
    /// by name (short name under .mjolnir/worktrees/) or by path.
    #[arg(short = 'w', long, num_args = 0..=1, default_missing_value = "")]
    worktree: Option<String>,

    /// Capture the agent subprocess's stderr to this file. When unset
    /// the agent's stderr is discarded via `Stdio::null()` (/dev/null on
    /// Unix, NUL on Windows) so it doesn't scribble over the TUI.
    #[arg(long, env = "BROKK_TUI_AGENT_STDERR")]
    agent_stderr: Option<PathBuf>,

    /// Maximum bytes for ACP filesystem text reads and writes.
    #[arg(
        long,
        global = true,
        env = "MJOLNIR_FS_MAX_TEXT_BYTES",
        default_value_t = acp::DEFAULT_FS_TEXT_BYTES,
        value_parser = parse_fs_max_text_bytes
    )]
    fs_max_text_bytes: u64,

    /// Skip the startup check for a newer mj release.
    #[arg(long, global = true, env = "MJOLNIR_NO_UPDATE_CHECK")]
    no_update_check: bool,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    #[command(hide = true)]
    ThorMcp,
    /// Resume an existing ACP session.
    ///
    /// Lists or loads sessions from the configured Thor backend. Without a
    /// session ID, opens an interactive session picker for that backend.
    ///
    /// Use `--list` to print sessions from the configured Thor backend
    /// in headless mode (no TUI).
    Resume(ResumeArgs),
    /// Start the local remote-control server.
    Server(ServerArgs),
}

fn parse_fs_max_text_bytes(value: &str) -> std::result::Result<u64, String> {
    let bytes = value
        .parse::<u64>()
        .map_err(|e| format!("invalid filesystem text byte limit: {e}"))?;
    if !(1..=acp::MAX_CONFIGURABLE_FS_TEXT_BYTES).contains(&bytes) {
        return Err(format!(
            "filesystem text byte limit must be between 1 and {}",
            acp::MAX_CONFIGURABLE_FS_TEXT_BYTES
        ));
    }
    Ok(bytes)
}

#[derive(Debug, clap::Args, Default)]
struct ServerArgs {
    /// Public hostname to embed in the login QR code and TLS certificate.
    #[arg(long)]
    hostname: Option<String>,
    /// Days of disconnected-session history to keep. Sessions (and their
    /// queued prompts) whose last update is older are deleted by the
    /// periodic sweeper. Pass 0 to keep history forever.
    #[arg(long, default_value_t = 30)]
    history_days: u32,
}

#[derive(Debug, clap::Args)]
struct ResumeArgs {
    /// Session ID to resume from the configured Thor backend. When omitted,
    /// opens an interactive picker that fetches that backend's session list.
    session_id: Option<String>,

    /// List available sessions and exit (headless, no TUI). Optionally
    /// filtered by `--cwd`.
    #[arg(short, long, conflicts_with = "session_id")]
    list: bool,

    /// Output format for `--list`.
    #[arg(long, value_enum, default_value_t = HeadlessOutputFormat::Text, requires = "list")]
    format: HeadlessOutputFormat,

    /// Working directory filter for `--list` and the resumed session.
    /// Defaults to the current directory.
    #[arg(long)]
    cwd: Option<PathBuf>,

    /// Additional absolute workspace directory to expose to the resumed agent.
    ///
    /// Repeat to pass multiple directories. These expand workspace scope
    /// for ACP file and terminal requests but do not imply trust.
    #[arg(
        long = "additional-directory",
        visible_alias = "add-dir",
        value_name = "PATH"
    )]
    additional_directories: Vec<PathBuf>,

    /// Run the resumed ACP session in a Git worktree.
    ///
    /// With no value, creates a new linked worktree under
    /// <project>/.mjolnir/worktrees/. With a value, reuses an existing
    /// worktree by name or by path.
    #[arg(short = 'w', long, num_args = 0..=1, default_missing_value = "")]
    worktree: Option<String>,

    /// Capture the agent subprocess's stderr to this file.
    #[arg(long, env = "BROKK_TUI_AGENT_STDERR")]
    agent_stderr: Option<PathBuf>,

    /// Use the legacy alternate-screen full-screen chat TUI.
    #[arg(long)]
    fullscreen_tui: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum HeadlessOutputFormat {
    Text,
    Json,
    StreamJson,
}

impl From<HeadlessOutputFormat> for headless::OutputFormat {
    fn from(value: HeadlessOutputFormat) -> Self {
        match value {
            HeadlessOutputFormat::Text => Self::Text,
            HeadlessOutputFormat::Json => Self::Json,
            HeadlessOutputFormat::StreamJson => Self::StreamJson,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum HeadlessPermissionMode {
    Default,
    #[value(name = "acceptEdits", alias = "accept-edits")]
    AcceptEdits,
    #[value(name = "bypassPermissions", alias = "bypass-permissions")]
    BypassPermissions,
}

impl From<HeadlessPermissionMode> for headless::PermissionMode {
    fn from(value: HeadlessPermissionMode) -> Self {
        match value {
            HeadlessPermissionMode::Default => Self::Default,
            HeadlessPermissionMode::AcceptEdits => Self::AcceptEdits,
            HeadlessPermissionMode::BypassPermissions => Self::BypassPermissions,
        }
    }
}

fn ui_mode(fullscreen_tui: bool) -> UiMode {
    if fullscreen_tui {
        UiMode::FullscreenTui
    } else {
        UiMode::InlineChat
    }
}

fn should_run_startup_update_check(cli: &Cli) -> bool {
    if cli.no_update_check || cli.print.is_some() {
        return false;
    }
    match &cli.command {
        Some(Commands::Resume(args)) => !args.list,
        Some(Commands::Server(_)) => false,
        Some(Commands::ThorMcp) => false,
        None => true,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(cli.log_file.as_deref())?;
    let fullscreen_tui = cli.fullscreen_tui;

    if should_run_startup_update_check(&cli)
        && let Err(e) = self_update::check_prompt_and_restart_if_accepted().await
    {
        tracing::warn!("startup update check failed: {e:#}");
    }

    let cwd = match cli.cwd.clone() {
        Some(p) => absolutize_cwd(p)?,
        None => std::env::current_dir().context("current dir")?,
    };

    // Dispatch to subcommand if provided.
    let fs_max_text_bytes = cli.fs_max_text_bytes;
    let top_level_additional_directories = cli.additional_directories.clone();

    if let Some(command) = cli.command {
        return match command {
            Commands::ThorMcp => thor_mcp::run_stdio().await,
            Commands::Resume(mut args) => {
                args.fullscreen_tui |= fullscreen_tui;
                run_resume(args, fs_max_text_bytes, top_level_additional_directories).await
            }
            Commands::Server(args) => {
                let workspace_roots =
                    validate_workspace_roots(&cwd, &top_level_additional_directories)?;
                remote::run_server(
                    args.hostname,
                    args.history_days,
                    cwd,
                    workspace_roots.additional_directories().to_vec(),
                    fs_max_text_bytes,
                )
                .await
            }
        };
    }

    if let Some(prompt_arg) = cli.print {
        let workspace_roots = validate_workspace_roots(&cwd, &top_level_additional_directories)?;
        let prompt = read_headless_prompt(prompt_arg)?;
        return headless::run(headless::RunConfig {
            prompt,
            cwd,
            additional_directories: workspace_roots.additional_directories().to_vec(),
            resume_session: cli.resume_session,
            agent_stderr: cli.agent_stderr,
            fs_max_text_bytes,
            output_format: cli.output_format.into(),
            permission_mode: cli.permission_mode.into(),
        })
        .await;
    }

    let (cwd, worktree) = prepare_worktree_for_arg(cwd, cli.worktree.as_deref())?;
    let workspace_roots = validate_workspace_roots(&cwd, &top_level_additional_directories)?;
    let worktree_label = worktree_label(worktree.as_ref());
    let project_label = project_label(&cwd);

    let result = run_app(
        cwd,
        RuntimeOptions {
            agent_stderr: cli.agent_stderr,
            additional_directories: workspace_roots.additional_directories().to_vec(),
            fs_max_text_bytes,
        },
        project_label,
        worktree_label.clone(),
        None,
        None,
        ui_mode(fullscreen_tui),
    )
    .await;

    let worktree_kept = handle_worktree_after_tui(worktree.as_ref(), Some(ui_mode(fullscreen_tui)));

    // Print resume hint so the user can come back to this session.
    match &result {
        Ok(Some(session_id)) => {
            if worktree_kept {
                print_resume_hint(
                    session_id,
                    worktree_label.as_deref(),
                    workspace_roots.additional_directories(),
                );
            }
        }
        Ok(None) => {}
        Err(_) => {}
    }

    result.map(|_| ())
}

/// Print a hint showing how to resume the session.
fn print_resume_hint(session_id: &str, worktree_label: Option<&str>, additional_roots: &[PathBuf]) {
    println!(
        "To resume: {}",
        resume_hint_command(session_id, worktree_label, additional_roots)
    );
}

fn resume_hint_command(
    session_id: &str,
    worktree_label: Option<&str>,
    additional_roots: &[PathBuf],
) -> String {
    let mut command = format!("mj resume {}", shell_quote(session_id));
    if let Some(label) = worktree_label {
        command.push_str(" --worktree ");
        command.push_str(&shell_quote(label));
    }
    for root in additional_roots {
        command.push_str(" --additional-directory ");
        command.push_str(&shell_quote(&root.display().to_string()));
    }
    command
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Handle the `mj resume` subcommand: pick the agent to resume from, list
/// sessions, pick one interactively, or resume directly by ID.
async fn run_resume(
    args: ResumeArgs,
    fs_max_text_bytes: u64,
    top_level_additional_directories: Vec<PathBuf>,
) -> Result<()> {
    let mode = ui_mode(args.fullscreen_tui);
    let cwd = match args.cwd.clone() {
        Some(p) => absolutize_cwd(p)?,
        None => std::env::current_dir().context("current dir")?,
    };
    let mut requested_additional_directories = top_level_additional_directories;
    requested_additional_directories.extend(args.additional_directories.iter().cloned());
    let (cwd, worktree) = prepare_worktree_for_arg(cwd, args.worktree.as_deref())?;
    let workspace_roots = validate_workspace_roots(&cwd, &requested_additional_directories)?;
    let additional_directories = workspace_roots.additional_directories().to_vec();
    let worktree_label = worktree_label(worktree.as_ref());
    let project_label = project_label(&cwd);

    // `--list`: headless listing, print and exit.
    if args.list {
        // Load the configured Thor backend, creating the default when needed.
        let config_path = config::default_config_path();
        let mut cfg = Config::load(&config_path)
            .with_context(|| format!("load {}", config_path.display()))?;
        let agent = ensure_thor_default_agent(&mut cfg);
        cfg.save(&config_path)
            .with_context(|| format!("save {}", config_path.display()))?;
        let sessions = session::list_sessions(&agent, cwd, args.agent_stderr.as_deref()).await?;
        match args.format {
            HeadlessOutputFormat::Json | HeadlessOutputFormat::StreamJson => {
                let json: Vec<SessionEntryJson> =
                    sessions.iter().map(SessionEntryJson::from).collect();
                println!("{}", serde_json::to_string_pretty(&json)?);
            }
            HeadlessOutputFormat::Text => {
                if sessions.is_empty() {
                    println!("no sessions found");
                } else {
                    for s in &sessions {
                        let title = s.title.as_deref().unwrap_or("(untitled)");
                        let cwd_str = s.cwd.display();
                        let updated = s.updated_at.as_deref().unwrap_or("");
                        println!("{}  {}  {}  {}", s.session_id, title, cwd_str, updated);
                    }
                }
            }
        }
        if worktree.as_ref().is_some_and(|w| w.was_created) {
            let _ = handle_worktree_after_tui(worktree.as_ref(), None);
        }
        return Ok(());
    }

    let agent = pick_agent_for_resume().await;
    let Some(agent) = agent? else {
        eprintln!("Cancelled.");
        let _ = handle_worktree_after_tui(worktree.as_ref(), Some(mode));
        return Ok(());
    };

    // Direct ID: launch the TUI with the configured Thor backend and session.
    if let Some(session_id) = args.session_id.clone() {
        // Look up the chosen session's title so the resumed header shows it
        // immediately rather than waiting for the agent's first
        // SessionInfoUpdate. A failed lookup is non-fatal — resume proceeds
        // with no title and the agent fills it in shortly after.
        let title =
            match session::list_sessions(&agent, cwd.clone(), args.agent_stderr.as_deref()).await {
                Ok(sessions) => sessions
                    .into_iter()
                    .find(|entry| entry.session_id == session_id)
                    .and_then(|entry| entry.title),
                Err(e) => {
                    tracing::warn!("list sessions for title lookup failed: {e:#}");
                    None
                }
            };
        let result = run_app(
            cwd,
            RuntimeOptions {
                agent_stderr: args.agent_stderr.clone(),
                additional_directories: additional_directories.clone(),
                fs_max_text_bytes,
            },
            project_label,
            worktree_label.clone(),
            Some(ResumeTarget {
                session_id: session_id.clone(),
                title,
            }),
            Some(agent),
            mode,
        )
        .await;
        let worktree_kept = handle_worktree_after_tui(worktree.as_ref(), Some(mode));
        // Show resume hint for the session we just ran
        if let Ok(Some(resumed_id)) = &result
            && worktree_kept
        {
            print_resume_hint(
                resumed_id,
                worktree_label.as_deref(),
                workspace_roots.additional_directories(),
            );
        }
        return result.map(|_| ());
    }

    let mut notice = None;
    loop {
        // Fetch sessions from the configured Thor backend first (the backend
        // process is killed after listing), then set up the TUI to show the
        // session picker and launch the chosen session with a fresh process.
        eprintln!("Fetching sessions from agent...");
        let listing = session::list_sessions_with_capabilities(
            &agent,
            cwd.clone(),
            args.agent_stderr.as_deref(),
        )
        .await?;
        if listing.sessions.is_empty() {
            eprintln!("No sessions available.");
            let _ = handle_worktree_after_tui(worktree.as_ref(), Some(mode));
            return Ok(());
        }

        let outcome = run_session_picker_once(
            listing.sessions,
            listing.delete_supported,
            notice.take(),
            Config::load(&config::default_config_path())
                .map(|cfg| cfg.theme.palette())
                .unwrap_or_else(|_| theme::TerminalThemeKind::default().palette()),
        )
        .await?;
        match outcome {
            session::ResumeOutcome::Cancelled => {
                eprintln!("Cancelled.");
                let _ = handle_worktree_after_tui(worktree.as_ref(), Some(mode));
                return Ok(());
            }
            session::ResumeOutcome::DeleteRequested(entry) => {
                notice =
                    Some(delete_session_notice(&agent, entry, args.agent_stderr.as_deref()).await);
            }
            session::ResumeOutcome::Selected(entry) => {
                eprintln!("Resuming session: {}", entry.session_id);
                let session_title = entry.title.clone();
                let result = run_app(
                    cwd,
                    RuntimeOptions {
                        agent_stderr: args.agent_stderr,
                        additional_directories: additional_directories.clone(),
                        fs_max_text_bytes,
                    },
                    project_label,
                    worktree_label.clone(),
                    Some(ResumeTarget {
                        session_id: entry.session_id,
                        title: session_title,
                    }),
                    Some(agent),
                    mode,
                )
                .await;
                let worktree_kept = handle_worktree_after_tui(worktree.as_ref(), Some(mode));
                // Show resume hint for the session we just ran
                if let Ok(Some(resumed_id)) = &result
                    && worktree_kept
                {
                    print_resume_hint(
                        resumed_id,
                        worktree_label.as_deref(),
                        workspace_roots.additional_directories(),
                    );
                }
                return result.map(|_| ());
            }
        }
    }
}

async fn pick_agent_for_resume() -> Result<Option<SelectedAgent>> {
    let config_path = config::default_config_path();
    let mut cfg =
        Config::load(&config_path).with_context(|| format!("load {}", config_path.display()))?;
    let selected = ensure_thor_default_agent(&mut cfg);
    cfg.save(&config_path)
        .with_context(|| format!("save {}", config_path.display()))?;
    Ok(Some(selected))
}

fn read_headless_prompt(prompt_arg: String) -> Result<String> {
    if prompt_arg != "-" {
        return Ok(prompt_arg);
    }
    use std::io::Read;
    let mut prompt = String::new();
    std::io::stdin()
        .read_to_string(&mut prompt)
        .context("read prompt from stdin")?;
    Ok(prompt)
}

fn prepare_worktree_for_arg(
    cwd: PathBuf,
    worktree_arg: Option<&str>,
) -> Result<(PathBuf, Option<CreatedWorktree>)> {
    match worktree_arg {
        None => Ok((cwd, None)),
        Some("") => {
            // `--worktree` with no value: create a new one.
            let created = prepare_new_worktree(&cwd)?;
            Ok((created.session_cwd.clone(), Some(created)))
        }
        Some(name_or_path) => {
            // `--worktree <name>`: reuse an existing one.
            let opened = prepare_existing_worktree(&cwd, name_or_path)?;
            Ok((opened.session_cwd.clone(), Some(opened)))
        }
    }
}

fn absolutize_cwd(cwd: PathBuf) -> Result<PathBuf> {
    if cwd.is_absolute() {
        Ok(cwd)
    } else {
        Ok(std::env::current_dir().context("current dir")?.join(cwd))
    }
}

fn validate_workspace_roots(
    cwd: &Path,
    additional_directories: &[PathBuf],
) -> Result<paths::WorkspaceRoots> {
    paths::WorkspaceRoots::new(cwd, additional_directories)
}

fn worktree_label(worktree: Option<&CreatedWorktree>) -> Option<String> {
    worktree.map(|w| paths::folder_label(&w.worktree_root))
}

fn project_label(cwd: &std::path::Path) -> String {
    paths::display_path_with_tilde(cwd)
}

fn handle_worktree_after_tui(worktree: Option<&CreatedWorktree>, mode: Option<UiMode>) -> bool {
    let Some(w) = worktree else {
        return true;
    };

    if mode == Some(UiMode::InlineChat) {
        // Inline mode restores the cursor to the host prompt row. Move to a
        // fresh line before printing post-session worktree messages so they do
        // not end up appended to the shell prompt.
        let stdout = std::io::stdout();
        let mut output = stdout.lock();
        if let Err(e) = writeln!(output) {
            tracing::warn!("worktree cleanup spacing failed: {e}");
        } else if let Err(e) = output.flush() {
            tracing::warn!("worktree cleanup spacing flush failed: {e}");
        }
    }

    // Remind the user where the worktree lives so they don't lose track
    // of their work — the alt-screen has just been torn down, so writes
    // to stdout now land in their normal scrollback.
    println!("Worktree: {}", w.worktree_root.display());
    if !w.was_created {
        return true;
    }

    // Offer to clean up a freshly-created worktree. Skip the prompt for
    // reused worktrees — the user explicitly asked to work in an
    // existing one, so removing it would be surprising.
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    match worktree::prompt_remove_on_exit(w, &mut input, &mut output) {
        Ok(removed) => !removed,
        Err(e) => {
            tracing::warn!("worktree cleanup prompt failed: {e:#}");
            true
        }
    }
}

fn prepare_new_worktree(cwd: &std::path::Path) -> Result<CreatedWorktree> {
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    let created = worktree::create_for_cwd_prompting(cwd, &mut input, &mut output)?;
    tracing::info!(
        project_root = %created.project_root.display(),
        worktree_root = %created.worktree_root.display(),
        session_cwd = %created.session_cwd.display(),
        "created git worktree"
    );
    // Print before the TUI takes over the terminal so the path lands in
    // the user's normal scrollback and is visible during the session.
    println!("Created worktree: {}", created.worktree_root.display());
    Ok(created)
}

fn prepare_existing_worktree(cwd: &std::path::Path, name_or_path: &str) -> Result<CreatedWorktree> {
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    let opened =
        worktree::open_existing_for_cwd_prompting(cwd, name_or_path, &mut input, &mut output)?;
    tracing::info!(
        project_root = %opened.project_root.display(),
        worktree_root = %opened.worktree_root.display(),
        session_cwd = %opened.session_cwd.display(),
        "reusing existing git worktree"
    );
    println!("Using worktree: {}", opened.worktree_root.display());
    Ok(opened)
}

fn should_open_thor_onboarding(cfg: &Config, initial_agent: Option<&SelectedAgent>) -> bool {
    initial_agent.is_none()
        && (!cfg.thor.onboarding_complete || cfg.thor.configured_acp_servers.is_empty())
}

fn ensure_thor_default_agent(cfg: &mut Config) -> SelectedAgent {
    cfg.agent
        .get_or_insert_with(thor::default_anvil_agent)
        .clone()
}

#[derive(Debug, Clone)]
struct RuntimeOptions {
    agent_stderr: Option<PathBuf>,
    additional_directories: Vec<PathBuf>,
    fs_max_text_bytes: u64,
}

struct RunSessionResult {
    reason: UiExitReason,
    session_id: Option<String>,
    session_title: Option<String>,
    theme_kind: theme::TerminalThemeKind,
    spinner_style: spinner::SpinnerStyle,
}

impl From<ui::UiRunResult> for RunSessionResult {
    fn from(result: ui::UiRunResult) -> Self {
        Self {
            reason: result.reason,
            session_id: result.session_id,
            session_title: result.session_title,
            theme_kind: result.theme_kind,
            spinner_style: result.spinner_style,
        }
    }
}

fn apply_session_result_to_config(cfg: &mut Config, result: &RunSessionResult) {
    cfg.theme = result.theme_kind;
    cfg.spinner = result.spinner_style;
}

async fn run_app(
    cwd: PathBuf,
    runtime_options: RuntimeOptions,
    project_label: String,
    worktree_label: Option<String>,
    resume_target: Option<ResumeTarget>,
    initial_agent: Option<SelectedAgent>,
    mode: UiMode,
) -> Result<Option<String>> {
    let config_path = config::default_config_path();
    let mut cfg = Config::load(&config_path)?;

    // Supervisor loop. Thor sessions use the configured backend when available
    // and create the Anvil default otherwise.
    // Consume resume_session and initial_agent on the first iteration only.
    let mut initial_resume = resume_target;
    let mut initial_agent = initial_agent;
    let mut thor_onboarding = should_open_thor_onboarding(&cfg, initial_agent.as_ref());
    loop {
        let resume = initial_resume.take();
        let agent = if let Some(agent) = initial_agent.take() {
            agent
        } else if thor_onboarding {
            thor_onboarding = false;
            match run_thor_onboarding(&mut cfg, &config_path, cwd.clone()).await? {
                Some(agent) => agent,
                None => return Ok(None),
            }
        } else {
            let agent = ensure_thor_default_agent(&mut cfg);
            cfg.save(&config_path)
                .with_context(|| format!("save {}", config_path.display()))?;
            agent
        };

        let session_result = run_session(
            &agent,
            cwd.clone(),
            runtime_options.clone(),
            HeaderLabels {
                project: project_label.clone(),
                worktree: worktree_label.clone(),
                additional_roots: runtime_options.additional_directories.len(),
                session_title: resume.as_ref().and_then(|target| target.title.clone()),
            },
            resume.as_ref().map(|target| target.session_id.clone()),
            mode,
            cfg.theme,
            cfg.spinner,
            cfg.thor.clone(),
        )
        .await?;
        apply_session_result_to_config(&mut cfg, &session_result);
        match session_result.reason {
            UiExitReason::Quit => return Ok(session_result.session_id),
            UiExitReason::NewSession => {
                initial_agent = Some(ensure_thor_default_agent(&mut cfg));
                continue;
            }
            UiExitReason::ClearSession => {
                initial_agent = Some(agent);
                continue;
            }
            UiExitReason::SwitchSession => {
                if let Some(session_id) = session_result.session_id {
                    initial_resume = Some(ResumeTarget {
                        session_id,
                        title: session_result.session_title,
                    });
                    initial_agent = Some(agent);
                    continue;
                }
                return Ok(None);
            }
            UiExitReason::LoadSession => {
                match run_session_picker_action_for_agent(
                    &agent,
                    cwd.clone(),
                    runtime_options.agent_stderr.as_deref(),
                    session_result.session_id,
                    session_result.session_title,
                    cfg.theme.palette(),
                )
                .await?
                {
                    SessionPickerAction::Resume { session_id, title } => {
                        initial_resume = Some(ResumeTarget { session_id, title });
                        initial_agent = Some(agent);
                        continue;
                    }
                    SessionPickerAction::Exit(session_id) => return Ok(session_id),
                }
            }
        }
    }
}

async fn run_thor_onboarding(
    cfg: &mut Config,
    config_path: &Path,
    cwd: PathBuf,
) -> Result<Option<SelectedAgent>> {
    let (selection, setup_agents, available_servers) = loop {
        let available_servers = thor_onboarding_servers(cfg).await;
        let registry_servers = thor_registry_onboarding_servers(cfg, &available_servers).await;
        let registry_agents = registry_servers
            .iter()
            .map(|server| thor_setup::ThorSetupRegistryAgent {
                source_id: server.source_id.clone(),
                name: server.name.clone(),
                description: server.description.clone(),
                setup_url: server.setup_url.clone(),
                command: selected_agent_command_label(&server.selected_agent()),
                setup_hint: registry_setup_hint(server),
            })
            .collect::<Vec<_>>();
        let available_agents = available_servers
            .iter()
            .map(ConfiguredAcpServer::selected_agent)
            .collect::<Vec<_>>();
        let validations = thor_probe::validate_agents(&available_agents, cwd.clone()).await;
        let setup_agents = available_agents
            .iter()
            .filter_map(|agent| {
                let server = available_servers
                    .iter()
                    .find(|server| server.source_id == agent.source_id)?;
                Some(thor_setup::ThorSetupAgent {
                    agent: agent.clone(),
                    name: server.name.clone(),
                    description: server.description.clone(),
                    setup_url: server.setup_url.clone(),
                    quota_backend: server.quota_backend,
                    validation: validations
                        .iter()
                        .find(|validation| validation.source_id == agent.source_id)
                        .cloned(),
                })
            })
            .collect::<Vec<_>>();
        let initial_host = cfg
            .agent
            .clone()
            .filter(|agent| {
                setup_agents
                    .iter()
                    .any(|candidate| candidate.agent.source_id == agent.source_id)
            })
            .unwrap_or_else(|| {
                setup_agents
                    .iter()
                    .find(|candidate| {
                        candidate
                            .validation
                            .as_ref()
                            .map(|validation| validation.usable)
                            .unwrap_or(true)
                    })
                    .map(|candidate| candidate.agent.clone())
                    .unwrap_or_else(thor::default_anvil_agent)
            });
        let Some(outcome) = run_thor_setup_once(
            cfg.theme.palette(),
            &cfg.thor,
            &setup_agents,
            &registry_agents,
            &initial_host,
        )
        .await?
        else {
            return Ok(None);
        };
        match outcome {
            thor_setup::ThorSetupOutcome::Selection(selection) => {
                break (selection, setup_agents, available_servers);
            }
            thor_setup::ThorSetupOutcome::AddCustom(custom) => {
                add_custom_thor_agent(cfg, custom)?;
                cfg.save(config_path)
                    .with_context(|| format!("save {}", config_path.display()))?;
            }
            thor_setup::ThorSetupOutcome::AddRegistry(source_id) => {
                add_registry_thor_agent(cfg, &registry_servers, &source_id)?;
                cfg.save(config_path)
                    .with_context(|| format!("save {}", config_path.display()))?;
            }
            thor_setup::ThorSetupOutcome::RetryValidation => {}
        }
    };
    cfg.thor.enabled_worker_source_ids = selection.enabled_worker_source_ids;
    cfg.thor.configured_acp_servers = setup_agents
        .iter()
        .filter(|setup_agent| {
            cfg.thor
                .enabled_worker_source_ids
                .iter()
                .any(|source_id| source_id == &setup_agent.agent.source_id)
                || setup_agent.agent.source_id == selection.host_source_id
        })
        .filter_map(|setup_agent| {
            available_servers
                .iter()
                .find(|server| server.source_id == setup_agent.agent.source_id)
                .cloned()
        })
        .collect();
    cfg.thor.optimization_mode = selection.optimization_mode;
    cfg.thor.coordinator_model = selection.coordinator_model;
    cfg.thor.coordinator_reasoning = selection.coordinator_reasoning;
    let agent = setup_agents
        .iter()
        .find(|setup_agent| setup_agent.agent.source_id == selection.host_source_id)
        .map(|setup_agent| setup_agent.agent.clone())
        .unwrap_or_else(thor::default_anvil_agent);
    cfg.agent = Some(agent.clone());
    cfg.save(config_path)
        .with_context(|| format!("save {}", config_path.display()))?;

    let Some(theme_kind) = run_theme_picker_once(cfg.theme).await? else {
        return Ok(None);
    };
    cfg.theme = theme_kind;
    cfg.save(config_path)
        .with_context(|| format!("save {}", config_path.display()))?;

    let Some(spinner_style) = run_spinner_picker_once(cfg.theme.palette(), cfg.spinner).await?
    else {
        return Ok(None);
    };
    cfg.spinner = spinner_style;
    cfg.thor.onboarding_complete = true;
    cfg.save(config_path)
        .with_context(|| format!("save {}", config_path.display()))?;
    Ok(Some(agent))
}

fn add_custom_thor_agent(cfg: &mut Config, custom: thor_setup::ThorSetupCustomAgent) -> Result<()> {
    let words = shell_words::split(&custom.command)
        .with_context(|| format!("parse ACP command for {}", custom.name))?;
    let Some((program, args)) = words.split_first() else {
        anyhow::bail!("ACP command cannot be empty");
    };
    let name = unique_custom_agent_name(cfg, &custom.name);
    let agent = config::CustomAgent {
        name,
        program: PathBuf::from(program),
        args: args.to_vec(),
        description: "Custom ACP command added during Thor setup".to_string(),
    };
    cfg.custom_agents.push(agent);
    Ok(())
}

fn selected_agent_command_label(agent: &SelectedAgent) -> String {
    let mut parts = vec![agent.program.to_string_lossy().into_owned()];
    parts.extend(agent.args.iter().cloned());
    parts.join(" ")
}

fn registry_setup_hint(server: &ConfiguredAcpServer) -> String {
    let install_hint = match server.program.to_string_lossy().as_ref() {
        "npx" => Some("requires Node.js/npm"),
        "uvx" => Some("requires uv"),
        _ => None,
    };
    let auth_hint = match server.source_id.as_str() {
        "claude-acp" => Some("uses Claude Code sign-in"),
        "codex-acp" => Some("uses Codex sign-in"),
        "gemini" => Some("uses Gemini CLI auth"),
        "opencode" => Some("uses OpenCode auth/config"),
        "cursor" => Some("uses Cursor auth"),
        "github-copilot-cli" => Some("uses GitHub Copilot auth"),
        _ => None,
    };
    [install_hint, auth_hint]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("; ")
}

fn unique_custom_agent_name(cfg: &Config, requested: &str) -> String {
    let base = requested.trim();
    let base = if base.is_empty() { "Custom ACP" } else { base };
    if !cfg.custom_agents.iter().any(|agent| agent.name == base) {
        return base.to_string();
    }
    for idx in 2..1000 {
        let candidate = format!("{base} {idx}");
        if !cfg
            .custom_agents
            .iter()
            .any(|agent| agent.name == candidate)
        {
            return candidate;
        }
    }
    format!("{base} {}", cfg.custom_agents.len() + 1)
}

fn add_registry_thor_agent(
    cfg: &mut Config,
    registry_servers: &[ConfiguredAcpServer],
    source_id: &str,
) -> Result<()> {
    let server = registry_servers
        .iter()
        .find(|server| server.source_id == source_id)
        .cloned()
        .with_context(|| format!("registry ACP server {source_id} is no longer available"))?;
    push_unique_server(&mut cfg.thor.configured_acp_servers, server);
    Ok(())
}

async fn thor_onboarding_servers(cfg: &Config) -> Vec<ConfiguredAcpServer> {
    let mut servers = Vec::new();
    for server in thor::configured_acp_servers(cfg) {
        push_unique_server(&mut servers, server);
    }
    for custom in &cfg.custom_agents {
        push_unique_server(
            &mut servers,
            ConfiguredAcpServer {
                source_id: format!("{}{}", config::CUSTOM_AGENT_SOURCE_PREFIX, custom.name),
                name: custom.name.clone(),
                program: custom.program.clone(),
                args: custom.args.clone(),
                env: Default::default(),
                description: custom.description.clone(),
                setup_url: String::new(),
                quota_backend: ThorQuotaBackend::None,
            },
        );
    }
    push_unique_server(&mut servers, thor::default_anvil_server());
    servers
}

async fn thor_registry_onboarding_servers(
    cfg: &Config,
    available_servers: &[ConfiguredAcpServer],
) -> Vec<ConfiguredAcpServer> {
    let registry = match tokio::time::timeout(
        Duration::from_secs(2),
        registry::load_with_cache(
            &registry::default_cache_path(),
            registry::CACHE_TTL,
            registry::REGISTRY_URL,
        ),
    )
    .await
    {
        Ok(Ok(registry)) => registry,
        Ok(Err(error)) => {
            tracing::warn!("load ACP registry for Thor onboarding: {error:#}");
            return Vec::new();
        }
        Err(_) => {
            tracing::warn!("load ACP registry for Thor onboarding timed out");
            return Vec::new();
        }
    };

    registry
        .configured_servers()
        .into_iter()
        .filter(|server| {
            !available_servers
                .iter()
                .any(|existing| existing.source_id == server.source_id)
                && !cfg
                    .thor
                    .configured_acp_servers
                    .iter()
                    .any(|existing| existing.source_id == server.source_id)
        })
        .collect()
}

fn push_unique_server(servers: &mut Vec<ConfiguredAcpServer>, server: ConfiguredAcpServer) {
    if servers
        .iter()
        .any(|existing| existing.source_id == server.source_id)
    {
        return;
    }
    servers.push(server);
}

async fn run_session_picker_action_for_agent(
    agent: &SelectedAgent,
    cwd: PathBuf,
    agent_stderr: Option<&Path>,
    current_session_id: Option<String>,
    current_session_title: Option<String>,
    theme: palette::TerminalTheme,
) -> Result<SessionPickerAction> {
    let mut notice = None;
    loop {
        let listing =
            session::list_sessions_with_capabilities(agent, cwd.clone(), agent_stderr).await?;
        if listing.sessions.is_empty() {
            return Ok(session_picker_empty_action(
                current_session_id,
                current_session_title,
            ));
        }

        let delete_supported = in_app_session_delete_supported(
            listing.delete_supported,
            current_session_id.as_deref(),
        );
        let outcome =
            run_session_picker_once(listing.sessions, delete_supported, notice.take(), theme)
                .await?;
        if let session::ResumeOutcome::DeleteRequested(entry) = outcome {
            if current_session_id.as_deref() == Some(entry.session_id.as_str()) {
                notice = Some(
                    "Cannot delete the active session from the session picker. Close it first."
                        .to_string(),
                );
            } else {
                notice = Some(delete_session_notice(agent, entry, agent_stderr).await);
            }
            continue;
        }

        return session_picker_action(outcome, current_session_id, current_session_title);
    }
}

fn in_app_session_delete_supported(
    agent_delete_supported: bool,
    current_session_id: Option<&str>,
) -> bool {
    agent_delete_supported && current_session_id.is_some()
}

fn session_picker_empty_action(
    current_session_id: Option<String>,
    current_session_title: Option<String>,
) -> SessionPickerAction {
    match current_session_id {
        Some(session_id) => SessionPickerAction::Resume {
            session_id,
            title: current_session_title,
        },
        None => SessionPickerAction::Exit(None),
    }
}

async fn delete_session_notice(
    agent: &SelectedAgent,
    entry: session::SessionEntry,
    agent_stderr: Option<&Path>,
) -> String {
    let label = entry
        .title
        .as_deref()
        .unwrap_or(entry.session_id.as_str())
        .to_string();
    match session::delete_session(agent, entry.session_id, agent_stderr).await {
        Ok(()) => format!("Deleted session: {label}"),
        Err(err) => format!("Delete failed for {label}: {err:#}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SessionPickerAction {
    Resume {
        session_id: String,
        title: Option<String>,
    },
    Exit(Option<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResumeTarget {
    session_id: String,
    title: Option<String>,
}

fn session_picker_action(
    outcome: session::ResumeOutcome,
    current_session_id: Option<String>,
    current_session_title: Option<String>,
) -> Result<SessionPickerAction> {
    match outcome {
        session::ResumeOutcome::Selected(entry) => Ok(SessionPickerAction::Resume {
            session_id: entry.session_id,
            title: entry.title,
        }),
        session::ResumeOutcome::DeleteRequested(_) => {
            anyhow::bail!("session delete request was not handled by picker flow")
        }
        // Cancelling the picker keeps the current session running, so carry
        // its known title forward instead of dropping it — otherwise the
        // header title would blank out until the agent's next SessionInfoUpdate.
        session::ResumeOutcome::Cancelled => Ok(match current_session_id {
            Some(session_id) => SessionPickerAction::Resume {
                session_id,
                title: current_session_title,
            },
            None => SessionPickerAction::Exit(None),
        }),
    }
}

async fn run_thor_setup_once(
    theme: palette::TerminalTheme,
    thor_config: &thor::ThorConfig,
    agents: &[thor_setup::ThorSetupAgent],
    registry_agents: &[thor_setup::ThorSetupRegistryAgent],
    host_agent: &SelectedAgent,
) -> Result<Option<thor_setup::ThorSetupOutcome>> {
    let mut terminal = ui::setup_fullscreen_terminal().context("setup terminal")?;
    let result = thor_setup::run_thor_setup(
        &mut terminal,
        theme,
        thor_config,
        agents,
        registry_agents,
        host_agent,
    )
    .await;
    if let Err(e) = ui::restore_fullscreen_terminal(&mut terminal) {
        tracing::warn!("restore terminal (Thor setup) failed: {e}");
    }
    settle_after_fullscreen_picker_restore().await;
    result
}

async fn run_theme_picker_once(
    initial: theme::TerminalThemeKind,
) -> Result<Option<theme::TerminalThemeKind>> {
    let mut terminal = ui::setup_fullscreen_terminal().context("setup terminal")?;
    let result = theme_picker::run_theme_picker(&mut terminal, initial).await;
    if let Err(e) = ui::restore_fullscreen_terminal(&mut terminal) {
        tracing::warn!("restore terminal (theme picker) failed: {e}");
    }
    settle_after_fullscreen_picker_restore().await;
    result
}

async fn run_spinner_picker_once(
    theme: palette::TerminalTheme,
    initial: spinner::SpinnerStyle,
) -> Result<Option<spinner::SpinnerStyle>> {
    let mut terminal = ui::setup_fullscreen_terminal().context("setup terminal")?;
    let result = spinner_picker::run_spinner_picker(&mut terminal, theme, initial).await;
    if let Err(e) = ui::restore_fullscreen_terminal(&mut terminal) {
        tracing::warn!("restore terminal (spinner picker) failed: {e}");
    }
    settle_after_fullscreen_picker_restore().await;
    result
}

async fn run_session_picker_once(
    sessions: Vec<session::SessionEntry>,
    delete_supported: bool,
    notice: Option<String>,
    theme: palette::TerminalTheme,
) -> Result<session::ResumeOutcome> {
    let mut terminal = ui::setup_fullscreen_terminal().context("setup terminal")?;
    let outcome =
        session::run_session_picker(&mut terminal, sessions, delete_supported, notice, theme).await;
    if let Err(e) = ui::restore_fullscreen_terminal(&mut terminal) {
        tracing::warn!("restore terminal (session picker) failed: {e}");
    }
    settle_after_fullscreen_picker_restore().await;
    outcome
}

async fn settle_after_fullscreen_picker_restore() {
    // Let the terminal finish leaving the alternate screen before the inline
    // viewport asks for a cursor position. Without this, some terminals answer
    // the CPR query late enough that crossterm times out and leaks the response
    // back to the shell prompt.
    tokio::time::sleep(Duration::from_millis(75)).await;
}

fn agent_header_label(agent: &SelectedAgent) -> String {
    remote::agent_display_label(agent)
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    agent: &SelectedAgent,
    cwd: PathBuf,
    runtime_options: RuntimeOptions,
    header_labels: HeaderLabels,
    resume_session: Option<String>,
    mode: UiMode,
    mut theme_kind: theme::TerminalThemeKind,
    mut spinner_style: spinner::SpinnerStyle,
    thor_config: thor::ThorConfig,
) -> Result<RunSessionResult> {
    let mut terminal = setup_session_terminal(mode)?;

    let (event_tx, runtime_event_rx) = mpsc::unbounded_channel();
    let (ui_event_tx, ui_event_rx) = mpsc::unbounded_channel();
    let (runtime_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (cmd_tx, mut ui_cmd_rx) = mpsc::unbounded_channel();
    let mut ui_event_rx = ui_event_rx;

    let runtime_cfg = acp::AcpRuntimeConfig {
        command: agent.program.clone(),
        args: agent.args.clone(),
        cwd: cwd.clone(),
        additional_directories: runtime_options.additional_directories.clone(),
        mcp_servers: thor_mcp::mcp_servers(config::default_config_path())?,
        resume_session,
        env: agent.env.clone(),
        agent_stderr: runtime_options.agent_stderr.clone(),
        fs_max_text_bytes: runtime_options.fs_max_text_bytes,
    };

    // Drive the Thor host ACP runtime on its own task so the UI can own the
    // current task's stdio (ratatui draws through stdout while ACP
    // talks to the agent's stdout/stdin, which are separate file
    // descriptors).
    let runtime_handle = tokio::spawn(async move {
        let result = acp::run(runtime_cfg, event_tx, cmd_rx).await;
        if let Err(e) = result {
            tracing::error!("runtime error: {e:#}");
        }
    });

    let hist_path = history_path();
    let export_dir = transcript_export_dir();
    let config_path = config::default_config_path();
    // Pre-fill the UI header with Thor; the selected ACP backend hosts Thor
    // and receives the local MCP bridge for worker delegation.
    let agent_display_name = Some("Thor".to_string());
    let tracker_project_label = header_labels.project.clone();
    let tracker_agent_label = agent_display_name
        .clone()
        .unwrap_or_else(|| agent_header_label(agent));
    let remote_tracker = remote::RemoteSessionTracker::new(
        tracker_project_label,
        tracker_agent_label,
        Some(cmd_tx.clone()),
        Some(ui_event_tx.clone()),
    );

    let event_tracker = remote_tracker.clone();
    let event_proxy = tokio::spawn(async move {
        let mut runtime_event_rx = runtime_event_rx;
        while let Some(event) = runtime_event_rx.recv().await {
            // Intercept before observing: permission prompts get their
            // responder wrapped so remote viewers see (and can answer)
            // the pending request.
            let event = event_tracker.intercept_event(event);
            event_tracker.observe_event(&event);
            if ui_event_tx.send(event).is_err() {
                break;
            }
        }
    });

    let cmd_tracker = remote_tracker.clone();
    let cmd_proxy = tokio::spawn(async move {
        let mut sent_thor_preamble = false;
        while let Some(command) = ui_cmd_rx.recv().await {
            cmd_tracker.observe_command(&command);
            let runtime_command = match command {
                UiCommand::SendPrompt { text, images } => {
                    let text = if sent_thor_preamble {
                        text
                    } else {
                        sent_thor_preamble = true;
                        thor::host_prompt(&thor_config, &text)
                    };
                    UiCommand::SendPrompt { text, images }
                }
                other => other,
            };
            if runtime_cmd_tx.send(runtime_command).is_err() {
                break;
            }
        }
    });

    let mut header_labels = header_labels;
    let ui_result = loop {
        let ui_result = ui::run(
            &mut terminal,
            &cmd_tx,
            &mut ui_event_rx,
            header_labels.clone(),
            agent_display_name.clone(),
            ui::UiRunOptions {
                persistence: ui::UiPersistencePaths {
                    history_path: Some(&hist_path),
                    transcript_export_dir: export_dir.as_deref(),
                    config_path: Some(&config_path),
                },
                mode,
                theme_kind,
                spinner_style,
            },
        )
        .await;

        let restore_result = restore_session_terminal(&mut terminal, mode);
        if let Err(e) = restore_result {
            tracing::warn!("restore terminal failed: {e}");
        }
        if let Ok(result) = ui_result.as_ref() {
            theme_kind = result.theme_kind;
            spinner_style = result.spinner_style;
        }
        if matches!(
            ui_result.as_ref().map(|result| result.reason),
            Ok(UiExitReason::ClearSession)
        ) && let Err(e) = ui::clear_terminal_screen(&mut terminal)
        {
            tracing::warn!("clear terminal for /clear failed: {e}");
        }

        let Ok(result) = ui_result else {
            break ui_result.map(Into::into);
        };
        if result.reason != UiExitReason::LoadSession {
            break Ok(result.into());
        }
        let current_session_id = result.session_id;
        let current_session_title = result.session_title;

        let action = match run_session_picker_action_for_agent(
            agent,
            cwd.clone(),
            runtime_options.agent_stderr.as_deref(),
            current_session_id.clone(),
            current_session_title.clone(),
            theme_kind.palette(),
        )
        .await
        {
            Ok(action) => action,
            Err(e) => {
                let _ = cmd_tx.send(UiCommand::Shutdown);
                break Err(e);
            }
        };
        let SessionPickerAction::Resume {
            session_id: target_session_id,
            title: target_title,
        } = action
        else {
            let _ = cmd_tx.send(UiCommand::Shutdown);
            break Ok(RunSessionResult {
                reason: UiExitReason::Quit,
                session_id: current_session_id,
                session_title: current_session_title,
                theme_kind,
                spinner_style,
            });
        };

        match request_inline_session_load(
            &cmd_tx,
            target_session_id.clone(),
            cwd.clone(),
            target_title.clone(),
        )
        .await
        {
            LoadSessionResult::Switched => {
                header_labels.session_title = target_title;
                terminal = match setup_session_terminal(mode) {
                    Ok(terminal) => terminal,
                    Err(e) => {
                        let _ = cmd_tx.send(UiCommand::Shutdown);
                        break Err(e);
                    }
                };
                continue;
            }
            LoadSessionResult::Fallback { message } => {
                tracing::info!("falling back to restart-based session load: {message}");
                let _ = cmd_tx.send(UiCommand::Shutdown);
                break Ok(RunSessionResult {
                    reason: UiExitReason::SwitchSession,
                    session_id: Some(target_session_id),
                    session_title: target_title,
                    theme_kind,
                    spinner_style,
                });
            }
        }
    };

    // Shutdown paths reaching this point:
    //
    // 1. User quit while idle (Ctrl-C/Ctrl-D/Esc with empty input):
    //    `ui::run` sends `UiCommand::Shutdown` and returns. `cmd_tx` is
    //    then dropped; `drive_session` sees `None` on its `recv()` and
    //    returns, then `acp::run` kills/reaps the child.
    //
    // 2. User cancelled mid-prompt and then quit: same as #1 once the
    //    cancel resolves into a `PromptDone(Cancelled)`. A force-quit
    //    via Ctrl-D before the cancel lands also works because
    //    `drive_prompt_turn` selects on the command channel and exits
    //    on `Shutdown` even while a prompt RPC is in flight.
    //
    // 3. Agent EOF / crash: `acp::run` races `drive_client` against
    //    `child.wait()`. The wait branch (or the post-drive snapshot)
    //    surfaces a single Fatal mentioning the unexpected exit, the
    //    UI flips to read-only, and the event channel closes.
    //
    // 4. Runtime wedged (e.g. agent stops responding but stdio stays
    //    open): the 2s `timeout` below trips and we `abort()` the
    //    task. `kill_on_drop(true)` on the `Command` then signals the
    //    child when the `Child` value is dropped during unwind.
    remote_tracker.shutdown().await;

    let abort_handle = runtime_handle.abort_handle();
    match tokio::time::timeout(Duration::from_secs(2), runtime_handle).await {
        Ok(join_res) => {
            if let Err(e) = join_res {
                tracing::warn!("acp task join: {e}");
            }
        }
        Err(_elapsed) => {
            tracing::warn!(
                "acp runtime did not exit within 2s; aborting (child may not be reaped)"
            );
            abort_handle.abort();
        }
    }

    wait_for_task("remote-control event proxy", event_proxy).await;
    wait_for_task("remote-control command proxy", cmd_proxy).await;

    ui_result
}

fn setup_session_terminal(
    mode: UiMode,
) -> Result<ratatui::Terminal<crate::term::TrackedBackend<std::io::Stdout>>> {
    match mode {
        UiMode::InlineChat => {
            ui::setup_inline_chat_terminal(ui::INLINE_CHAT_HEIGHT).context("setup terminal")
        }
        UiMode::FullscreenTui => ui::setup_fullscreen_terminal().context("setup terminal"),
    }
}

fn restore_session_terminal(
    terminal: &mut ratatui::Terminal<crate::term::TrackedBackend<std::io::Stdout>>,
    mode: UiMode,
) -> Result<()> {
    match mode {
        UiMode::InlineChat => ui::restore_inline_chat_terminal(terminal),
        UiMode::FullscreenTui => ui::restore_fullscreen_terminal(terminal),
    }
}

async fn request_inline_session_load(
    cmd_tx: &mpsc::UnboundedSender<UiCommand>,
    session_id: String,
    cwd: PathBuf,
    title: Option<String>,
) -> LoadSessionResult {
    let (responder, response) = tokio::sync::oneshot::channel();
    if cmd_tx
        .send(UiCommand::LoadSession {
            session_id,
            cwd,
            title,
            responder,
        })
        .is_err()
    {
        return LoadSessionResult::Fallback {
            message: "ACP runtime command channel closed".to_string(),
        };
    }
    match tokio::time::timeout(Duration::from_secs(15), response).await {
        Ok(Ok(result)) => result,
        Ok(Err(_closed)) => LoadSessionResult::Fallback {
            message: "ACP runtime closed before session switch completed".to_string(),
        },
        Err(_elapsed) => LoadSessionResult::Fallback {
            message: "ACP runtime did not complete session switch within 15s".to_string(),
        },
    }
}

async fn wait_for_task(label: &str, handle: tokio::task::JoinHandle<()>) {
    let abort_handle = handle.abort_handle();
    match tokio::time::timeout(Duration::from_secs(2), handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!("{label} join failed: {error}");
        }
        Err(_) => {
            tracing::warn!("{label} did not exit within 2s; aborting");
            abort_handle.abort();
        }
    }
}

fn init_logging(path: Option<&std::path::Path>) -> Result<()> {
    use tracing_subscriber::{EnvFilter, fmt};

    let Some(path) = path else {
        return Ok(());
    };
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
    if let Some(parent) = parent {
        std::fs::create_dir_all(parent).with_context(|| format!("create log dir {parent:?}"))?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open log {path:?}"))?;
    let filter =
        EnvFilter::try_from_env("BROKK_TUI_LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_writer(file)
        .with_env_filter(filter)
        .with_ansi(false)
        .init();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};
    use std::io::Write;

    #[test]
    fn agent_header_label_uses_registry_source_id() {
        let agent = SelectedAgent {
            source_id: "claude-acp".to_string(),
            program: PathBuf::from("npx"),
            args: vec!["-y".to_string(), "@x/claude@0.36.1".to_string()],
            env: Default::default(),
        };

        assert_eq!(agent_header_label(&agent), "claude-acp");
    }

    #[test]
    fn agent_header_label_uses_full_custom_command() {
        let agent = SelectedAgent {
            source_id: "custom".to_string(),
            program: PathBuf::from("/usr/local/bin/my agent"),
            args: vec!["--flag".to_string(), "value with space".to_string()],
            env: Default::default(),
        };

        assert_eq!(
            agent_header_label(&agent),
            "'/usr/local/bin/my agent' --flag 'value with space'"
        );
    }

    #[test]
    fn project_label_uses_full_worktree_session_path_with_tilde() {
        let worktree = CreatedWorktree {
            project_root: PathBuf::from("/Users/ryan/code/mjolnir"),
            worktree_root: PathBuf::from("/Users/ryan/code/mjolnir/.mjolnir/worktrees/bold-willow"),
            session_cwd: PathBuf::from(
                "/Users/ryan/code/mjolnir/.mjolnir/worktrees/bold-willow/src",
            ),
            was_created: false,
        };

        assert_eq!(
            project_label(&worktree.session_cwd),
            paths::display_path_with_tilde(&worktree.session_cwd)
        );
    }

    #[test]
    fn project_label_uses_full_directory_path_inside_mjolnir_worktree() {
        let cwd =
            std::path::Path::new("/Users/ryan/code/mjolnir/.mjolnir/worktrees/bold-willow/src");
        assert_eq!(project_label(cwd), paths::display_path_with_tilde(cwd));
    }

    #[test]
    fn project_label_uses_full_directory_path_without_worktree() {
        let cwd = std::path::Path::new("/Users/ryan/code/mjolnir/src");
        assert_eq!(project_label(cwd), paths::display_path_with_tilde(cwd));
    }

    #[test]
    fn inline_worktree_cleanup_output_starts_on_fresh_line() {
        let mut output = Vec::new();
        write!(&mut output, "shell$ ").expect("seed prompt");

        if Some(UiMode::InlineChat) == Some(UiMode::InlineChat) {
            writeln!(&mut output).expect("spacing newline");
            output.flush().expect("spacing flush");
        }
        writeln!(
            &mut output,
            "Worktree: /tmp/project/.mjolnir/worktrees/pale-tide"
        )
        .expect("worktree line");
        write!(&mut output, "Remove worktree 'pale-tide'? [y/N] ").expect("prompt");

        let rendered = String::from_utf8(output).expect("utf8");
        assert!(
            rendered.starts_with("shell$ \nWorktree: /tmp/project/.mjolnir/worktrees/pale-tide\n"),
            "inline cleanup output should begin on a fresh line: {rendered:?}"
        );
        assert!(
            rendered.contains("\nRemove worktree 'pale-tide'? [y/N] "),
            "cleanup prompt should not share the shell prompt line: {rendered:?}"
        );
    }

    #[test]
    fn thor_onboarding_opens_until_completed_marker_is_saved() {
        let configured = SelectedAgent {
            source_id: "claude-acp".to_string(),
            program: PathBuf::from("npx"),
            args: vec!["-y".to_string(), "@x/claude".to_string()],
            env: Default::default(),
        };
        let initial = SelectedAgent {
            source_id: "anvil".to_string(),
            program: PathBuf::from("uvx"),
            args: vec!["brokk".to_string(), "acp".to_string()],
            env: Default::default(),
        };

        assert!(should_open_thor_onboarding(&Config::default(), None));
        assert!(
            should_open_thor_onboarding(
                &Config {
                    theme: Default::default(),
                    spinner: Default::default(),
                    thor: Default::default(),
                    agent: Some(configured.clone()),
                    favorite_agents: Vec::new(),
                    custom_agents: Vec::new(),
                },
                None
            ),
            "old configs with a silently created host still need Thor onboarding"
        );
        assert!(
            should_open_thor_onboarding(
                &Config {
                    theme: Default::default(),
                    spinner: Default::default(),
                    thor: thor::ThorConfig {
                        onboarding_complete: true,
                        ..Default::default()
                    },
                    agent: Some(configured.clone()),
                    favorite_agents: Vec::new(),
                    custom_agents: Vec::new(),
                },
                None
            ),
            "completed legacy configs still need persisted Thor ACP servers"
        );
        assert!(!should_open_thor_onboarding(
            &Config {
                theme: Default::default(),
                spinner: Default::default(),
                thor: thor::ThorConfig {
                    onboarding_complete: true,
                    configured_acp_servers: vec![ConfiguredAcpServer {
                        source_id: configured.source_id.clone(),
                        name: "Claude".to_string(),
                        program: configured.program.clone(),
                        args: configured.args.clone(),
                        env: configured.env.clone(),
                        description: String::new(),
                        setup_url: String::new(),
                        quota_backend: ThorQuotaBackend::ClaudeCli,
                    }],
                    ..Default::default()
                },
                agent: Some(configured),
                favorite_agents: Vec::new(),
                custom_agents: Vec::new(),
            },
            None
        ));
        assert!(!should_open_thor_onboarding(
            &Config::default(),
            Some(&initial)
        ));
    }

    #[test]
    fn thor_default_agent_prefers_anvil_without_picker() {
        let mut cfg = Config::default();
        let agent = ensure_thor_default_agent(&mut cfg);

        assert_eq!(agent.source_id, "anvil");
        assert_eq!(agent.program, PathBuf::from("uvx"));
        assert_eq!(agent.args, vec!["brokk", "acp"]);
        assert_eq!(cfg.agent, Some(agent));
    }

    #[tokio::test]
    async fn thor_onboarding_servers_do_not_seed_full_registry() {
        let cfg = Config {
            theme: Default::default(),
            spinner: Default::default(),
            thor: thor::ThorConfig {
                configured_acp_servers: vec![ConfiguredAcpServer {
                    source_id: "claude-acp".to_string(),
                    name: "Claude".to_string(),
                    program: PathBuf::from("npx"),
                    args: vec!["-y".to_string(), "@x/claude".to_string()],
                    env: Default::default(),
                    description: String::new(),
                    setup_url: String::new(),
                    quota_backend: ThorQuotaBackend::ClaudeCli,
                }],
                ..Default::default()
            },
            agent: None,
            favorite_agents: Vec::new(),
            custom_agents: vec![config::CustomAgent {
                name: "local".to_string(),
                program: PathBuf::from("local-acp"),
                args: Vec::new(),
                description: "Local ACP".to_string(),
            }],
        };

        let source_ids = thor_onboarding_servers(&cfg)
            .await
            .into_iter()
            .map(|server| server.source_id)
            .collect::<Vec<_>>();

        assert_eq!(source_ids, vec!["claude-acp", "custom:local", "anvil"]);
    }

    #[test]
    fn add_custom_thor_agent_parses_and_persists_command() {
        let mut cfg = Config::default();

        add_custom_thor_agent(
            &mut cfg,
            thor_setup::ThorSetupCustomAgent {
                name: "Claude Code".to_string(),
                command: "npx -y @agentclientprotocol/claude-agent-acp".to_string(),
            },
        )
        .expect("add custom agent");

        assert_eq!(cfg.custom_agents.len(), 1);
        assert_eq!(cfg.custom_agents[0].name, "Claude Code");
        assert_eq!(cfg.custom_agents[0].program, PathBuf::from("npx"));
        assert_eq!(
            cfg.custom_agents[0].args,
            vec!["-y", "@agentclientprotocol/claude-agent-acp"]
        );
    }

    #[test]
    fn add_custom_thor_agent_keeps_names_unique() {
        let mut cfg = Config {
            custom_agents: vec![config::CustomAgent {
                name: "Claude Code".to_string(),
                program: PathBuf::from("npx"),
                args: Vec::new(),
                description: String::new(),
            }],
            ..Config::default()
        };

        add_custom_thor_agent(
            &mut cfg,
            thor_setup::ThorSetupCustomAgent {
                name: "Claude Code".to_string(),
                command: "npx -y @agentclientprotocol/claude-agent-acp".to_string(),
            },
        )
        .expect("add custom agent");

        assert_eq!(cfg.custom_agents[1].name, "Claude Code 2");
    }

    #[tokio::test]
    async fn custom_thor_agent_becomes_onboarding_server() {
        let mut cfg = Config::default();
        add_custom_thor_agent(
            &mut cfg,
            thor_setup::ThorSetupCustomAgent {
                name: "Local ACP".to_string(),
                command: "'/opt/local acp/bin/server' --mode thor".to_string(),
            },
        )
        .expect("add custom agent");

        let custom = thor_onboarding_servers(&cfg)
            .await
            .into_iter()
            .find(|server| server.source_id == "custom:Local ACP")
            .expect("custom server");

        assert_eq!(custom.name, "Local ACP");
        assert_eq!(custom.program, PathBuf::from("/opt/local acp/bin/server"));
        assert_eq!(custom.args, vec!["--mode", "thor"]);
    }

    #[test]
    fn add_registry_thor_agent_persists_selected_server() {
        let mut cfg = Config::default();
        let registry_servers = vec![ConfiguredAcpServer {
            source_id: "gemini".to_string(),
            name: "Gemini CLI".to_string(),
            program: PathBuf::from("npx"),
            args: vec![
                "-y".to_string(),
                "@google/gemini-cli".to_string(),
                "--acp".to_string(),
            ],
            env: Default::default(),
            description: "Google Gemini ACP".to_string(),
            setup_url: "https://geminicli.com".to_string(),
            quota_backend: ThorQuotaBackend::None,
        }];

        add_registry_thor_agent(&mut cfg, &registry_servers, "gemini").expect("add registry");

        assert_eq!(cfg.thor.configured_acp_servers, registry_servers);
    }

    #[test]
    fn registry_setup_hint_describes_install_and_auth_expectations() {
        let gemini = ConfiguredAcpServer {
            source_id: "gemini".to_string(),
            name: "Gemini CLI".to_string(),
            program: PathBuf::from("npx"),
            args: vec![
                "-y".to_string(),
                "@google/gemini-cli".to_string(),
                "--acp".to_string(),
            ],
            env: Default::default(),
            description: String::new(),
            setup_url: String::new(),
            quota_backend: ThorQuotaBackend::None,
        };
        let anvil = ConfiguredAcpServer {
            source_id: "anvil".to_string(),
            name: "Anvil".to_string(),
            program: PathBuf::from("uvx"),
            args: vec!["brokk".to_string(), "acp".to_string()],
            env: Default::default(),
            description: String::new(),
            setup_url: String::new(),
            quota_backend: ThorQuotaBackend::None,
        };

        assert_eq!(
            registry_setup_hint(&gemini),
            "requires Node.js/npm; uses Gemini CLI auth"
        );
        assert_eq!(registry_setup_hint(&anvil), "requires uv");
    }

    #[test]
    fn add_registry_thor_agent_deduplicates_existing_server() {
        let mut cfg = Config {
            thor: thor::ThorConfig {
                configured_acp_servers: vec![ConfiguredAcpServer {
                    source_id: "gemini".to_string(),
                    name: "Gemini CLI".to_string(),
                    program: PathBuf::from("npx"),
                    args: Vec::new(),
                    env: Default::default(),
                    description: String::new(),
                    setup_url: "https://geminicli.com".to_string(),
                    quota_backend: ThorQuotaBackend::None,
                }],
                ..Default::default()
            },
            ..Config::default()
        };
        let registry_servers = cfg.thor.configured_acp_servers.clone();

        add_registry_thor_agent(&mut cfg, &registry_servers, "gemini").expect("add registry");

        assert_eq!(cfg.thor.configured_acp_servers.len(), 1);
    }

    #[test]
    fn session_result_updates_supervisor_theme_before_next_action() {
        let mut cfg = Config::default();
        let result = RunSessionResult {
            reason: UiExitReason::ClearSession,
            session_id: Some("session-1".to_string()),
            session_title: Some("Current".to_string()),
            theme_kind: theme::TerminalThemeKind::AnsiLight,
            spinner_style: spinner::SpinnerStyle::Bars,
        };

        apply_session_result_to_config(&mut cfg, &result);

        assert_eq!(cfg.theme, theme::TerminalThemeKind::AnsiLight);
        assert_eq!(cfg.spinner, spinner::SpinnerStyle::Bars);
    }

    #[test]
    fn parse_accepts_debug_file_aliases() {
        let cli = Cli::try_parse_from(["mj", "--debug-file", "debug.log"]).expect("parse");
        assert_eq!(cli.log_file, Some(PathBuf::from("debug.log")));

        let cli = Cli::try_parse_from(["mj", "--log-file", "legacy.log"]).expect("parse");
        assert_eq!(cli.log_file, Some(PathBuf::from("legacy.log")));
    }

    #[test]
    fn parse_accepts_filesystem_text_limit() {
        let cli = Cli::try_parse_from(["mj", "--fs-max-text-bytes", "4096"]).expect("parse");
        assert_eq!(cli.fs_max_text_bytes, 4096);

        let cli = Cli::try_parse_from([
            "mj",
            "--fs-max-text-bytes",
            &acp::MAX_CONFIGURABLE_FS_TEXT_BYTES.to_string(),
        ])
        .expect("parse max");
        assert_eq!(cli.fs_max_text_bytes, acp::MAX_CONFIGURABLE_FS_TEXT_BYTES);

        let cli = Cli::try_parse_from(["mj", "server", "--fs-max-text-bytes", "8192"])
            .expect("parse server");
        assert_eq!(cli.fs_max_text_bytes, 8192);
    }

    #[test]
    fn parse_rejects_unsafe_filesystem_text_limit() {
        let err = Cli::try_parse_from(["mj", "--fs-max-text-bytes", "0"]).expect_err("reject 0");
        assert!(
            err.to_string()
                .contains("filesystem text byte limit must be between 1")
        );

        let too_large = (acp::MAX_CONFIGURABLE_FS_TEXT_BYTES + 1).to_string();
        let err = Cli::try_parse_from(["mj", "--fs-max-text-bytes", &too_large])
            .expect_err("reject too large");
        assert!(
            err.to_string()
                .contains("filesystem text byte limit must be between 1")
        );
    }

    #[test]
    fn parse_accepts_worktree_short_flag() {
        let cli = Cli::try_parse_from(["mj", "-w"]).expect("parse");
        assert_eq!(cli.worktree, Some(String::new()));

        let cli = Cli::try_parse_from(["mj", "-w", "named-tree"]).expect("parse");
        assert_eq!(cli.worktree.as_deref(), Some("named-tree"));
    }

    #[test]
    fn parse_accepts_fullscreen_tui_flags() {
        let cli = Cli::try_parse_from(["mj", "--fullscreen-tui"]).expect("parse");
        assert!(cli.fullscreen_tui);

        let cli = Cli::try_parse_from(["mj", "resume", "sess-123", "--fullscreen-tui"])
            .expect("parse resume");
        if let Some(Commands::Resume(args)) = cli.command {
            assert!(args.fullscreen_tui);
        } else {
            panic!("expected Resume subcommand");
        }

        let cli = Cli::try_parse_from(["mj", "--fullscreen-tui", "resume", "sess-123"])
            .expect("parse top-level resume");
        assert!(cli.fullscreen_tui);
    }

    #[test]
    fn startup_update_check_runs_only_for_interactive_modes() {
        let cli = Cli::try_parse_from(["mj"]).expect("parse");
        assert!(should_run_startup_update_check(&cli));

        let cli = Cli::try_parse_from(["mj", "--no-update-check"]).expect("parse");
        assert!(!should_run_startup_update_check(&cli));

        let cli = Cli::try_parse_from(["mj", "--print", "hi"]).expect("parse");
        assert!(!should_run_startup_update_check(&cli));

        let cli = Cli::try_parse_from(["mj", "resume", "--list"]).expect("parse");
        assert!(!should_run_startup_update_check(&cli));

        let cli = Cli::try_parse_from(["mj", "resume", "sess-123"]).expect("parse");
        assert!(should_run_startup_update_check(&cli));

        let cli = Cli::try_parse_from(["mj", "server"]).expect("parse");
        assert!(!should_run_startup_update_check(&cli));
    }

    #[test]
    fn parse_accepts_permission_mode_canonical_and_legacy_values() {
        let canonical = Cli::try_parse_from(["mj", "--permission-mode", "acceptEdits"])
            .expect("parse canonical");
        assert!(matches!(
            canonical.permission_mode,
            HeadlessPermissionMode::AcceptEdits
        ));

        let legacy =
            Cli::try_parse_from(["mj", "--permission-mode", "accept-edits"]).expect("parse legacy");
        assert!(matches!(
            legacy.permission_mode,
            HeadlessPermissionMode::AcceptEdits
        ));

        let canonical = Cli::try_parse_from(["mj", "--permission-mode", "bypassPermissions"])
            .expect("parse canonical");
        assert!(matches!(
            canonical.permission_mode,
            HeadlessPermissionMode::BypassPermissions
        ));

        let legacy = Cli::try_parse_from(["mj", "--permission-mode", "bypass-permissions"])
            .expect("parse legacy");
        assert!(matches!(
            legacy.permission_mode,
            HeadlessPermissionMode::BypassPermissions
        ));
    }

    #[test]
    fn parse_rejects_unknown_permission_mode_value() {
        let err = Cli::try_parse_from(["mj", "--permission-mode", "auto"]).expect_err("reject");
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    fn parse_accepts_resume_session() {
        let cli = Cli::try_parse_from(["mj", "--print", "hi", "--resume-session", "sess-123"])
            .expect("parse");
        assert_eq!(cli.resume_session.as_deref(), Some("sess-123"));
    }

    #[test]
    fn help_shows_canonical_flags_and_values() {
        let mut cmd = Cli::command();
        let help = cmd.render_long_help().to_string();

        assert!(help.contains("--debug-file <LOG_FILE>"));
        assert!(help.contains("[aliases: --log-file]"));
        assert!(help.contains("--fs-max-text-bytes <FS_MAX_TEXT_BYTES>"));
        assert!(help.contains("-w, --worktree [<WORKTREE>]"));
        assert!(help.contains("--fullscreen-tui"));
        assert!(!help.contains("--resume-session"));
        assert!(help.contains("[possible values: default, acceptEdits, bypassPermissions]"));
        assert!(!help.contains("accept-edits"));
        assert!(!help.contains("bypass-permissions"));
    }

    #[test]
    fn parse_resume_subcommand_without_args() {
        let cli = Cli::try_parse_from(["mj", "resume"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Resume(_))));
        if let Some(Commands::Resume(args)) = cli.command {
            assert!(args.session_id.is_none());
            assert!(!args.list);
            assert!(matches!(args.format, HeadlessOutputFormat::Text));
            assert!(args.cwd.is_none());
            assert!(args.agent_stderr.is_none());
        }
    }

    #[test]
    fn parse_server_subcommand() {
        let cli = Cli::try_parse_from(["mj", "server"]).expect("parse");
        match cli.command {
            Some(Commands::Server(args)) => assert!(args.hostname.is_none()),
            _ => panic!("expected Server subcommand"),
        }
    }

    #[test]
    fn parse_server_subcommand_with_global_cwd() {
        let cli = Cli::try_parse_from(["mj", "--cwd", "/tmp/test", "server"]).expect("parse");
        assert_eq!(cli.cwd, Some(PathBuf::from("/tmp/test")));
        assert!(matches!(cli.command, Some(Commands::Server(_))));
    }

    #[test]
    fn parse_server_subcommand_with_hostname() {
        let cli =
            Cli::try_parse_from(["mj", "server", "--hostname", "example.com"]).expect("parse");
        match cli.command {
            Some(Commands::Server(args)) => {
                assert_eq!(args.hostname.as_deref(), Some("example.com"))
            }
            _ => panic!("expected Server subcommand"),
        }
    }

    #[test]
    fn parse_resume_subcommand_with_session_id() {
        let cli = Cli::try_parse_from(["mj", "resume", "sess-123"]).expect("parse");
        if let Some(Commands::Resume(args)) = cli.command {
            assert_eq!(args.session_id, Some("sess-123".to_string()));
            assert!(!args.list);
        } else {
            panic!("expected Resume subcommand");
        }
    }

    #[test]
    fn parse_resume_subcommand_with_list_flag() {
        let cli = Cli::try_parse_from(["mj", "resume", "--list"]).expect("parse");
        if let Some(Commands::Resume(args)) = cli.command {
            assert!(args.list);
            assert!(args.session_id.is_none());
        } else {
            panic!("expected Resume subcommand");
        }
    }

    #[test]
    fn parse_resume_subcommand_with_list_and_format() {
        let cli =
            Cli::try_parse_from(["mj", "resume", "--list", "--format", "json"]).expect("parse");
        if let Some(Commands::Resume(args)) = cli.command {
            assert!(args.list);
            assert!(matches!(args.format, HeadlessOutputFormat::Json));
        } else {
            panic!("expected Resume subcommand");
        }
    }

    #[test]
    fn parse_resume_subcommand_with_cwd() {
        let cli = Cli::try_parse_from(["mj", "resume", "--cwd", "/tmp/test"]).expect("parse");
        if let Some(Commands::Resume(args)) = cli.command {
            assert_eq!(args.cwd, Some(PathBuf::from("/tmp/test")));
        } else {
            panic!("expected Resume subcommand");
        }
    }

    #[test]
    fn parse_additional_directories_for_new_and_resume_sessions() {
        let cli = Cli::try_parse_from([
            "mj",
            "--additional-directory",
            "/tmp/one",
            "--add-dir",
            "/tmp/two",
        ])
        .expect("parse");
        assert_eq!(
            cli.additional_directories,
            vec![PathBuf::from("/tmp/one"), PathBuf::from("/tmp/two")]
        );

        let cli = Cli::try_parse_from([
            "mj",
            "resume",
            "sess-123",
            "--additional-directory",
            "/tmp/extra",
        ])
        .expect("parse resume");
        if let Some(Commands::Resume(args)) = cli.command {
            assert_eq!(
                args.additional_directories,
                vec![PathBuf::from("/tmp/extra")]
            );
        } else {
            panic!("expected Resume subcommand");
        }

        let cli = Cli::try_parse_from(["mj", "--add-dir", "/tmp/top", "resume", "sess-123"])
            .expect("parse top-level add-dir before resume");
        assert_eq!(cli.additional_directories, vec![PathBuf::from("/tmp/top")]);
    }

    #[test]
    fn validate_workspace_roots_canonicalizes_and_deduplicates() {
        let temp = tempfile::tempdir().expect("tempdir");
        let primary = tempfile::tempdir().expect("primary");
        let canonical = std::fs::canonicalize(temp.path()).expect("canonical");

        let validated = validate_workspace_roots(
            primary.path(),
            &[temp.path().to_path_buf(), canonical.clone()],
        )
        .expect("validated");

        assert_eq!(validated.additional_directories(), &[canonical]);
    }

    #[test]
    fn validate_workspace_roots_deduplicates_additional_roots_against_cwd() {
        let primary = tempfile::tempdir().expect("primary");
        let validated = validate_workspace_roots(primary.path(), &[primary.path().to_path_buf()])
            .expect("validated");

        assert!(validated.additional_directories().is_empty());
    }

    #[test]
    fn validate_workspace_roots_rejects_relative_missing_and_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let primary = tempfile::tempdir().expect("primary");
        let file = temp.path().join("file.txt");
        std::fs::write(&file, "not a directory").expect("write file");

        assert!(validate_workspace_roots(primary.path(), &[PathBuf::from("relative")]).is_err());
        assert!(validate_workspace_roots(primary.path(), &[temp.path().join("missing")]).is_err());
        assert!(validate_workspace_roots(primary.path(), &[file]).is_err());
    }

    #[test]
    fn resume_hint_includes_worktree_and_shell_quoted_additional_roots() {
        let command = resume_hint_command(
            "sess-123",
            Some("named tree"),
            &[
                PathBuf::from("/tmp/extra root"),
                PathBuf::from("/tmp/quote'root"),
            ],
        );

        assert_eq!(
            command,
            "mj resume sess-123 --worktree 'named tree' --additional-directory '/tmp/extra root' --additional-directory '/tmp/quote'\\''root'"
        );
    }

    #[test]
    fn parse_resume_subcommand_with_worktree() {
        let cli = Cli::try_parse_from(["mj", "resume", "sess-123", "--worktree", "named-tree"])
            .expect("parse");
        if let Some(Commands::Resume(args)) = cli.command {
            assert_eq!(args.session_id, Some("sess-123".to_string()));
            assert_eq!(args.worktree.as_deref(), Some("named-tree"));
        } else {
            panic!("expected Resume subcommand");
        }

        let cli = Cli::try_parse_from(["mj", "resume", "sess-123", "--worktree"])
            .expect("parse missing value");
        if let Some(Commands::Resume(args)) = cli.command {
            assert_eq!(args.worktree.as_deref(), Some(""));
        } else {
            panic!("expected Resume subcommand");
        }
    }

    #[test]
    fn parse_resume_subcommand_rejects_list_with_session_id() {
        let err = Cli::try_parse_from(["mj", "resume", "sess-123", "--list"]).expect_err("reject");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn parse_resume_subcommand_rejects_format_without_list() {
        let err = Cli::try_parse_from(["mj", "resume", "--format", "json"]).expect_err("reject");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn parse_resume_subcommand_with_agent_stderr() {
        let cli =
            Cli::try_parse_from(["mj", "resume", "--agent-stderr", "agent.log"]).expect("parse");
        if let Some(Commands::Resume(args)) = cli.command {
            assert_eq!(args.agent_stderr, Some(PathBuf::from("agent.log")));
        } else {
            panic!("expected Resume subcommand");
        }
    }

    #[test]
    fn parse_resume_subcommand_combined_flags() {
        let cli = Cli::try_parse_from([
            "mj",
            "resume",
            "sess-456",
            "--cwd",
            "/home/user",
            "--agent-stderr",
            "err.log",
        ])
        .expect("parse");
        if let Some(Commands::Resume(args)) = cli.command {
            assert_eq!(args.session_id, Some("sess-456".to_string()));
            assert_eq!(args.cwd, Some(PathBuf::from("/home/user")));
            assert_eq!(args.agent_stderr, Some(PathBuf::from("err.log")));
            assert!(!args.list);
        } else {
            panic!("expected Resume subcommand");
        }
    }

    #[test]
    fn cancelling_session_picker_resumes_current_session_preserving_title() {
        let action = session_picker_action(
            session::ResumeOutcome::Cancelled,
            Some("current-session".to_string()),
            Some("Current title".to_string()),
        )
        .expect("cancel should resume current session");

        assert_eq!(
            action,
            SessionPickerAction::Resume {
                session_id: "current-session".to_string(),
                title: Some("Current title".to_string()),
            }
        );
    }

    #[test]
    fn cancelling_session_picker_without_current_session_exits() {
        let action = session_picker_action(session::ResumeOutcome::Cancelled, None, None)
            .expect("cancel without current session should exit");

        assert_eq!(action, SessionPickerAction::Exit(None));
    }

    #[test]
    fn in_app_session_delete_requires_known_current_session_id() {
        assert!(!in_app_session_delete_supported(true, None));
        assert!(!in_app_session_delete_supported(
            false,
            Some("current-session")
        ));
        assert!(in_app_session_delete_supported(
            true,
            Some("current-session")
        ));
    }

    #[test]
    fn unhandled_delete_request_is_an_error() {
        let err = session_picker_action(
            session::ResumeOutcome::DeleteRequested(session::SessionEntry {
                session_id: "delete-me".into(),
                cwd: PathBuf::from("/tmp/project"),
                title: None,
                updated_at: None,
            }),
            Some("current-session".to_string()),
            Some("Current title".to_string()),
        )
        .expect_err("delete outcomes must be handled before action conversion");

        assert!(err.to_string().contains("delete request was not handled"));
    }

    #[test]
    fn empty_session_picker_resumes_current_session_preserving_title() {
        let action = session_picker_empty_action(
            Some("current-session".to_string()),
            Some("Current title".to_string()),
        );

        assert_eq!(
            action,
            SessionPickerAction::Resume {
                session_id: "current-session".to_string(),
                title: Some("Current title".to_string()),
            }
        );
    }

    #[test]
    fn empty_session_picker_without_current_session_exits() {
        let action = session_picker_empty_action(None, None);

        assert_eq!(action, SessionPickerAction::Exit(None));
    }

    #[test]
    fn selecting_session_picker_entry_resumes_selected_session() {
        let action = session_picker_action(
            session::ResumeOutcome::Selected(session::SessionEntry {
                session_id: "selected-session".into(),
                cwd: PathBuf::from("/tmp/project"),
                title: Some("My selected session".to_string()),
                updated_at: None,
            }),
            Some("current-session".to_string()),
            Some("ignored current title".to_string()),
        )
        .expect("select should resume selected session");

        assert_eq!(
            action,
            SessionPickerAction::Resume {
                session_id: "selected-session".to_string(),
                title: Some("My selected session".to_string()),
            }
        );
    }

    #[test]
    fn absolutize_cwd_resolves_relative_paths() {
        let cwd = absolutize_cwd(PathBuf::from("relative/project")).expect("absolutize");
        assert!(cwd.is_absolute());
        assert!(cwd.ends_with("relative/project"));

        let absolute = std::env::current_dir()
            .expect("current dir")
            .join("already");
        assert_eq!(
            absolutize_cwd(absolute.clone()).expect("absolute"),
            absolute
        );
    }

    #[test]
    fn resume_help_shows_subcommand_info() {
        let mut cmd = Cli::command();
        let help = cmd.render_long_help().to_string();
        assert!(help.contains("resume"));
        assert!(help.contains("Resume an existing ACP session"));
    }
}
