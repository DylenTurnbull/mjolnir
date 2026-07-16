//! Persistent user config for `mj`.
//!
//! Stores role-owned Council preferences and custom ACP launches. Lives at
//! `~/.config/mj/config.toml`.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::paths::{expand_home_shortcut, normalize_spawn_program};
use crate::spinner::SpinnerStyle;
use crate::theme::TerminalThemeKind;

pub const DISABLED_MODEL: &str = "disabled";
pub const CONFIG_VERSION: u32 = 2;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleModelOverrides {
    pub thor: Option<String>,
    pub loki: Option<String>,
    pub eitri: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Config {
    pub version: u32,
    #[serde(default, skip_serializing_if = "TerminalThemeKind::is_default")]
    pub theme: TerminalThemeKind,
    #[serde(default, skip_serializing_if = "SpinnerStyle::is_default")]
    pub spinner: SpinnerStyle,
    /// Thor's coordination and review behavior.
    #[serde(default, skip_serializing_if = "ThorConfig::is_default")]
    pub thor: ThorConfig,
    /// Loki's model preference; `disabled` turns the role off.
    #[serde(default, skip_serializing_if = "LokiConfig::is_default")]
    pub loki: LokiConfig,
    /// Eitri's model preference.
    #[serde(default, skip_serializing_if = "EitriConfig::is_default")]
    pub eitri: EitriConfig,
    /// Council-wide runtime behavior.
    #[serde(default, skip_serializing_if = "CouncilConfig::is_default")]
    pub council: CouncilConfig,
    /// ACP adapter enablement and explicit user-provisioned servers.
    #[serde(default, skip_serializing_if = "AcpConfig::is_default")]
    pub acp: AcpConfig,
    /// `/ragnarok` battle knobs.
    #[serde(default, skip_serializing_if = "RagnarokConfig::is_default")]
    pub ragnarok: RagnarokConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            theme: TerminalThemeKind::default(),
            spinner: SpinnerStyle::default(),
            thor: ThorConfig::default(),
            loki: LokiConfig::default(),
            eitri: EitriConfig::default(),
            council: CouncilConfig::default(),
            acp: AcpConfig::default(),
            ragnarok: RagnarokConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct CouncilConfig {
    #[serde(default = "default_true")]
    pub auto_failover: bool,
    #[serde(default)]
    pub permission_mode: CouncilPermissionMode,
}

impl Default for CouncilConfig {
    fn default() -> Self {
        Self {
            auto_failover: true,
            permission_mode: CouncilPermissionMode::default(),
        }
    }
}

impl CouncilConfig {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CouncilPermissionMode {
    Manual,
    #[default]
    Auto,
    Yolo,
}

impl CouncilPermissionMode {
    pub const ALL: [Self; 3] = [Self::Manual, Self::Auto, Self::Yolo];

    pub fn label(self) -> &'static str {
        match self {
            Self::Manual => "Manual",
            Self::Auto => "Auto",
            Self::Yolo => "YOLO",
        }
    }
}

impl std::fmt::Display for CouncilPermissionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

fn default_auto() -> String {
    "auto".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ModelsConfig {
    #[serde(default = "default_auto")]
    pub thor: String,
    #[serde(default = "default_auto")]
    pub loki: String,
    #[serde(default = "default_auto")]
    pub eitri: String,
}

impl Default for ModelsConfig {
    fn default() -> Self {
        Self {
            thor: default_auto(),
            loki: default_auto(),
            eitri: default_auto(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ThorConfig {
    #[serde(default = "default_auto")]
    pub model: String,
    #[serde(default = "default_true")]
    pub discrete_review: bool,
}

impl Default for ThorConfig {
    fn default() -> Self {
        Self {
            model: default_auto(),
            discrete_review: true,
        }
    }
}

impl ThorConfig {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct LokiConfig {
    #[serde(default = "default_auto")]
    pub model: String,
}

impl Default for LokiConfig {
    fn default() -> Self {
        Self {
            model: default_auto(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EitriConfig {
    #[serde(default = "default_auto")]
    pub model: String,
    #[serde(default = "default_max_parallel_explores")]
    pub max_parallel_explores: usize,
}

impl Default for EitriConfig {
    fn default() -> Self {
        Self {
            model: default_auto(),
            max_parallel_explores: default_max_parallel_explores(),
        }
    }
}

fn default_max_parallel_explores() -> usize {
    6
}

impl EitriConfig {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AcpConfig {
    /// Policy overrides for built-in auto-detected servers. Missing means Auto.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub policies: BTreeMap<String, AcpServerPolicy>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub servers: Vec<ConfiguredAcpServer>,
}

impl AcpConfig {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcpServerPolicy {
    #[default]
    Auto,
    Enabled,
    Disabled,
}

impl std::fmt::Display for AcpServerPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Enabled => f.write_str("on"),
            Self::Disabled => f.write_str("off"),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcpServerOrigin {
    Registry,
    Custom,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredAcpServer {
    pub id: String,
    pub label: String,
    pub command: PathBuf,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    pub origin: AcpServerOrigin,
    #[serde(default = "enabled_policy", skip_serializing_if = "is_enabled_policy")]
    pub policy: AcpServerPolicy,
}

fn enabled_policy() -> AcpServerPolicy {
    AcpServerPolicy::Enabled
}

fn is_enabled_policy(policy: &AcpServerPolicy) -> bool {
    *policy == AcpServerPolicy::Enabled
}

impl LokiConfig {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// Knobs for `/ragnarok` battles.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RagnarokConfig {
    /// Hard cap on how many champions Thor may field (2-10). Thor still
    /// decides the count from task complexity; this caps the bill.
    #[serde(default = "default_max_competitors")]
    pub max_competitors: usize,
}

fn default_max_competitors() -> usize {
    10
}

impl Default for RagnarokConfig {
    fn default() -> Self {
        Self {
            max_competitors: default_max_competitors(),
        }
    }
}

impl RagnarokConfig {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

fn default_true() -> bool {
    true
}

/// Concrete ACP launch selected by the model catalog for a session.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SelectedAgent {
    pub source_id: String,
    pub program: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
}

impl Config {
    pub fn path_has_current_version(path: &Path) -> bool {
        let Ok(contents) = std::fs::read_to_string(path) else {
            return false;
        };
        toml::from_str::<toml::Value>(&contents)
            .ok()
            .and_then(|document| document.get("version").and_then(toml::Value::as_integer))
            == Some(i64::from(CONFIG_VERSION))
    }

    pub fn apply_role_model_overrides(&mut self, overrides: &RoleModelOverrides) {
        if let Some(model) = &overrides.thor {
            self.thor.model.clone_from(model);
        }
        if let Some(model) = &overrides.loki {
            self.loki.model.clone_from(model);
        }
        if let Some(model) = &overrides.eitri {
            self.eitri.model.clone_from(model);
        }
    }

    pub fn set_acp_server_policy(&mut self, id: &str, policy: AcpServerPolicy) -> bool {
        if matches!(id, "codex-acp" | "claude-acp" | "anvil" | "opencode-acp") {
            if policy == AcpServerPolicy::Auto {
                self.acp.policies.remove(id);
            } else {
                self.acp.policies.insert(id.to_string(), policy);
            }
            return true;
        }
        let Some(server) = self.acp.servers.iter_mut().find(|server| server.id == id) else {
            return false;
        };
        server.policy = policy;
        true
    }

    pub fn role_models(&self) -> ModelsConfig {
        ModelsConfig {
            thor: self.thor.model.clone(),
            loki: self.loki.model.clone(),
            eitri: self.eitri.model.clone(),
        }
    }

    /// Read the config from `path`. Returns `Config::default()` when the
    /// file does not exist; surfaces a parse error otherwise.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let s =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let document: toml::Value =
            toml::from_str(&s).with_context(|| format!("parse {}", path.display()))?;
        let version = document.get("version").and_then(toml::Value::as_integer);
        if version != Some(i64::from(CONFIG_VERSION)) {
            tracing::warn!(
                path = %path.display(),
                found_version = ?version,
                expected_version = CONFIG_VERSION,
                "ignoring incompatible config and starting fresh"
            );
            return Ok(Self::default());
        }
        let mut cfg: Self =
            toml::from_str(&s).with_context(|| format!("parse {}", path.display()))?;
        cfg.normalize()?;
        Ok(cfg)
    }

    /// Atomic-ish save: write to a tmp sibling then rename. Creates the
    /// parent directory on demand.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create config dir {}", parent.display()))?;
        }
        let body = toml::to_string_pretty(self).context("serialize config")?;
        let tmp = path.with_extension("toml.tmp");
        std::fs::write(&tmp, body).with_context(|| format!("write {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }

    fn normalize(&mut self) -> Result<()> {
        if self.loki.model.eq_ignore_ascii_case("none") {
            self.loki.model = DISABLED_MODEL.to_string();
        }
        if self.eitri.model.eq_ignore_ascii_case("none") {
            self.eitri.model = DISABLED_MODEL.to_string();
        }
        anyhow::ensure!(
            self.eitri.max_parallel_explores <= 16,
            "eitri.max_parallel_explores must be between 0 and 16"
        );

        let mut names = std::collections::HashSet::new();
        for server in &mut self.acp.servers {
            let valid_name = !server.id.is_empty()
                && server
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':'));
            anyhow::ensure!(
                valid_name,
                "ACP server id '{}' contains unsupported characters",
                server.id
            );
            anyhow::ensure!(
                names.insert(server.id.clone()),
                "duplicate configured ACP server id '{}'",
                server.id
            );
            anyhow::ensure!(
                !server.command.as_os_str().is_empty(),
                "configured ACP server '{}' has an empty command",
                server.id
            );
            server.command = expand_home_shortcut(&server.command.to_string_lossy());
            server.command = normalize_spawn_program(server.command.clone());
            server.args = server
                .args
                .iter()
                .map(|arg| expand_home_shortcut(arg).to_string_lossy().into_owned())
                .collect();
        }
        Ok(())
    }
}

impl AcpConfig {
    pub fn policy(&self, id: &str) -> AcpServerPolicy {
        self.policies.get(id).copied().unwrap_or_default()
    }
}

/// Default config path: `$XDG_CONFIG_HOME/mj/config.toml` (or
/// `~/.config/mj/config.toml` when `XDG_CONFIG_HOME` is unset).
pub fn default_config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from(".config"))
        .join("mj")
        .join("config.toml")
}

/// Directory for exported conversation transcripts:
/// `$XDG_CONFIG_HOME/mj/transcripts`.
pub fn transcript_export_dir() -> Option<PathBuf> {
    dirs::config_dir().map(|dir| dir.join("mj").join("transcripts"))
}

/// Path for the persisted prompt-history file (NUL-delimited format to
/// support multiline prompts): `$XDG_CONFIG_HOME/mj/history.txt`.
pub fn history_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from(".config"))
        .join("mj")
        .join("history.txt")
}

/// Maximum number of history entries kept on disk. Older entries are
/// trimmed when the limit is exceeded.
pub const HISTORY_MAX_ENTRIES: usize = 100;

/// Load the prompt history from a NUL-delimited file (supports multiline
/// prompts). Returns an empty `Vec` when the file does not exist or is
/// unreadable.
pub fn load_history(path: &Path) -> Vec<String> {
    match std::fs::read_to_string(path).map_err(|e| tracing::warn!("load_history {path:?}: {e}")) {
        Ok(body) => body
            .split('\0')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Persist the prompt history to disk in NUL-delimited format, capped
/// at `HISTORY_MAX_ENTRIES`.
pub fn save_history(path: &Path, entries: &[String]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create history dir {}", parent.display()))?;
    }
    let tail = if entries.len() > HISTORY_MAX_ENTRIES {
        &entries[entries.len() - HISTORY_MAX_ENTRIES..]
    } else {
        entries
    };
    let body = tail.join("\0");
    std::fs::write(path, body).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_history_returns_empty_for_missing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("history.txt");
        let entries = load_history(&path);
        assert!(entries.is_empty());
    }

    #[test]
    fn load_save_history_roundtrips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("history.txt");
        let entries: Vec<String> = (0..5).map(|i| format!("prompt {i}")).collect();
        save_history(&path, &entries).expect("save");
        let loaded = load_history(&path);
        assert_eq!(loaded, entries);
    }

    #[test]
    fn save_history_caps_at_max_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("history.txt");
        let entries: Vec<String> = (0..120).map(|i| format!("prompt {i}")).collect();
        save_history(&path, &entries).expect("save");
        let loaded = load_history(&path);
        assert_eq!(loaded.len(), HISTORY_MAX_ENTRIES);
        // Keeps the most recent entries (tail).
        assert_eq!(loaded[0], format!("prompt {}", 120 - HISTORY_MAX_ENTRIES));
        assert_eq!(loaded[loaded.len() - 1], "prompt 119");
    }

    #[test]
    fn save_history_creates_parent_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("deep").join("history.txt");
        save_history(&path, &["hi".to_string()]).expect("save");
        assert_eq!(load_history(&path), vec!["hi".to_string()]);
    }

    #[test]
    fn save_load_history_preserves_multiline_prompts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("history.txt");
        let entries = vec![
            "single line".to_string(),
            "line one\nline two\nline three".to_string(),
            "another single".to_string(),
        ];
        save_history(&path, &entries).expect("save");
        let loaded = load_history(&path);
        assert_eq!(loaded, entries);
    }

    #[test]
    fn save_empty_history_writes_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("history.txt");
        save_history(&path, &[]).expect("save");
        let body = std::fs::read_to_string(&path).expect("read");
        assert_eq!(body, "");
        let loaded = load_history(&path);
        assert!(loaded.is_empty());
    }

    #[test]
    fn ragnarok_max_competitors_roundtrips_and_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");

        // Default cap is omitted from the serialized form.
        Config::default().save(&path).expect("save default");
        let body = std::fs::read_to_string(&path).expect("read");
        assert!(
            !body.contains("ragnarok"),
            "default ragnarok config should not be serialized: {body:?}"
        );
        assert_eq!(
            Config::load(&path).expect("load").ragnarok.max_competitors,
            10
        );

        // A custom cap survives the round trip.
        std::fs::write(
            &path,
            format!("version = {CONFIG_VERSION}\n[ragnarok]\nmax_competitors = 3\n"),
        )
        .expect("write");
        let cfg = Config::load(&path).expect("load custom");
        assert_eq!(cfg.ragnarok.max_competitors, 3);
        cfg.save(&path).expect("save custom");
        let body = std::fs::read_to_string(&path).expect("read saved");
        assert!(body.contains("max_competitors = 3"), "body: {body:?}");
    }

    #[test]
    fn load_missing_file_returns_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nope.toml");
        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg.theme, TerminalThemeKind::Dark);
        assert_eq!(cfg.role_models(), ModelsConfig::default());
        assert!(cfg.thor.discrete_review);
        assert_eq!(cfg.loki.model, "auto");
    }

    #[test]
    fn incompatible_config_starts_fresh() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[thor]\ndiscrete_review = false\n\n[loki]\nstreaming_review = false\n",
        )
        .expect("write config");

        let cfg = Config::load(&path).expect("load config");
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn only_current_schema_counts_as_an_existing_config() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "version = 1\n").expect("old config");
        assert!(!Config::path_has_current_version(&path));
        Config::default().save(&path).expect("current config");
        assert!(Config::path_has_current_version(&path));
    }

    #[test]
    fn save_then_load_roundtrips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let cfg = Config {
            theme: TerminalThemeKind::Light,
            thor: ThorConfig {
                model: "gpt-5-6-sol".to_string(),
                discrete_review: false,
            },
            council: CouncilConfig {
                auto_failover: false,
                permission_mode: CouncilPermissionMode::Manual,
            },
            acp: AcpConfig {
                servers: vec![ConfiguredAcpServer {
                    id: "custom:company".to_string(),
                    label: "company".to_string(),
                    command: PathBuf::from("/usr/local/bin/company-acp"),
                    args: vec!["--stdio".to_string()],
                    env: HashMap::new(),
                    origin: AcpServerOrigin::Custom,
                    policy: AcpServerPolicy::Enabled,
                }],
                ..AcpConfig::default()
            },
            ..Config::default()
        };
        cfg.save(&path).expect("save");
        let loaded = Config::load(&path).expect("load");
        assert_eq!(loaded.theme, TerminalThemeKind::Light);
        assert_eq!(loaded.thor.model, "gpt-5-6-sol");
        assert!(!loaded.thor.discrete_review);
        assert!(!loaded.council.auto_failover);
        assert_eq!(
            loaded.council.permission_mode,
            CouncilPermissionMode::Manual
        );
        assert_eq!(loaded.acp.servers[0].id, "custom:company");
        assert_eq!(loaded.acp.servers[0].args, vec!["--stdio"]);
    }

    #[test]
    fn role_model_overrides_do_not_mutate_the_source_config() {
        let saved = Config::default();
        let mut invocation = saved.clone();
        invocation.apply_role_model_overrides(&RoleModelOverrides {
            thor: Some("gpt-test".to_string()),
            loki: Some(DISABLED_MODEL.to_string()),
            eitri: Some("qwen-test".to_string()),
        });

        assert_eq!(saved.role_models(), ModelsConfig::default());
        assert_eq!(invocation.thor.model, "gpt-test");
        assert_eq!(invocation.loki.model, DISABLED_MODEL);
        assert_eq!(invocation.eitri.model, "qwen-test");
    }

    #[test]
    fn save_creates_parent_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("deep").join("config.toml");
        let cfg = Config {
            theme: TerminalThemeKind::Dark,
            ..Config::default()
        };
        cfg.save(&path).expect("save");
        assert!(path.exists());
    }

    #[test]
    fn missing_version_discards_old_model_table() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[models]
thor = "gpt-5-6-sol"
loki = "claude-opus-4-8"
eitri = "gpt-5-6-luna"
"#,
        )
        .expect("write");

        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn load_parse_error_is_surfaced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, b"not = valid = toml = @@@").expect("write");
        let err = Config::load(&path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("parse"), "error mentions parse: {msg}");
    }

    #[test]
    fn load_expands_home_shortcuts_in_configured_servers() {
        let Some(home) = dirs::home_dir() else {
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
version = 2
[[acp.servers]]
id = "custom:my-agent"
label = "my-agent"
command = "~/bin/agent"
args = ["--config", "$HOME/.config/agent.toml"]
origin = "custom"
"#,
        )
        .expect("write");

        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg.acp.servers.len(), 1);
        let server = &cfg.acp.servers[0];
        assert_eq!(server.command, home.join("bin/agent"));
        assert_eq!(
            server.args,
            vec![
                "--config".to_string(),
                home.join(".config/agent.toml").display().to_string(),
            ]
        );
    }

    #[test]
    fn configured_acp_servers_validate_ids_commands_and_duplicates() {
        for (body, expected) in [
            (
                "version = 2\n[[acp.servers]]\nid = 'bad name'\nlabel = 'bad'\ncommand = 'server'\norigin = 'custom'\n",
                "unsupported characters",
            ),
            (
                "version = 2\n[[acp.servers]]\nid = 'empty'\nlabel = 'empty'\ncommand = ''\norigin = 'custom'\n",
                "empty command",
            ),
            (
                "version = 2\n[[acp.servers]]\nid = 'same'\nlabel = 'one'\ncommand = 'one'\norigin = 'custom'\n[[acp.servers]]\nid = 'same'\nlabel = 'two'\ncommand = 'two'\norigin = 'custom'\n",
                "duplicate",
            ),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("config.toml");
            std::fs::write(&path, body).expect("write");
            let error = Config::load(&path).expect_err("invalid custom server");
            assert!(error.to_string().contains(expected), "{error:#}");
        }
    }

    #[test]
    fn incompatible_version_is_discarded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
agent = "legacy"
favorite_agents = ["old"]

[scores]
source = "arena"

[session_config.old]
mode = "ask"
"#,
        )
        .expect("write");
        let config = Config::load(&path).expect("load incompatible config");
        assert_eq!(config, Config::default());
    }

    #[test]
    fn server_policies_update_builtins_and_configured_servers() {
        let mut config = Config {
            acp: AcpConfig {
                servers: vec![ConfiguredAcpServer {
                    id: "custom:company".to_string(),
                    label: "company".to_string(),
                    command: PathBuf::from("company-acp"),
                    args: Vec::new(),
                    env: HashMap::new(),
                    origin: AcpServerOrigin::Custom,
                    policy: AcpServerPolicy::Enabled,
                }],
                ..AcpConfig::default()
            },
            ..Config::default()
        };

        assert!(config.set_acp_server_policy("codex-acp", AcpServerPolicy::Disabled));
        assert!(config.set_acp_server_policy("custom:company", AcpServerPolicy::Disabled));
        assert!(!config.set_acp_server_policy("custom:missing", AcpServerPolicy::Disabled));
        assert_eq!(config.acp.policy("codex-acp"), AcpServerPolicy::Disabled);
        assert_eq!(config.acp.servers[0].policy, AcpServerPolicy::Disabled);
    }

    #[test]
    fn default_config_serializes_only_its_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        Config::default().save(&path).expect("save");
        let body = std::fs::read_to_string(&path).expect("read");
        assert!(body.contains("version = 2"), "config: {body:?}");
        assert!(
            !body.contains("theme"),
            "default theme should not be serialized: {body:?}"
        );
    }

    #[test]
    fn theme_config_defaulting_and_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "").expect("write");
        let cfg = Config::load(&path).expect("load default");
        assert_eq!(cfg.theme, TerminalThemeKind::Dark);

        let cfg = Config {
            theme: TerminalThemeKind::AnsiLight,
            ..Config::default()
        };
        cfg.save(&path).expect("save");
        let body = std::fs::read_to_string(&path).expect("read");
        assert!(body.contains("theme = \"ansi-light\""));

        let loaded = Config::load(&path).expect("load saved");
        assert_eq!(loaded.theme, TerminalThemeKind::AnsiLight);
    }

    #[test]
    fn spinner_config_defaulting_and_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "").expect("write");
        let cfg = Config::load(&path).expect("load default");
        assert_eq!(cfg.spinner, SpinnerStyle::default());

        // Default style is omitted from the serialized form.
        Config::default().save(&path).expect("save default");
        let body = std::fs::read_to_string(&path).expect("read");
        assert!(
            !body.contains("spinner"),
            "default spinner should not be serialized: {body:?}"
        );

        let cfg = Config {
            spinner: SpinnerStyle::Bars,
            ..Config::default()
        };
        cfg.save(&path).expect("save");
        let body = std::fs::read_to_string(&path).expect("read");
        assert!(body.contains("spinner = \"bars\""));

        let loaded = Config::load(&path).expect("load saved");
        assert_eq!(loaded.spinner, SpinnerStyle::Bars);
    }
}
