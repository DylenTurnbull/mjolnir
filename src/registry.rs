//! ACP registry loading and Thor setup resolution.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::config::{ConfiguredAcpServer, ThorQuotaBackend};
use crate::paths::normalize_spawn_program;

pub const REGISTRY_URL: &str =
    "https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json";
pub const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Deserialize)]
pub struct Agent {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub repository: String,
    #[serde(default)]
    pub website: String,
    #[serde(default, alias = "setupHint")]
    pub setup_hint: String,
    #[serde(default)]
    pub setup: SetupMetadata,
    #[serde(default)]
    pub distribution: Distribution,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SetupMetadata {
    #[serde(default)]
    pub hint: String,
    #[serde(default, alias = "installHint")]
    pub install: String,
    #[serde(default, alias = "authHint")]
    pub auth: String,
    #[serde(default, alias = "docsUrl")]
    pub docs_url: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Distribution {
    #[serde(default)]
    pub npx: Option<NpxPackage>,
    #[serde(default)]
    pub uvx: Option<UvxPackage>,
    #[serde(default)]
    pub binary: Option<HashMap<String, BinaryTarget>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NpxPackage {
    pub package: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UvxPackage {
    pub package: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinaryTarget {
    pub archive: String,
    pub cmd: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DistributionKind {
    Npx,
    Uvx,
    Binary,
}

impl Agent {
    pub fn preferred_kind(&self) -> Option<DistributionKind> {
        if self.distribution.npx.is_some() {
            Some(DistributionKind::Npx)
        } else if self.distribution.uvx.is_some() {
            Some(DistributionKind::Uvx)
        } else if self.binary_target_for_current_platform().is_some() {
            Some(DistributionKind::Binary)
        } else {
            None
        }
    }

    pub fn to_configured_server(&self) -> Option<ConfiguredAcpServer> {
        match self.preferred_kind()? {
            DistributionKind::Npx => {
                let package = self.distribution.npx.as_ref()?;
                let mut args = vec!["-y".to_string(), package.package.clone()];
                args.extend(package.args.iter().cloned());
                Some(ConfiguredAcpServer {
                    source_id: self.id.clone(),
                    name: self.name.clone(),
                    program: normalize_spawn_program(PathBuf::from("npx")),
                    args,
                    env: package.env.clone(),
                    description: self.description.clone(),
                    setup_hint: self.setup_hint(),
                    setup_install: self.setup_install(),
                    setup_auth: self.setup_auth(),
                    setup_url: self.setup_url(),
                    quota_backend: quota_backend_for_registry_id(&self.id),
                })
            }
            DistributionKind::Uvx => {
                let package = self.distribution.uvx.as_ref()?;
                let mut args = vec![package.package.clone()];
                args.extend(package.args.iter().cloned());
                Some(ConfiguredAcpServer {
                    source_id: self.id.clone(),
                    name: self.name.clone(),
                    program: PathBuf::from("uvx"),
                    args,
                    env: package.env.clone(),
                    description: self.description.clone(),
                    setup_hint: self.setup_hint(),
                    setup_install: self.setup_install(),
                    setup_auth: self.setup_auth(),
                    setup_url: self.setup_url(),
                    quota_backend: quota_backend_for_registry_id(&self.id),
                })
            }
            DistributionKind::Binary => {
                let target = self.binary_target_for_current_platform()?;
                Some(ConfiguredAcpServer {
                    source_id: self.id.clone(),
                    name: self.name.clone(),
                    program: binary_command_name(&target.cmd),
                    args: target.args.clone(),
                    env: HashMap::new(),
                    description: self.description.clone(),
                    setup_hint: self.setup_hint(),
                    setup_install: self.setup_install(),
                    setup_auth: self.setup_auth(),
                    setup_url: self.setup_url(),
                    quota_backend: quota_backend_for_registry_id(&self.id),
                })
            }
        }
    }

    fn setup_url(&self) -> String {
        if !self.setup.docs_url.trim().is_empty() {
            return self.setup.docs_url.clone();
        }
        if !self.website.trim().is_empty() {
            self.website.clone()
        } else {
            self.repository.clone()
        }
    }

    fn setup_hint(&self) -> String {
        if !self.setup_hint.trim().is_empty() {
            return self.setup_hint.clone();
        }
        if !self.setup.hint.trim().is_empty() {
            return self.setup.hint.clone();
        }
        let exact_hint = [self.setup.install.trim(), self.setup.auth.trim()]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("; ");
        if !exact_hint.trim().is_empty() {
            return exact_hint;
        }
        if let Some(hint) = known_provider_setup_hint(&self.id) {
            return hint.to_string();
        }
        self.distribution_setup_hint()
    }

    fn setup_install(&self) -> String {
        if !self.setup.install.trim().is_empty() {
            return self.setup.install.clone();
        }
        if let Some((install, _auth)) = known_provider_setup_parts(&self.id) {
            return install.to_string();
        }
        self.distribution_install_hint()
    }

    fn setup_auth(&self) -> String {
        if !self.setup.auth.trim().is_empty() {
            return self.setup.auth.clone();
        }
        if let Some((_install, auth)) = known_provider_setup_parts(&self.id) {
            return auth.to_string();
        }
        self.distribution_auth_hint()
    }

    fn distribution_setup_hint(&self) -> String {
        [
            self.distribution_install_hint(),
            self.distribution_auth_hint(),
        ]
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join("; ")
    }

    fn distribution_install_hint(&self) -> String {
        let name = self.name.trim();
        let name = if name.is_empty() { "this agent" } else { name };
        if self.distribution.npx.is_some() {
            return "install Node.js/npm".to_string();
        }
        if self.distribution.uvx.is_some() {
            return "install uv".to_string();
        }
        if self.binary_target_for_current_platform().is_some() {
            return format!("install {name}");
        }
        String::new()
    }

    fn distribution_auth_hint(&self) -> String {
        let name = self.name.trim();
        let name = if name.is_empty() { "this agent" } else { name };
        if self.distribution.npx.is_some() || self.distribution.uvx.is_some() {
            return format!("configure or sign in to {name} if prompted");
        }
        if self.binary_target_for_current_platform().is_some() {
            return "configure or sign in if prompted".to_string();
        }
        String::new()
    }

    fn binary_target_for_current_platform(&self) -> Option<&BinaryTarget> {
        let key = current_binary_platform_key()?;
        self.distribution.binary.as_ref()?.get(key)
    }
}

fn known_provider_setup_hint(id: &str) -> Option<&'static str> {
    let _ = known_provider_setup_parts(id)?;
    // Keep the legacy combined hint stable for existing UI and configs.
    match id {
        "anvil" => Some("install uv; Brokk/Anvil signs in when required"),
        "claude-acp" => Some("install Node.js/npm; install and sign in to Claude Code"),
        "codex-acp" => Some("install Node.js/npm; sign in to Codex"),
        "gemini" => Some("install Node.js/npm; sign in with Gemini CLI"),
        "opencode" => Some("install OpenCode CLI; configure OpenCode provider credentials"),
        "goose" => Some("install Goose; configure a Goose provider"),
        "cursor" => Some("install Cursor Agent; sign in to Cursor"),
        "github-copilot-cli" => Some("install Node.js/npm; sign in to GitHub Copilot"),
        _ => None,
    }
}

fn known_provider_setup_parts(id: &str) -> Option<(&'static str, &'static str)> {
    match id {
        "anvil" => Some(("install uv", "Brokk/Anvil signs in when required")),
        "claude-acp" => Some(("install Node.js/npm", "install and sign in to Claude Code")),
        "codex-acp" => Some(("install Node.js/npm", "sign in to Codex")),
        "gemini" => Some(("install Node.js/npm", "sign in with Gemini CLI")),
        "opencode" => Some((
            "install OpenCode CLI",
            "configure OpenCode provider credentials",
        )),
        "goose" => Some(("install Goose", "configure a Goose provider")),
        "cursor" => Some(("install Cursor Agent", "sign in to Cursor")),
        "github-copilot-cli" => Some(("install Node.js/npm", "sign in to GitHub Copilot")),
        _ => None,
    }
}

fn current_binary_platform_key() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Some("darwin-aarch64"),
        ("macos", "x86_64") => Some("darwin-x86_64"),
        ("linux", "aarch64") => Some("linux-aarch64"),
        ("linux", "x86_64") => Some("linux-x86_64"),
        ("windows", "aarch64") => Some("windows-aarch64"),
        ("windows", "x86_64") => Some("windows-x86_64"),
        _ => None,
    }
}

fn binary_command_name(cmd: &str) -> PathBuf {
    let normalized = cmd.trim().trim_start_matches("./").replace('\\', "/");
    Path::new(&normalized)
        .file_name()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(cmd))
}

fn quota_backend_for_registry_id(id: &str) -> ThorQuotaBackend {
    match id {
        "claude-acp" => ThorQuotaBackend::ClaudeCli,
        "codex-acp" => ThorQuotaBackend::CodexAppserver,
        _ => ThorQuotaBackend::None,
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Registry {
    #[serde(default)]
    pub agents: Vec<Agent>,
}

impl Registry {
    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s).context("parse registry json")
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn get(&self, id: &str) -> Option<&Agent> {
        self.agents.iter().find(|agent| agent.id == id)
    }

    pub fn configured_servers(&self) -> Vec<ConfiguredAcpServer> {
        self.agents
            .iter()
            .filter_map(Agent::to_configured_server)
            .collect()
    }
}

#[allow(dead_code)]
pub fn default_cache_path() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("mj")
        .join("registry.json")
}

pub async fn load_with_cache(cache_path: &Path, ttl: Duration, url: &str) -> Result<Registry> {
    let cache_is_fresh = match cache_path.metadata() {
        Ok(meta) => match meta.modified() {
            Ok(mtime) => SystemTime::now()
                .duration_since(mtime)
                .map(|age| age < ttl)
                .unwrap_or(false),
            Err(_) => false,
        },
        Err(_) => false,
    };

    if cache_is_fresh {
        match std::fs::read_to_string(cache_path) {
            Ok(s) => match Registry::from_json(&s) {
                Ok(registry) => return Ok(registry),
                Err(error) => tracing::warn!("cached registry unparseable, refetching: {error:#}"),
            },
            Err(error) => tracing::warn!("read cached registry: {error:#}"),
        }
    }

    match fetch_fresh(url).await {
        Ok(json) => {
            if let Some(parent) = cache_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(error) = std::fs::write(cache_path, &json) {
                tracing::warn!("write registry cache {cache_path:?}: {error:#}");
            }
            Registry::from_json(&json)
        }
        Err(fetch_error) => match std::fs::read_to_string(cache_path) {
            Ok(s) => {
                tracing::warn!(
                    "registry fetch failed ({fetch_error:#}); using cached copy at {cache_path:?}"
                );
                Registry::from_json(&s)
            }
            Err(_) => Err(fetch_error.context("no cached registry available")),
        },
    }
}

pub(crate) async fn fetch_fresh(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent(concat!("mj/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("build http client")?;
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("GET {url}: HTTP {status}");
    }
    resp.text().await.context("read registry body")
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"{
        "agents": [
            {
                "id": "claude-acp",
                "name": "Claude",
                "version": "0.36.1",
                "description": "Claude ACP agent",
                "repository": "https://github.com/agentclientprotocol/claude-agent-acp",
                "distribution": {
                    "npx": { "package": "@agentclientprotocol/claude-agent-acp@0.36.1" }
                }
            },
            {
                "id": "codex-acp",
                "name": "Codex",
                "version": "0.14.0",
                "description": "Codex ACP agent",
                "distribution": {
                    "npx": { "package": "@zed-industries/codex-acp@0.14.0" }
                }
            },
            {
                "id": "gemini",
                "name": "Gemini CLI",
                "version": "0.49.0",
                "description": "Gemini ACP agent",
                "website": "https://geminicli.com",
                "distribution": {
                    "npx": { "package": "@google/gemini-cli@0.49.0", "args": ["--acp"] }
                }
            },
            {
                "id": "generic-npx",
                "name": "Generic NPM Agent",
                "version": "1.0.0",
                "distribution": {
                    "npx": { "package": "generic-npm-agent@1.0.0", "args": ["--acp"] }
                }
            },
            {
                "id": "generic-acp",
                "name": "Generic",
                "version": "1.0.0",
                "website": "https://example.com/generic",
                "setup": {
                    "install": "install Generic CLI",
                    "auth": "run generic login",
                    "docsUrl": "https://example.com/generic/setup"
                },
                "distribution": {
                    "uvx": { "package": "generic-acp==1.0.0", "args": ["--acp"] }
                }
            },
            {
                "id": "binary-only",
                "name": "Binary",
                "version": "1.0.0",
                "website": "https://example.com/binary",
                "distribution": {
                    "binary": {
                        "darwin-aarch64": {
                            "archive": "https://example.com/bin.tar.gz",
                            "cmd": "./bin",
                            "args": ["acp"]
                        },
                        "darwin-x86_64": {
                            "archive": "https://example.com/bin.tar.gz",
                            "cmd": "./bin",
                            "args": ["acp"]
                        },
                        "linux-aarch64": {
                            "archive": "https://example.com/bin.tar.gz",
                            "cmd": "./bin",
                            "args": ["acp"]
                        },
                        "linux-x86_64": {
                            "archive": "https://example.com/bin.tar.gz",
                            "cmd": "./bin",
                            "args": ["acp"]
                        },
                        "windows-aarch64": {
                            "archive": "https://example.com/bin.zip",
                            "cmd": ".\\package\\bin.exe",
                            "args": ["acp"]
                        },
                        "windows-x86_64": {
                            "archive": "https://example.com/bin.zip",
                            "cmd": ".\\package\\bin.exe",
                            "args": ["acp"]
                        }
                    }
                }
            }
        ]
    }"#;

    #[test]
    fn registry_entries_resolve_to_configured_acp_servers_with_quota_metadata() {
        let registry = Registry::from_json(FIXTURE).expect("parse");
        let servers = registry.configured_servers();

        let claude = servers
            .iter()
            .find(|server| server.source_id == "claude-acp")
            .expect("claude");
        assert_eq!(
            claude.program,
            normalize_spawn_program(PathBuf::from("npx"))
        );
        assert_eq!(claude.quota_backend, ThorQuotaBackend::ClaudeCli);
        assert_eq!(
            claude.setup_url,
            "https://github.com/agentclientprotocol/claude-agent-acp"
        );
        assert_eq!(
            claude.setup_hint,
            "install Node.js/npm; install and sign in to Claude Code"
        );
        assert_eq!(claude.setup_install, "install Node.js/npm");
        assert_eq!(claude.setup_auth, "install and sign in to Claude Code");

        let codex = servers
            .iter()
            .find(|server| server.source_id == "codex-acp")
            .expect("codex");
        assert_eq!(codex.quota_backend, ThorQuotaBackend::CodexAppserver);
        assert_eq!(codex.setup_hint, "install Node.js/npm; sign in to Codex");
        assert_eq!(codex.setup_install, "install Node.js/npm");
        assert_eq!(codex.setup_auth, "sign in to Codex");

        let gemini = servers
            .iter()
            .find(|server| server.source_id == "gemini")
            .expect("gemini");
        assert_eq!(
            gemini.setup_hint,
            "install Node.js/npm; sign in with Gemini CLI"
        );
        assert_eq!(gemini.setup_install, "install Node.js/npm");
        assert_eq!(gemini.setup_auth, "sign in with Gemini CLI");
        assert_eq!(gemini.setup_url, "https://geminicli.com");

        let generic_npx = servers
            .iter()
            .find(|server| server.source_id == "generic-npx")
            .expect("generic npx");
        assert_eq!(
            generic_npx.setup_hint,
            "install Node.js/npm; configure or sign in to Generic NPM Agent if prompted"
        );
        assert_eq!(generic_npx.setup_install, "install Node.js/npm");
        assert_eq!(
            generic_npx.setup_auth,
            "configure or sign in to Generic NPM Agent if prompted"
        );

        let generic = servers
            .iter()
            .find(|server| server.source_id == "generic-acp")
            .expect("generic");
        assert_eq!(generic.program, PathBuf::from("uvx"));
        assert_eq!(generic.args, vec!["generic-acp==1.0.0", "--acp"]);
        assert_eq!(generic.quota_backend, ThorQuotaBackend::None);
        assert_eq!(generic.setup_url, "https://example.com/generic/setup");
        assert_eq!(generic.setup_hint, "install Generic CLI; run generic login");
        assert_eq!(generic.setup_install, "install Generic CLI");
        assert_eq!(generic.setup_auth, "run generic login");

        let binary = servers
            .iter()
            .find(|server| server.source_id == "binary-only")
            .expect("binary");
        if cfg!(windows) {
            assert_eq!(binary.program, PathBuf::from("bin.exe"));
        } else {
            assert_eq!(binary.program, PathBuf::from("bin"));
        }
        assert_eq!(binary.args, vec!["acp"]);
        assert_eq!(binary.setup_url, "https://example.com/binary");
        assert_eq!(
            binary.setup_hint,
            "install Binary; configure or sign in if prompted"
        );
        assert_eq!(binary.setup_install, "install Binary");
        assert_eq!(binary.setup_auth, "configure or sign in if prompted");
    }

    #[test]
    fn registry_exact_setup_metadata_overrides_known_provider_fallback() {
        let registry = Registry::from_json(
            r#"{
                "agents": [{
                    "id": "gemini",
                    "name": "Gemini CLI",
                    "setup": {
                        "install": "install exact Gemini package",
                        "auth": "run exact Gemini login",
                        "docsUrl": "https://example.com/exact-gemini"
                    },
                    "distribution": {
                        "npx": { "package": "@google/gemini-cli@0.49.0", "args": ["--acp"] }
                    }
                }]
            }"#,
        )
        .expect("parse");

        let server = registry
            .configured_servers()
            .into_iter()
            .next()
            .expect("server");

        assert_eq!(
            server.setup_hint,
            "install exact Gemini package; run exact Gemini login"
        );
        assert_eq!(server.setup_install, "install exact Gemini package");
        assert_eq!(server.setup_auth, "run exact Gemini login");
        assert_eq!(server.setup_url, "https://example.com/exact-gemini");
    }

    #[tokio::test]
    async fn load_with_cache_uses_fresh_cache_without_network() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("registry.json");
        std::fs::write(&path, FIXTURE).expect("write cache");

        let registry = load_with_cache(&path, Duration::from_secs(3600), "http://127.0.0.1:1/")
            .await
            .expect("load");
        assert_eq!(registry.agents.len(), 6);
    }

    #[tokio::test]
    async fn load_with_cache_falls_back_to_stale_cache_when_offline() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("registry.json");
        std::fs::write(&path, FIXTURE).expect("write cache");
        let old = SystemTime::now() - Duration::from_secs(48 * 3600);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open")
            .set_modified(old)
            .expect("set_modified");

        let registry = load_with_cache(&path, Duration::from_secs(3600), "http://127.0.0.1:1/")
            .await
            .expect("load stale");
        assert_eq!(registry.agents.len(), 6);
    }
}
