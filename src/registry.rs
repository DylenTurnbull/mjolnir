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
    #[serde(default)]
    pub distribution: Distribution,
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
}

impl Agent {
    pub fn preferred_kind(&self) -> Option<DistributionKind> {
        if self.distribution.npx.is_some() {
            Some(DistributionKind::Npx)
        } else if self.distribution.uvx.is_some() {
            Some(DistributionKind::Uvx)
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
                    setup_url: self.setup_url(),
                    quota_backend: quota_backend_for_registry_id(&self.id),
                })
            }
        }
    }

    fn setup_url(&self) -> String {
        if !self.website.trim().is_empty() {
            self.website.clone()
        } else {
            self.repository.clone()
        }
    }
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
                "id": "generic-acp",
                "name": "Generic",
                "version": "1.0.0",
                "website": "https://example.com/generic",
                "distribution": {
                    "uvx": { "package": "generic-acp==1.0.0", "args": ["--acp"] }
                }
            },
            {
                "id": "binary-only",
                "name": "Binary",
                "version": "1.0.0",
                "distribution": {
                    "binary": {
                        "darwin-aarch64": {
                            "archive": "https://example.com/bin.tar.gz",
                            "cmd": "./bin"
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

        let codex = servers
            .iter()
            .find(|server| server.source_id == "codex-acp")
            .expect("codex");
        assert_eq!(codex.quota_backend, ThorQuotaBackend::CodexAppserver);

        let generic = servers
            .iter()
            .find(|server| server.source_id == "generic-acp")
            .expect("generic");
        assert_eq!(generic.program, PathBuf::from("uvx"));
        assert_eq!(generic.args, vec!["generic-acp==1.0.0", "--acp"]);
        assert_eq!(generic.quota_backend, ThorQuotaBackend::None);
        assert_eq!(generic.setup_url, "https://example.com/generic");

        assert!(
            !servers
                .iter()
                .any(|server| server.source_id == "binary-only")
        );
    }

    #[tokio::test]
    async fn load_with_cache_uses_fresh_cache_without_network() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("registry.json");
        std::fs::write(&path, FIXTURE).expect("write cache");

        let registry = load_with_cache(&path, Duration::from_secs(3600), "http://127.0.0.1:1/")
            .await
            .expect("load");
        assert_eq!(registry.agents.len(), 4);
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
        assert_eq!(registry.agents.len(), 4);
    }
}
