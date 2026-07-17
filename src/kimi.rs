//! Discovery and installation of the official Kimi Code ACP binary.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::registry::{Agent, BinaryTarget};

static INSTALL_STATE: LazyLock<Mutex<InstallState>> =
    LazyLock::new(|| Mutex::new(InstallState::Idle));

#[derive(Debug, Clone)]
enum InstallState {
    Idle,
    Installing,
    Ready(ManagedLaunch),
    Failed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManagedLaunch {
    version: String,
    command: PathBuf,
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct Detection {
    pub path: Option<PathBuf>,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub evidence: String,
    pub installing: bool,
    pub error: Option<String>,
}

pub fn detect() -> Detection {
    if let Some(path) = crate::auth::executable(crate::auth::AuthVendor::Kimi) {
        return Detection {
            path: Some(path),
            args: vec!["acp".to_string()],
            env: HashMap::new(),
            evidence: "Kimi Code on PATH".to_string(),
            installing: false,
            error: None,
        };
    }
    if let Some(launch) = read_manifest().filter(valid_launch) {
        return detected_managed(launch);
    }
    let state = INSTALL_STATE
        .lock()
        .map(|state| state.clone())
        .unwrap_or_else(|_| InstallState::Failed("Kimi installer state is unavailable".into()));
    match state {
        InstallState::Idle => Detection {
            path: None,
            args: vec!["acp".to_string()],
            env: HashMap::new(),
            evidence: "Kimi Code is not installed".to_string(),
            installing: false,
            error: None,
        },
        InstallState::Installing => Detection {
            path: None,
            args: vec!["acp".to_string()],
            env: HashMap::new(),
            evidence: "installing managed Kimi Code".to_string(),
            installing: true,
            error: None,
        },
        InstallState::Ready(launch) => detected_managed(launch),
        InstallState::Failed(error) => Detection {
            path: None,
            args: vec!["acp".to_string()],
            env: HashMap::new(),
            evidence: "managed Kimi Code install failed".to_string(),
            installing: false,
            error: Some(error),
        },
    }
}

fn detected_managed(launch: ManagedLaunch) -> Detection {
    Detection {
        path: Some(launch.command),
        args: launch.args,
        env: launch.env,
        evidence: format!("managed Kimi Code {}", launch.version),
        installing: false,
        error: None,
    }
}

pub fn start_background_install() {
    let detection = detect();
    if detection.path.is_some() || detection.installing {
        return;
    }
    if let Ok(mut state) = INSTALL_STATE.lock() {
        if !matches!(*state, InstallState::Idle | InstallState::Failed(_)) {
            return;
        }
        *state = InstallState::Installing;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        set_failure("Kimi background installation requires an async runtime".into());
        return;
    };
    runtime.spawn(async {
        match install_latest().await {
            Ok(launch) => {
                if let Ok(mut state) = INSTALL_STATE.lock() {
                    *state = InstallState::Ready(launch);
                }
            }
            Err(error) => set_failure(format!("{error:#}")),
        }
    });
}

pub async fn wait_until_ready() -> Result<PathBuf> {
    start_background_install();
    loop {
        let detection = detect();
        if let Some(path) = detection.path {
            return Ok(path);
        }
        if let Some(error) = detection.error {
            anyhow::bail!(error);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn set_failure(error: String) {
    if let Ok(mut state) = INSTALL_STATE.lock() {
        *state = InstallState::Failed(error);
    }
}

async fn install_latest() -> Result<ManagedLaunch> {
    let registry = crate::registry::load().await?;
    let agent = registry
        .agents
        .into_iter()
        .find(|agent| agent.id == "kimi")
        .context("Kimi Code is absent from the ACP registry")?;
    install_agent(agent).await
}

async fn install_agent(agent: Agent) -> Result<ManagedLaunch> {
    let platform = crate::registry::current_platform();
    let target: BinaryTarget = agent
        .distribution
        .binary
        .and_then(|targets| targets.get(&platform).cloned())
        .with_context(|| format!("no Kimi Code binary for {platform}"))?;
    let (progress_tx, _progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let (command, args) =
        crate::install::install_or_resolve("kimi", &agent.version, &target, progress_tx).await?;
    let launch = ManagedLaunch {
        version: agent.version,
        command,
        args,
        env: target.env,
    };
    write_manifest(&launch)?;
    Ok(launch)
}

fn manifest_path() -> PathBuf {
    crate::install::default_install_root()
        .join("kimi")
        .join("current.json")
}

fn read_manifest() -> Option<ManagedLaunch> {
    let bytes = std::fs::read(manifest_path()).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_manifest(launch: &ManagedLaunch) -> Result<()> {
    let path = manifest_path();
    let parent = path.parent().context("Kimi manifest has no parent")?;
    std::fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temp, launch)?;
    temp.persist(&path)
        .map_err(|error| error.error)
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn valid_launch(launch: &ManagedLaunch) -> bool {
    let root = crate::install::default_install_root().join("kimi");
    let Ok(root) = std::fs::canonicalize(root) else {
        return false;
    };
    let Ok(command) = std::fs::canonicalize(&launch.command) else {
        return false;
    };
    command.starts_with(root) && command.is_file()
}
