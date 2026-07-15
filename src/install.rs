//! Installs binary ACP server distributions owned by Mjolnir.

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::registry::BinaryTarget;

#[derive(Debug, Clone)]
pub enum Progress {
    Started { total_bytes: Option<u64> },
    Downloaded { downloaded_bytes: u64 },
    Extracting,
    Done,
}

pub fn default_install_root() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from(".local/share"))
        .join("mj")
        .join("agents")
}

pub async fn install_or_resolve(
    agent_id: &str,
    version: &str,
    target: &BinaryTarget,
    progress_tx: mpsc::UnboundedSender<Progress>,
) -> Result<(PathBuf, Vec<String>)> {
    anyhow::ensure!(
        safe_path_component(agent_id),
        "invalid ACP registry agent id"
    );
    anyhow::ensure!(safe_path_component(version), "invalid ACP registry version");
    let directory = default_install_root().join(agent_id).join(version);
    let sentinel = directory.join(".installed");
    if !sentinel.exists() {
        std::fs::create_dir_all(&directory)
            .with_context(|| format!("create install directory {}", directory.display()))?;
        download_and_extract(target, &directory, &progress_tx).await?;
        std::fs::write(&sentinel, "ok").with_context(|| format!("write {}", sentinel.display()))?;
    }
    let command = resolve_command(&directory, &target.cmd)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&command)?.permissions();
        permissions.set_mode(permissions.mode() | 0o111);
        std::fs::set_permissions(&command, permissions)?;
    }
    let _ = progress_tx.send(Progress::Done);
    Ok((command, target.args.clone()))
}

fn safe_path_component(value: &str) -> bool {
    !value.is_empty()
        && Path::new(value).components().count() == 1
        && matches!(
            Path::new(value).components().next(),
            Some(std::path::Component::Normal(_))
        )
}

fn resolve_command(directory: &Path, command: &str) -> Result<PathBuf> {
    let candidate = directory.join(command.strip_prefix("./").unwrap_or(command));
    let root = std::fs::canonicalize(directory)
        .with_context(|| format!("canonicalize {}", directory.display()))?;
    let command = std::fs::canonicalize(&candidate)
        .with_context(|| format!("locate installed command {}", candidate.display()))?;
    anyhow::ensure!(
        command.starts_with(root),
        "installed command resolves outside its installation directory"
    );
    Ok(command)
}

async fn download_and_extract(
    target: &BinaryTarget,
    destination: &Path,
    progress_tx: &mpsc::UnboundedSender<Progress>,
) -> Result<()> {
    let url = &target.archive;
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .user_agent(concat!("mj/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("build ACP installer client")?
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("GET {url}"))?;
    let total_bytes = response.content_length();
    let _ = progress_tx.send(Progress::Started { total_bytes });
    let mut bytes = Vec::with_capacity(total_bytes.unwrap_or(0) as usize);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.context("read ACP server archive")?);
        let _ = progress_tx.send(Progress::Downloaded {
            downloaded_bytes: bytes.len() as u64,
        });
    }
    if !target.sha256.is_empty() {
        let actual = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        anyhow::ensure!(
            actual.eq_ignore_ascii_case(&target.sha256),
            "ACP server archive checksum mismatch: expected {}, got {actual}",
            target.sha256
        );
    }
    let _ = progress_tx.send(Progress::Extracting);
    let destination = destination.to_path_buf();
    let archive = target.archive.clone();
    let command = target.cmd.clone();
    tokio::task::spawn_blocking(move || extract(&bytes, &archive, &command, &destination))
        .await
        .context("join ACP archive extraction")??;
    Ok(())
}

fn extract(bytes: &[u8], archive: &str, command: &str, destination: &Path) -> Result<()> {
    let archive = archive
        .split_once('?')
        .map_or(archive, |(path, _)| path)
        .to_ascii_lowercase();
    if archive.ends_with(".tar.gz") || archive.ends_with(".tgz") {
        return tar::Archive::new(flate2::read::GzDecoder::new(bytes))
            .unpack(destination)
            .with_context(|| format!("extract archive into {}", destination.display()));
    }
    if archive.ends_with(".tar.bz2") || archive.ends_with(".tbz2") {
        return tar::Archive::new(bzip2::read::BzDecoder::new(bytes))
            .unpack(destination)
            .with_context(|| format!("extract archive into {}", destination.display()));
    }
    if !archive.ends_with(".zip") {
        let relative = Path::new(command.strip_prefix("./").unwrap_or(command));
        anyhow::ensure!(
            relative.is_relative()
                && !relative
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir)),
            "raw binary command path escapes its installation directory"
        );
        let path = destination.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
        }
        return Ok(());
    }
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).context("open zip archive")?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).context("read zip entry")?;
        let Some(name) = entry.enclosed_name() else {
            continue;
        };
        let path = destination.join(name);
        if entry.is_dir() {
            std::fs::create_dir_all(&path)?;
            continue;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut output = std::fs::File::create(&path)?;
        let mut contents = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut contents)?;
        std::io::Write::write_all(&mut output, &contents)?;
        #[cfg(unix)]
        if let Some(mode) = entry.unix_mode() {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn installed_command_cannot_escape_root() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::NamedTempFile::new().expect("outside");
        let command = root.path().join("escape");
        std::os::unix::fs::symlink(outside.path(), &command).expect("symlink");
        assert!(resolve_command(root.path(), "escape").is_err());
    }

    #[test]
    fn raw_binary_is_written_to_the_declared_command() {
        let root = tempfile::tempdir().expect("root");
        extract(
            b"binary",
            "https://example.com/agent",
            "./bin/agent",
            root.path(),
        )
        .expect("extract raw binary");
        assert_eq!(
            std::fs::read(root.path().join("bin/agent")).expect("read binary"),
            b"binary"
        );
    }

    #[test]
    fn install_coordinates_reject_path_traversal() {
        assert!(safe_path_component("agent-name"));
        assert!(safe_path_component("1.2.3"));
        assert!(!safe_path_component("../agent"));
        assert!(!safe_path_component(""));
    }
}
