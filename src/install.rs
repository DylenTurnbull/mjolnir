//! Download and extract runtime archives used by mj.

use std::io::{Cursor, Read};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::sync::mpsc;

/// Streaming progress event emitted while downloading.
#[derive(Debug, Clone)]
pub enum Progress {
    /// Started fetching an archive.
    Started,
    /// Received another archive chunk.
    Downloaded,
    /// Download finished; extraction begins.
    Extracting,
}

pub(crate) async fn download_and_extract(
    url: &str,
    dest: &Path,
    progress_tx: &mpsc::UnboundedSender<Progress>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
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

    let total_bytes = resp.content_length();
    let _ = progress_tx.send(Progress::Started);

    let mut bytes: Vec<u8> = Vec::with_capacity(total_bytes.unwrap_or(0) as usize);
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("read chunk from {url}"))?;
        bytes.extend_from_slice(&chunk);
        let _ = progress_tx.send(Progress::Downloaded);
    }

    let _ = progress_tx.send(Progress::Extracting);
    let kind = archive_kind(url);
    // Extraction is CPU-bound; run it on a blocking thread so we don't
    // stall the tokio runtime.
    let dest_owned = dest.to_path_buf();
    tokio::task::spawn_blocking(move || extract(&bytes, kind, &dest_owned))
        .await
        .context("join extract task")??;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ArchiveKind {
    TarGz,
    Zip,
}

fn archive_kind(url: &str) -> ArchiveKind {
    let lower = url.to_lowercase();
    if lower.ends_with(".zip") {
        ArchiveKind::Zip
    } else {
        // .tar.gz / .tgz / unknown all default to tar.gz, matching the
        // dominant pattern in the registry.
        ArchiveKind::TarGz
    }
}

fn extract(bytes: &[u8], kind: ArchiveKind, dest: &Path) -> Result<()> {
    match kind {
        ArchiveKind::TarGz => extract_tar_gz(bytes, dest),
        ArchiveKind::Zip => extract_zip(bytes, dest),
    }
}

fn extract_tar_gz(bytes: &[u8], dest: &Path) -> Result<()> {
    let gz = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(gz);
    archive
        .unpack(dest)
        .with_context(|| format!("unpack tar.gz to {}", dest.display()))?;
    Ok(())
}

fn extract_zip(bytes: &[u8], dest: &Path) -> Result<()> {
    let reader = Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(reader).context("open zip archive")?;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .with_context(|| format!("zip entry {i}"))?;
        let out_path = match entry.enclosed_name() {
            Some(p) => dest.join(p),
            None => continue, // skip entries with absolute / parent-traversal names
        };
        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)
                .with_context(|| format!("create dir {}", out_path.display()))?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create dir {}", parent.display()))?;
            }
            let mut out = std::fs::File::create(&out_path)
                .with_context(|| format!("create file {}", out_path.display()))?;
            let mut buf = Vec::with_capacity(entry.size() as usize);
            entry
                .read_to_end(&mut buf)
                .with_context(|| format!("read zip entry {}", out_path.display()))?;
            std::io::Write::write_all(&mut out, &buf)
                .with_context(|| format!("write {}", out_path.display()))?;
            #[cfg(unix)]
            if let Some(mode) = entry.unix_mode() {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(mode));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Build a small tar.gz in memory containing one executable file.
    fn make_tar_gz(file_name: &str, content: &[u8]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;

        let mut header = tar::Header::new_gnu();
        header.set_path(file_name).expect("path");
        header.set_size(content.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();

        let mut tar_bytes: Vec<u8> = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_bytes);
            builder.append(&header, content).expect("append");
            builder.finish().expect("finish");
        }

        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(&tar_bytes).expect("gz write");
        gz.finish().expect("gz finish")
    }

    #[test]
    fn archive_kind_dispatch() {
        assert!(matches!(archive_kind("foo.zip"), ArchiveKind::Zip));
        assert!(matches!(
            archive_kind("https://example.com/x.tar.gz"),
            ArchiveKind::TarGz
        ));
        assert!(matches!(archive_kind("path/x.tgz"), ArchiveKind::TarGz));
        assert!(matches!(archive_kind("no-ext"), ArchiveKind::TarGz));
    }

    #[test]
    fn extract_tar_gz_creates_expected_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let archive = make_tar_gz("agent-bin", b"#!/bin/sh\necho ok\n");
        extract(&archive, ArchiveKind::TarGz, dir.path()).expect("extract");
        let path = dir.path().join("agent-bin");
        assert!(path.exists(), "expected {} to exist", path.display());
        let content = std::fs::read_to_string(&path).expect("read");
        assert!(content.contains("echo ok"));
    }
}
