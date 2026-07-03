//! Tailscale integration for `mj server --tailscale`.
//!
//! Discovers the local tailscale CLI, reads the node's HTTPS certificate
//! domain from `tailscale status --json`, and mints a publicly trusted
//! Let's Encrypt certificate via `tailscale cert`. Tailscale answers the
//! ACME DNS-01 challenge on its own `ts.net` zone, so this works even
//! though the machine is unreachable from the public internet — and the
//! resulting certificate produces no browser warning on any device.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

/// A running tailscale daemon that can issue certificates for this node.
#[derive(Debug, Clone)]
pub struct Tailscale {
    binary: PathBuf,
    /// Fully qualified `ts.net` name certificates can be issued for,
    /// e.g. `mybox.tail1234.ts.net` (no trailing dot).
    pub cert_domain: String,
}

impl Tailscale {
    /// Locate the tailscale CLI and confirm the node can mint certificates.
    pub fn discover() -> Result<Self> {
        let binary = find_binary().ok_or_else(|| {
            anyhow!(
                "tailscale CLI not found in PATH; install tailscale \
                 (https://tailscale.com/download) or start `mj server` without --tailscale"
            )
        })?;
        let output = Command::new(&binary)
            .args(["status", "--json"])
            .output()
            .with_context(|| format!("run `{} status --json`", binary.display()))?;
        if !output.status.success() {
            bail!(
                "`tailscale status` failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let status: Status = serde_json::from_slice(&output.stdout)
            .context("parse `tailscale status --json` output")?;
        let cert_domain = cert_domain(&status)?;
        Ok(Self {
            binary,
            cert_domain,
        })
    }

    /// Write a certificate chain and private key for `cert_domain` to the
    /// given paths. Tailscale caches issued certificates, so this only talks
    /// to Let's Encrypt on first run or when renewal is due; re-running it
    /// periodically is the supported renewal mechanism.
    pub fn mint_cert(&self, cert_path: &Path, key_path: &Path) -> Result<()> {
        let output = Command::new(&self.binary)
            .arg("cert")
            .arg("--cert-file")
            .arg(cert_path)
            .arg("--key-file")
            .arg(key_path)
            .arg(&self.cert_domain)
            .output()
            .with_context(|| format!("run `{} cert`", self.binary.display()))?;
        if !output.status.success() {
            bail!(
                "`tailscale cert {}` failed: {}",
                self.cert_domain,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }
}

fn find_binary() -> Option<PathBuf> {
    if let Some(path_var) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let candidate = dir.join("tailscale");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    // The macOS app (App Store and standalone) does not put the CLI on PATH.
    #[cfg(target_os = "macos")]
    {
        let app = PathBuf::from("/Applications/Tailscale.app/Contents/MacOS/Tailscale");
        if app.is_file() {
            return Some(app);
        }
    }
    None
}

/// Minimal slice of `tailscale status --json`.
#[derive(Debug, Deserialize)]
struct Status {
    #[serde(rename = "BackendState")]
    backend_state: String,
    /// Domains this node may request certificates for. Empty or absent when
    /// the tailnet has not enabled MagicDNS + HTTPS Certificates.
    #[serde(rename = "CertDomains")]
    cert_domains: Option<Vec<String>>,
}

fn cert_domain(status: &Status) -> Result<String> {
    if status.backend_state != "Running" {
        bail!(
            "tailscale is not running (state: {}); run `tailscale up` first",
            status.backend_state
        );
    }
    status
        .cert_domains
        .as_deref()
        .unwrap_or_default()
        .first()
        .map(|domain| domain.trim_end_matches('.').to_string())
        .ok_or_else(|| {
            anyhow!(
                "this tailnet has no HTTPS certificate domains; enable MagicDNS and \
                 HTTPS Certificates under the DNS tab of the Tailscale admin console \
                 (https://login.tailscale.com/admin/dns), then retry"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(backend_state: &str, cert_domains: Option<Vec<&str>>) -> Status {
        Status {
            backend_state: backend_state.to_string(),
            cert_domains: cert_domains
                .map(|domains| domains.into_iter().map(str::to_string).collect()),
        }
    }

    #[test]
    fn cert_domain_prefers_first_entry_and_strips_trailing_dot() {
        let status = status("Running", Some(vec!["mybox.tail1234.ts.net."]));
        assert_eq!(
            cert_domain(&status).expect("domain"),
            "mybox.tail1234.ts.net"
        );
    }

    #[test]
    fn cert_domain_rejects_stopped_daemon() {
        let status = status("Stopped", Some(vec!["mybox.tail1234.ts.net"]));
        let error = cert_domain(&status).expect_err("stopped");
        assert!(error.to_string().contains("tailscale up"), "{error}");
    }

    #[test]
    fn cert_domain_requires_https_certificates_enabled() {
        for cert_domains in [None, Some(vec![])] {
            let status = status("Running", cert_domains);
            let error = cert_domain(&status).expect_err("no domains");
            assert!(error.to_string().contains("HTTPS Certificates"), "{error}");
        }
    }

    #[test]
    fn status_parses_real_output_shape() {
        let json = r#"{
            "Version": "1.80.0",
            "BackendState": "Running",
            "CertDomains": ["mybox.tail1234.ts.net"],
            "Self": {"DNSName": "mybox.tail1234.ts.net.", "Online": true},
            "MagicDNSSuffix": "tail1234.ts.net"
        }"#;
        let status: Status = serde_json::from_str(json).expect("parse");
        assert_eq!(status.backend_state, "Running");
        assert_eq!(
            cert_domain(&status).expect("domain"),
            "mybox.tail1234.ts.net"
        );
    }

    #[test]
    fn status_tolerates_missing_cert_domains() {
        let json = r#"{"BackendState": "NeedsLogin"}"#;
        let status: Status = serde_json::from_str(json).expect("parse");
        assert!(cert_domain(&status).is_err());
    }
}
