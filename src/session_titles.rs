use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
struct SessionTitleStore {
    #[serde(default)]
    titles: BTreeMap<String, String>,
}

pub fn path_for_config(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .map(|parent| parent.join("session-titles.toml"))
        .unwrap_or_else(|| PathBuf::from("session-titles.toml"))
}

pub fn title_from_prompt(prompt: &str) -> Option<String> {
    let sanitized = crate::notifications::sanitize_message(prompt);
    let title = sanitized
        .trim()
        .trim_matches(|ch: char| ch == '"' || ch == '\'')
        .to_string();
    if title.is_empty() {
        return None;
    }
    const MAX_TITLE_CHARS: usize = 80;
    let mut chars = title.chars();
    let truncated = chars.by_ref().take(MAX_TITLE_CHARS).collect::<String>();
    if chars.next().is_some() {
        Some(format!("{truncated}..."))
    } else {
        Some(truncated)
    }
}

pub fn remember_prompt_title(path: &Path, session_id: &str, prompt: &str) -> Result<()> {
    let Some(title) = title_from_prompt(prompt) else {
        return Ok(());
    };
    remember_title(path, session_id, &title)
}

pub fn remember_title(path: &Path, session_id: &str, title: &str) -> Result<()> {
    let session_id = session_id.trim();
    let title = title.trim();
    if session_id.is_empty() || title.is_empty() {
        return Ok(());
    }
    let mut store = load_store(path)?;
    store
        .titles
        .insert(session_id.to_string(), title.to_string());
    save_store(path, &store)
}

#[cfg(test)]
fn title_for_session(path: &Path, session_id: &str) -> Option<String> {
    load_store(path)
        .ok()
        .and_then(|store| store.titles.get(session_id).cloned())
        .filter(|title| !title.trim().is_empty())
}

pub fn apply_to_entries(path: &Path, sessions: &mut [crate::session::SessionEntry]) {
    let Ok(store) = load_store(path) else {
        return;
    };
    for session in sessions {
        if let Some(title) = store.titles.get(&session.session_id)
            && !title.trim().is_empty()
        {
            session.title = Some(title.clone());
        }
    }
}

pub fn forget(path: &Path, session_id: &str) -> Result<()> {
    let mut store = load_store(path)?;
    if store.titles.remove(session_id).is_some() {
        save_store(path, &store)?;
    }
    Ok(())
}

fn load_store(path: &Path) -> Result<SessionTitleStore> {
    if !path.exists() {
        return Ok(SessionTitleStore::default());
    }
    let body = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    toml::from_str(&body).with_context(|| format!("parse {}", path.display()))
}

fn save_store(path: &Path, store: &SessionTitleStore) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create session title dir {}", parent.display()))?;
    }
    let body = toml::to_string_pretty(store).context("serialize session titles")?;
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, body).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_title_is_sanitized_and_truncated() {
        let prompt = format!("  \"{}\"  ", "a".repeat(90));
        let title = title_from_prompt(&prompt).expect("title");
        assert_eq!(title, format!("{}...", "a".repeat(80)));
    }

    #[test]
    fn remember_and_apply_title_overrides_provider_title() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("session-titles.toml");
        remember_prompt_title(&path, "sess-1", "Fix Thor title memory").expect("remember");

        let mut sessions = vec![crate::session::SessionEntry {
            session_id: "sess-1".to_string(),
            cwd: PathBuf::from("/tmp/repo"),
            title: Some("Thor coordinator".to_string()),
            updated_at: None,
        }];
        apply_to_entries(&path, &mut sessions);

        assert_eq!(sessions[0].title.as_deref(), Some("Fix Thor title memory"));
    }

    #[test]
    fn forget_removes_stored_title() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("session-titles.toml");
        remember_title(&path, "sess-1", "Task").expect("remember");
        forget(&path, "sess-1").expect("forget");
        assert_eq!(title_for_session(&path, "sess-1"), None);
    }
}
