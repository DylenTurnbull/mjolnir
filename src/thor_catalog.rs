//! Local model catalog cache for Thor routing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::thor::ThorConfig;

const CACHE_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ModelCatalog {
    pub version: u32,
    pub generated_at_unix: u64,
    pub sources: Vec<CatalogSourceStatus>,
    pub models: Vec<CatalogModel>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogSourceStatus {
    pub name: String,
    pub url: String,
    pub refreshed_at_unix: Option<u64>,
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogModel {
    pub id: String,
    pub name: Option<String>,
    pub provider: Option<String>,
    pub arena_score: Option<f64>,
    pub input_price_per_million: Option<f64>,
    pub output_price_per_million: Option<f64>,
    pub context_length: Option<u64>,
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRequest {
    #[serde(default)]
    pub refresh: bool,
    #[serde(default = "default_max_age_seconds")]
    pub max_age_seconds: u64,
}

impl Default for CatalogRequest {
    fn default() -> Self {
        Self {
            refresh: false,
            max_age_seconds: default_max_age_seconds(),
        }
    }
}

fn default_max_age_seconds() -> u64 {
    24 * 60 * 60
}

pub async fn load_or_refresh_catalog(
    thor: &ThorConfig,
    request: CatalogRequest,
) -> Result<ModelCatalog> {
    let cache_path = catalog_cache_path();
    if !request.refresh
        && let Some(catalog) = read_fresh_catalog(&cache_path, request.max_age_seconds)?
    {
        return Ok(catalog);
    }

    match refresh_catalog(thor).await {
        Ok(catalog) => {
            write_catalog(&cache_path, &catalog)?;
            Ok(catalog)
        }
        Err(refresh_error) => {
            let mut catalog = read_catalog(&cache_path)
                .with_context(|| format!("refresh catalog failed: {refresh_error:#}"))?;
            catalog.sources.push(CatalogSourceStatus {
                name: "refresh".to_string(),
                url: String::new(),
                refreshed_at_unix: Some(now_unix()),
                ok: false,
                message: refresh_error.to_string(),
            });
            Ok(catalog)
        }
    }
}

fn read_fresh_catalog(path: &Path, max_age_seconds: u64) -> Result<Option<ModelCatalog>> {
    let catalog = match read_catalog(path) {
        Ok(catalog) => catalog,
        Err(_) => return Ok(None),
    };
    let age = now_unix().saturating_sub(catalog.generated_at_unix);
    if age <= max_age_seconds {
        Ok(Some(catalog))
    } else {
        Ok(None)
    }
}

fn read_catalog(path: &Path) -> Result<ModelCatalog> {
    let body = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("parse {}", path.display()))
}

fn write_catalog(path: &Path, catalog: &ModelCatalog) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create catalog cache dir {}", parent.display()))?;
    }
    let body = serde_json::to_vec_pretty(catalog).context("serialize model catalog")?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, body).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn catalog_cache_path() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("mj")
        .join("thor")
        .join("model-catalog.json")
}

async fn refresh_catalog(thor: &ThorConfig) -> Result<ModelCatalog> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("build catalog HTTP client")?;

    let (arena, openrouter) = tokio::join!(
        fetch_source(&client, "lm-arena", &thor.leaderboard_url),
        fetch_source(&client, "openrouter", &thor.pricing_url),
    );

    let mut sources = Vec::new();
    let mut models: HashMap<String, CatalogModel> = HashMap::new();

    match arena {
        Ok((status, body)) => {
            sources.push(status);
            merge_arena_models(&mut models, parse_arena_models(&body));
        }
        Err(error) => sources.push(CatalogSourceStatus {
            name: "lm-arena".to_string(),
            url: thor.leaderboard_url.clone(),
            refreshed_at_unix: Some(now_unix()),
            ok: false,
            message: error.to_string(),
        }),
    }

    match openrouter {
        Ok((status, body)) => {
            sources.push(status);
            merge_openrouter_models(&mut models, parse_openrouter_models(&body)?);
        }
        Err(error) => sources.push(CatalogSourceStatus {
            name: "openrouter".to_string(),
            url: thor.pricing_url.clone(),
            refreshed_at_unix: Some(now_unix()),
            ok: false,
            message: error.to_string(),
        }),
    }

    if models.is_empty() {
        anyhow::bail!("no model metadata could be parsed from configured sources");
    }

    let mut models = models.into_values().collect::<Vec<_>>();
    models.sort_by(|a, b| {
        b.arena_score
            .unwrap_or_default()
            .total_cmp(&a.arena_score.unwrap_or_default())
            .then_with(|| a.id.cmp(&b.id))
    });

    Ok(ModelCatalog {
        version: CACHE_VERSION,
        generated_at_unix: now_unix(),
        sources,
        models,
    })
}

async fn fetch_source(
    client: &reqwest::Client,
    name: &str,
    url: &str,
) -> Result<(CatalogSourceStatus, String)> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("read {url}"))?;
    if !status.is_success() {
        anyhow::bail!("GET {url} returned {status}");
    }
    Ok((
        CatalogSourceStatus {
            name: name.to_string(),
            url: url.to_string(),
            refreshed_at_unix: Some(now_unix()),
            ok: true,
            message: format!("cached {} bytes", body.len()),
        },
        body,
    ))
}

fn parse_openrouter_models(body: &str) -> Result<Vec<CatalogModel>> {
    let value: Value = serde_json::from_str(body).context("parse OpenRouter JSON")?;
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .context("OpenRouter JSON missing data array")?;
    Ok(data.iter().filter_map(openrouter_model).collect())
}

fn openrouter_model(value: &Value) -> Option<CatalogModel> {
    let id = value.get("id")?.as_str()?.to_string();
    let pricing = value.get("pricing");
    Some(CatalogModel {
        provider: id.split('/').next().map(str::to_string),
        name: value
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string),
        input_price_per_million: pricing
            .and_then(|p| p.get("prompt"))
            .and_then(price_per_million),
        output_price_per_million: pricing
            .and_then(|p| p.get("completion"))
            .and_then(price_per_million),
        context_length: value.get("context_length").and_then(Value::as_u64),
        arena_score: None,
        sources: vec!["openrouter".to_string()],
        id,
    })
}

fn price_per_million(value: &Value) -> Option<f64> {
    let price = match value {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => text.parse::<f64>().ok()?,
        _ => return None,
    };
    Some(price * 1_000_000.0)
}

fn parse_arena_models(body: &str) -> Vec<CatalogModel> {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    let mut models = Vec::new();
    collect_arena_models(&value, &mut models);
    models
}

fn collect_arena_models(value: &Value, models: &mut Vec<CatalogModel>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_arena_models(value, models);
            }
        }
        Value::Object(object) => {
            if let (Some(id), Some(score)) = (model_name_from_object(object), arena_score(object)) {
                models.push(CatalogModel {
                    id: id.clone(),
                    name: Some(id),
                    provider: None,
                    arena_score: Some(score),
                    input_price_per_million: None,
                    output_price_per_million: None,
                    context_length: None,
                    sources: vec!["lm-arena".to_string()],
                });
            }
            for value in object.values() {
                collect_arena_models(value, models);
            }
        }
        _ => {}
    }
}

fn model_name_from_object(object: &serde_json::Map<String, Value>) -> Option<String> {
    ["model", "model_name", "name", "Model", "Model Name"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::to_string)
}

fn arena_score(object: &serde_json::Map<String, Value>) -> Option<f64> {
    [
        "arena_score",
        "arenaScore",
        "score",
        "rating",
        "elo",
        "Elo",
        "Arena Score",
    ]
    .iter()
    .find_map(|key| object.get(*key).and_then(number_value))
}

fn number_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn merge_openrouter_models(
    models: &mut HashMap<String, CatalogModel>,
    openrouter_models: Vec<CatalogModel>,
) {
    for model in openrouter_models {
        let entry = models
            .entry(model.id.clone())
            .or_insert_with(|| model.clone());
        entry.name = entry.name.take().or(model.name);
        entry.provider = entry.provider.take().or(model.provider);
        entry.input_price_per_million = entry
            .input_price_per_million
            .or(model.input_price_per_million);
        entry.output_price_per_million = entry
            .output_price_per_million
            .or(model.output_price_per_million);
        entry.context_length = entry.context_length.or(model.context_length);
        add_source(entry, "openrouter");
    }
}

fn merge_arena_models(models: &mut HashMap<String, CatalogModel>, arena_models: Vec<CatalogModel>) {
    for model in arena_models {
        let key = matching_key(models, &model.id).unwrap_or_else(|| model.id.clone());
        let entry = models.entry(key).or_insert_with(|| model.clone());
        entry.arena_score = entry.arena_score.or(model.arena_score);
        add_source(entry, "lm-arena");
    }
}

fn matching_key(models: &HashMap<String, CatalogModel>, arena_id: &str) -> Option<String> {
    let normalized = normalize_model_name(arena_id);
    models
        .keys()
        .find(|id| normalize_model_name(id).contains(&normalized))
        .cloned()
}

fn normalize_model_name(name: &str) -> String {
    name.to_ascii_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect()
}

fn add_source(model: &mut CatalogModel, source: &str) {
    if !model.sources.iter().any(|existing| existing == source) {
        model.sources.push(source.to_string());
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_openrouter_prices_per_million() {
        let body = r#"
        {
          "data": [{
            "id": "openai/gpt-example",
            "name": "GPT Example",
            "context_length": 128000,
            "pricing": { "prompt": "0.000001", "completion": "0.000004" }
          }]
        }
        "#;

        let models = parse_openrouter_models(body).expect("parse");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "openai/gpt-example");
        assert_eq!(models[0].provider.as_deref(), Some("openai"));
        assert_eq!(models[0].input_price_per_million, Some(1.0));
        assert_eq!(models[0].output_price_per_million, Some(4.0));
        assert_eq!(models[0].context_length, Some(128000));
    }

    #[test]
    fn parses_nested_arena_json() {
        let body = r#"
        { "leaderboard": [
          { "model": "GPT Example", "arena_score": 1234.5 },
          { "name": "Claude Example", "rating": "1201" }
        ] }
        "#;

        let models = parse_arena_models(body);
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].id, "GPT Example");
        assert_eq!(models[0].arena_score, Some(1234.5));
        assert_eq!(models[1].arena_score, Some(1201.0));
    }

    #[test]
    fn merges_arena_score_into_openrouter_model_by_name() {
        let mut models = HashMap::new();
        merge_openrouter_models(
            &mut models,
            vec![CatalogModel {
                id: "openai/gpt-example".to_string(),
                name: Some("GPT Example".to_string()),
                provider: Some("openai".to_string()),
                arena_score: None,
                input_price_per_million: Some(1.0),
                output_price_per_million: Some(4.0),
                context_length: None,
                sources: vec!["openrouter".to_string()],
            }],
        );
        merge_arena_models(
            &mut models,
            vec![CatalogModel {
                id: "GPT Example".to_string(),
                name: Some("GPT Example".to_string()),
                provider: None,
                arena_score: Some(1234.0),
                input_price_per_million: None,
                output_price_per_million: None,
                context_length: None,
                sources: vec!["lm-arena".to_string()],
            }],
        );

        let model = models.get("openai/gpt-example").expect("merged model");
        assert_eq!(model.arena_score, Some(1234.0));
        assert!(model.sources.iter().any(|source| source == "lm-arena"));
    }
}
