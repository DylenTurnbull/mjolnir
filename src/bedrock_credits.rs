//! Bedrock credit status supplied by Anvil in ACP usage-update metadata.

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub enum BedrockCreditsStatus {
    Available(BedrockCreditsReport),
    Unavailable(String),
}

impl BedrockCreditsStatus {
    pub fn compact_label(&self) -> String {
        match self {
            Self::Available(report) => report.compact_label(),
            Self::Unavailable(reason) => format!("Bedrock credits unavailable: {reason}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BedrockCreditsReport {
    pub amounts: Vec<CreditAmount>,
    pub earliest_expiration: Option<String>,
    pub as_of: String,
}

impl BedrockCreditsReport {
    fn compact_label(&self) -> String {
        let mut parts: Vec<String> = self
            .amounts
            .iter()
            .map(|amount| format!("{} {:.2}", amount.currency, amount.amount))
            .collect();
        if parts.is_empty() {
            parts.push("none".to_string());
        }
        if let Some(expiration) = &self.earliest_expiration {
            parts.push(format!("expires {expiration}"));
        }
        parts.push(format!("as of {}", self.as_of));
        format!("Bedrock credits: {}", parts.join(" · "))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreditAmount {
    pub currency: String,
    pub amount: f64,
}

/// Parse `anvil.bedrockCredits`. Invalid and unknown payloads are ignored so a
/// malformed provider update cannot erase the last known good UI status.
pub fn from_usage_meta(
    meta: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<BedrockCreditsStatus> {
    let value = meta?.get("anvil")?.get("bedrockCredits")?.clone();
    let wire: WireStatus = serde_json::from_value(value).ok()?;
    match wire {
        WireStatus::Available {
            amounts,
            earliest_expiration,
            as_of,
        } if !as_of.trim().is_empty() => {
            let amounts = amounts
                .into_iter()
                .map(|amount| CreditAmount {
                    currency: amount.currency.trim().to_string(),
                    amount: amount.amount,
                })
                .collect::<Vec<_>>();
            if amounts
                .iter()
                .any(|amount| amount.currency.is_empty() || !amount.amount.is_finite())
            {
                return None;
            }
            Some(BedrockCreditsStatus::Available(BedrockCreditsReport {
                amounts,
                earliest_expiration,
                as_of,
            }))
        }
        WireStatus::Unavailable { reason, .. } if !reason.trim().is_empty() => {
            Some(BedrockCreditsStatus::Unavailable(reason))
        }
        _ => None,
    }
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum WireStatus {
    Available {
        amounts: Vec<WireAmount>,
        #[serde(rename = "earliestExpiration")]
        earliest_expiration: Option<String>,
        #[serde(rename = "asOf")]
        as_of: String,
    },
    Unavailable {
        reason: String,
        #[serde(rename = "asOf")]
        _as_of: String,
    },
}
#[derive(Deserialize)]
struct WireAmount {
    currency: String,
    amount: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_available_anvil_metadata() {
        let meta = serde_json::json!({"anvil":{"bedrockCredits":{"status":"available","amounts":[{"currency":"USD","amount":12.5}],"earliestExpiration":"2026-12-31","asOf":"2026-07-15T18:42:00Z"}}});
        let status = from_usage_meta(meta.as_object()).unwrap();
        assert_eq!(
            status.compact_label(),
            "Bedrock credits: USD 12.50 · expires 2026-12-31 · as of 2026-07-15T18:42:00Z"
        );
    }
    #[test]
    fn parses_unavailable_anvil_metadata() {
        let meta = serde_json::json!({"anvil":{"bedrockCredits":{"status":"unavailable","reason":"request timed out","asOf":"2026-07-15T18:42:00Z"}}});
        assert_eq!(
            from_usage_meta(meta.as_object()),
            Some(BedrockCreditsStatus::Unavailable(
                "request timed out".into()
            ))
        );
    }
}
