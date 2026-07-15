//! OpenRouter balance status supplied by Anvil in ACP usage-update metadata.

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub enum OpenRouterBalanceStatus {
    Available(OpenRouterBalanceReport),
    Unavailable(String),
}

impl OpenRouterBalanceStatus {
    pub fn compact_label(&self) -> String {
        match self {
            Self::Available(report) => report.compact_label(),
            Self::Unavailable(reason) => format!("OpenRouter balance unavailable: {reason}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenRouterBalanceReport {
    pub remaining_usd: f64,
    pub total_credits_usd: f64,
    pub total_usage_usd: f64,
    pub as_of: String,
}

impl OpenRouterBalanceReport {
    fn compact_label(&self) -> String {
        format!(
            "OpenRouter balance: USD {:.2} remaining · as of {}",
            self.remaining_usd, self.as_of
        )
    }
}

/// Parse `anvil.openrouterBalance`. Invalid and unknown payloads are ignored
/// so a malformed provider update cannot erase the last known good UI status.
pub fn from_usage_meta(
    meta: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<OpenRouterBalanceStatus> {
    let value = meta?.get("anvil")?.get("openrouterBalance")?.clone();
    let wire: WireStatus = serde_json::from_value(value).ok()?;
    match wire {
        WireStatus::Available {
            remaining_usd,
            total_credits_usd,
            total_usage_usd,
            as_of,
        } if !as_of.trim().is_empty()
            && remaining_usd.is_finite()
            && total_credits_usd.is_finite()
            && total_usage_usd.is_finite() =>
        {
            Some(OpenRouterBalanceStatus::Available(
                OpenRouterBalanceReport {
                    remaining_usd,
                    total_credits_usd,
                    total_usage_usd,
                    as_of,
                },
            ))
        }
        WireStatus::Unavailable { reason, .. } if !reason.trim().is_empty() => {
            Some(OpenRouterBalanceStatus::Unavailable(reason))
        }
        _ => None,
    }
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum WireStatus {
    Available {
        #[serde(rename = "remainingUsd")]
        remaining_usd: f64,
        #[serde(rename = "totalCreditsUsd")]
        total_credits_usd: f64,
        #[serde(rename = "totalUsageUsd")]
        total_usage_usd: f64,
        #[serde(rename = "asOf")]
        as_of: String,
    },
    Unavailable {
        reason: String,
        #[serde(rename = "asOf")]
        _as_of: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_available_anvil_metadata_with_compact_label() {
        let meta = serde_json::json!({"anvil":{"openrouterBalance":{"status":"available","remainingUsd":12.5,"totalCreditsUsd":20.0,"totalUsageUsd":7.5,"asOf":"2026-07-15T18:42:00Z"}}});

        let status = from_usage_meta(meta.as_object()).unwrap();

        assert_eq!(
            status.compact_label(),
            "OpenRouter balance: USD 12.50 remaining · as of 2026-07-15T18:42:00Z"
        );
    }

    #[test]
    fn zero_and_negative_balances_are_available() {
        for remaining_usd in [0.0, -1.25] {
            let meta = serde_json::json!({"anvil":{"openrouterBalance":{"status":"available","remainingUsd":remaining_usd,"totalCreditsUsd":20.0,"totalUsageUsd":21.25,"asOf":"2026-07-15T18:42:00Z"}}});

            assert!(matches!(
                from_usage_meta(meta.as_object()),
                Some(OpenRouterBalanceStatus::Available(OpenRouterBalanceReport {
                    remaining_usd: value,
                    ..
                })) if value == remaining_usd
            ));
        }
    }

    #[test]
    fn ignores_malformed_unknown_missing_and_nonfinite_values() {
        let invalid = [
            serde_json::json!({"anvil":{"openrouterBalance":{"status":"future"}}}),
            serde_json::json!({"anvil":{"openrouterBalance":{"status":"available","remainingUsd":1.0,"totalCreditsUsd":2.0,"asOf":"2026-07-15T18:42:00Z"}}}),
            serde_json::json!({"anvil":{"openrouterBalance":{"status":"available","remainingUsd":f64::INFINITY,"totalCreditsUsd":2.0,"totalUsageUsd":1.0,"asOf":"2026-07-15T18:42:00Z"}}}),
            serde_json::json!({"anvil":{"openrouterBalance":{"status":"available","remainingUsd":1.0,"totalCreditsUsd":2.0,"totalUsageUsd":1.0,"asOf":" "}}}),
        ];

        for meta in invalid {
            assert_eq!(from_usage_meta(meta.as_object()), None);
        }
    }

    #[test]
    fn parses_unavailable_anvil_metadata() {
        let meta = serde_json::json!({"anvil":{"openrouterBalance":{"status":"unavailable","reason":"billing credentials are unavailable","asOf":"2026-07-15T18:42:00Z"}}});

        assert_eq!(
            from_usage_meta(meta.as_object()),
            Some(OpenRouterBalanceStatus::Unavailable(
                "billing credentials are unavailable".to_string()
            ))
        );
    }
}
