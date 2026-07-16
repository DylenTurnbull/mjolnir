//! DeepSeek balance status supplied by Anvil in ACP usage-update metadata.

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub enum DeepSeekBalanceStatus {
    Available(DeepSeekBalanceReport),
    Unavailable(DeepSeekBalanceUnavailable),
}

impl DeepSeekBalanceStatus {
    pub fn compact_label(&self) -> String {
        match self {
            Self::Available(report) => report.compact_label(),
            Self::Unavailable(unavailable) => format!(
                "DeepSeek balance unavailable: {} · as of {}",
                unavailable.reason, unavailable.as_of
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeepSeekBalanceReport {
    pub balances: Vec<DeepSeekBalance>,
    pub as_of: String,
}

impl DeepSeekBalanceReport {
    fn compact_label(&self) -> String {
        let balances = if self.balances.is_empty() {
            "none".to_string()
        } else {
            self.balances
                .iter()
                .map(|balance| {
                    format!(
                        "{} total {} · granted {} · topped up {}",
                        balance.currency,
                        balance.total_balance,
                        balance.granted_balance,
                        balance.topped_up_balance
                    )
                })
                .collect::<Vec<_>>()
                .join(" · ")
        };
        format!("DeepSeek balance: {balances} · as of {}", self.as_of)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeepSeekBalance {
    pub currency: String,
    pub total_balance: String,
    pub granted_balance: String,
    pub topped_up_balance: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeepSeekBalanceUnavailable {
    pub reason: String,
    pub as_of: String,
}

/// Parse `anvil.deepseekBalance`. Invalid and unknown payloads are ignored so
/// a malformed provider update cannot erase the last known good UI status.
pub fn from_usage_meta(
    meta: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<DeepSeekBalanceStatus> {
    let value = meta?.get("anvil")?.get("deepseekBalance")?.clone();
    let wire: WireStatus = serde_json::from_value(value).ok()?;
    match wire {
        WireStatus::Available { balances, as_of }
            if !as_of.trim().is_empty()
                && balances.iter().all(|balance| {
                    !balance.currency.trim().is_empty()
                        && !balance.total_balance.trim().is_empty()
                        && !balance.granted_balance.trim().is_empty()
                        && !balance.topped_up_balance.trim().is_empty()
                }) =>
        {
            Some(DeepSeekBalanceStatus::Available(DeepSeekBalanceReport {
                balances: balances
                    .into_iter()
                    .map(|balance| DeepSeekBalance {
                        currency: balance.currency,
                        total_balance: balance.total_balance,
                        granted_balance: balance.granted_balance,
                        topped_up_balance: balance.topped_up_balance,
                    })
                    .collect(),
                as_of,
            }))
        }
        WireStatus::Unavailable { reason, as_of }
            if !reason.trim().is_empty() && !as_of.trim().is_empty() =>
        {
            Some(DeepSeekBalanceStatus::Unavailable(
                DeepSeekBalanceUnavailable { reason, as_of },
            ))
        }
        _ => None,
    }
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum WireStatus {
    Available {
        balances: Vec<WireBalance>,
        #[serde(rename = "asOf")]
        as_of: String,
    },
    Unavailable {
        reason: String,
        #[serde(rename = "asOf")]
        as_of: String,
    },
}

#[derive(Deserialize)]
struct WireBalance {
    currency: String,
    #[serde(rename = "totalBalance")]
    total_balance: String,
    #[serde(rename = "grantedBalance")]
    granted_balance: String,
    #[serde(rename = "toppedUpBalance")]
    topped_up_balance: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_official_multi_currency_metadata_without_changing_strings_or_order() {
        let meta = serde_json::json!({"anvil":{"deepseekBalance":{"status":"available","balances":[{"currency":"CNY","totalBalance":"123.4500","grantedBalance":"23.0000","toppedUpBalance":"100.4500"},{"currency":"USD","totalBalance":"0.010","grantedBalance":"0.000","toppedUpBalance":"0.010"}],"asOf":"2026-07-15T18:42:00Z"}}});

        assert_eq!(
            from_usage_meta(meta.as_object()),
            Some(DeepSeekBalanceStatus::Available(DeepSeekBalanceReport {
                balances: vec![
                    DeepSeekBalance {
                        currency: "CNY".to_string(),
                        total_balance: "123.4500".to_string(),
                        granted_balance: "23.0000".to_string(),
                        topped_up_balance: "100.4500".to_string(),
                    },
                    DeepSeekBalance {
                        currency: "USD".to_string(),
                        total_balance: "0.010".to_string(),
                        granted_balance: "0.000".to_string(),
                        topped_up_balance: "0.010".to_string(),
                    },
                ],
                as_of: "2026-07-15T18:42:00Z".to_string(),
            }))
        );
    }

    #[test]
    fn parses_unavailable_reason_and_timestamp() {
        let meta = serde_json::json!({"anvil":{"deepseekBalance":{"status":"unavailable","reason":"billing credentials are unavailable","asOf":"2026-07-15T18:42:00Z"}}});

        assert_eq!(
            from_usage_meta(meta.as_object()),
            Some(DeepSeekBalanceStatus::Unavailable(
                DeepSeekBalanceUnavailable {
                    reason: "billing credentials are unavailable".to_string(),
                    as_of: "2026-07-15T18:42:00Z".to_string(),
                }
            ))
        );
    }

    #[test]
    fn ignores_malformed_payloads() {
        let invalid = [
            serde_json::json!({"anvil":{"deepseekBalance":{"status":"future"}}}),
            serde_json::json!({"anvil":{"deepseekBalance":{"status":"available","balances":[{"currency":"CNY","totalBalance":123.45,"grantedBalance":"23.0000","toppedUpBalance":"100.4500"}],"asOf":"2026-07-15T18:42:00Z"}}}),
            serde_json::json!({"anvil":{"deepseekBalance":{"status":"unavailable","reason":"","asOf":"2026-07-15T18:42:00Z"}}}),
        ];

        for meta in invalid {
            assert_eq!(from_usage_meta(meta.as_object()), None);
        }
    }
}
