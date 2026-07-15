//! AWS promotional/account credits applicable to Amazon Bedrock.
//!
//! This is billing telemetry for an active Bedrock-through-Anvil route, not a
//! Bedrock quota and not an Anvil-owned balance. AWS has documented
//! `Billing.GetCredits`, but does not yet generate it in its Rust SDK, so this
//! module uses the released standard credential chain and SigV4 signer.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use serde::Deserialize;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const BILLING_ENDPOINT: &str = "https://billing.us-east-1.api.aws/";
const STS_ENDPOINT: &str = "https://sts.amazonaws.com/";

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
        let mut parts = if self.amounts.is_empty() {
            vec!["none".to_string()]
        } else {
            self.amounts
                .iter()
                .map(|amount| format!("{} {:.2}", amount.currency, amount.amount))
                .collect()
        };
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

#[derive(Debug)]
pub enum QueryError {
    MissingCredentials(String),
    AccessDenied(String),
    TimedOut,
    Unsupported(String),
    Protocol(String),
}

impl QueryError {
    pub fn user_reason(&self) -> &'static str {
        match self {
            Self::MissingCredentials(_) => "billing credentials are unavailable",
            Self::AccessDenied(_) => "access denied (requires billing:GetCredits)",
            Self::TimedOut => "request timed out",
            Self::Unsupported(_) => "AWS Billing GetCredits is unavailable",
            Self::Protocol(_) => "billing response unavailable",
        }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCredentials(detail) => {
                write!(f, "AWS billing credentials unavailable: {detail}")
            }
            Self::AccessDenied(detail) => write!(f, "AWS billing access denied: {detail}"),
            Self::TimedOut => write!(f, "AWS billing request timed out"),
            Self::Unsupported(detail) => write!(f, "AWS Billing GetCredits unsupported: {detail}"),
            Self::Protocol(detail) => write!(f, "AWS billing protocol error: {detail}"),
        }
    }
}

impl std::error::Error for QueryError {}

/// Query active AWS credits that explicitly list Amazon Bedrock as applicable.
pub async fn query() -> Result<BedrockCreditsReport, QueryError> {
    tokio::time::timeout(REQUEST_TIMEOUT, query_inner())
        .await
        .map_err(|_| QueryError::TimedOut)?
}

async fn query_inner() -> Result<BedrockCreditsReport, QueryError> {
    let credential_http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https();
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .http_client(credential_http_client)
        .load()
        .await;
    let provider = config.credentials_provider().ok_or_else(|| {
        QueryError::MissingCredentials("the standard AWS credential chain is not configured".into())
    })?;
    let credentials = provider
        .provide_credentials()
        .await
        .map_err(|error| QueryError::MissingCredentials(error.to_string()))?;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|error| QueryError::Protocol(error.to_string()))?;

    let account_id = caller_account_id(&client, &credentials).await?;
    let today = Utc::now().date_naive();
    let start_date = (today - ChronoDuration::days(364))
        .and_hms_opt(0, 0, 0)
        .expect("a date always has a midnight")
        .and_utc()
        .timestamp();
    let body = serde_json::to_vec(&serde_json::json!({
        "accountId": account_id,
        "startDate": start_date,
    }))
    .map_err(|error| QueryError::Protocol(error.to_string()))?;
    let response = send_signed(
        &client,
        &credentials,
        BILLING_ENDPOINT,
        "billing",
        "application/x-amz-json-1.0",
        Some("AWSBilling.GetCredits"),
        body,
    )
    .await?;
    parse_report(&response, today)
}

async fn caller_account_id(
    client: &reqwest::Client,
    credentials: &Credentials,
) -> Result<String, QueryError> {
    let response = send_signed(
        client,
        credentials,
        STS_ENDPOINT,
        "sts",
        "application/x-www-form-urlencoded; charset=utf-8",
        None,
        b"Action=GetCallerIdentity&Version=2011-06-15".to_vec(),
    )
    .await?;
    extract_xml_tag(&response, "Account").ok_or_else(|| {
        QueryError::Protocol("STS GetCallerIdentity response did not contain an account".into())
    })
}

async fn send_signed(
    client: &reqwest::Client,
    credentials: &Credentials,
    endpoint: &str,
    service: &str,
    content_type: &str,
    target: Option<&str>,
    body: Vec<u8>,
) -> Result<Vec<u8>, QueryError> {
    let mut request = http::Request::builder()
        .method("POST")
        .uri(endpoint)
        .header("content-type", content_type);
    if let Some(target) = target {
        request = request.header("x-amz-target", target);
    }
    let mut request = request
        .body(body.clone())
        .map_err(|error| QueryError::Protocol(error.to_string()))?;
    let headers = request
        .headers()
        .iter()
        .filter_map(|(name, value)| value.to_str().ok().map(|value| (name.as_str(), value)));
    let signable = SignableRequest::new("POST", endpoint, headers, SignableBody::Bytes(&body))
        .map_err(|error| QueryError::Protocol(error.to_string()))?;
    let identity = Identity::from(credentials.clone());
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name(service)
        .time(SystemTime::now())
        .settings(SigningSettings::default())
        .build()
        .map_err(|error| QueryError::Protocol(error.to_string()))?
        .into();
    let (instructions, _) = sign(signable, &params)
        .map_err(|error| QueryError::Protocol(error.to_string()))?
        .into_parts();
    instructions.apply_to_request_http1x(&mut request);

    let mut builder = client.post(endpoint).body(body);
    for (name, value) in request.headers() {
        builder = builder.header(name, value);
    }
    let response = builder.send().await.map_err(classify_transport_error)?;
    let status = response.status();
    let bytes = response.bytes().await.map_err(classify_transport_error)?;
    if status.is_success() {
        return Ok(bytes.to_vec());
    }
    Err(classify_response_error(status.as_u16(), &bytes))
}

fn classify_transport_error(error: reqwest::Error) -> QueryError {
    if error.is_timeout() {
        QueryError::TimedOut
    } else {
        QueryError::Protocol(error.to_string())
    }
}

fn classify_response_error(status: u16, body: &[u8]) -> QueryError {
    let detail = String::from_utf8_lossy(body);
    let lower = detail.to_ascii_lowercase();
    if status == 401
        || status == 403
        || lower.contains("accessdenied")
        || lower.contains("access denied")
    {
        QueryError::AccessDenied(detail.into_owned())
    } else if status == 404
        || lower.contains("unknownoperation")
        || lower.contains("unsupportedoperation")
        || lower.contains("not implemented")
    {
        QueryError::Unsupported(detail.into_owned())
    } else {
        QueryError::Protocol(format!("HTTP {status}: {detail}"))
    }
}

fn extract_xml_tag(body: &[u8], tag: &str) -> Option<String> {
    let body = std::str::from_utf8(body).ok()?;
    let start = format!("<{tag}>");
    let end = format!("</{tag}>");
    let value = body.split_once(&start)?.1.split_once(&end)?.0;
    (!value.is_empty()).then(|| value.to_string())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetCreditsResponse {
    #[serde(default)]
    credits: Vec<Credit>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Credit {
    remaining_amount: Option<Amount>,
    estimated_amount: Option<Amount>,
    #[serde(default)]
    applicable_product_names: Vec<String>,
    credit_status: Option<String>,
    end_date: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Amount {
    currency_amount: String,
    currency_code: String,
}

fn parse_report(body: &[u8], today: NaiveDate) -> Result<BedrockCreditsReport, QueryError> {
    let response: GetCreditsResponse = serde_json::from_slice(body)
        .map_err(|error| QueryError::Protocol(format!("invalid GetCredits response: {error}")))?;
    let mut totals = BTreeMap::<String, f64>::new();
    let mut earliest_expiration: Option<f64> = None;
    let today_timestamp = today
        .and_hms_opt(0, 0, 0)
        .expect("a date always has a midnight")
        .and_utc()
        .timestamp() as f64;
    for credit in response.credits {
        if !is_bedrock_credit(&credit) || !is_enabled_and_unexpired(&credit, today_timestamp) {
            continue;
        }
        let Some(amount) = credit
            .estimated_amount
            .as_ref()
            .or(credit.remaining_amount.as_ref())
        else {
            continue;
        };
        let currency = amount.currency_code.trim();
        if currency.is_empty() {
            continue;
        }
        let Ok(value) = amount.currency_amount.parse::<f64>() else {
            continue;
        };
        if !value.is_finite() {
            continue;
        }
        *totals.entry(currency.to_string()).or_default() += value;
        if let Some(end_date) = credit.end_date
            && end_date.is_finite()
            && earliest_expiration
                .as_ref()
                .is_none_or(|current| end_date < *current)
        {
            earliest_expiration = Some(end_date);
        }
    }
    Ok(BedrockCreditsReport {
        amounts: totals
            .into_iter()
            .map(|(currency, amount)| CreditAmount { currency, amount })
            .collect(),
        earliest_expiration: earliest_expiration.and_then(format_timestamp_date),
        as_of: today.format("%Y-%m-%d").to_string(),
    })
}

fn format_timestamp_date(timestamp: f64) -> Option<String> {
    let seconds = timestamp.trunc();
    if !(i64::MIN as f64..=i64::MAX as f64).contains(&seconds) {
        return None;
    }
    chrono::DateTime::from_timestamp(seconds as i64, 0)
        .map(|date| date.date_naive().format("%Y-%m-%d").to_string())
}

fn is_bedrock_credit(credit: &Credit) -> bool {
    credit
        .applicable_product_names
        .iter()
        .any(|product| product.eq_ignore_ascii_case("Amazon Bedrock"))
}

fn is_enabled_and_unexpired(credit: &Credit, today_timestamp: f64) -> bool {
    let enabled = credit.credit_status.as_deref().is_none_or(|status| {
        status.eq_ignore_ascii_case("active") || status.eq_ignore_ascii_case("enabled")
    });
    let unexpired = credit
        .end_date
        .is_none_or(|end| end.is_finite() && end >= today_timestamp);
    enabled && unexpired
}

#[cfg(test)]
mod tests {
    use super::*;

    const TODAY: &str = "2026-07-15";

    fn report(body: &str) -> BedrockCreditsReport {
        parse_report(
            body.as_bytes(),
            NaiveDate::parse_from_str(TODAY, "%Y-%m-%d").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn aggregates_bedrock_credits_by_currency_and_prefers_estimated_amount() {
        let result = report(
            r#"{"credits":[
            {"remainingAmount":{"currencyAmount":"2.50","currencyCode":"USD"},"estimatedAmount":{"currencyAmount":"3.00","currencyCode":"USD"},"applicableProductNames":["Amazon Bedrock"],"creditStatus":"ENABLED","endDate":1796083200},
            {"remainingAmount":{"currencyAmount":"4","currencyCode":"USD"},"applicableProductNames":["amazon bedrock"],"creditStatus":"ENABLED","endDate":1790812800},
            {"remainingAmount":{"currencyAmount":"5.25","currencyCode":"EUR"},"applicableProductNames":["Amazon Bedrock"],"endDate":1798761600}
        ]}"#,
        );
        assert_eq!(
            result.amounts,
            vec![
                CreditAmount {
                    currency: "EUR".into(),
                    amount: 5.25
                },
                CreditAmount {
                    currency: "USD".into(),
                    amount: 7.0
                },
            ]
        );
        assert_eq!(result.earliest_expiration.as_deref(), Some("2026-10-01"));
        assert_eq!(
            result.compact_label(),
            "Bedrock credits: EUR 5.25 · USD 7.00 · expires 2026-10-01 · as of 2026-07-15"
        );
    }

    #[test]
    fn filters_non_bedrock_disabled_and_expired_credits() {
        let result = report(
            r#"{"credits":[
            {"remainingAmount":{"currencyAmount":"2","currencyCode":"USD"},"applicableProductNames":["Amazon EC2"],"creditStatus":"ENABLED","endDate":1798761600},
            {"remainingAmount":{"currencyAmount":"2","currencyCode":"USD"},"applicableProductNames":["Amazon Bedrock"],"creditStatus":"DISABLED","endDate":1798761600},
            {"remainingAmount":{"currencyAmount":"2","currencyCode":"USD"},"applicableProductNames":["Amazon Bedrock"],"creditStatus":"ENABLED","endDate":1783987200}
        ]}"#,
        );
        assert!(result.amounts.is_empty());
        assert_eq!(
            result.compact_label(),
            "Bedrock credits: none · as of 2026-07-15"
        );
    }

    #[test]
    fn classifies_unavailable_errors_concisely() {
        assert_eq!(
            QueryError::MissingCredentials("x".into()).user_reason(),
            "billing credentials are unavailable"
        );
        assert_eq!(
            classify_response_error(403, b"AccessDeniedException").user_reason(),
            "access denied (requires billing:GetCredits)"
        );
        assert_eq!(QueryError::TimedOut.user_reason(), "request timed out");
        assert_eq!(
            classify_response_error(404, b"UnknownOperationException").user_reason(),
            "AWS Billing GetCredits is unavailable"
        );
        assert_eq!(
            QueryError::Protocol("bad JSON".into()).user_reason(),
            "billing response unavailable"
        );
    }
}
