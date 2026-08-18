use std::collections::HashMap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Database {
    #[serde(default = "db_version")]
    pub version: u32,
    pub next_port: u32,
    #[serde(default)]
    pub released_ports: Vec<u16>,
    #[serde(default)]
    pub accounts: Vec<Account>,
}
fn db_version() -> u32 {
    1
}

impl Database {
    pub fn empty(first_port: u16) -> Self {
        Self {
            version: 1,
            next_port: first_port.into(),
            released_ports: vec![],
            accounts: vec![],
        }
    }
    pub fn normalize(&mut self, first_port: u16, default_provider: &str) {
        if self.version == 0 {
            self.version = 1;
        }
        if self.next_port == 0 {
            self.next_port = first_port.into();
        }
        for account in &mut self.accounts {
            if account.id.is_empty() {
                account.id = Uuid::new_v4().to_string();
            }
            if account.provider.is_empty() {
                account.provider = default_provider.into();
            }
            if account.status.is_empty() {
                account.status = "active".into();
            }
            if account.created_at.is_empty() {
                account.created_at = now();
            }
            if account.updated_at.is_empty() {
                account.updated_at.clone_from(&account.created_at);
            }
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub provider: String,
    pub port: Option<u16>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub metadata: AccountMetadata,
    #[serde(default)]
    pub tokens: Tokens,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_request_at: Option<String>,
    #[serde(default)]
    pub last_status: Option<u16>,
    #[serde(default)]
    pub usage_snapshot: Option<UsageSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSnapshot {
    #[serde(default)]
    pub plan_type: Option<String>,
    #[serde(default)]
    pub windows: Vec<Value>,
    #[serde(default)]
    pub fetched_at: Option<String>,
    #[serde(default)]
    pub refresh_attempted_at: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountMetadata {
    #[serde(default, flatten)]
    pub values: HashMap<String, Value>,
}
impl AccountMetadata {
    pub fn string(&self, key: &str) -> Option<&str> {
        self.values.get(key).and_then(Value::as_str)
    }
    pub fn merge(&mut self, other: Self) {
        self.values.extend(other.values);
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tokens {
    #[serde(default)]
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: String,
    #[serde(default)]
    pub id_token: String,
    #[serde(default)]
    pub token_type: String,
    #[serde(default)]
    pub expires_at: String,
}

#[derive(Debug, Clone)]
pub struct PendingOauth {
    pub provider_id: String,
    pub verifier: String,
    pub redirect_uri: String,
    pub reauth_account_id: Option<String>,
    pub created_at: std::time::Instant,
}

pub fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
pub fn parse_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn older_accounts_without_usage_snapshots_remain_compatible() {
        let account: Account = serde_json::from_value(json!({
            "id":"account-1",
            "provider":"chatgpt",
            "port":18001,
            "status":"active",
            "metadata":{},
            "tokens":{},
            "createdAt":"2026-01-01T00:00:00.000Z",
            "updatedAt":"2026-01-01T00:00:00.000Z"
        }))
        .unwrap();
        assert!(account.usage_snapshot.is_none());
    }

    #[test]
    fn usage_snapshot_uses_the_existing_camel_case_database_shape() {
        let snapshot = UsageSnapshot {
            plan_type: Some("pro".into()),
            windows: vec![json!({"remainingPercent":75})],
            fetched_at: Some("2026-01-01T00:00:00.000Z".into()),
            refresh_attempted_at: Some("2026-01-01T00:00:00.000Z".into()),
            error: None,
        };
        let value = serde_json::to_value(snapshot).unwrap();
        assert_eq!(value["planType"], "pro");
        assert_eq!(value["windows"][0]["remainingPercent"], 75);
        assert!(value.get("fetchedAt").is_some());
        assert!(value.get("refreshAttemptedAt").is_some());
    }
}
