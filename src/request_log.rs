use std::path::PathBuf;
use std::time::Instant;

use serde_json::Value;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::model::now;

const SCHEMA_VERSION: i64 = 2;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS upstream_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  account_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  kind TEXT NOT NULL,
  method TEXT NOT NULL,
  url TEXT NOT NULL,
  request_body TEXT,
  response_status INTEGER,
  response_body TEXT,
  duration_ms INTEGER NOT NULL,
  error TEXT,
  model TEXT,
  reasoning_effort TEXT,
  input_tokens INTEGER,
  output_tokens INTEGER,
  total_tokens INTEGER,
  cached_tokens INTEGER,
  reasoning_tokens INTEGER
);
CREATE INDEX IF NOT EXISTS idx_upstream_requests_account_ts ON upstream_requests(account_id, ts);
CREATE INDEX IF NOT EXISTS idx_upstream_requests_ts ON upstream_requests(ts);
CREATE TABLE IF NOT EXISTS usage_windows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER NOT NULL REFERENCES upstream_requests(id),
  ts TEXT NOT NULL,
  account_id TEXT NOT NULL,
  limit_id TEXT NOT NULL,
  limit_name TEXT,
  label TEXT NOT NULL,
  remaining_percent REAL NOT NULL,
  resets_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_usage_windows_account_ts ON usage_windows(account_id, ts);
"#;

/// First-class cost attribution parsed out of the raw bodies. Everything is
/// optional: models/usage rows have no token concept, and failed or partial
/// responses may carry no usage block.
#[derive(Debug, Clone, Default)]
pub struct Parsed {
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
}

/// One usage window snapshot. All window types (5-hour, weekly, and whatever
/// the provider adds later) share this table, distinguished by
/// `limit_id`/`label`.
#[derive(Debug, Clone)]
pub struct UsageWindow {
    pub limit_id: String,
    pub limit_name: Option<String>,
    pub label: String,
    pub remaining_percent: f64,
    pub resets_at: Option<i64>,
}

/// Raw upstream request/response record. Bodies are stored as perfect
/// plaintext copies with no truncation or redaction; `parsed` holds the
/// first-class cost columns and `windows` (usage fetches only) the
/// per-window snapshots, inserted atomically with the parent row.
#[derive(Debug, Clone)]
pub struct StoredEntry {
    pub ts: String,
    pub account_id: String,
    pub provider: String,
    pub kind: String,
    pub method: String,
    pub url: String,
    pub request_body: Option<String>,
    pub response_status: Option<i64>,
    pub response_body: Option<String>,
    pub duration_ms: i64,
    pub error: Option<String>,
    pub parsed: Parsed,
    pub windows: Vec<UsageWindow>,
}

/// In-flight request context. Created in `provider_fetch` when the upstream
/// HTTP request is sent, completed once the response body finishes (or fails).
/// The request body is always complete; only the response may be partial.
#[derive(Debug)]
pub struct PendingLog {
    pub ts: String,
    pub account_id: String,
    pub provider: String,
    pub kind: String,
    pub method: String,
    pub url: String,
    pub request_body: Option<String>,
    pub start: Instant,
}

impl PendingLog {
    pub fn complete(
        self,
        response_status: Option<u16>,
        response_body: Option<String>,
        error: Option<String>,
    ) -> StoredEntry {
        let parsed = parse_entry(self.request_body.as_deref(), response_body.as_deref());
        StoredEntry {
            ts: self.ts,
            account_id: self.account_id,
            provider: self.provider,
            kind: self.kind,
            method: self.method,
            url: self.url,
            request_body: self.request_body,
            response_status: response_status.map(i64::from),
            response_body,
            duration_ms: self.start.elapsed().as_millis() as i64,
            error,
            parsed,
            windows: Vec::new(),
        }
    }
}

/// Parse first-class cost fields out of raw upstream bodies.
fn parse_entry(request_body: Option<&str>, response_body: Option<&str>) -> Parsed {
    let mut out = Parsed::default();
    if let Some(body) = request_body
        && let Ok(value) = serde_json::from_str::<Value>(body)
    {
        if let Some(model) = value.get("model").and_then(Value::as_str) {
            out.model = Some(model.to_owned());
        }
        out.reasoning_effort = crate::convert::reasoning_effort(&value);
    }
    if let Some(body) = response_body {
        // Raw SSE: the last response.completed event carries the final usage.
        for line in body.lines() {
            let data = match line.strip_prefix("data: ") {
                Some(data) => data,
                None => line.strip_prefix("data:").unwrap_or(""),
            };
            if data.is_empty() || data == "[DONE]" {
                continue;
            }
            let Ok(payload) = serde_json::from_str::<Value>(data) else {
                continue;
            };
            if payload.get("type").and_then(Value::as_str) != Some("response.completed") {
                continue;
            }
            let Some(usage) = payload.pointer("/response/usage") else {
                continue;
            };
            out.input_tokens = int_field(usage, "input_tokens");
            out.output_tokens = int_field(usage, "output_tokens");
            out.total_tokens = int_field(usage, "total_tokens");
            out.cached_tokens = usage
                .pointer("/input_tokens_details/cached_tokens")
                .and_then(as_i64);
            out.reasoning_tokens = usage
                .pointer("/output_tokens_details/reasoning_tokens")
                .and_then(as_i64);
        }
    }
    out
}

fn int_field(usage: &Value, key: &str) -> Option<i64> {
    usage.get(key).and_then(as_i64)
}

fn as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|v| v.try_into().ok()))
}

/// Convert normalized usage windows (the same shape stored on the account
/// snapshot) into snapshot rows.
pub fn usage_windows_from_normalized(windows: &[Value]) -> Vec<UsageWindow> {
    windows
        .iter()
        .filter_map(|window| {
            Some(UsageWindow {
                limit_id: window.get("limitId")?.as_str()?.to_owned(),
                limit_name: window
                    .get("limitName")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                label: window.get("label")?.as_str()?.to_owned(),
                remaining_percent: window.get("remainingPercent")?.as_f64()?,
                resets_at: window.get("resetsAt").and_then(as_i64).or_else(|| {
                    window
                        .get("resetsAt")
                        .and_then(Value::as_f64)
                        .map(|v| v.round() as i64)
                }),
            })
        })
        .collect()
}

/// Cloneable handle to the background SQLite writer. Logging is
/// fire-and-forget and never fails a proxied request.
#[derive(Debug, Clone, Default)]
pub struct RequestLog {
    sender: Option<UnboundedSender<StoredEntry>>,
}

impl RequestLog {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create {}: {e}", parent.display()))?;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move { writer_task(path, rx).await });
        Ok(Self { sender: Some(tx) })
    }

    pub fn log(&self, entry: StoredEntry) {
        if let Some(tx) = &self.sender {
            let _ = tx.send(entry);
        }
    }

    pub fn finish(
        &self,
        pending: PendingLog,
        response_status: Option<u16>,
        response_body: Option<String>,
        error: Option<String>,
    ) {
        self.log(pending.complete(response_status, response_body, error));
    }
}

async fn writer_task(path: PathBuf, mut rx: UnboundedReceiver<StoredEntry>) {
    let conn = match open_and_migrate(&path) {
        Ok((conn, wiped)) => {
            if wiped {
                println!(
                    "{}",
                    serde_json::json!({"ts": now(), "event":"request_log_fresh_start", "message":"Older request-log schema found; starting a fresh ai_proxy.sqlite3", "path":path.display().to_string()})
                );
            }
            conn
        }
        Err(e) => {
            eprintln!(
                "{}",
                serde_json::json!({"ts": now(), "event":"request_log_failed", "message":format!("Failed to open {}: {e}", path.display())})
            );
            return;
        }
    };
    while let Some(entry) = rx.recv().await {
        if let Err(e) = insert(&conn, &entry) {
            eprintln!(
                "{}",
                serde_json::json!({"ts": now(), "event":"request_log_write_failed", "message":e.to_string()})
            );
        }
    }
}

fn open_and_migrate(path: &PathBuf) -> Result<(rusqlite::Connection, bool), String> {
    let conn = rusqlite::Connection::open(path).map_err(|e| e.to_string())?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .map_err(|e| e.to_string())?;
    let wiped = migrate(&conn)?;
    Ok((conn, wiped))
}

/// Bring the database to the current schema. Pre-v2 databases are wiped for
/// a fresh start (no production data exists yet); returns whether a wipe
/// happened.
fn migrate(conn: &rusqlite::Connection) -> Result<bool, String> {
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|e| e.to_string())?;
    let has_table: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='upstream_requests'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count > 0)
        .map_err(|e| e.to_string())?;
    let wiped = version < SCHEMA_VERSION && has_table;
    if wiped {
        conn.execute_batch(
            "DROP TABLE IF EXISTS usage_windows; DROP TABLE IF EXISTS upstream_requests;",
        )
        .map_err(|e| e.to_string())?;
    }
    conn.execute_batch(SCHEMA).map_err(|e| e.to_string())?;
    conn.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION}"))
        .map_err(|e| e.to_string())?;
    Ok(wiped)
}

fn insert(conn: &rusqlite::Connection, entry: &StoredEntry) -> Result<(), String> {
    conn.execute(
        "INSERT INTO upstream_requests (ts, account_id, provider, kind, method, url, request_body, response_status, response_body, duration_ms, error, model, reasoning_effort, input_tokens, output_tokens, total_tokens, cached_tokens, reasoning_tokens) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
        rusqlite::params![
            entry.ts,
            entry.account_id,
            entry.provider,
            entry.kind,
            entry.method,
            entry.url,
            entry.request_body,
            entry.response_status,
            entry.response_body,
            entry.duration_ms,
            entry.error,
            entry.parsed.model,
            entry.parsed.reasoning_effort,
            entry.parsed.input_tokens,
            entry.parsed.output_tokens,
            entry.parsed.total_tokens,
            entry.parsed.cached_tokens,
            entry.parsed.reasoning_tokens,
        ],
    )
    .map_err(|e| e.to_string())?;
    let request_id = conn.last_insert_rowid();
    for window in &entry.windows {
        conn.execute(
            "INSERT INTO usage_windows (request_id, ts, account_id, limit_id, limit_name, label, remaining_percent, resets_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                request_id,
                entry.ts,
                entry.account_id,
                window.limit_id,
                window.limit_name,
                window.label,
                window.remaining_percent,
                window.resets_at,
            ],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Default SQLite path: `ai_proxy.sqlite3` next to the JSON db.
pub fn default_path(db_path: &std::path::Path) -> PathBuf {
    const NAME: &str = "ai_proxy.sqlite3";
    db_path
        .parent()
        .map(|p| {
            if p.as_os_str().is_empty() {
                PathBuf::from(NAME)
            } else {
                p.join(NAME)
            }
        })
        .unwrap_or_else(|| PathBuf::from(NAME))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const V1_SCHEMA: &str = r#"
CREATE TABLE upstream_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  account_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  kind TEXT NOT NULL,
  method TEXT NOT NULL,
  url TEXT NOT NULL,
  request_body TEXT,
  response_status INTEGER,
  response_body TEXT,
  duration_ms INTEGER NOT NULL,
  error TEXT
);"#;

    #[test]
    fn default_path_is_ai_proxy_sqlite3() {
        assert_eq!(
            default_path(std::path::Path::new("./orche-proxy.db.json")),
            PathBuf::from("./ai_proxy.sqlite3")
        );
        assert_eq!(
            default_path(std::path::Path::new("/data/foo.json")),
            PathBuf::from("/data/ai_proxy.sqlite3")
        );
        assert_eq!(
            default_path(std::path::Path::new("orche-proxy.db.json")),
            PathBuf::from("ai_proxy.sqlite3")
        );
    }

    #[test]
    fn schema_creates_both_tables() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        assert!(!migrate(&conn).unwrap());
        for table in ["upstream_requests", "usage_windows"] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0);
        }
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn v1_databases_are_wiped_for_a_fresh_start() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(V1_SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO upstream_requests (ts, account_id, provider, kind, method, url, duration_ms) VALUES ('t','a','p','k','GET','u',1)",
            [],
        )
        .unwrap();
        assert!(migrate(&conn).unwrap());
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM upstream_requests", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
        // New cost columns exist.
        conn.execute(
            "INSERT INTO upstream_requests (ts, account_id, provider, kind, method, url, duration_ms, model, reasoning_effort, input_tokens) VALUES ('t','a','p','k','GET','u',1,'m','high',5)",
            [],
        )
        .unwrap();
    }

    #[test]
    fn pending_complete_computes_duration() {
        let pending = PendingLog {
            ts: "2026-01-01T00:00:00.000Z".into(),
            account_id: "a".into(),
            provider: "chatgpt".into(),
            kind: "models".into(),
            method: "GET".into(),
            url: "https://example.com/models".into(),
            request_body: None,
            start: Instant::now(),
        };
        let entry = pending.complete(Some(200), Some("{}".into()), None);
        assert_eq!(entry.response_status, Some(200));
        assert!(entry.duration_ms >= 0);
    }

    #[test]
    fn parses_model_and_reasoning_effort_from_request() {
        let parsed = parse_entry(
            Some(r#"{"model":"gpt-5.5","reasoning":{"effort":"xhigh"}}"#),
            None,
        );
        assert_eq!(parsed.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(parsed.reasoning_effort.as_deref(), Some("xhigh"));
        assert_eq!(parsed.input_tokens, None);
    }

    #[test]
    fn parses_token_usage_from_raw_sse() {
        let sse = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{",
            "\"input_tokens\":10,\"output_tokens\":5,\"total_tokens\":15,",
            "\"input_tokens_details\":{\"cached_tokens\":4,\"cache_write_tokens\":1},",
            "\"output_tokens_details\":{\"reasoning_tokens\":3}}}}\n\n",
        );
        let parsed = parse_entry(Some(r#"{"model":"gpt-5.6-luna"}"#), Some(sse));
        assert_eq!(parsed.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(parsed.input_tokens, Some(10));
        assert_eq!(parsed.output_tokens, Some(5));
        assert_eq!(parsed.total_tokens, Some(15));
        assert_eq!(parsed.cached_tokens, Some(4));
        assert_eq!(parsed.reasoning_tokens, Some(3));
    }

    #[test]
    fn missing_usage_leaves_token_columns_null() {
        let parsed = parse_entry(
            Some(r#"{"model":"gpt-test"}"#),
            Some("event: response.failed\ndata: {\"type\":\"response.failed\"}\n\n"),
        );
        assert_eq!(parsed.model.as_deref(), Some("gpt-test"));
        assert_eq!(parsed.input_tokens, None);
        assert_eq!(parsed.total_tokens, None);
    }

    #[test]
    fn converts_normalized_usage_windows() {
        let windows = usage_windows_from_normalized(&[
            json!({"limitId":"codex","limitName":null,"label":"5-hour window","remainingPercent":87.5,"resetsAt":1790035501.0}),
            json!({"limitId":"codex","limitName":null,"label":"Weekly window","remainingPercent":28.0,"resetsAt":null}),
        ]);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].limit_id, "codex");
        assert_eq!(windows[0].label, "5-hour window");
        assert_eq!(windows[0].remaining_percent, 87.5);
        assert_eq!(windows[0].resets_at, Some(1790035501));
        assert_eq!(windows[1].resets_at, None);
    }
}
