use std::path::PathBuf;
use std::time::Instant;

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::model::now;

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
  error TEXT
);
CREATE INDEX IF NOT EXISTS idx_upstream_requests_account_ts ON upstream_requests(account_id, ts);
CREATE INDEX IF NOT EXISTS idx_upstream_requests_ts ON upstream_requests(ts);
"#;

/// Raw upstream request/response record. Bodies are stored as perfect
/// plaintext copies with no truncation or redaction.
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
        }
    }
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
        Ok(conn) => conn,
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

fn open_and_migrate(path: &PathBuf) -> Result<rusqlite::Connection, String> {
    let conn = rusqlite::Connection::open(path).map_err(|e| e.to_string())?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .map_err(|e| e.to_string())?;
    conn.execute_batch(SCHEMA).map_err(|e| e.to_string())?;
    Ok(conn)
}

fn insert(conn: &rusqlite::Connection, entry: &StoredEntry) -> Result<(), String> {
    conn.execute(
        "INSERT INTO upstream_requests (ts, account_id, provider, kind, method, url, request_body, response_status, response_body, duration_ms, error) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
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
        ],
    )
    .map_err(|e| e.to_string())?;
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
    fn schema_creates_table_in_memory() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM upstream_requests", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
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
}
