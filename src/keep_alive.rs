use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use axum::http::{HeaderValue, Method, StatusCode};
use futures_util::future::join_all;
use reqwest::header::HeaderMap as ReqwestHeaders;
use serde_json::{Value, json};

use crate::{AppError, AppState, Result, log_json, model::Account, read_sse_response};

const POLL_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MODEL: &str = "gpt-5.6-luna";
const PROMPT: &str = "Reply with OK.";

#[derive(Debug, Clone, PartialEq)]
struct WindowState {
    id: String,
    reset_at: Option<i64>,
    remaining_percent: f64,
}

struct CheckResult {
    account_id: String,
    windows: Vec<WindowState>,
    retry: bool,
}

pub(crate) async fn worker(state: AppState) {
    let mut baselines = HashMap::<String, Vec<WindowState>>::new();
    let mut retries = HashSet::<String>::new();

    loop {
        let accounts: Vec<_> = state
            .active_accounts()
            .await
            .into_iter()
            .filter(|account| account.status == "active")
            .collect();
        let active_ids: HashSet<_> = accounts.iter().map(|account| account.id.clone()).collect();
        baselines.retain(|id, _| active_ids.contains(id));
        retries.retain(|id| active_ids.contains(id));

        let checks = accounts.into_iter().map(|account| {
            let state = state.clone();
            let previous = baselines.get(&account.id).cloned();
            let retry = retries.contains(&account.id);
            async move { check_account(&state, account, previous, retry).await }
        });
        for result in join_all(checks).await {
            if result.retry {
                retries.insert(result.account_id.clone());
            } else {
                retries.remove(&result.account_id);
            }
            baselines.insert(result.account_id, result.windows);
        }

        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => {},
            _ = state.0.keep_alive_wake.notified() => {},
        }
    }
}

async fn check_account(
    state: &AppState,
    account: Account,
    previous: Option<Vec<WindowState>>,
    retry: bool,
) -> CheckResult {
    let first_check = previous.is_none();
    let previous = previous.unwrap_or_default();

    if first_check || retry {
        let reason = if retry { "retry" } else { "startup" };
        let succeeded = run_keep_alive(state, &account, reason).await;
        state.refresh_usage_and_wait(&account.id).await;
        let windows = current_windows(state, &account.id)
            .await
            .unwrap_or(previous);
        return CheckResult {
            account_id: account.id,
            windows,
            retry: !succeeded,
        };
    }

    state.refresh_usage_and_wait(&account.id).await;
    let Some(mut current) = current_windows(state, &account.id).await else {
        return CheckResult {
            account_id: account.id,
            windows: previous,
            retry: false,
        };
    };

    if !reset_detected(&previous, &current, chrono::Utc::now().timestamp()) {
        return CheckResult {
            account_id: account.id,
            windows: current,
            retry: false,
        };
    }

    let succeeded = run_keep_alive(state, &account, "usage_reset").await;
    if succeeded {
        state.refresh_usage_and_wait(&account.id).await;
        if let Some(updated) = current_windows(state, &account.id).await {
            current = updated;
        }
    }
    CheckResult {
        account_id: account.id,
        windows: current,
        retry: !succeeded,
    }
}

async fn current_windows(state: &AppState, id: &str) -> Option<Vec<WindowState>> {
    let account = state.account(id).await?;
    let snapshot = account.usage_snapshot.as_ref()?;
    if snapshot.error.is_some() {
        return None;
    }
    Some(window_states(&snapshot.windows))
}

async fn run_keep_alive(state: &AppState, account: &Account, reason: &str) -> bool {
    log_json(
        json!({"event":"codex_window_keep_alive_attempt", "accountId":account.id, "provider":account.provider, "model":MODEL, "reason":reason}),
        false,
    );
    match send_keep_alive(state, account).await {
        Ok(()) => {
            log_json(
                json!({"event":"codex_window_keep_alive_succeeded", "accountId":account.id, "provider":account.provider, "model":MODEL, "reason":reason}),
                false,
            );
            true
        }
        Err(error) => {
            log_json(
                json!({"event":"codex_window_keep_alive_failed", "accountId":account.id, "provider":account.provider, "model":MODEL, "reason":reason, "status":error.status.as_u16(), "message":error.message}),
                true,
            );
            false
        }
    }
}

async fn send_keep_alive(state: &AppState, account: &Account) -> Result<()> {
    let provider = state.provider(&account.provider)?;
    if provider.mode != "responses-adapter" {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "Codex window keep alive requires a Responses adapter provider",
        ));
    }
    let path = if provider.api.responses_path.is_empty() {
        "/responses"
    } else {
        &provider.api.responses_path
    };
    let body = json!({
        "model": MODEL,
        "input": [{"role":"user", "content":PROMPT}],
        "reasoning": {"effort":"none"},
        "store": false,
        "stream": true
    });
    let mut headers = ReqwestHeaders::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let response = state
        .provider_fetch(
            account,
            path,
            Method::POST,
            headers,
            Some(serde_json::to_vec(&body).unwrap().into()),
            Some(Duration::from_secs(120)),
        )
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(AppError::new(
            status,
            format!("Keepalive request returned HTTP {}", status.as_u16()),
        ));
    }

    let events = read_sse_response(response).await?;
    let mut completed = false;
    for event in events {
        let Ok(payload) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        match payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(&event.event)
        {
            "response.completed" => completed = true,
            "response.failed" | "response.incomplete" => {
                return Err(AppError::new(
                    StatusCode::BAD_GATEWAY,
                    payload
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("Keepalive response failed"),
                ));
            }
            _ => {}
        }
    }
    if !completed {
        return Err(AppError::new(
            StatusCode::BAD_GATEWAY,
            "Keepalive response ended before completion",
        ));
    }
    Ok(())
}

fn window_states(windows: &[Value]) -> Vec<WindowState> {
    let mut states: Vec<_> = windows
        .iter()
        .filter_map(|window| {
            if window.get("limitId").and_then(Value::as_str) != Some("codex") {
                return None;
            }
            let id = match (
                window.get("limitId").and_then(Value::as_str),
                window.get("label").and_then(Value::as_str),
            ) {
                (Some(limit_id), Some(label)) => format!("{limit_id}:{label}"),
                _ => window.get("id")?.as_str()?.to_owned(),
            };
            Some(WindowState {
                id,
                reset_at: window
                    .get("resetsAt")
                    .and_then(Value::as_f64)
                    .map(|value| value.round() as i64),
                remaining_percent: window
                    .get("remainingPercent")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0),
            })
        })
        .collect();
    states.sort_by(|left, right| left.id.cmp(&right.id));
    states
}

fn reset_detected(previous: &[WindowState], current: &[WindowState], now: i64) -> bool {
    let previous: HashMap<_, _> = previous
        .iter()
        .map(|window| (window.id.as_str(), window))
        .collect();

    current.iter().any(|window| {
        let Some(old) = previous.get(window.id.as_str()) else {
            return true;
        };
        window.reset_at != old.reset_at
            || window.reset_at.is_some_and(|reset_at| reset_at <= now)
            || window.remaining_percent > old.remaining_percent + 0.01
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn states(windows: Vec<Value>) -> Vec<WindowState> {
        window_states(&windows)
    }

    #[test]
    fn normal_usage_does_not_look_like_a_reset() {
        let previous = states(vec![json!({
            "id":"codex:primary:0", "limitId":"codex", "label":"5-hour window",
            "remainingPercent":80, "resetsAt":2000
        })]);
        let current = states(vec![json!({
            "id":"codex:primary:0", "limitId":"codex", "label":"5-hour window",
            "remainingPercent":60, "resetsAt":2000
        })]);
        assert!(!reset_detected(&previous, &current, 1000));
    }

    #[test]
    fn changed_or_expired_window_looks_like_a_reset() {
        let previous = states(vec![json!({
            "id":"codex:primary:0", "limitId":"codex", "label":"5-hour window",
            "remainingPercent":50, "resetsAt":1000
        })]);
        let changed = states(vec![json!({
            "id":"codex:primary:0", "limitId":"codex", "label":"5-hour window",
            "remainingPercent":100, "resetsAt":2800
        })]);
        assert!(reset_detected(&previous, &changed, 900));
        assert!(reset_detected(&previous, &previous, 1000));
    }

    #[test]
    fn increased_usage_or_new_window_looks_like_a_reset() {
        let previous = states(vec![json!({
            "id":"codex:primary:0", "limitId":"codex", "label":"5-hour window",
            "remainingPercent":25, "resetsAt":2000
        })]);
        let increased = states(vec![json!({
            "id":"codex:primary:0", "limitId":"codex", "label":"5-hour window",
            "remainingPercent":100, "resetsAt":2000
        })]);
        let added = states(vec![
            json!({
                "id":"codex:primary:0", "limitId":"codex", "label":"5-hour window",
                "remainingPercent":25, "resetsAt":2000
            }),
            json!({
                "id":"codex:secondary:1", "limitId":"codex", "label":"Weekly window",
                "remainingPercent":100, "resetsAt":3000
            }),
        ]);
        assert!(reset_detected(&previous, &increased, 1000));
        assert!(reset_detected(&previous, &added, 1000));
    }

    #[test]
    fn ignores_non_codex_windows() {
        let windows = states(vec![json!({
            "id":"reviews:primary:0", "limitId":"code_reviews", "label":"Weekly window",
            "remainingPercent":100, "resetsAt":2000
        })]);
        assert!(windows.is_empty());
    }
}
