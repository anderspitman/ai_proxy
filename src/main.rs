mod config;
mod convert;
mod dashboard;
mod model;

use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Extension, Form, Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{any, get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use config::{APP_NAME, APP_VERSION, Config, Provider};
use convert::{
    SseDecoder, SseEvent, build_responses_request, chat_usage, collect_responses_events,
    normalize_models, normalize_responses_request, random_id, reasoning_effort, response_to_chat,
};
use futures_util::{StreamExt, future::join_all};
use model::{
    Account, AccountMetadata, Database, PendingOauth, Tokens, UsageSnapshot, now, parse_time,
};
use rand::RngCore;
use reqwest::header::HeaderMap as ReqwestHeaders;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify, broadcast},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const BODY_LIMIT: usize = 20 * 1024 * 1024;
const REFRESH_WINDOW: Duration = Duration::from_secs(5 * 60);

#[derive(Clone)]
struct AppState(Arc<Inner>);
struct Inner {
    config: Config,
    db: Mutex<Database>,
    save_lock: Mutex<()>,
    refresh_lock: Mutex<()>,
    pending_oauth: Mutex<HashMap<String, PendingOauth>>,
    account_servers: Mutex<HashMap<String, AccountServer>>,
    usage_refreshes: Mutex<HashMap<String, Arc<UsageRefresh>>>,
    usage_events: broadcast::Sender<Value>,
    client: reqwest::Client,
}
struct UsageRefresh {
    state: Mutex<UsageRefreshState>,
    finished: Notify,
}
#[derive(Default)]
struct UsageRefreshState {
    running: bool,
    pending: bool,
}
struct AccountServer {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}
struct UsageRefreshOnDrop {
    state: AppState,
    account_id: String,
}
impl Drop for UsageRefreshOnDrop {
    fn drop(&mut self) {
        let state = self.state.clone();
        let account_id = self.account_id.clone();
        tokio::spawn(async move { state.schedule_usage_refresh(&account_id).await });
    }
}

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
    details: Option<Value>,
    auth: bool,
}
impl AppError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            details: None,
            auth: false,
        }
    }
    fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
    fn auth(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
            details: None,
            auth: true,
        }
    }
}
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if self.status.is_server_error() {
            log_json(
                json!({"event":"server_error", "status":self.status.as_u16(), "message":self.message}),
                true,
            );
        }
        json_response(
            self.status,
            json!({ "error": { "message": self.message, "type": if self.auth { "auth_error" } else if self.status.is_server_error() { "server_error" } else { "invalid_request_error" }, "details": self.details } }),
        )
    }
}
type Result<T> = std::result::Result<T, AppError>;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        log_json(json!({"event":"fatal", "message":error}), true);
        std::process::exit(1);
    }
}
async fn run() -> std::result::Result<(), String> {
    let cli = config::parse_args()?;
    if cli.help {
        print!("{}", config::help());
        return Ok(());
    }
    let config = config::load(&cli).await?;
    let mut db = if tokio::fs::try_exists(&config.db_path)
        .await
        .map_err(|e| e.to_string())?
    {
        let text = tokio::fs::read_to_string(&config.db_path)
            .await
            .map_err(|e| format!("Failed to read {}: {e}", config.db_path.display()))?;
        serde_json::from_str(&text)
            .map_err(|e| format!("Invalid DB {}: {e}", config.db_path.display()))?
    } else {
        Database::empty(config.port_range.start)
    };
    db.normalize(config.port_range.start, &config.default_provider);
    let (usage_event_tx, _) = broadcast::channel(128);
    let state = AppState(Arc::new(Inner {
        config,
        db: Mutex::new(db),
        save_lock: Mutex::new(()),
        refresh_lock: Mutex::new(()),
        pending_oauth: Mutex::new(HashMap::new()),
        account_servers: Mutex::new(HashMap::new()),
        usage_refreshes: Mutex::new(HashMap::new()),
        usage_events: usage_event_tx,
        client: reqwest::Client::builder()
            .build()
            .map_err(|e| e.to_string())?,
    }));

    let oauth_listener = bind(
        &state.0.config.host,
        state.0.config.oauth_port,
        "OAuth redirect",
    )
    .await?;
    let admin_listener = bind(
        &state.0.config.host,
        state.0.config.admin_port,
        "admin dashboard",
    )
    .await?;
    state.start_stored_accounts().await?;
    for account in state.active_accounts().await {
        if account
            .usage_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.fetched_at.as_ref())
            .is_none()
        {
            state.schedule_usage_refresh(&account.id).await;
        }
    }

    let oauth_app = Router::new()
        .fallback(any(oauth_request))
        .with_state(state.clone());
    let admin_app = Router::new()
        .route("/", get(admin_home))
        .route("/api/accounts", get(admin_accounts))
        .route("/api/usage", get(admin_usage))
        .route("/api/usage/events", get(usage_events))
        .route("/api/usage/refresh", post(refresh_all_usage))
        .route("/accounts", post(add_account))
        .route("/accounts/{id}/reauth", post(reauth_account))
        .route("/accounts/{id}/remove", post(remove_account_handler))
        .fallback(any(admin_not_found))
        .with_state(state.clone());
    tokio::spawn(async move {
        if let Err(e) = axum::serve(oauth_listener, oauth_app).await {
            log_json(
                json!({"event":"server_error","message":e.to_string()}),
                true,
            );
        }
    });
    tokio::spawn(async move {
        if let Err(e) = axum::serve(admin_listener, admin_app).await {
            log_json(
                json!({"event":"server_error","message":e.to_string()}),
                true,
            );
        }
    });

    log_json(
        json!({"event":"admin_listening", "url":state.admin_url("/"), "host":state.0.config.host, "port":state.0.config.admin_port}),
        false,
    );
    log_json(
        json!({"event":"oauth_listening", "url":format!("http://localhost:{}{}",state.0.config.oauth_port,state.oauth_paths()[0]), "host":state.0.config.host, "port":state.0.config.oauth_port}),
        false,
    );
    for account in state.active_accounts().await {
        log_json(
            json!({"event":"account_listening","accountId":account.id,"provider":account.provider,"status":account.status,"port":account.port,"baseUrl":state.base_url(&account)}),
            false,
        );
    }
    tokio::signal::ctrl_c().await.map_err(|e| e.to_string())?;
    Ok(())
}
async fn bind(host: &str, port: u16, label: &str) -> std::result::Result<TcpListener, String> {
    TcpListener::bind(format!("{host}:{port}"))
        .await
        .map_err(|e| format!("Failed to bind {label} on {host}:{port}: {e}"))
}

impl AppState {
    async fn active_accounts(&self) -> Vec<Account> {
        self.0
            .db
            .lock()
            .await
            .accounts
            .iter()
            .filter(|a| a.status != "removed")
            .cloned()
            .collect()
    }
    async fn account(&self, id: &str) -> Option<Account> {
        self.0
            .db
            .lock()
            .await
            .accounts
            .iter()
            .find(|a| a.id == id && a.status != "removed")
            .cloned()
    }
    fn provider(&self, id: &str) -> Result<Provider> {
        self.0
            .config
            .providers
            .get(id)
            .cloned()
            .ok_or_else(|| AppError::internal(format!("Provider not configured: {id}")))
    }
    async fn save_db(&self) -> Result<()> {
        let _save = self.0.save_lock.lock().await;
        let snapshot = self.0.db.lock().await.clone();
        if let Some(parent) = self.0.config.db_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| AppError::internal(e.to_string()))?;
        }
        let tmp = self.0.config.db_path.with_extension(format!(
            "{}.{}.tmp",
            std::process::id(),
            chrono::Utc::now().timestamp_millis()
        ));
        let data =
            serde_json::to_vec_pretty(&snapshot).map_err(|e| AppError::internal(e.to_string()))?;
        tokio::fs::write(&tmp, [&data[..], b"\n"].concat())
            .await
            .map_err(|e| AppError::internal(e.to_string()))?;
        tokio::fs::rename(&tmp, &self.0.config.db_path)
            .await
            .map_err(|e| AppError::internal(e.to_string()))
    }
    fn base_url(&self, account: &Account) -> String {
        format!(
            "http://{}:{}/v1",
            self.0.config.public_host,
            account.port.unwrap_or(0)
        )
    }
    fn admin_url(&self, suffix: &str) -> String {
        format!(
            "http://{}:{}{}",
            self.0.config.public_host, self.0.config.admin_port, suffix
        )
    }
    fn oauth_paths(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .0
            .config
            .providers
            .values()
            .map(|p| {
                if p.oauth.redirect_path.is_empty() {
                    "/auth/callback".into()
                } else {
                    p.oauth.redirect_path.clone()
                }
            })
            .collect();
        paths.sort();
        paths.dedup();
        paths
    }
    fn oauth_redirect_uri(&self, provider: &Provider) -> String {
        format!(
            "http://localhost:{}{}",
            self.0.config.oauth_port,
            if provider.oauth.redirect_path.is_empty() {
                "/auth/callback"
            } else {
                &provider.oauth.redirect_path
            }
        )
    }
    async fn start_stored_accounts(&self) -> std::result::Result<(), String> {
        for account in self.active_accounts().await {
            let port = account
                .port
                .ok_or_else(|| format!("Stored account {} has no port", account.id))?;
            let listener = bind(&self.0.config.host, port, &format!("stable account {}", account.id)).await.map_err(|e| format!("Stable downstream port {port} for {} is unavailable. Downstream agents may depend on {}. Stop the conflicting process or edit {}.\nCause: {e}", account_label(&account), self.base_url(&account), self.0.config.db_path.display()))?;
            self.spawn_account(account.id, listener).await;
        }
        Ok(())
    }
    async fn spawn_account(&self, id: String, listener: TcpListener) {
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let app = Router::new()
            .fallback(any(account_request))
            .layer(Extension(id.clone()))
            .with_state(self.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await;
        });
        self.0
            .account_servers
            .lock()
            .await
            .insert(id, AccountServer { cancel, task });
    }
    async fn stop_account(&self, id: &str) {
        if let Some(server) = self.0.account_servers.lock().await.remove(id) {
            server.cancel.cancel();
            let _ = server.task.await;
        }
    }
}

async fn admin_home(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Html<String> {
    let accounts = state.active_accounts().await;
    Html(dashboard::render(
        &state.0.config,
        &accounts,
        query.contains_key("removed"),
        &state.oauth_paths(),
    ))
}
async fn admin_accounts(State(state): State<AppState>) -> Response {
    let accounts: Vec<Value> = state
        .active_accounts()
        .await
        .iter()
        .map(|a| public_account(&state, a))
        .collect();
    json_response(StatusCode::OK, json!({"accounts":accounts}))
}
async fn admin_usage(State(state): State<AppState>) -> Response {
    json_response(StatusCode::OK, state.current_usage_snapshots().await)
}
async fn usage_events(State(state): State<AppState>) -> Response {
    state.usage_event_stream().await
}
async fn refresh_all_usage(State(state): State<AppState>) -> Response {
    let accounts = state.active_accounts().await;
    join_all(
        accounts
            .iter()
            .map(|account| state.refresh_usage_and_wait(&account.id)),
    )
    .await;
    let snapshots = join_all(
        accounts
            .iter()
            .map(|account| state.usage_snapshot_for_id(&account.id)),
    )
    .await;
    let failed = snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.get("error").is_some() || snapshot.get("refreshError").is_some()
        })
        .count();
    json_response(
        StatusCode::OK,
        json!({"refreshedAt":now(), "failed":failed, "accounts":snapshots}),
    )
}
async fn add_account(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> Result<Redirect> {
    let provider = form
        .get("provider")
        .cloned()
        .unwrap_or_else(|| state.0.config.default_provider.clone());
    Ok(Redirect::to(&state.begin_oauth(&provider, None).await?))
}
async fn reauth_account(State(state): State<AppState>, Path(id): Path<String>) -> Result<Redirect> {
    let account = state
        .account(&id)
        .await
        .ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Account not found"))?;
    Ok(Redirect::to(
        &state.begin_oauth(&account.provider, Some(id)).await?,
    ))
}
async fn remove_account_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Redirect> {
    state.remove_account(&id).await?;
    Ok(Redirect::to("/?removed=1"))
}
async fn admin_not_found(method: Method) -> Response {
    if method == Method::OPTIONS {
        return cors_empty();
    }
    (
        StatusCode::NOT_FOUND,
        Html(dashboard::message_page(
            "Not found",
            "No admin route matched this request.",
            "/",
        )),
    )
        .into_response()
}

async fn oauth_request(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
) -> Result<Response> {
    let url = Url::parse(&format!("http://localhost{uri}"))
        .map_err(|e| AppError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
    if method == Method::GET && url.path() == "/" {
        return Ok(Html(dashboard::message_page("OAuth Redirect Listener", "This listener is waiting for an OAuth callback. Return to the dashboard to add an account.", &state.admin_url("/"))).into_response());
    }
    if method != Method::GET || !state.oauth_paths().iter().any(|p| p == url.path()) {
        return Ok((
            StatusCode::NOT_FOUND,
            Html(dashboard::message_page(
                "Not found",
                "No OAuth route matched this request.",
                &state.admin_url("/"),
            )),
        )
            .into_response());
    }
    let params: HashMap<_, _> = url.query_pairs().into_owned().collect();
    if let Some(error) = params.get("error") {
        let description = params
            .get("error_description")
            .map(String::as_str)
            .unwrap_or("The provider rejected the login request.");
        return Ok((
            StatusCode::BAD_REQUEST,
            Html(dashboard::message_page(
                "Login failed",
                &format!("{}: {}", escape_html(error), escape_html(description)),
                &state.admin_url("/"),
            )),
        )
            .into_response());
    }
    let code = params.get("code").ok_or_else(|| {
        AppError::new(
            StatusCode::BAD_REQUEST,
            "OAuth callback is missing code or state",
        )
    })?;
    let oauth_state = params.get("state").ok_or_else(|| {
        AppError::new(
            StatusCode::BAD_REQUEST,
            "OAuth callback is missing code or state",
        )
    })?;
    let pending = state
        .0
        .pending_oauth
        .lock()
        .await
        .remove(oauth_state)
        .ok_or_else(|| {
            AppError::new(
                StatusCode::BAD_REQUEST,
                "OAuth state was not recognized. Start login again from the dashboard.",
            )
        })?;
    let account = state.finish_oauth(pending, code).await?;
    let message = format!(
        "{} is available at <code>{}</code>.",
        escape_html(&account_label(&account)),
        escape_html(&state.base_url(&account))
    );
    Ok(Html(dashboard::message_page(
        "Login complete",
        &message,
        &state.admin_url(&format!("/?account={}", account.id)),
    ))
    .into_response())
}

async fn account_request(
    State(state): State<AppState>,
    Extension(id): Extension<String>,
    request: Request<Body>,
) -> Response {
    let started = std::time::Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let refresh_usage = (method == Method::GET
        && (path == "/v1/models" || path.starts_with("/v1/models/")))
        || (method == Method::POST
            && matches!(path.as_str(), "/v1/responses" | "/v1/chat/completions"));
    let mut metadata = (None::<String>, None::<String>);
    let request = if method == Method::POST
        && matches!(path.as_str(), "/v1/responses" | "/v1/chat/completions")
    {
        let (parts, body) = request.into_parts();
        match to_bytes(body, BODY_LIMIT).await {
            Ok(bytes) => {
                if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                    metadata = (
                        value
                            .get("model")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        reasoning_effort(&value),
                    );
                }
                Ok(Request::from_parts(parts, Body::from(bytes)))
            }
            Err(_) => Err(AppError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body is too large",
            )),
        }
    } else {
        Ok(request)
    };
    let mut response = match request {
        Ok(request) => match state.dispatch_account(&id, request).await {
            Ok(r) => r,
            Err(e) => e.into_response(),
        },
        Err(error) => error.into_response(),
    };
    let status = response.status();
    let account = state.account(&id).await;
    if let Some(account) = &account {
        let mut db = state.0.db.lock().await;
        if let Some(stored) = db.accounts.iter_mut().find(|a| a.id == id) {
            stored.last_request_at = Some(now());
            stored.last_status = Some(status.as_u16());
        }
        log_json(
            json!({"event":"request","port":account.port,"accountId":id,"provider":account.provider,"method":method.as_str(),"path":path,"status":status.as_u16(),"durationMs":started.elapsed().as_millis(),"model":metadata.0,"reasoningEffort":metadata.1}),
            false,
        );
    }
    if refresh_usage {
        response = refresh_usage_after_response(response, state, id);
    }
    response
}

impl AppState {
    async fn dispatch_account(&self, id: &str, request: Request<Body>) -> Result<Response> {
        if request.method() == Method::OPTIONS {
            return Ok(cors_empty());
        }
        let account = self
            .account(id)
            .await
            .ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Account not found"))?;
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        if method == Method::GET && path == "/" {
            return Ok(json_response(
                StatusCode::OK,
                json!({"object":"ai_proxy.account","account":public_account(self,&account),"primaryEndpoint":"/v1/responses","endpoints":["/v1/models","/v1/responses","/v1/chat/completions"]}),
            ));
        }
        if method == Method::GET && path == "/v1/models" {
            return self.models(&account).await;
        }
        if method == Method::GET && path.starts_with("/v1/models/") {
            let id = percent_decode(&path[11..]);
            let response = self.models(&account).await?;
            if !response.status().is_success() {
                return Ok(response);
            }
            let bytes = to_bytes(response.into_body(), BODY_LIMIT)
                .await
                .map_err(|e| AppError::internal(e.to_string()))?;
            let list: Value =
                serde_json::from_slice(&bytes).map_err(|e| AppError::internal(e.to_string()))?;
            if let Some(model) = list
                .get("data")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .find(|m| m.get("id").and_then(Value::as_str) == Some(&id))
            {
                return Ok(json_response(StatusCode::OK, model.clone()));
            }
            return Ok(json_response(
                StatusCode::NOT_FOUND,
                json!({"error":{"message":format!("Model not found: {id}"),"type":"invalid_request_error"}}),
            ));
        }
        let (parts, body) = request.into_parts();
        let bytes = to_bytes(body, BODY_LIMIT).await.map_err(|_| {
            AppError::new(StatusCode::PAYLOAD_TOO_LARGE, "Request body is too large")
        })?;
        if method == Method::POST && path == "/v1/responses" {
            return self.responses(&account, &parts.headers, bytes).await;
        }
        if method == Method::POST && path == "/v1/chat/completions" {
            return self.chat_completions(&account, &parts.headers, bytes).await;
        }
        Ok(json_response(
            StatusCode::NOT_FOUND,
            json!({"error":{"message":format!("Unsupported endpoint: {method} {path}"),"type":"invalid_request_error"}}),
        ))
    }
    async fn models(&self, account: &Account) -> Result<Response> {
        let provider = self.provider(&account.provider)?;
        let path = if provider.api.models_path.is_empty() {
            "/models"
        } else {
            &provider.api.models_path
        };
        let upstream = self
            .provider_fetch(
                account,
                path,
                Method::GET,
                ReqwestHeaders::new(),
                None,
                None,
            )
            .await?;
        let status = upstream.status();
        let headers = upstream.headers().clone();
        let bytes = upstream
            .bytes()
            .await
            .map_err(|e| AppError::internal(e.to_string()))?;
        if !status.is_success() {
            return Ok(raw_response(status, &headers, bytes));
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| {
            AppError::internal(format!("Upstream models returned invalid JSON: {e}"))
        })?;
        Ok(json_response(StatusCode::OK, normalize_models(&value)))
    }
    async fn responses(
        &self,
        account: &Account,
        incoming_headers: &HeaderMap,
        bytes: Bytes,
    ) -> Result<Response> {
        let provider = self.provider(&account.provider)?;
        if provider.mode != "responses-adapter" {
            if provider.api.responses_path.is_empty() {
                return Ok(json_response(
                    StatusCode::NOT_FOUND,
                    json!({"error":{"message":"Provider does not expose a Responses endpoint","type":"invalid_request_error"}}),
                ));
            }
            let upstream = self
                .provider_fetch(
                    account,
                    &provider.api.responses_path,
                    Method::POST,
                    filtered_headers(incoming_headers),
                    Some(bytes),
                    None,
                )
                .await?;
            return Ok(proxy_response(upstream));
        }
        let body: Value = serde_json::from_slice(&bytes).map_err(|_| {
            AppError::new(StatusCode::BAD_REQUEST, "Request body must be valid JSON")
        })?;
        let stream = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
        let normalized = normalize_responses_request(&body);
        let path = if provider.api.responses_path.is_empty() {
            "/responses"
        } else {
            &provider.api.responses_path
        };
        let mut headers = ReqwestHeaders::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let upstream = self
            .provider_fetch(
                account,
                path,
                Method::POST,
                headers,
                Some(serde_json::to_vec(&normalized).unwrap().into()),
                None,
            )
            .await?;
        if !upstream.status().is_success() {
            return Ok(proxy_response(upstream));
        }
        if stream {
            return Ok(proxy_response_with_default(
                upstream,
                "text/event-stream; charset=utf-8",
            ));
        }
        let events = read_sse_response(upstream).await?;
        let response = collect_responses_events(events)
            .map_err(|e| AppError::new(StatusCode::BAD_GATEWAY, e))?;
        Ok(json_response(StatusCode::OK, response))
    }
    async fn chat_completions(
        &self,
        account: &Account,
        headers: &HeaderMap,
        bytes: Bytes,
    ) -> Result<Response> {
        let provider = self.provider(&account.provider)?;
        if provider.mode == "chat-passthrough" {
            let path = provider
                .api
                .chat_completions_path
                .as_deref()
                .unwrap_or("/chat/completions");
            let upstream = self
                .provider_fetch(
                    account,
                    path,
                    Method::POST,
                    filtered_headers(headers),
                    Some(bytes),
                    None,
                )
                .await?;
            return Ok(proxy_response(upstream));
        }
        if provider.mode != "responses-adapter" {
            return Err(AppError::internal(format!(
                "Unsupported provider mode: {}",
                provider.mode
            )));
        }
        let chat: Value = serde_json::from_slice(&bytes).map_err(|_| {
            AppError::new(StatusCode::BAD_REQUEST, "Request body must be valid JSON")
        })?;
        let mut converted = build_responses_request(&chat).map_err(|unsupported| {
            AppError::new(
                StatusCode::BAD_REQUEST,
                format!(
                    "Only text chat content is supported for now. Unsupported content: {}",
                    unsupported.join(", ")
                ),
            )
        })?;
        converted["stream"] = true.into();
        let path = if provider.api.responses_path.is_empty() {
            "/responses"
        } else {
            &provider.api.responses_path
        };
        let mut request_headers = ReqwestHeaders::new();
        request_headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let upstream = self
            .provider_fetch(
                account,
                path,
                Method::POST,
                request_headers,
                Some(serde_json::to_vec(&converted).unwrap().into()),
                None,
            )
            .await?;
        if !upstream.status().is_success() {
            return Ok(proxy_response(upstream));
        }
        let model = chat
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        if chat.get("stream").and_then(Value::as_bool).unwrap_or(false) {
            Ok(stream_chat_response(upstream, model))
        } else {
            let events = read_sse_response(upstream).await?;
            let response = collect_responses_events(events)
                .map_err(|e| AppError::new(StatusCode::BAD_GATEWAY, e))?;
            Ok(json_response(
                StatusCode::OK,
                response_to_chat(&response, &model),
            ))
        }
    }
}

impl AppState {
    async fn provider_fetch(
        &self,
        account: &Account,
        upstream_path: &str,
        method: Method,
        extra: ReqwestHeaders,
        body: Option<Bytes>,
        timeout: Option<Duration>,
    ) -> Result<reqwest::Response> {
        self.ensure_access_token(&account.id).await?;
        for attempt in 0..2 {
            let current = self
                .account(&account.id)
                .await
                .ok_or_else(|| AppError::auth("Account was removed"))?;
            let provider = self.provider(&current.provider)?;
            let url =
                if upstream_path.starts_with("http://") || upstream_path.starts_with("https://") {
                    upstream_path.to_owned()
                } else {
                    format!(
                        "{}{}{}",
                        provider.api.base_url.trim_end_matches('/'),
                        if upstream_path.starts_with('/') {
                            ""
                        } else {
                            "/"
                        },
                        upstream_path
                    )
                };
            let mut headers = ReqwestHeaders::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", current.tokens.access_token))
                    .map_err(|e| AppError::internal(e.to_string()))?,
            );
            headers.insert(
                reqwest::header::USER_AGENT,
                HeaderValue::from_str(&user_agent(&provider))
                    .map_err(|e| AppError::internal(e.to_string()))?,
            );
            if let Some(id) = current.metadata.string("accountId") {
                headers.insert(
                    HeaderName::from_static("chatgpt-account-id"),
                    HeaderValue::from_str(id).map_err(|e| AppError::internal(e.to_string()))?,
                );
            }
            for (key, value) in &provider.api.headers {
                if let (Ok(name), Some(value)) = (HeaderName::try_from(key), value.as_str())
                    && let Ok(value) = HeaderValue::from_str(value)
                {
                    headers.insert(name, value);
                }
            }
            headers.extend(extra.clone());
            let mut request = self.0.client.request(method.clone(), url).headers(headers);
            if let Some(body) = body.clone() {
                request = request.body(body);
            }
            if let Some(timeout) = timeout {
                request = request.timeout(timeout);
            }
            let response = request.send().await.map_err(|e| {
                AppError::new(
                    StatusCode::BAD_GATEWAY,
                    format!("Upstream request failed: {e}"),
                )
            })?;
            if response.status() != StatusCode::UNAUTHORIZED
                || attempt == 1
                || current.tokens.refresh_token.is_empty()
            {
                return Ok(response);
            }
            self.refresh_token(&current.id, true).await?;
        }
        unreachable!()
    }
    async fn ensure_access_token(&self, id: &str) -> Result<()> {
        let account = self
            .account(id)
            .await
            .ok_or_else(|| AppError::auth("Account was removed"))?;
        if account.status == "needs_reauth" {
            return Err(AppError::auth("Account requires re-authentication"));
        }
        if account.tokens.access_token.is_empty() {
            self.mark_reauth(id, "Missing access token").await?;
            return Err(AppError::auth("Account is missing an access token"));
        }
        let should_refresh = parse_time(&account.tokens.expires_at).is_some_and(|t| {
            t - chrono::Utc::now() < chrono::Duration::from_std(REFRESH_WINDOW).unwrap()
        });
        if should_refresh && !account.tokens.refresh_token.is_empty() {
            self.refresh_token(id, false).await?;
        }
        Ok(())
    }
    async fn refresh_token(&self, id: &str, force: bool) -> Result<()> {
        let _refresh = self.0.refresh_lock.lock().await;
        let account = self
            .account(id)
            .await
            .ok_or_else(|| AppError::auth("Account was removed"))?;
        if !force {
            let still_valid = parse_time(&account.tokens.expires_at).is_some_and(|t| {
                t - chrono::Utc::now() >= chrono::Duration::from_std(REFRESH_WINDOW).unwrap()
            });
            if still_valid {
                return Ok(());
            }
        }
        let provider = self.provider(&account.provider)?;
        if provider.oauth.token_url.is_empty()
            || provider.oauth.client_id.is_empty()
            || account.tokens.refresh_token.is_empty()
        {
            self.mark_reauth(id, "Token refresh is not configured")
                .await?;
            return Err(AppError::auth("Token refresh is not configured"));
        }
        let response=self.0.client.post(&provider.oauth.token_url).header(reqwest::header::USER_AGENT,user_agent(&provider)).json(&json!({"client_id":provider.oauth.client_id,"grant_type":"refresh_token","refresh_token":account.tokens.refresh_token})).send().await.map_err(|e|AppError::new(StatusCode::BAD_GATEWAY,e.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| AppError::internal(e.to_string()))?;
        if !status.is_success() {
            let message = token_error(&text)
                .unwrap_or_else(|| format!("Token refresh failed with HTTP {}", status.as_u16()));
            if status == StatusCode::BAD_REQUEST || status == StatusCode::UNAUTHORIZED {
                self.mark_reauth(id, &message).await?;
                return Err(AppError::auth(message));
            }
            return Err(AppError::new(status, message));
        }
        let token: Value =
            serde_json::from_str(&text).map_err(|e| AppError::internal(e.to_string()))?;
        {
            let mut db = self.0.db.lock().await;
            let stored = db
                .accounts
                .iter_mut()
                .find(|a| a.id == id)
                .ok_or_else(|| AppError::auth("Account was removed"))?;
            apply_token_response(stored, &token);
            stored.status = "active".into();
            stored.last_error = None;
            stored.updated_at = now();
        }
        self.save_db().await
    }
    async fn mark_reauth(&self, id: &str, message: &str) -> Result<()> {
        {
            let mut db = self.0.db.lock().await;
            if let Some(a) = db.accounts.iter_mut().find(|a| a.id == id) {
                a.status = "needs_reauth".into();
                a.last_error = Some(message.into());
                a.updated_at = now();
            }
        }
        self.save_db().await
    }
}

impl AppState {
    async fn begin_oauth(&self, provider_id: &str, reauth: Option<String>) -> Result<String> {
        let provider = self.provider(provider_id)?;
        let oauth = &provider.oauth;
        if oauth.authorize_url.is_empty()
            || oauth.client_id.is_empty()
            || oauth.token_url.is_empty()
        {
            return Err(AppError::new(
                StatusCode::BAD_REQUEST,
                format!("Provider {provider_id} does not have OAuth configured"),
            ));
        }
        let verifier = random_base64(32);
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let state = random_base64(32);
        let redirect_uri = self.oauth_redirect_uri(&provider);
        self.0
            .pending_oauth
            .lock()
            .await
            .retain(|_, p| p.created_at.elapsed() < Duration::from_secs(600));
        self.0.pending_oauth.lock().await.insert(
            state.clone(),
            PendingOauth {
                provider_id: provider_id.into(),
                verifier,
                redirect_uri: redirect_uri.clone(),
                reauth_account_id: reauth,
                created_at: std::time::Instant::now(),
            },
        );
        let mut url =
            Url::parse(&oauth.authorize_url).map_err(|e| AppError::internal(e.to_string()))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("response_type", "code")
                .append_pair("client_id", &oauth.client_id)
                .append_pair("redirect_uri", &redirect_uri)
                .append_pair(
                    "scope",
                    if oauth.scope.is_empty() {
                        "openid profile email offline_access"
                    } else {
                        &oauth.scope
                    },
                )
                .append_pair("code_challenge", &challenge)
                .append_pair("code_challenge_method", "S256")
                .append_pair("state", &state);
            for (k, v) in &oauth.extra_authorize_params {
                q.append_pair(k, v.as_str().unwrap_or(&v.to_string()));
            }
        }
        Ok(url.into())
    }
    async fn finish_oauth(&self, pending: PendingOauth, code: &str) -> Result<Account> {
        let provider = self.provider(&pending.provider_id)?;
        let response = self
            .0
            .client
            .post(&provider.oauth.token_url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .header(reqwest::header::USER_AGENT, user_agent(&provider))
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", &pending.redirect_uri),
                ("client_id", &provider.oauth.client_id),
                ("code_verifier", &pending.verifier),
            ])
            .send()
            .await
            .map_err(|e| AppError::new(StatusCode::BAD_GATEWAY, e.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| AppError::internal(e.to_string()))?;
        if !status.is_success() {
            return Err(AppError::new(
                status,
                format!("OAuth token exchange failed with HTTP {}", status.as_u16()),
            ));
        }
        let token: Value =
            serde_json::from_str(&text).map_err(|e| AppError::internal(e.to_string()))?;
        let metadata = metadata_from_tokens(&token);
        let existing = {
            let db = self.0.db.lock().await;
            pending
                .reauth_account_id
                .and_then(|id| db.accounts.iter().position(|a| a.id == id))
                .or_else(|| {
                    metadata.string("accountId").and_then(|aid| {
                        db.accounts.iter().position(|a| {
                            a.status != "removed"
                                && a.provider == pending.provider_id
                                && a.metadata.string("accountId") == Some(aid)
                        })
                    })
                })
        };
        if let Some(index) = existing {
            {
                let mut db = self.0.db.lock().await;
                let a = &mut db.accounts[index];
                a.provider = pending.provider_id;
                a.metadata.merge(metadata);
                apply_token_response(a, &token);
                a.status = "active".into();
                a.last_error = None;
                a.updated_at = now();
            }
            self.save_db().await?;
            let account = self
                .0
                .db
                .lock()
                .await
                .accounts
                .get(index)
                .cloned()
                .ok_or_else(|| AppError::internal("Account disappeared"))?;
            self.schedule_usage_refresh(&account.id).await;
            return Ok(account);
        }
        let mut account = Account {
            id: Uuid::new_v4().to_string(),
            provider: pending.provider_id,
            port: None,
            status: "active".into(),
            metadata,
            tokens: Tokens::default(),
            created_at: now(),
            updated_at: now(),
            last_error: None,
            last_request_at: None,
            last_status: None,
            usage_snapshot: None,
        };
        apply_token_response(&mut account, &token);
        let mut skipped = HashSet::new();
        loop {
            let port = self.allocate_port(&skipped).await.ok_or_else(|| {
                AppError::internal("No downstream ports are available in the configured range")
            })?;
            match bind(
                &self.0.config.host,
                port,
                &format!("account {}", account.id),
            )
            .await
            {
                Ok(listener) => {
                    account.port = Some(port);
                    self.0.db.lock().await.accounts.push(account.clone());
                    self.save_db().await?;
                    self.spawn_account(account.id.clone(), listener).await;
                    self.schedule_usage_refresh(&account.id).await;
                    return Ok(account);
                }
                Err(e) => {
                    skipped.insert(port);
                    if !e.contains("address already in use") && !e.contains("os error 98") {
                        return Err(AppError::internal(e));
                    }
                }
            }
        }
    }
    async fn allocate_port(&self, skip: &HashSet<u16>) -> Option<u16> {
        let mut db = self.0.db.lock().await;
        let start = self.0.config.port_range.start;
        let end = self.0.config.port_range.end;
        let used: HashSet<u16> = db
            .accounts
            .iter()
            .filter(|a| a.status != "removed")
            .filter_map(|a| a.port)
            .collect();
        let next = (db.next_port.max(start.into())).min(u16::MAX.into()) as u16;
        for port in next..=end {
            if !used.contains(&port) && !skip.contains(&port) {
                db.next_port = port as u32 + 1;
                return Some(port);
            }
        }
        while !db.released_ports.is_empty() {
            let port = db.released_ports.remove(0);
            if port >= start && port <= end && !used.contains(&port) && !skip.contains(&port) {
                return Some(port);
            }
        }
        (start..=end).find(|p| !used.contains(p) && !skip.contains(p))
    }
    async fn remove_account(&self, id: &str) -> Result<()> {
        let account = {
            let mut db = self.0.db.lock().await;
            let index = db
                .accounts
                .iter()
                .position(|a| a.id == id && a.status != "removed")
                .ok_or_else(|| AppError::new(StatusCode::NOT_FOUND, "Account not found"))?;
            let account = db.accounts.remove(index);
            if let Some(port) = account.port
                && !db.released_ports.contains(&port)
            {
                db.released_ports.push(port);
            }
            account
        };
        self.stop_account(&account.id).await;
        self.0.usage_refreshes.lock().await.remove(&account.id);
        self.save_db().await
    }
}

impl AppState {
    async fn current_usage_snapshots(&self) -> Value {
        let accounts: Vec<Value> = self
            .active_accounts()
            .await
            .iter()
            .map(usage_snapshot_for_account)
            .collect();
        json!({"fetchedAt":now(), "accounts":accounts})
    }

    async fn usage_snapshot_for_id(&self, id: &str) -> Value {
        self.account(id).await.map_or_else(
            || json!({"accountId":id, "status":"removed", "windows":[], "error":"Account was removed"}),
            |account| usage_snapshot_for_account(&account),
        )
    }

    async fn usage_event_stream(&self) -> Response {
        let mut receiver = self.0.usage_events.subscribe();
        let state = self.clone();
        let stream = async_stream::stream! {
            yield Ok::<Bytes, Infallible>(Bytes::from("retry: 3000\n\n"));
            yield Ok(named_sse("snapshot", &state.current_usage_snapshots().await));
            let mut keep_alive = tokio::time::interval(Duration::from_secs(20));
            keep_alive.tick().await;
            loop {
                tokio::select! {
                    event = receiver.recv() => match event {
                        Ok(snapshot) => yield Ok(named_sse("usage", &snapshot)),
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            yield Ok(named_sse("snapshot", &state.current_usage_snapshots().await));
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    _ = keep_alive.tick() => yield Ok(Bytes::from_static(b": keep-alive\n\n")),
                }
            }
        };
        let mut response = Response::new(Body::from_stream(stream));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );
        add_stream_headers(response.headers_mut());
        response
    }

    async fn schedule_usage_refresh(&self, id: &str) {
        let refresh = {
            let mut refreshes = self.0.usage_refreshes.lock().await;
            refreshes
                .entry(id.to_owned())
                .or_insert_with(|| {
                    Arc::new(UsageRefresh {
                        state: Mutex::new(UsageRefreshState::default()),
                        finished: Notify::new(),
                    })
                })
                .clone()
        };
        let should_spawn = {
            let mut state = refresh.state.lock().await;
            state.pending = true;
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };
        if should_spawn {
            let state = self.clone();
            let id = id.to_owned();
            tokio::spawn(async move { state.usage_refresh_worker(id, refresh).await });
        }
    }

    async fn refresh_usage_and_wait(&self, id: &str) {
        self.schedule_usage_refresh(id).await;
        let refresh = self.0.usage_refreshes.lock().await.get(id).cloned();
        let Some(refresh) = refresh else { return };
        loop {
            let finished = refresh.finished.notified();
            if !refresh.state.lock().await.running {
                return;
            }
            finished.await;
        }
    }

    async fn usage_refresh_worker(&self, id: String, refresh: Arc<UsageRefresh>) {
        loop {
            let should_refresh = {
                let mut state = refresh.state.lock().await;
                if state.pending {
                    state.pending = false;
                    true
                } else {
                    state.running = false;
                    false
                }
            };
            if !should_refresh {
                refresh.finished.notify_waiters();
                return;
            }
            self.refresh_usage_snapshot(&id).await;
        }
    }

    async fn refresh_usage_snapshot(&self, id: &str) {
        let attempted_at = now();
        let Some(account) = self.account(id).await else {
            return;
        };
        let result = self.fetch_usage(&account).await;
        let error = result.as_ref().err().map(|error| error.message.clone());
        {
            let mut db = self.0.db.lock().await;
            let Some(stored) = db.accounts.iter_mut().find(|account| account.id == id) else {
                return;
            };
            match result {
                Ok((plan_type, windows)) => {
                    stored.usage_snapshot = Some(UsageSnapshot {
                        plan_type,
                        windows,
                        fetched_at: Some(now()),
                        refresh_attempted_at: Some(attempted_at),
                        error: None,
                    })
                }
                Err(_) => {
                    let snapshot = stored
                        .usage_snapshot
                        .get_or_insert_with(UsageSnapshot::default);
                    snapshot.refresh_attempted_at = Some(attempted_at);
                    snapshot.error.clone_from(&error);
                }
            }
        }
        if let Some(message) = error {
            log_json(
                json!({"event":"usage_refresh_failed", "accountId":id, "provider":account.provider, "message":message}),
                true,
            );
        }
        if let Err(error) = self.save_db().await {
            log_json(
                json!({"event":"usage_snapshot_save_failed", "accountId":id, "message":error.message}),
                true,
            );
        }
        if let Some(account) = self.account(id).await {
            let _ = self
                .0
                .usage_events
                .send(usage_snapshot_for_account(&account));
        }
    }

    async fn fetch_usage(&self, account: &Account) -> Result<(Option<String>, Vec<Value>)> {
        let provider = self.provider(&account.provider)?;
        if provider.api.usage_url.is_empty() {
            return Err(AppError::new(
                StatusCode::BAD_REQUEST,
                format!("Usage is not configured for {}", provider.label),
            ));
        }
        let mut h = ReqwestHeaders::new();
        h.insert(
            reqwest::header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
        let response = self
            .provider_fetch(
                account,
                &provider.api.usage_url,
                Method::GET,
                h,
                None,
                Some(Duration::from_secs(15)),
            )
            .await?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| AppError::internal(e.to_string()))?;
        if !status.is_success() {
            return Err(AppError::new(
                status,
                format!("Usage API returned HTTP {}", status.as_u16()),
            ));
        }
        let payload: Value = serde_json::from_str(&text).map_err(|_| {
            AppError::new(StatusCode::BAD_GATEWAY, "Usage API returned invalid JSON")
        })?;
        Ok((
            payload
                .get("plan_type")
                .and_then(Value::as_str)
                .map(str::to_owned),
            normalize_usage_windows(&payload),
        ))
    }
}

fn refresh_usage_after_response(
    response: Response,
    state: AppState,
    account_id: String,
) -> Response {
    let (parts, body) = response.into_parts();
    let stream = async_stream::stream! {
        let _refresh = UsageRefreshOnDrop { state, account_id };
        let mut data = body.into_data_stream();
        while let Some(item) = data.next().await {
            yield item;
        }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

async fn read_sse_response(response: reqwest::Response) -> Result<Vec<SseEvent>> {
    let mut stream = response.bytes_stream();
    let mut decoder = SseDecoder::default();
    let mut events = Vec::new();
    while let Some(chunk) = stream.next().await {
        events.extend(
            decoder
                .push(&chunk.map_err(|e| AppError::new(StatusCode::BAD_GATEWAY, e.to_string()))?),
        );
    }
    if let Some(e) = decoder.finish() {
        events.push(e);
    }
    Ok(events)
}
fn proxy_response(upstream: reqwest::Response) -> Response {
    proxy_response_inner(upstream, None)
}
fn proxy_response_with_default(
    upstream: reqwest::Response,
    content_type: &'static str,
) -> Response {
    proxy_response_inner(upstream, Some(content_type))
}
fn proxy_response_inner(upstream: reqwest::Response, default: Option<&'static str>) -> Response {
    let status = upstream.status();
    let source = upstream.headers().clone();
    let stream = upstream
        .bytes_stream()
        .map(|item| item.map_err(std::io::Error::other));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    copy_response_headers(&source, response.headers_mut());
    if !response.headers().contains_key(header::CONTENT_TYPE)
        && let Some(v) = default
    {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(v));
    }
    response
}
fn stream_chat_response(upstream: reqwest::Response, model: String) -> Response {
    let stream = async_stream::stream! {
        let id = random_id("chatcmpl");
        let created = chrono::Utc::now().timestamp();
        yield Ok::<Bytes, Infallible>(sse_bytes(chat_chunk(
            &id, created, &model, json!({"role":"assistant"}), Value::Null, None,
        )));
        let mut input = upstream.bytes_stream();
        let mut decoder = SseDecoder::default();
        let mut tool_indexes = HashMap::<u64, u64>::new();
        let mut seen_args = HashSet::new();
        let mut next = 0;
        let mut saw_tool = false;
        let mut final_sent = false;
        while let Some(chunk) = input.next().await {
            let Ok(chunk) = chunk else { break };
            for event in decoder.push(&chunk) {
                let Ok(payload) = serde_json::from_str::<Value>(&event.data) else { continue };
                let kind = payload.get("type").and_then(Value::as_str).unwrap_or(&event.event);
                match kind {
                    "response.output_text.delta" => if let Some(delta) = payload.get("delta").and_then(Value::as_str) {
                        yield Ok(sse_bytes(chat_chunk(&id, created, &model, json!({"content":delta}), Value::Null, None)));
                    },
                    "response.output_item.added" if payload.pointer("/item/type").and_then(Value::as_str) == Some("function_call") => {
                        let output_index = payload.get("output_index").or_else(||payload.get("index")).and_then(Value::as_u64).unwrap_or(next);
                        let tool_index = next;
                        next += 1;
                        saw_tool = true;
                        tool_indexes.insert(output_index, tool_index);
                        yield Ok(sse_bytes(chat_chunk(&id, created, &model, json!({"tool_calls":[{
                            "index":tool_index,
                            "id":payload.pointer("/item/call_id").or_else(||payload.pointer("/item/id")).cloned().unwrap_or_else(||random_id("call").into()),
                            "type":"function",
                            "function":{"name":payload.pointer("/item/name").and_then(Value::as_str).unwrap_or(""),"arguments":""}
                        }]}), Value::Null, None)));
                    },
                    "response.function_call_arguments.delta" => if let Some(delta) = payload.get("delta").and_then(Value::as_str) {
                        let output_index = payload.get("output_index").or_else(||payload.get("index")).and_then(Value::as_u64).unwrap_or(0);
                        let tool_index = *tool_indexes.get(&output_index).unwrap_or(&0);
                        saw_tool = true;
                        seen_args.insert(tool_index);
                        yield Ok(sse_bytes(chat_chunk(&id, created, &model, json!({"tool_calls":[{"index":tool_index,"function":{"arguments":delta}}]}), Value::Null, None)));
                    },
                    "response.output_item.done" if payload.pointer("/item/type").and_then(Value::as_str) == Some("function_call") => {
                        let output_index = payload.get("output_index").or_else(||payload.get("index")).and_then(Value::as_u64).unwrap_or(0);
                        let tool_index = *tool_indexes.get(&output_index).unwrap_or(&0);
                        saw_tool = true;
                        if !seen_args.contains(&tool_index)
                            && let Some(args) = payload.pointer("/item/arguments").and_then(Value::as_str)
                        {
                            yield Ok(sse_bytes(chat_chunk(&id, created, &model, json!({"tool_calls":[{"index":tool_index,"function":{"arguments":args}}]}), Value::Null, None)));
                        }
                    },
                    "response.completed" => {
                        let finish = if saw_tool { "tool_calls" } else { "stop" };
                        yield Ok(sse_bytes(chat_chunk(&id, created, &model, json!({}), finish.into(), payload.pointer("/response/usage"))));
                        final_sent = true;
                    },
                    "response.failed" | "response.incomplete" => {
                        let message = payload.pointer("/error/message").and_then(Value::as_str).unwrap_or("Upstream response failed");
                        yield Ok(sse_bytes(chat_chunk(&id, created, &model, json!({"content":format!("\n[{message}]")}), "stop".into(), None)));
                        final_sent = true;
                    },
                    _ => {},
                }
            }
        }
        if !final_sent {
            let finish = if saw_tool { "tool_calls" } else { "stop" };
            yield Ok(sse_bytes(chat_chunk(&id, created, &model, json!({}), finish.into(), None)));
        }
        yield Ok(Bytes::from_static(b"data: [DONE]\n\n"));
    };
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    add_stream_headers(response.headers_mut());
    response
}
fn chat_chunk(
    id: &str,
    created: i64,
    model: &str,
    delta: Value,
    finish: Value,
    usage: Option<&Value>,
) -> Value {
    let mut value = json!({"id":id,"object":"chat.completion.chunk","created":created,"model":model,"choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    if usage.is_some() {
        value["usage"] = chat_usage(usage);
    }
    value
}
fn sse_bytes(v: Value) -> Bytes {
    Bytes::from(format!("data: {}\n\n", serde_json::to_string(&v).unwrap()))
}
fn named_sse(event: &str, value: &Value) -> Bytes {
    Bytes::from(format!(
        "event: {event}\ndata: {}\n\n",
        serde_json::to_string(value).unwrap()
    ))
}

fn raw_response(status: StatusCode, headers: &ReqwestHeaders, bytes: Bytes) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    copy_response_headers(headers, response.headers_mut());
    response
}
fn copy_response_headers(from: &ReqwestHeaders, to: &mut HeaderMap) {
    for (k, v) in from {
        if !hop_header(k.as_str()) && k != header::CONTENT_LENGTH {
            to.insert(k.clone(), v.clone());
        }
    }
    to.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
}
fn filtered_headers(from: &HeaderMap) -> ReqwestHeaders {
    let mut out = ReqwestHeaders::new();
    for (k, v) in from {
        if !hop_header(k.as_str())
            && k != header::AUTHORIZATION
            && k != header::HOST
            && k != header::CONTENT_LENGTH
        {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}
fn hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}
fn json_response(status: StatusCode, value: Value) -> Response {
    let mut r = Response::new(Body::from(format!(
        "{}\n",
        serde_json::to_string(&value).unwrap()
    )));
    *r.status_mut() = status;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    r.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    r
}
fn cors_empty() -> Response {
    let mut r = Response::new(Body::empty());
    *r.status_mut() = StatusCode::NO_CONTENT;
    let h = r.headers_mut();
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    h.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET,POST,OPTIONS"),
    );
    h.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization,content-type,openai-organization,openai-project"),
    );
    h.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("86400"),
    );
    r
}
fn add_stream_headers(h: &mut HeaderMap) {
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    h.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    h.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
}

fn apply_token_response(account: &mut Account, token: &Value) {
    if let Some(v) = token.get("access_token").and_then(Value::as_str) {
        account.tokens.access_token = v.into();
    }
    if let Some(v) = token.get("refresh_token").and_then(Value::as_str) {
        account.tokens.refresh_token = v.into();
    }
    if let Some(v) = token.get("id_token").and_then(Value::as_str) {
        account.tokens.id_token = v.into();
    }
    account.tokens.token_type = token
        .get("token_type")
        .and_then(Value::as_str)
        .unwrap_or(if account.tokens.token_type.is_empty() {
            "Bearer"
        } else {
            &account.tokens.token_type
        })
        .into();
    let expiry = token
        .get("expires_in")
        .and_then(Value::as_i64)
        .map(|seconds| chrono::Utc::now() + chrono::Duration::seconds(seconds))
        .or_else(|| {
            decode_jwt(&account.tokens.access_token)
                .and_then(|v| v.get("exp").and_then(Value::as_i64))
                .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
        })
        .unwrap_or_else(|| chrono::Utc::now() + chrono::Duration::minutes(50));
    account.tokens.expires_at = expiry.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    account.metadata.merge(metadata_from_tokens(token));
}
fn metadata_from_tokens(token: &Value) -> AccountMetadata {
    let id = token
        .get("id_token")
        .and_then(Value::as_str)
        .and_then(decode_jwt)
        .unwrap_or(Value::Null);
    let access = token
        .get("access_token")
        .and_then(Value::as_str)
        .and_then(decode_jwt)
        .unwrap_or(Value::Null);
    let auth = id
        .get("https://api.openai.com/auth")
        .unwrap_or(&Value::Null);
    let profile = id
        .get("https://api.openai.com/profile")
        .unwrap_or(&Value::Null);
    let mut values = HashMap::new();
    for (key, val) in [
        ("email", id.get("email")),
        (
            "userId",
            auth.get("user_id")
                .or_else(|| auth.get("chatgpt_user_id"))
                .or_else(|| access.get("sub")),
        ),
        (
            "accountId",
            auth.get("chatgpt_account_id")
                .or_else(|| auth.get("account_id")),
        ),
        ("planType", auth.get("chatgpt_plan_type")),
        ("profileName", profile.get("name")),
    ] {
        if let Some(v) = val.filter(|v| !v.is_null()) {
            values.insert(key.into(), v.clone());
        }
    }
    AccountMetadata { values }
}
fn decode_jwt(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&bytes).ok()
}
fn token_error(text: &str) -> Option<String> {
    let parsed: Value = serde_json::from_str(text).ok()?;
    parsed
        .get("error_description")
        .or_else(|| parsed.pointer("/error/message"))
        .or_else(|| parsed.get("error"))
        .or_else(|| parsed.get("message"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}
fn user_agent(provider: &Provider) -> String {
    if provider.api.user_agent.is_empty() {
        format!("{APP_NAME}/{APP_VERSION}")
    } else {
        provider.api.user_agent.clone()
    }
}
fn random_base64(n: usize) -> String {
    let mut bytes = vec![0; n];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}
fn public_account(state: &AppState, a: &Account) -> Value {
    json!({"id":a.id,"provider":a.provider,"label":state.0.config.providers.get(&a.provider).map(|p|if p.label.is_empty(){&a.provider}else{&p.label}).unwrap_or(&a.provider),"status":a.status,"port":a.port,"baseUrl":state.base_url(a),"metadata":a.metadata,"lastRequestAt":a.last_request_at,"lastStatus":a.last_status,"lastError":a.last_error,"createdAt":a.created_at,"updatedAt":a.updated_at})
}
fn account_label(a: &Account) -> String {
    a.metadata
        .string("email")
        .or_else(|| a.metadata.string("accountId"))
        .unwrap_or(&a.id)
        .into()
}
fn usage_snapshot_for_account(account: &Account) -> Value {
    let mut result = json!({
        "accountId":account.id,
        "label":account_label(account),
        "status":account.status,
        "windows":account.usage_snapshot.as_ref().map(|snapshot| &snapshot.windows).cloned().unwrap_or_default(),
    });
    let object = result.as_object_mut().unwrap();
    let Some(snapshot) = &account.usage_snapshot else {
        object.insert("error".into(), "Usage has not been captured yet".into());
        return result;
    };
    if let Some(plan_type) = &snapshot.plan_type {
        object.insert("planType".into(), plan_type.clone().into());
    }
    if let Some(fetched_at) = &snapshot.fetched_at {
        object.insert("fetchedAt".into(), fetched_at.clone().into());
    }
    if let Some(error) = &snapshot.error {
        object.insert(
            if snapshot.fetched_at.is_some() {
                "refreshError"
            } else {
                "error"
            }
            .into(),
            error.clone().into(),
        );
    }
    result
}
fn percent_decode(v: &str) -> String {
    url::form_urlencoded::parse(v.as_bytes())
        .map(|(a, _)| a.into_owned())
        .collect()
}
fn escape_html(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
fn log_json(mut value: Value, stderr: bool) {
    if let Some(map) = value.as_object_mut() {
        map.insert("ts".into(), now().into());
    }
    let line = serde_json::to_string(&value).unwrap_or_else(|_| "{}".into());
    if stderr {
        eprintln!("{line}")
    } else {
        println!("{line}")
    }
}

fn normalize_usage_windows(payload: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    let captured = chrono::Utc::now().timestamp() as f64;
    fn number(v: &Value, keys: &[&str]) -> Option<f64> {
        keys.iter()
            .find_map(|k| v.get(k))
            .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
    }
    fn add_window(
        out: &mut Vec<Value>,
        window: &Value,
        position: &str,
        limit_id: &str,
        limit_name: Option<&str>,
        captured: f64,
    ) {
        if !window.is_object() {
            return;
        }
        let used = number(window, &["used_percent", "usedPercent"]);
        let returned = number(window, &["remaining_percent", "remainingPercent"]);
        if used.is_none() && returned.is_none() {
            return;
        }
        let seconds = number(
            window,
            &["limit_window_seconds", "window_seconds", "windowSeconds"],
        )
        .or_else(|| number(window, &["window_minutes", "windowMinutes"]).map(|v| v * 60.));
        let resets = number(window, &["reset_at", "resets_at", "resetsAt"]).or_else(|| {
            number(window, &["reset_after_seconds", "resetAfterSeconds"])
                .map(|v| (captured + v).round())
        });
        let remaining = returned
            .unwrap_or(100. - used.unwrap_or(0.))
            .clamp(0., 100.);
        out.push(json!({"id":format!("{limit_id}:{position}:{}",out.len()),"limitId":limit_id,"limitName":limit_name,"label":usage_label(seconds,position),"remainingPercent":remaining,"resetsAt":resets}));
    }
    fn add_limit(out: &mut Vec<Value>, limit: &Value, id: &str, name: Option<&str>, captured: f64) {
        if let Some(w) = limit.get("windows").and_then(Value::as_array) {
            for (i, v) in w.iter().enumerate() {
                add_window(out, v, &format!("window-{}", i + 1), id, name, captured)
            }
            return;
        }
        add_window(
            out,
            limit
                .get("primary_window")
                .or_else(|| limit.get("primary"))
                .unwrap_or(&Value::Null),
            "primary",
            id,
            name,
            captured,
        );
        add_window(
            out,
            limit
                .get("secondary_window")
                .or_else(|| limit.get("secondary"))
                .unwrap_or(&Value::Null),
            "secondary",
            id,
            name,
            captured,
        );
    }
    if let Some(v) = payload.get("rate_limit") {
        add_limit(&mut out, v, "codex", None, captured)
    } else {
        add_limit(&mut out, payload, "codex", None, captured)
    }
    for key in ["rate_limits", "additional_rate_limits"] {
        for (i, entry) in payload
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let id = entry
                .get("metered_feature")
                .or_else(|| entry.get("limit_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("limit-{}", i + 1));
            add_limit(
                &mut out,
                entry.get("rate_limit").unwrap_or(entry),
                &id,
                entry.get("limit_name").and_then(Value::as_str),
                captured,
            )
        }
    }
    out
}
fn usage_label(seconds: Option<f64>, fallback: &str) -> String {
    let Some(s) = seconds.filter(|v| *v > 0.) else {
        return match fallback {
            "primary" => "Primary window",
            "secondary" => "Secondary window",
            _ => "Usage window",
        }
        .into();
    };
    let s = s.round() as i64;
    if s == 604800 {
        "Weekly window".into()
    } else if s % 604800 == 0 {
        format!("{}-week window", s / 604800)
    } else if s % 86400 == 0 {
        format!("{}-day window", s / 86400)
    } else if s % 3600 == 0 {
        format!("{}-hour window", s / 3600)
    } else if s % 60 == 0 {
        format!("{}-minute window", s / 60)
    } else {
        format!("{s}-second window")
    }
}
