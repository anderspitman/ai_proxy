use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const APP_NAME: &str = "ai_proxy";
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
pub struct Cli {
    pub admin_port: u16,
    pub oauth_port: u16,
    pub host: String,
    pub public_host: String,
    pub db_path: PathBuf,
    pub config_path: PathBuf,
    pub port_range: PortRange,
    pub provider: String,
    pub help: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub admin_port: u16,
    pub oauth_port: u16,
    pub host: String,
    pub public_host: String,
    pub db_path: PathBuf,
    pub port_range: PortRange,
    pub default_provider: String,
    pub providers: HashMap<String, Provider>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Provider {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub label: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub oauth: OauthConfig,
    #[serde(default)]
    pub api: ApiConfig,
}

fn default_mode() -> String {
    "responses-adapter".into()
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OauthConfig {
    #[serde(default)]
    pub issuer: String,
    #[serde(default)]
    pub authorize_url: String,
    #[serde(default)]
    pub token_url: String,
    #[serde(default)]
    pub revoke_url: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub redirect_path: String,
    #[serde(default)]
    pub extra_authorize_params: HashMap<String, Value>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiConfig {
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub models_path: String,
    #[serde(default)]
    pub responses_path: String,
    #[serde(default)]
    pub usage_url: String,
    #[serde(default)]
    pub chat_completions_path: Option<String>,
    #[serde(default)]
    pub user_agent: String,
    #[serde(default)]
    pub headers: HashMap<String, Value>,
}

pub fn parse_args() -> Result<Cli, String> {
    let cwd = env::current_dir().map_err(|e| e.to_string())?;
    let mut cli = Cli {
        admin_port: env_port("ORCHE_PROXY_ADMIN_PORT", 17800)?,
        oauth_port: env_port("ORCHE_PROXY_OAUTH_PORT", 1455)?,
        host: env::var("ORCHE_PROXY_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        public_host: env::var("ORCHE_PROXY_PUBLIC_HOST").unwrap_or_else(|_| "localhost".into()),
        db_path: env::var_os("ORCHE_PROXY_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| cwd.join("orche-proxy.db.json")),
        config_path: env::var_os("ORCHE_PROXY_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| cwd.join("orche-proxy.config.json")),
        port_range: parse_port_range(
            &env::var("ORCHE_PROXY_PORT_RANGE").unwrap_or_else(|_| "18001-18100".into()),
        )?,
        provider: env::var("ORCHE_PROXY_PROVIDER").unwrap_or_else(|_| "chatgpt".into()),
        help: false,
    };
    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--help" || arg == "-h" {
            cli.help = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("Missing value for {arg}"))?;
        if value.starts_with("--") {
            return Err(format!("Missing value for {arg}"));
        }
        match arg.as_str() {
            "--admin-port" => cli.admin_port = parse_port(value, arg)?,
            "--oauth-port" => cli.oauth_port = parse_port(value, arg)?,
            "--host" => cli.host.clone_from(value),
            "--public-host" => cli.public_host.clone_from(value),
            "--db" => cli.db_path = absolute(value, &cwd),
            "--config" => cli.config_path = absolute(value, &cwd),
            "--port-range" => cli.port_range = parse_port_range(value)?,
            "--provider" => cli.provider.clone_from(value),
            _ => return Err(format!("Unknown argument: {arg}")),
        }
        i += 2;
    }
    cli.db_path = absolute_path(&cli.db_path, &cwd);
    cli.config_path = absolute_path(&cli.config_path, &cwd);
    Ok(cli)
}

fn env_port(name: &str, fallback: u16) -> Result<u16, String> {
    match env::var(name) {
        Ok(value) => parse_port(&value, name),
        Err(_) => Ok(fallback),
    }
}
fn parse_port(value: &str, label: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(|| format!("{label} must be a TCP port between 1 and 65535"))
}
fn parse_port_range(value: &str) -> Result<PortRange, String> {
    let (a, b) = value
        .trim()
        .split_once('-')
        .ok_or_else(|| format!("Invalid port range: {value}. Expected START-END."))?;
    let start = parse_port(a, "port range start")?;
    let end = parse_port(b, "port range end")?;
    if end < start {
        return Err("Port range end must be >= start".into());
    }
    Ok(PortRange { start, end })
}
fn absolute(value: &str, cwd: &Path) -> PathBuf {
    absolute_path(Path::new(value), cwd)
}
fn absolute_path(value: &Path, cwd: &Path) -> PathBuf {
    if value.is_absolute() {
        value.into()
    } else {
        cwd.join(value)
    }
}

pub async fn load(cli: &Cli) -> Result<Config, String> {
    let mut providers = default_providers();
    if tokio::fs::try_exists(&cli.config_path)
        .await
        .map_err(|e| e.to_string())?
    {
        let text = tokio::fs::read_to_string(&cli.config_path)
            .await
            .map_err(|e| format!("Failed to read {}: {e}", cli.config_path.display()))?;
        let file: Value = serde_json::from_str(&text)
            .map_err(|e| format!("Invalid config {}: {e}", cli.config_path.display()))?;
        if let Some(overrides) = file.get("providers") {
            deep_merge(&mut providers, overrides.clone());
        }
    }
    let Value::Object(map) = providers else {
        return Err("providers must be an object".into());
    };
    let mut typed = HashMap::new();
    for (id, value) in map {
        let mut provider: Provider =
            serde_json::from_value(value).map_err(|e| format!("Invalid provider {id}: {e}"))?;
        if provider.id.is_empty() {
            provider.id.clone_from(&id);
        }
        typed.insert(id, provider);
    }
    if !typed.contains_key(&cli.provider) {
        return Err(format!(
            "Default provider {} is not configured",
            cli.provider
        ));
    }
    Ok(Config {
        admin_port: cli.admin_port,
        oauth_port: cli.oauth_port,
        host: cli.host.clone(),
        public_host: cli.public_host.clone(),
        db_path: cli.db_path.clone(),
        port_range: cli.port_range,
        default_provider: cli.provider.clone(),
        providers: typed,
    })
}

fn deep_merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                if let Some(current) = base.get_mut(&key) {
                    deep_merge(current, value);
                } else {
                    base.insert(key, value);
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

fn default_providers() -> Value {
    json!({ "chatgpt": {
        "id": "chatgpt", "label": "ChatGPT Plus/Pro", "mode": "responses-adapter",
        "oauth": {
            "issuer": "https://auth.openai.com", "authorizeUrl": "https://auth.openai.com/oauth/authorize",
            "tokenUrl": "https://auth.openai.com/oauth/token", "revokeUrl": "https://auth.openai.com/oauth/revoke",
            "clientId": "app_EMoamEEZ73f0CkXaXp7hrann",
            "scope": "openid profile email offline_access api.connectors.read api.connectors.invoke",
            "redirectPath": "/auth/callback", "extraAuthorizeParams": {
                "id_token_add_organizations": "true", "codex_cli_simplified_flow": "true", "originator": APP_NAME
            }
        },
        "api": {
            "baseUrl": "https://chatgpt.com/backend-api/codex", "modelsPath": "/models?client_version=0.142.5",
            "responsesPath": "/responses", "usageUrl": "https://chatgpt.com/backend-api/wham/usage",
            "chatCompletionsPath": null, "userAgent": format!("{APP_NAME}/{APP_VERSION}"), "headers": {}
        }
    }})
}

pub fn help() -> &'static str {
    "Usage: ai_proxy [options]\n\nOptions:\n  --admin-port <port>     Admin dashboard port (default: 17800)\n  --oauth-port <port>     OAuth redirect port (default: 1455)\n  --port-range <a-b>      Downstream account port range (default: 18001-18100)\n  --host <host>           Bind host for local servers (default: 127.0.0.1)\n  --public-host <host>    Host shown in dashboard URLs (default: localhost)\n  --db <file>             JSON database path (default: ./orche-proxy.db.json)\n  --config <file>         Optional provider config JSON path\n  --provider <id>         Default provider for new accounts (default: chatgpt)\n  --help                  Show this help\n\nEnvironment variables use the ORCHE_PROXY_* equivalents.\n"
}
