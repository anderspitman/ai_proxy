use crate::{config::Config, model::Account};

const DASHBOARD_HTML: &str = include_str!("../web/dashboard.html");
const DASHBOARD_CSS: &str = include_str!("../web/dashboard.css");
const DASHBOARD_JS: &str = include_str!("../web/dashboard.js");
const ACCOUNT_CARD_HTML: &str = include_str!("../web/account-card.html");
const MESSAGE_HTML: &str = include_str!("../web/message.html");
const MESSAGE_CSS: &str = include_str!("../web/message.css");

pub fn render(
    config: &Config,
    accounts: &[Account],
    removed: bool,
    oauth_paths: &[String],
) -> String {
    let mut providers: Vec<_> = config.providers.values().collect();
    providers.sort_by(|a, b| a.id.cmp(&b.id));
    let options = providers
        .iter()
        .map(|provider| {
            format!(
                "<option value=\"{}\"{}>{}</option>",
                esc(&provider.id),
                if provider.id == config.default_provider {
                    " selected"
                } else {
                    ""
                },
                esc(if provider.label.is_empty() {
                    &provider.id
                } else {
                    &provider.label
                })
            )
        })
        .collect::<String>();
    let cards: String = if accounts.is_empty() {
        "<div class=\"empty\">No accounts yet. Add a ChatGPT account to get started.</div>".into()
    } else {
        accounts.iter().map(render_account_card).collect()
    };
    let oauth_path = oauth_paths
        .first()
        .map(String::as_str)
        .unwrap_or("/auth/callback");

    DASHBOARD_HTML
        .replace("%%STYLES%%", DASHBOARD_CSS)
        .replace("%%SCRIPT%%", DASHBOARD_JS)
        .replace("%%ADMIN_PORT%%", &config.admin_port.to_string())
        .replace("%%OAUTH_PORT%%", &config.oauth_port.to_string())
        .replace("%%OAUTH_PATH%%", &esc(oauth_path))
        .replace(
            "%%PORT_RANGE%%",
            &format!("{}-{}", config.port_range.start, config.port_range.end),
        )
        .replace(
            "%%NOTICE%%",
            if removed {
                "<div class=\"notice\">Account removed.</div>"
            } else {
                ""
            },
        )
        .replace("%%PROVIDER_OPTIONS%%", &options)
        .replace("%%ACCOUNT_CARDS%%", &cards)
}

fn render_account_card(account: &Account) -> String {
    let label = account
        .metadata
        .string("email")
        .or_else(|| account.metadata.string("accountId"))
        .unwrap_or(&account.id);
    let last_request = account
        .last_request_at
        .as_ref()
        .map(|at| {
            format!(
                "<time data-local-time=\"{}\" datetime=\"{}\">{}</time>{}",
                esc(at),
                esc(at),
                esc(at),
                account
                    .last_status
                    .map(|status| format!("<code>HTTP {status}</code>"))
                    .unwrap_or_default()
            )
        })
        .unwrap_or_else(|| "<span class=\"empty\">None yet</span>".into());
    let account_error = account
        .last_error
        .as_ref()
        .map(|error| format!("<small class=\"account-error\">{}</small>", esc(error)))
        .unwrap_or_default();
    let account_id = esc(&account.id);

    ACCOUNT_CARD_HTML
        .replace("%%ACCOUNT_ID%%", &account_id)
        .replace("%%ACCOUNT_LABEL%%", &esc(label))
        .replace("%%ACCOUNT_STATUS_CLASS%%", &esc(&account.status))
        .replace("%%ACCOUNT_STATUS%%", &esc(&account.status))
        .replace("%%ACCOUNT_ERROR%%", &account_error)
        .replace(
            "%%ACCOUNT_PORT%%",
            &account.port.unwrap_or_default().to_string(),
        )
        .replace("%%LAST_REQUEST%%", &last_request)
}

pub fn message_page(title: &str, message: &str, href: &str) -> String {
    MESSAGE_HTML
        .replace("%%STYLES%%", MESSAGE_CSS)
        .replace("%%TITLE%%", &esc(title))
        // Callers may deliberately include safe markup such as <code> in messages.
        .replace("%%MESSAGE%%", message)
        .replace("%%HREF%%", &esc(href))
}

fn esc(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_templates_have_no_unexpanded_markers() {
        let config = Config {
            admin_port: 17800,
            oauth_port: 1455,
            host: "127.0.0.1".into(),
            public_host: "localhost".into(),
            db_path: "test.json".into(),
            port_range: crate::config::PortRange {
                start: 18001,
                end: 18100,
            },
            default_provider: "chatgpt".into(),
            providers: std::collections::HashMap::new(),
        };
        let html = render(&config, &[], false, &["/auth/callback".into()]);
        assert!(!html.contains("%%"));
        assert!(html.contains("Sync all usage"));
        assert!(html.contains("new EventSource"));
    }
}
