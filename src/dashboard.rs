use crate::{config::Config, model::Account};

const DASHBOARD_SCRIPT: &str = r#"
(() => {
  const cards = new Map(Array.from(document.querySelectorAll("[data-usage-account]"))
    .map((card) => [card.dataset.usageAccount, card]));

  document.querySelectorAll("[data-local-time]").forEach((time) => {
    const date = new Date(time.dataset.localTime);
    if (!Number.isNaN(date.getTime())) {
      time.textContent = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
    }
  });

  const formatPercent = (value) => {
    const rounded = Math.round(Number(value) * 10) / 10;
    return Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(1);
  };
  const formatReset = (value) => {
    const seconds = Number(value);
    if (!Number.isFinite(seconds)) return "Reset time unavailable";
    const date = new Date(seconds * 1000);
    if (Number.isNaN(date.getTime())) return "Reset time unavailable";
    return "Resets " + new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
  };

  const renderWindow = (window) => {
    const remaining = Math.max(0, Math.min(100, Number(window.remainingPercent) || 0));
    const item = document.createElement("div");
    item.className = "usage-window";
    const heading = document.createElement("div");
    heading.className = "usage-window-heading";
    const label = document.createElement("span");
    const limitPrefix = window.limitName && window.limitId !== "codex" ? window.limitName + " · " : "";
    label.textContent = limitPrefix + window.label;
    const amount = document.createElement("span");
    amount.textContent = formatPercent(remaining) + "% remaining";
    heading.append(label, amount);
    const track = document.createElement("div");
    track.className = "usage-track";
    track.setAttribute("role", "progressbar");
    track.setAttribute("aria-label", label.textContent);
    track.setAttribute("aria-valuemin", "0");
    track.setAttribute("aria-valuemax", "100");
    track.setAttribute("aria-valuenow", String(remaining));
    const fill = document.createElement("div");
    fill.className = "usage-fill" + (remaining <= 10 ? " critical" : remaining <= 25 ? " low" : "");
    fill.style.width = remaining + "%";
    track.append(fill);
    const reset = document.createElement("small");
    reset.className = "usage-reset";
    reset.textContent = formatReset(window.resetsAt);
    item.append(heading, track, reset);
    return item;
  };

  const renderAccount = (account) => {
    const card = cards.get(account.accountId);
    if (!card) return;
    const container = card.querySelector(".usage-windows");
    container.replaceChildren();
    if (account.error) {
      const error = document.createElement("p");
      error.className = "usage-error";
      error.textContent = account.error;
      container.append(error);
      return;
    }
    if (!Array.isArray(account.windows) || account.windows.length === 0) {
      const empty = document.createElement("span");
      empty.className = "empty";
      empty.textContent = "No usage windows were returned for this account.";
      container.append(empty);
      return;
    }
    account.windows.forEach((window) => container.append(renderWindow(window)));
    if (account.refreshError) {
      const warning = document.createElement("p");
      warning.className = "usage-error";
      warning.textContent = "Latest usage refresh failed: " + account.refreshError;
      container.append(warning);
    }
  };

  const liveStatus = document.getElementById("usage-live-status");
  const refreshAllButton = document.getElementById("refresh-all-usage");
  const events = new EventSource("/api/usage/events");
  const readEvent = (event) => {
    try {
      return JSON.parse(event.data);
    } catch {
      liveStatus.textContent = "Received an invalid live usage update";
      return null;
    }
  };
  events.addEventListener("open", () => { liveStatus.textContent = "Live usage updates connected"; });
  events.addEventListener("snapshot", (event) => {
    const payload = readEvent(event);
    if (payload) (payload.accounts || []).forEach(renderAccount);
  });
  events.addEventListener("usage", (event) => {
    const account = readEvent(event);
    if (account) renderAccount(account);
  });
  events.addEventListener("error", () => {
    liveStatus.textContent = "Live usage updates disconnected; retrying…";
  });

  refreshAllButton.addEventListener("click", async () => {
    refreshAllButton.disabled = true;
    refreshAllButton.textContent = "Syncing…";
    liveStatus.textContent = "Fetching usage for all accounts…";
    try {
      const response = await fetch("/api/usage/refresh", {
        method: "POST",
        headers: { accept: "application/json" },
      });
      if (!response.ok) throw new Error("Usage sync failed with HTTP " + response.status);
      const payload = await response.json();
      (payload.accounts || []).forEach(renderAccount);
      const count = (payload.accounts || []).length;
      const failed = Number(payload.failed) || 0;
      const accountLabel = count + " account" + (count === 1 ? "" : "s");
      liveStatus.textContent = failed > 0
        ? "Synchronized " + accountLabel + "; " + failed + " failed"
        : "Synchronized " + accountLabel;
    } catch (error) {
      liveStatus.textContent = error.message || "Usage synchronization failed";
    } finally {
      refreshAllButton.disabled = false;
      refreshAllButton.textContent = "Sync all usage";
    }
  });
})();
"#;

pub fn render(
    config: &Config,
    accounts: &[Account],
    removed: bool,
    oauth_paths: &[String],
) -> String {
    let notice = if removed {
        "<div class=\"notice\">Account removed.</div>"
    } else {
        ""
    };
    let mut providers: Vec<_> = config.providers.values().collect();
    providers.sort_by(|a, b| a.id.cmp(&b.id));
    let options = providers
        .iter()
        .map(|p| {
            format!(
                "<option value=\"{}\"{}>{}</option>",
                esc(&p.id),
                if p.id == config.default_provider {
                    " selected"
                } else {
                    ""
                },
                esc(if p.label.is_empty() { &p.id } else { &p.label })
            )
        })
        .collect::<String>();
    let cards: String = if accounts.is_empty() {
        "<div class=\"empty\">No accounts yet. Add a ChatGPT account to get started.</div>".into()
    } else {
        accounts.iter().map(card).collect()
    };
    format!(
        r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>ai_proxy</title>
<style>
:root{{color-scheme:light dark;font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}body{{margin:0;padding:32px;background:Canvas;color:CanvasText}}main{{max-width:1280px;margin:0 auto}}h1{{margin:0 0 8px;font-size:28px}}p{{color:color-mix(in srgb,CanvasText 70%,Canvas)}}.panel{{border:1px solid color-mix(in srgb,CanvasText 18%,Canvas);border-radius:14px;padding:18px;margin:20px 0}}.notice{{border:1px solid #3a7;background:color-mix(in srgb,#3a7 16%,Canvas);padding:12px;border-radius:10px;margin:16px 0}}code{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px}}button,select{{font:inherit;border-radius:9px;border:1px solid color-mix(in srgb,CanvasText 22%,Canvas);padding:8px 10px;background:Canvas;color:CanvasText}}button.primary{{background:CanvasText;color:Canvas}}.actions{{display:flex;gap:8px;flex-wrap:wrap}}.status{{display:inline-block;border-radius:999px;padding:3px 8px;font-size:12px}}.status.active{{background:color-mix(in srgb,#2a7 24%,Canvas)}}.status.needs_reauth{{background:color-mix(in srgb,#c72 24%,Canvas)}}.empty{{color:color-mix(in srgb,CanvasText 60%,Canvas)}}.section-heading{{display:flex;align-items:baseline;justify-content:space-between;gap:12px;margin-bottom:16px}}.section-heading h2{{margin:0;font-size:19px}}.section-heading small,.field-label{{color:color-mix(in srgb,CanvasText 60%,Canvas)}}.usage-controls{{display:flex;align-items:center;justify-content:flex-end;gap:10px;flex-wrap:wrap}}.usage-grid{{display:flex;flex-direction:column;gap:12px}}.usage-card{{display:grid;grid-template-columns:minmax(230px,1.1fr) minmax(300px,2.2fr) 70px minmax(145px,.8fr) max-content;align-items:center;gap:20px;border:1px solid color-mix(in srgb,CanvasText 13%,Canvas);border-radius:11px;padding:16px;min-width:0}}.usage-account-heading{{min-width:0}}.usage-account-heading strong{{display:block;overflow-wrap:anywhere}}.usage-account-heading .status{{margin-top:7px}}.account-error{{display:block;margin-top:6px;color:#c44848;overflow-wrap:anywhere}}.usage-windows{{min-width:0}}.usage-window+.usage-window{{margin-top:13px}}.usage-window-heading{{display:flex;justify-content:space-between;gap:12px;margin-bottom:7px;font-size:13px}}.usage-window-heading span:last-child{{font-variant-numeric:tabular-nums;font-weight:650;white-space:nowrap}}.usage-track{{height:12px;overflow:hidden;border-radius:999px;background:color-mix(in srgb,CanvasText 12%,Canvas)}}.usage-fill{{height:100%;border-radius:inherit;background:#2a8f62}}.usage-fill.low{{background:#c47a21}}.usage-fill.critical{{background:#c44848}}.usage-reset{{display:block;margin-top:6px;color:color-mix(in srgb,CanvasText 62%,Canvas);font-size:12px}}.usage-error{{margin:0;color:#c44848;font-size:13px}}.usage-window+.usage-error{{margin-top:10px}}.field-label{{display:block;margin-bottom:5px;font-size:11px;font-weight:650;letter-spacing:.06em;text-transform:uppercase}}.last-request time,.last-request code{{display:block;font-size:12px}}.last-request code{{margin-top:4px}}.account-actions{{justify-content:flex-end}}.account-actions form{{margin:0}}@media(max-width:950px){{.usage-card{{grid-template-columns:1fr;gap:15px}}.usage-account-heading{{display:flex;align-items:center;gap:10px;flex-wrap:wrap}}.usage-account-heading .status{{margin-top:0}}.account-actions{{justify-content:flex-start}}}}@media(max-width:760px){{body{{padding:18px}}.section-heading{{align-items:flex-start;flex-direction:column}}}}
</style></head><body><main><h1>ai_proxy</h1><p>Admin port <code>{}</code>, OAuth redirect <code>localhost:{}{}</code>, downstream range <code>{}-{}</code>.</p>{}
<section class="panel"><form method="post" action="/accounts" class="actions"><select name="provider">{}</select><button class="primary" type="submit">Add account</button></form></section>
<section class="panel" aria-labelledby="usage-title"><div class="section-heading"><h2 id="usage-title">Accounts &amp; usage</h2><div class="usage-controls"><small id="usage-live-status" aria-live="polite">Connecting live usage updates…</small><button id="refresh-all-usage" type="button">Sync all usage</button></div></div><div class="usage-grid">{}</div></section></main>
<script>{}</script></body></html>"##,
        config.admin_port,
        config.oauth_port,
        esc(oauth_paths
            .first()
            .map(String::as_str)
            .unwrap_or("/auth/callback")),
        config.port_range.start,
        config.port_range.end,
        notice,
        options,
        cards,
        DASHBOARD_SCRIPT
    )
}

fn card(account: &Account) -> String {
    let label = account
        .metadata
        .string("email")
        .or_else(|| account.metadata.string("accountId"))
        .unwrap_or(&account.id);
    let last = account
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
                    .map(|s| format!("<code>HTTP {s}</code>"))
                    .unwrap_or_default()
            )
        })
        .unwrap_or_else(|| "<span class=\"empty\">None yet</span>".into());
    format!(
        r#"<article class="usage-card" data-usage-account="{}"><div class="usage-account-heading"><strong>{}</strong><span class="status {}">{}</span>{}</div><div class="usage-windows"><span class="empty">Loading current usage…</span></div><div class="account-fact"><small class="field-label">Port</small><code>{}</code></div><div class="account-fact last-request"><small class="field-label">Last request</small>{}</div><div class="account-actions actions"><form method="post" action="/accounts/{}/reauth"><button type="submit">Re-auth</button></form><form method="post" action="/accounts/{}/remove"><button type="submit">Remove</button></form></div></article>"#,
        esc(&account.id),
        esc(label),
        esc(&account.status),
        esc(&account.status),
        account
            .last_error
            .as_ref()
            .map(|e| format!("<small class=\"account-error\">{}</small>", esc(e)))
            .unwrap_or_default(),
        account.port.unwrap_or(0),
        last,
        esc(&account.id),
        esc(&account.id)
    )
}

pub fn message_page(title: &str, message: &str, href: &str) -> String {
    format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{}</title><style>body{{font-family:ui-sans-serif,system-ui;margin:40px;line-height:1.5}}main{{max-width:760px}}code{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}}a{{display:inline-block;margin-top:20px}}</style></head><body><main><h1>{}</h1><p>{}</p><a href="{}">Back to dashboard</a></main></body></html>"#,
        esc(title),
        esc(title),
        message,
        esc(href)
    )
}
fn esc(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
