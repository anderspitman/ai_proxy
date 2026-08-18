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
