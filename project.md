# ai_proxy — Project Memory

Purpose of this file: everything a fresh coding agent needs to be productive
on this repo — what the project is, how it currently works, what we learned
and why decisions were made, and what comes next, in priority order.
Read this before implementing anything.

## 1. What this is

`ai_proxy` is a small Rust binary (Tokio + Axum + Reqwest) that exposes
ChatGPT Plus/Pro accounts as local OpenAI-compatible endpoints. Each account
gets a stable downstream base URL like `http://localhost:18001/v1` backed by
the ChatGPT Codex backend (`https://chatgpt.com/backend-api/codex`).
Primary client: the pi coding agent (Earendil Works), plus other
OpenAI-compatible tools (OpenCode).

- Primary downstream API: `GET /v1/models`, `POST /v1/responses` (SSE streaming).
- Compatibility: `POST /v1/chat/completions` (adapted onto Responses internally).
- Per-account ports from a configured range, stable across restarts, stored in
  a JSON database (`orche-proxy.db.json` — local only, git-ignored).
- OAuth login via dashboard + localhost redirect listener; tokens refreshed
  automatically; dead refresh tokens mark accounts `needs_reauth`.
- Admin dashboard with SSE-pushed usage snapshots; per-account usage refreshed
  after each downstream request; "Sync all usage" button; Codex quota-window
  keepalive worker (minimal `gpt-5.6-luna` requests on startup/window events).
- Canonical docs: `README.md` (user-facing), `design.md` (original design).
  This file (`project.md`) is the running history + plan. Handoff/review docs
  from parallel agent work live next to the repo (`~/TRANSPARENT_CODEX_TUNNEL_HANDOFF.md`,
  `~/TRANSPARENT_CODEX_TUNNEL_REVIEW.md`).

## 2. Current functionality (as of `main` @ `050d09d`)

- **Upstream request log** (`src/request_log.rs`, SQLite `ai_proxy.sqlite3`
  next to the JSON db, WAL mode). One row per upstream call with routing
  metadata + first-class cost columns: `model`, `reasoning_effort`,
  `input/output/total_tokens`, `cached_tokens`, `reasoning_tokens`,
  `prompt_cache_key`, `cache_write_tokens`, plus `error`/`duration_ms`.
  `usage_windows` table holds one row per quota window per fetch, FK-linked
  to its fetch row. Raw bodies are NOT persisted (see §3).
- **Flags**: `--request-log` (`ORCHE_PROXY_REQUEST_LOG`), `--log-bodies`
  (temporary raw capture for debugging, default off),
  `--purge-bodies` (NULLs stored bodies + VACUUMs, prints stats and exits).
- **Header transparency**: adapter mode forwards downstream application
  headers (`session-id`, `thread-id`, `x-client-request-id`, `openai-beta`,
  …) via `adapter_headers()`/`filtered_headers()`; auth/host/length/cookies/
  forwarding headers are always stripped and account credentials re-applied
  in `provider_fetch`, so downstream can never override the account token.
  Streaming responses use `StreamLogGuard`, which still logs partial bodies
  as `incomplete_stream` on disconnect.
- **Schema discipline**: `request_log.rs` versions via `PRAGMA user_version`
  (currently 4). v1→v2 wiped, v2→v3 purged bodies + VACUUMed, v3→v4 added
  columns non-destructively. Continue the sequence; never restart it.
- **Guiding principle**: transparent by default; intervene only for auth,
  transport, and documented provider requirements — and log each
  intervention (see §4 for why).

## 3. History: what happened and why

1. **JSON → SQLite logging.** Goal was an eventual SQLite migration; first
   step was logging every upstream request for analytics/session
   reconstruction. Centralized on `provider_fetch()` + streaming tees.
2. **Raw bodies dropped.** An hour of traffic produced ~28MB (74% request
   bodies resending full conversation history each turn, 25% SSE wrappers;
   a single `/v1/models` poll stored 105KB of unchanging catalog). Decision:
   parse-then-drop — cost columns are extracted in memory, raw bodies persist
   only under `--log-bodies`. Measured: 28MB → 110KB on existing data.
   Delta-encoding/compression for requests was considered and rejected as
   too clever for now (corruption risk vs. a "perfect record" goal).
3. **Cost columns + `usage_windows`.** Added to answer "tokens per
   percentage point per model": token totals, cached/reasoning splits,
   per-window percentage time series. Later additions: `prompt_cache_key`
   (stable per pi session — free session grouping), `cache_write_tokens`
   (captured for completeness; observed always 0 server-side).
4. **The cache-miss saga (the big one).** `gpt-5.6-sol/high` showed 0%
   cache hits across 2.5M input tokens while `gpt-5.6-luna/max` hit 50–95%
   on the same account/backend. Prefix-diff of captured bodies proved the
   client innocent (stable key, byte-identical 236-item prefixes, 12s apart,
   still missed). Codex CLI source + pi's bundled provider revealed the
   mechanism: ChatGPT derives cache affinity from the Responses `session-id`
   header, which our adapter silently dropped (it built fresh headers with
   only `Content-Type`). Pi natively sends `session-id`/`x-client-request-id`
   and `prompt_cache_key = sessionId`; it also prefers WebSocket with
   `previous_response_id` deltas and falls back to full-body SSE through
   our HTTP-only proxy. The ~20-line header-forwarding fix took hit rates
   14.6% → 95–98%, including 400K-input turns at 99%. Lesson institutionalized
   as the transparency principle (§2).
5. **Stale `client_version`.** Default models path pinned `0.142.5`, which no
   longer lists Sol (min 0.144.0). Bumped to 0.153.0 from the Codex checkout;
   lesson: hardcoded upstream versions rot — see §5.
6. **Subagent episodes.** A `codex_diff` research agent produced
   `/tmp/codex-diff-report.md` (field-by-field CLI diff). A `headers-first`
   implementer agent (muse-spark/xhigh exception to the usual OpenAI-only
   agent policy, granted for quota reasons) wrote the header fix but shipped
   a flaky test (two async tests sharing one temp dir, ~1/6 failure rate).
   Lesson now required in subagent briefs: **repeat test runs (5x minimum)
   for anything async/filesystem** — a single green run proves little.

## 4. Standing decisions / conventions

- Transparency principle (§2); body-field strip list (`temperature`,
  `top_p`, `metadata`, …) is still unevaluated tech debt — audit per field
  against the live API before un-stripping.
- Never fail a proxied request because logging failed (fire-and-forget writer).
- Never log secrets: no tokens, no header values beyond names, no
  downstream auth passthrough.
- Test/prod hygiene: credentials live in `$HOME/orche-proxy.db.json` —
  ALWAYS copy to `/tmp` for testing, never run test proxies against it.
  Mock upstream scripts live in `/tmp` (ephemeral); test ports 17880+ to
  avoid clashing with real instances.
- Verification bar: `cargo fmt --check`, `cargo clippy --all-targets -- -D
  warnings`, `cargo test` (repeated on any async/filesystem change).
- Subagent protocol (per project instructions): interactive pi session in a
  new tmux window, sentinel file for DONE/QUESTION signals, work in git
  worktrees, commit on feature branches, never merge to `main` unreviewed.

## 5. Plan (priority order)

1. **Drift watchdog (NEXT).** The proxy must notice it's becoming wrong
   before the quota bill does. Periodic self-check from SQLite: cache hit
   rate over trailing ~20 model turns (alert if <50% with stable keys),
   model-catalog shrink vs baseline, upstream 4xx spike, unknown SSE event
   types. Surface as JSON log alerts + dashboard banner.
2. **Monthly upstream-diff process.** Re-run the Codex CLI comparison from
   the existing brief template; bump `client_version` default along with it.
   Alert-triggered runs whenever the watchdog fires.
3. **Session tracking.** `prompt_cache_key` column exists; remaining work is
   the `x-session-id` header via pi's `before_provider_headers` hook (docs
   bless this exact pattern) plus a nullable `session_id` log column. Check
   first whether pi's default attribution headers already carry something
   session-stable (one header-dump experiment).
4. **Deferred (do NOT build yet):** WebSocket tunnel with `previous_response_id`
   deltas (bandwidth optimization; cache correctness no longer needs it),
   request delta-encoding/compression, retention auto-cleanup (currently
   unnecessary at KB/hr volumes), downstream TLS, CLI-binary CI harness.
5. **Open externals:** a support ticket to OpenAI is viable if needed
   (byte-identical-prefix miss pairs with request IDs are on file); weekly
   window quota pressure is genuine usage now, not a bug.
