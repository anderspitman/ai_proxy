# ai_proxy

A small Rust OpenAI-compatible proxy for ChatGPT Plus/Pro accounts.

## Build and run

```sh
cargo build --release
./target/release/ai_proxy --admin-port 17800 --oauth-port 1455 --port-range 18001-18100
```

For development, `cargo run -- [options]` works as well. Existing `orche-proxy.db.json` databases and provider configuration files from the original Node.js implementation remain compatible.

Open the dashboard at `http://localhost:17800/`, then add a ChatGPT account.

Each account gets a stable downstream base URL like:

```text
http://localhost:18001/v1
```

Primary downstream endpoints:

- `GET /v1/models`
- `GET /v1/models/:id`
- `POST /v1/responses`

Compatibility downstream endpoint:

- `POST /v1/chat/completions`

Responses example:

```sh
curl http://localhost:18001/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-5.5",
    "input": "Hi there",
    "reasoning": { "effort": "medium" },
    "stream": true
  }'
```

Chat completions is retained for OpenAI-compatible clients that do not support Responses yet.

## Usage snapshots

The dashboard reads usage from account-scoped snapshots stored in the local JSON database. After a request to a supported `/v1` endpoint finishes, the proxy refreshes only the snapshot for the account serving that request. Concurrent requests for one account are coalesced and serialized without refreshing any other account.

An open dashboard receives snapshot changes over a local SSE connection, so usage charts update without a page refresh. Connecting to the dashboard or its event stream does not itself contact the provider usage API.

Use **Sync all usage** on the dashboard to explicitly fetch fresh upstream usage for every active account. This is useful when account usage may also be generated outside the proxy; each refreshed account is sent to the page through the same SSE stream.

## Codex window keep alive

The proxy automatically keeps Codex quota windows active for every active account. It sends one minimal `gpt-5.6-luna` request when the proxy starts or discovers a new account, then checks usage every five minutes. If a Codex window expires, its reset timestamp moves by more than one minute, or its available usage increases, the proxy sends one more request and refreshes usage again. Failed keepalive requests are retried at the next check.

This also detects resets and usage caused by other Codex clients or proxy instances. Separate instances may occasionally send duplicate keepalive requests. Keepalives are real upstream requests and consume a small amount of Codex allowance.

## Command-line options

```text
Usage: ai_proxy [options]

  --admin-port <port>     Admin dashboard port (default: 17800)
  --oauth-port <port>     OAuth redirect port (default: 1455)
  --port-range <a-b>      Downstream account ports (default: 18001-18100)
  --host <host>           Bind host (default: 127.0.0.1)
  --public-host <host>    Host displayed in URLs (default: localhost)
  --db <file>             JSON database (default: ./orche-proxy.db.json)
  --config <file>         Provider config (default: ./orche-proxy.config.json)
  --provider <id>         Default provider (default: chatgpt)
```

## Config

Runtime options can be passed as CLI flags or `ORCHE_PROXY_*` environment variables. Provider details can be overridden in `orche-proxy.config.json`:

```json
{
  "providers": {
    "chatgpt": {
      "api": {
        "baseUrl": "https://chatgpt.com/backend-api/codex",
        "modelsPath": "/models?client_version=0.142.5",
        "responsesPath": "/responses"
      }
    }
  }
}
```

The default ChatGPT OAuth client and backend URLs are based on the public Codex CLI implementation, but are intentionally configurable because these upstream details may change.

## OpenCode

Use `@ai-sdk/openai`, not `@ai-sdk/openai-compatible`, so OpenCode sends Responses API requests.

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "elodin": {
      "npm": "@ai-sdk/openai",
      "name": "Elodin",
      "options": {
        "baseURL": "http://localhost:18001/v1",
        "apiKey": "unused",
        "timeout": false,
        "chunkTimeout": 300000
      },
      "models": {
        "gpt-5.5": {
          "name": "GPT-5.5",
          "reasoning": true,
          "tool_call": true,
          "temperature": false,
          "attachment": false,
          "limit": {
            "context": 272000,
            "output": 128000
          }
        }
      }
    }
  }
}
```

OpenCode's `Ctrl-T` cycles model variants. If you want explicit reasoning-effort variants for this custom model, add:

```json
"variants": {
  "minimal": { "reasoningEffort": "minimal", "reasoningSummary": "auto", "include": ["reasoning.encrypted_content"] },
  "low": { "reasoningEffort": "low", "reasoningSummary": "auto", "include": ["reasoning.encrypted_content"] },
  "medium": { "reasoningEffort": "medium", "reasoningSummary": "auto", "include": ["reasoning.encrypted_content"] },
  "high": { "reasoningEffort": "high", "reasoningSummary": "auto", "include": ["reasoning.encrypted_content"] },
  "xhigh": { "reasoningEffort": "xhigh", "reasoningSummary": "auto", "include": ["reasoning.encrypted_content"] }
}
```

Restart OpenCode after changing `opencode.json`.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

## Logs

Runtime logs are newline-delimited JSON. Request logs include basic request/status fields plus `model` and `reasoningEffort` so you can verify clients are using Responses and reasoning variants correctly.

Example:

```json
{"ts":"2026-07-07T15:59:57.167Z","event":"request","port":18001,"accountId":"d71524f0-6fe7-4753-9d2b-5b1f54bc26c5","provider":"chatgpt","method":"POST","path":"/v1/responses","status":200,"durationMs":1758,"model":"gpt-5.5","reasoningEffort":"xhigh"}
```

Prompts, messages, tool arguments, headers, OAuth tokens, and refresh tokens are not logged.

## Upstream request log

Every upstream provider request (models, responses, chat completions,
usage, keepalive) is recorded in SQLite (default `./ai_proxy.sqlite3`,
override with `--request-log` or `ORCHE_PROXY_REQUEST_LOG`). Request and
response bodies are stored as perfect plaintext copies with no truncation,
so sessions can be reconstructed and costs reconciled later. Raw SSE text
is stored for streaming responses.

```sql
CREATE TABLE upstream_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  account_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  kind TEXT NOT NULL,       -- models | responses | chat_completions | usage | keep_alive
  method TEXT NOT NULL,
  url TEXT NOT NULL,
  request_body TEXT,         -- raw JSON, NULL for GETs
  response_status INTEGER,
  response_body TEXT,        -- raw JSON or raw SSE text
  duration_ms INTEGER NOT NULL,
  error TEXT,                -- transport errors and incomplete streams
  model TEXT,                -- parsed from the request (NULL for models/usage)
  reasoning_effort TEXT,     -- e.g. none, xhigh (NULL for models/usage)
  input_tokens INTEGER,      -- parsed from response.completed usage
  output_tokens INTEGER,
  total_tokens INTEGER,
  cached_tokens INTEGER,     -- input_tokens_details.cached_tokens
  reasoning_tokens INTEGER   -- output_tokens_details.reasoning_tokens
);
-- One row per usage window per fetch, for all window types the provider
-- exposes (5-hour, weekly, ...). Join to upstream_requests via request_id.
CREATE TABLE usage_windows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER NOT NULL REFERENCES upstream_requests(id),
  ts TEXT NOT NULL,
  account_id TEXT NOT NULL,
  limit_id TEXT NOT NULL,    -- e.g. codex
  limit_name TEXT,
  label TEXT NOT NULL,       -- e.g. 5-hour window
  remaining_percent REAL NOT NULL,
  resets_at INTEGER
);
```

Cost correlation example: sum `total_tokens` per `model`/`reasoning_effort`
between two consecutive `usage_windows` readings for an account, divided by
the `remaining_percent` delta, gives tokens per percentage point over time.

Notes:

- Logging is fire-and-forget and never fails a proxied request. SQLite runs
  in WAL mode so you can query while the proxy is running.
- If a downstream client disconnects mid-stream, the partial upstream body
  is still logged with `error = 'incomplete_stream: ...'` so usage/cost
  reconciliation does not silently miss it.
- Token refresh and OAuth exchanges are not logged yet.
- The JSON database remains primary for now; SQLite is an append-only log.
