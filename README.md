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

An open dashboard receives snapshot changes over a local SSE connection, so usage charts update without a page refresh. Connecting to the dashboard or its event stream does not contact the provider usage API, and no polling is used. A new account, or an older database without a snapshot, is initialized in the background.

Use **Sync all usage** on the dashboard to explicitly fetch fresh upstream usage for every active account. This is useful when account usage may also be generated outside the proxy; each refreshed account is sent to the page through the same SSE stream.

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
