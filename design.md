This is a simple OpenAI-compatible API proxy for ChatGPT Plus/Pro accounts. It
lets you log in to multiple ChatGPT subscriptions and exposes each account on a
different local port. There is no downstream auth for now, but the design should
leave room to add it later.

The primary client use case is coding agents and OpenAI-compatible tools such as
OpenCode and pi.dev.

## Runtime

- The implementation is a Rust binary named `ai_proxy`.
- Use Tokio and Axum for the asynchronous HTTP servers and Reqwest for upstream
  HTTP requests.
- The database is a single plaintext JSON file. Its schema remains compatible
  with the original Node.js implementation.

## Admin UI and OAuth

- Provide a minimal web dashboard on a configurable admin port.
- The dashboard should show current accounts, health/status, and port mappings.
- The dashboard should let users add and remove accounts.
- Support the OAuth2 authorization code flow.
- The OAuth redirect listener uses `localhost:1455`.
- After a successful OAuth redirect, the UX should take the user back to the
  dashboard, or otherwise make returning to the dashboard obvious.
- After login, immediately start serving the account and communicate which port
  it is using.

## Accounts and Ports

- Each account is exposed as its own OpenAI-compatible base URL, for example
  `http://localhost:18001/v1`.
- A range of candidate downstream ports is provided at startup.
- Port assignment is automatic.
- Assigned ports are stable across restarts and stored in the JSON database.
- If a previously assigned port is unavailable at startup, startup should fail
  with a clear error because downstream agents may depend on that address.
- New accounts should continue along the configured range. If the range fills
  up, freed old ports may be reused.

## Stored Data

Store the minimum necessary data for good UX, including:

- Provider/account metadata.
- OAuth2 access tokens.
- OAuth2 refresh tokens.
- Stable port assignments.

Do not store model lists unless needed. Prefer proxying or deriving model lists
from the upstream provider.

Plaintext tokens in the JSON database are acceptable for now.

## Provider Design

- Start with ChatGPT Plus/Pro support.
- The implementation should be configurable enough to switch provider details or
  add other providers later.
- Keep provider-specific behavior isolated so future support for other providers
  or full OAuth2 provider functionality can be added without rewriting the core
  proxy.
- Figure out the ChatGPT upstream OAuth/API details during implementation, and
  make provider endpoints/configuration adjustable where practical.

## OpenAI-Compatible API

Primary downstream API:

- `GET /v1/models`
- `POST /v1/responses`
- Streaming responses via Server-Sent Events.
- Text-only input initially.
- Tool/function calling support where the upstream provider supports it.
- Reasoning controls such as `reasoning.effort` should be supported through
  `POST /v1/responses` when the upstream provider supports them.

Compatibility downstream API:

- `POST /v1/chat/completions`
- Streaming chat completions via Server-Sent Events.
- Tool/function calling translated between chat-completions format and the
  provider's native format.

Do not prioritize legacy `POST /v1/completions`.

For ChatGPT Plus/Pro, treat `POST /v1/responses` as the canonical internal wire
format. `POST /v1/chat/completions` is a compatibility adapter layered on top of
Responses.

## ChatGPT Codex Provider

- The current ChatGPT provider targets the ChatGPT Codex backend at
  `https://chatgpt.com/backend-api/codex`.
- The ChatGPT Codex backend is Responses-based, not native chat-completions.
- `GET /models` requires a `client_version` query parameter. Keep this
  configurable because upstream requirements may change.
- The ChatGPT Codex backend requires upstream `stream: true`; non-streaming
  downstream requests should be implemented by aggregating upstream SSE.
- The ChatGPT Codex backend requires `store: false`; downstream `store` should
  be accepted but overridden for this provider.
- Unsupported OpenAI parameters may be ignored or translated by this provider
  adapter to preserve compatibility with OpenAI-compatible clients.
- These normalizations are provider-specific and should not be applied globally
  to future providers that support those fields natively.

## Models

- `GET /v1/models` should expose the models available to the upstream ChatGPT
  account in OpenAI-compatible response shape.
- Model IDs should preferably pass through from the upstream provider.
- Provider-specific model mapping or aliases may live inside provider adapters or
  config, not in the proxy core.

## Request Handling

- Prefer passthrough behavior.
- Pass through upstream errors rather than automatically failing over to another
  account.
- Automatically refresh OAuth tokens when they expire.
- If a refresh token becomes invalid, mark the account as requiring re-auth in
  the dashboard.
- Log newline-delimited JSON (`jsonl`) to stdout/stderr.
- Request logs should include basic routing/status information plus safe model
  metadata needed for debugging client behavior: `model` and
  `reasoningEffort`.
- Do not log prompts, messages, tool arguments, headers, OAuth tokens, or
  refresh tokens.

## Usage Snapshots

- Store the latest provider usage snapshot on each account in the JSON database.
- Refresh an account's snapshot after a supported downstream request completes.
- Coalesce and serialize concurrent refreshes for the same account.
- Stream snapshot changes to open dashboards over local SSE without polling.
- Provide an explicit dashboard action to synchronize all active accounts with
  their upstream usage APIs.
- Loading the dashboard or connecting to its event stream must not itself fetch
  upstream usage.
