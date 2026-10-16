# Nexora

A secure, cost-aware LLM gateway. One OpenAI-compatible endpoint in front of many providers.

**Today (v0.2):** API-key auth (hashed) with live key management, per-tenant RPM/TPM limits (reserve-then-settle), concurrency caps and USD budgets, ordered provider fallback with single-probe circuit breaker, streaming pass-through with exact token/cost accounting, append-only ledger, Prometheus metrics on a separate port.
**Next:** traffic replay / shadow evaluation of model changes (the differentiator), capability-aware fallback, cache, PII. See [docs/DECISIONS.md](docs/DECISIONS.md) for the decisions, their falsifiers, measurements and known gaps.

## Run

```sh
cargo build --release
./target/release/nexora keygen            # prints the API key once + the hash for the config
cp .env.example .env                      # fill in providers, aliases, tenants (git-ignored)
./target/release/nexora                   # reads ./.env; real env vars override it
```

All settings live in the environment (see [.env.example](.env.example)): `NEXORA_PROVIDER_<NAME>_URL|_KEY`, `NEXORA_ALIAS_<NAME>="provider/model, provider/model"` (ordered fallback), `NEXORA_TENANT_<NAME>_KEY_HASH|_RPM|_TPM|_MODELS`, `NEXORA_PRICES`. A missing or invalid setting stops startup with the offending variable named. In Docker: `docker run --env-file .env ...`.

```sh
curl localhost:8080/v1/chat/completions -H "Authorization: Bearer $KEY" \
  -d '{"model":"smart","messages":[{"role":"user","content":"hi"}]}'
```

No Rust toolchain? `docker build -t nexora .` produces a distroless, non-root image.

## Tenants, keys and budgets

Tenants can be declared in the environment (static) or managed live through the admin API on the control-plane port (requires `NEXORA_ADMIN_TOKEN`). Keys are shown once and stored hashed; revocation takes effect on the next request.

```sh
A=http://127.0.0.1:9090/admin/tenants; H="Authorization: Bearer $NEXORA_ADMIN_TOKEN"
curl -X POST $A -H "$H" -d '{"name":"acme","rpm":100,"max_concurrent":8,"daily_budget_usd":5,"models":["smart"]}'   # -> {"api_key":"nx_live_..."}
curl $A -H "$H"                      # limits and spend (never keys)
curl -X DELETE $A/acme -H "$H"       # immediate revocation
```

Over budget returns `429 budget_exceeded` with `Retry-After` until the UTC day/month rolls over. Spend is restored from the ledger after a restart. Budgets are checked at admission: requests already in flight can overshoot by their own cost.

## Tests

```sh
cargo test        # unit + end-to-end (real binary, mock upstream, real HTTP); ~1 s once compiled
```

Data plane `:8080` (`/v1/chat/completions`, `/v1/embeddings`, `/v1/models`). Control plane `127.0.0.1:9090` (`/healthz`, `/metrics`).
