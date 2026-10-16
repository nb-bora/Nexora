# Decisions, and what would prove them wrong

Every decision below has a falsifier. If the falsifier fires, we change the decision — not the benchmark.

| # | Decision | Why | Falsifier (revisit if...) |
|---|---|---|---|
| 1 | Rust, single crate | Memory safety on the component that holds every provider key; no GC pauses on long SSE streams; one static binary | A Go prototype of the same scope is < 60% of the code **and** within 10% on p99 overhead and RSS under the same load |
| 2 | Relay the JSON body as `serde_json::Value`; do not model the OpenAI schema | Least code; new request fields work on day one | A feature needs deep field rewriting on > 3 paths (then add typed structs for those paths only) |
| 3 | One adapter: OpenAI-compatible upstreams | Covers OpenAI, Gemini, Mistral, vLLM, Ollama... | Native-only features (Anthropic prompt caching controls, thinking blocks) matter to users; add a native adapter behind the same `Upstream` interface |
| 4 | Fallback only before the first byte | A mid-stream switch silently corrupts answers | Users ask for mid-stream resume and accept its semantics (explicit opt-in per alias) |
| 5 | Reserve-then-settle token limits | Output length is unknown up front; post-hoc counting is bypassable by bursts | Measured over-reservation starves tenants in practice (then lower the default `max_tokens` reservation) |
| 6 | Keys stored as SHA-256 only, hash-map lookup | 256-bit random keys: no timing or brute-force exposure | Human-chosen secrets are ever allowed (then use HMAC with a pepper + constant-time compare) |
| 7 | Control plane on a separate listener, loopback by default | A leak of the data port never exposes metrics/admin | Never — this one is a principle, not a bet |
| 8 | Append-only SQLite ledger (trigger-enforced), written off the hot path, dropping rows rather than blocking | Request latency must not depend on disk | Row drops observed (`nexora_ledger_dropped_total` > 0 in normal load): move to a batched/WAL queue or Postgres |
| 9 | `panic = unwind` (not abort) | One bad request must not kill every in-flight stream | — |
| 10 | Configuration only from environment variables, `.env` loaded first, real env wins; malformed `.env` or any invalid value stops startup | One config path; works unchanged with Docker/Kubernetes/secret injectors; fail-fast | Config grows past what flat variables express readably (> ~10 tenants or per-alias policies): add a structured file for *non-secret* policy, keep secrets in env |
| 11 | Budgets enforced at admission from in-memory spend, restored from the ledger at startup; a budget requires every reachable model to be priced | No disk read on the hot path; an unpriced model would make the cap silently ineffective | In-flight overshoot becomes material (long streams, high concurrency): reserve estimated cost at admission and settle afterwards, like tokens |
| 12 | Tenants and keys manageable through an admin API on the control plane only, disabled unless a token is set; revocation = removal from the in-memory map | Keys must be rotatable without a restart; the admin surface must not exist by default | The admin API is ever exposed beyond loopback/private network without mTLS or an allow-list: that is a deployment error, document and refuse |
| 13 | Concurrency slot is owned by the response stream and freed when it is dropped | Streams are long-lived; counting requests instead of open streams would not protect connections | — |

## Test discipline

`tests/e2e.rs` runs the real binary against a mock upstream. The suite was mutation-checked by hand: removing budget booking, the concurrency check, the revocation, or the provider key each turns a specific test red. Re-do this whenever a guard is added. Always read response bodies in tests: an abandoned response is, to the gateway, a client cancellation (no usage, slot held until dropped).

## CI (`.github/workflows/ci.yml`)

Three jobs on every PR and on `main`: `fmt` + `clippy -D warnings` + all tests; `cargo audit` with warnings denied; docker build + smoke test. Dependabot proposes weekly updates for cargo, actions and the base image. Before it existed, a local audit found four advisories in `rustls 0.23.45` (fixed by `0.23.46`): the audit job is what keeps that from recurring.

Honest limits of this setup:
- The workflow was linted with `actionlint` and each job's commands were replayed locally in Docker, but it had **not run on GitHub** when it was written.
- Actions are pinned to major tags (`@v4`), not commit SHAs: a compromised tag would run in CI. Dependabot keeps them current; SHA pinning is the stricter option.
- The Rust toolchain floats on `stable`: a new Rust release can add a clippy lint and turn CI red without any change here. That is a deliberate trade for staying current; pin a version in `rust-toolchain.toml` if it becomes a nuisance.
- `cargo audit` fails on *any* new advisory in the lockfile, even one unrelated to the change being merged. Intentional: fix or consciously ignore it with a reason.
- No branch protection is configured by this repository; a PR can be merged with a red check unless the GitHub repo is set to require it.

## Measured (not claimed)

Reproduce with `docker compose -f bench/compose.yml up --build --abort-on-container-exit --exit-code-from bench` (see `bench/README.md`). Release build, Node mock upstream, client / mock / gateway on one Docker network, 5 000 requests per point, non-streaming, one Windows laptop under Docker Desktop (run-to-run noise is of the order of the p99 differences below).

| Concurrency | Direct p50 / p99 | Via gateway p50 / p99 | Overhead |
|---|---|---|---|
| 1 | 0.62 / 3.46 ms | 1.28 / 3.09 ms | **+0.66 ms p50**; p99 within noise |
| 32 | 8.32 / 21.85 ms | 12.51 / 30.82 ms | not meaningful: client/mock-bound |

An earlier run gave +0.86 ms p50: treat the overhead as "about 0.7-0.9 ms on this setup", not as a precise figure.

What this does **not** show: maximum throughput, behavior under real provider latency, streaming time-to-first-token, memory over hours, or any comparison with other gateways. The harness can host a competitor (`EXTRA`), but none has been run: do not publish a "faster than X" claim until one has, with pinned image digests.

Bug found *by* this benchmark: missing `TCP_NODELAY` on the listener added a flat ~40 ms to every request (Nagle + delayed ACK). Fixed in `main.rs`.
A first run also reported the gateway as "faster than direct" — an invalid result (the test sent an unknown alias, so it got 404s). The harness now counts non-200 responses.

## Known gaps (honest list)

- Single adapter; no native Anthropic/Gemini features.
- Limiter is per-instance memory: with N replicas the effective limit is N x configured. Needs Redis (or sticky routing) before multi-instance.
- Usage is read from the last 4 KB of the response. If a provider puts `usage` earlier in a large non-streamed body, the row is flagged `estimated=1` and the reservation is kept (conservative, but not exact).
- No cache, no PII handling, no replay: these are the differentiators and come next, in this order: replay/shadow -> capability-aware fallback -> exact cache -> PII.
- No throttling of repeated failed authentication (infeasible to brute-force 256-bit keys, but a flood still costs CPU).
- Failed upstream attempts are counted in metrics but not written to the ledger (only the final outcome is).
- Env-defined tenants and provider settings still require a restart; only API-managed tenants are live.
- The ledger and tenants share one SQLite file: single node only. Multi-instance needs a shared store (and a shared limiter); API-created tenants would not propagate between instances today.
- No per-IP throttling of the admin API's failed logins (loopback by default; do not expose it).
- Provider keys sit in plain text in `.env` / the process environment. Acceptable for a laptop or a dev box; in production inject them from a secret manager (the env-var interface stays identical) and never bake `.env` into an image.
- Environment-variable names cannot contain `-` or `.`: aliases, tenants and providers are limited to `[A-Z0-9_]`. Model names are unaffected (they live in values).
- Variables are read once at startup; a name typo such as `NEXORA_TENANT_X_RMP` is silently an unknown variable (only the known suffixes are validated).
