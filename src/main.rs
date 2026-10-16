mod admin;
mod config;
mod gate;
mod ledger;
mod sync;
mod upstream;

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use gate::Tenant;
use rusqlite::Connection;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering::Relaxed, mpsc::SyncSender, Arc, Mutex, RwLock},
    time::Instant,
};
use sync::RwLockExt;

pub struct App {
    cfg: config::Config,
    tenants: RwLock<HashMap<String, Arc<Tenant>>>, // sha256(key) -> tenant; mutated by the admin API
    up: upstream::Upstream,
    ledger: SyncSender<ledger::Row>,
    db: Mutex<Connection>, // tenants table (rare writes from the admin API)
}

/// Uniform error envelope. Upstream errors are translated, never relayed raw.
struct E(StatusCode, &'static str, String, Option<u64>);

impl IntoResponse for E {
    fn into_response(self) -> Response {
        let mut r = (self.0, Json(json!({"error": {"type": self.1, "message": self.2}}))).into_response();
        if let Some(s) = self.3 {
            r.headers_mut().insert(header::RETRY_AFTER, s.into());
        }
        r
    }
}

const TAIL: usize = 4096;

/// Observes the byte stream (bounded memory), then on Drop settles limits, budget, ledger and metrics.
/// Drop also fires on client disconnect, so cancelled streams are still accounted for.
/// It also owns the tenant's concurrency slot, released exactly once here.
struct Meter {
    app: Arc<App>,
    tenant: Arc<Tenant>,
    id: String,
    alias: String,
    provider: String,
    model: String,
    reserved: f64,
    t0: Instant,
    status: u16,
    attempts: u32,
    tail: Vec<u8>,
}

impl Meter {
    fn push(&mut self, b: &[u8]) {
        self.tail.extend_from_slice(b);
        let n = self.tail.len();
        if n > 2 * TAIL {
            self.tail.drain(..n - TAIL);
        }
    }

    /// Last `"usage":{...}` in the tail; works for plain JSON and SSE alike.
    fn usage(&self) -> Option<(u64, u64)> {
        let i = self.tail.windows(8).rposition(|w| w == b"\"usage\":")?;
        let v: Value = serde_json::Deserializer::from_slice(&self.tail[i + 8..]).into_iter::<Value>().next()?.ok()?;
        Some((v.get("prompt_tokens")?.as_u64()?, v.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0)))
    }
}

impl Drop for Meter {
    fn drop(&mut self) {
        let u = self.usage();
        let (pt, ct) = u.unwrap_or((0, 0));
        // Unknown usage on a successful call: keep the reservation (conservative). On failure: refund.
        let actual = if u.is_some() {
            (pt + ct) as f64
        } else if self.status < 300 {
            self.reserved
        } else {
            0.0
        };
        self.tenant.limiter.settle(self.reserved, actual);
        let cost = self
            .app
            .cfg
            .prices
            .get(&format!("{}/{}", self.provider, self.model))
            .map_or(0.0, |p| (pt as f64 * p.input + ct as f64 * p.output) / 1e6);
        self.tenant.add_spend(cost);
        self.tenant.inflight.fetch_sub(1, Relaxed);
        let ms = self.t0.elapsed().as_millis() as u64;
        let row = ledger::Row {
            id: std::mem::take(&mut self.id),
            tenant: self.tenant.name().to_owned(),
            alias: self.alias.clone(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            prompt_tokens: pt,
            completion_tokens: ct,
            cost_usd: cost,
            latency_ms: ms,
            status: self.status,
            attempts: self.attempts,
            estimated: u.is_none() && self.status < 300,
        };
        if self.app.ledger.try_send(row).is_err() {
            metrics::counter!("nexora_ledger_dropped_total").increment(1);
        }
        let (t, a, p) = (self.tenant.name().to_owned(), self.alias.clone(), self.provider.clone());
        metrics::counter!("nexora_requests_total", "tenant" => t.clone(), "alias" => a.clone(), "provider" => p.clone(), "status" => self.status.to_string()).increment(1);
        metrics::histogram!("nexora_request_seconds", "alias" => a, "provider" => p).record(ms as f64 / 1e3);
        metrics::counter!("nexora_tokens_total", "tenant" => t.clone(), "kind" => "prompt").increment(pt);
        metrics::counter!("nexora_tokens_total", "tenant" => t.clone(), "kind" => "completion").increment(ct);
        if cost > 0.0 {
            metrics::counter!("nexora_cost_micro_usd_total", "tenant" => t).increment((cost * 1e6) as u64);
        }
    }
}

fn authenticate(app: &App, h: &HeaderMap) -> Result<Arc<Tenant>, E> {
    h.get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        // Keys are 256-bit random, so a hash-map lookup on their SHA-256 leaks nothing useful.
        .and_then(|k| {
            let hash = hex::encode(Sha256::digest(k.as_bytes()));
            app.tenants.rd().get(&hash).cloned()
        })
        .ok_or(E(StatusCode::UNAUTHORIZED, "unauthorized", "invalid API key".into(), None))
}

async fn run(app: &Arc<App>, h: &HeaderMap, raw: Bytes, path: &'static str, id: &str) -> Result<Response, E> {
    let t0 = Instant::now();
    let tenant = authenticate(app, h)?;
    let bad = |m: &str| E(StatusCode::BAD_REQUEST, "invalid_request", m.into(), None);
    let mut body: Value = serde_json::from_slice(&raw).map_err(|_| bad("body must be valid JSON"))?;
    let alias = body["model"].as_str().ok_or_else(|| bad("`model` is required"))?.to_owned();
    // Same answer for "unknown" and "not allowed": no alias enumeration.
    let targets = app
        .cfg
        .aliases
        .get(&alias)
        .filter(|_| tenant.allows(&alias))
        .ok_or_else(|| E(StatusCode::NOT_FOUND, "model_not_found", format!("unknown model `{alias}`"), None))?;

    if let Some(wait) = tenant.over_budget() {
        return Err(E(StatusCode::TOO_MANY_REQUESTS, "budget_exceeded", "spending budget exhausted".into(), Some(wait)));
    }
    let out =
        if path == "/embeddings" { 0.0 } else { body["max_completion_tokens"].as_f64().or(body["max_tokens"].as_f64()).unwrap_or(1024.0) };
    let reserved = tenant
        .limiter
        .admit(raw.len() as f64 / 4.0 + out)
        .map_err(|w| E(StatusCode::TOO_MANY_REQUESTS, "rate_limited", "rate limit exceeded".into(), Some(w.ceil().max(1.0) as u64)))?;
    if tenant.inflight.fetch_add(1, Relaxed) >= tenant.cfg.max_concurrent {
        tenant.inflight.fetch_sub(1, Relaxed);
        tenant.limiter.settle(reserved, 0.0);
        return Err(E(StatusCode::TOO_MANY_REQUESTS, "concurrency_limit", "too many concurrent requests".into(), Some(1)));
    }

    // From here the Meter owns the concurrency slot and the token reservation.
    let mut meter = Meter {
        app: app.clone(),
        tenant: tenant.clone(),
        id: id.to_owned(),
        alias: alias.clone(),
        provider: String::new(),
        model: String::new(),
        reserved,
        t0,
        status: 503,
        attempts: 0,
        tail: Vec::new(),
    };
    let (resp, i, attempts) = match app.up.forward(path, targets, &mut body).await {
        Ok(x) => x,
        Err(attempts) => {
            meter.attempts = attempts;
            return Err(E(
                StatusCode::SERVICE_UNAVAILABLE,
                "upstream_unavailable",
                "all upstreams failed or are unavailable".into(),
                Some(5),
            ));
        }
    };
    meter.provider = targets[i].provider.clone();
    meter.model = targets[i].model.clone();
    meter.attempts = attempts;
    let status = resp.status();
    meter.status = status.as_u16();

    if !status.is_success() {
        let b = resp.bytes().await.unwrap_or_default();
        let msg = serde_json::from_slice::<Value>(&b[..b.len().min(65_536)])
            .ok()
            .and_then(|v| v["error"]["message"].as_str().map(str::to_owned))
            .unwrap_or_else(|| "upstream rejected the request".into());
        return Err(E(status, "upstream_rejected", msg, None));
    }

    let ct = resp.headers().get(header::CONTENT_TYPE).cloned();
    let (provider, model) = (meter.provider.clone(), meter.model.clone());
    let stream = resp.bytes_stream().map(move |c| {
        match &c {
            Ok(b) => meter.push(b),
            Err(_) => meter.status = 599,
        }
        c
    });
    let mut r = Response::new(Body::from_stream(stream));
    *r.status_mut() = status;
    if let Some(ct) = ct {
        r.headers_mut().insert(header::CONTENT_TYPE, ct);
    }
    // Provider and model names come from configuration and may not be valid header values:
    // skip the header rather than panic on every request that reaches that target.
    for (name, value) in [("x-nexora-provider", &provider), ("x-nexora-model", &model)] {
        if let Ok(v) = value.parse() {
            r.headers_mut().insert(name, v);
        }
    }
    r.headers_mut().insert("x-nexora-attempts", attempts.into());
    Ok(r)
}

async fn proxy(app: Arc<App>, h: HeaderMap, raw: Bytes, path: &'static str) -> Response {
    let id = uuid::Uuid::new_v4().to_string();
    let mut r = run(&app, &h, raw, path, &id).await.unwrap_or_else(IntoResponse::into_response);
    if let Ok(v) = id.parse() {
        r.headers_mut().insert("x-request-id", v);
    }
    r
}

async fn models(State(app): State<Arc<App>>, h: HeaderMap) -> Response {
    match authenticate(&app, &h) {
        Ok(t) => {
            let data: Vec<_> = app.cfg.aliases.keys().filter(|a| t.allows(a)).map(|a| json!({"id": a, "object": "model"})).collect();
            Json(json!({"object": "list", "data": data})).into_response()
        }
        Err(e) => e.into_response(),
    }
}

fn keygen() {
    let (key, hash) = admin::new_key();
    println!("API key (shown once): {key}\nkey_hash for config : {hash}");
}

async fn stop() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.ok();
}

/// Env tenants + API tenants from the database; spend restored from the ledger.
fn load_tenants(cfg: &config::Config, db: &Connection) -> HashMap<String, Arc<Tenant>> {
    let today = gate::now_days();
    let day = ledger::spent_since(&cfg.ledger_path, today * 86_400);
    let month = ledger::spent_since(&cfg.ledger_path, gate::month_start(today) * 86_400);
    let stored = admin::load(db);
    let (mut map, mut names) = (HashMap::new(), HashSet::new());
    for (c, dynamic) in cfg.tenants.iter().cloned().map(|c| (c, false)).chain(stored.into_iter().map(|c| (c, true))) {
        cfg.check_tenant(&c).unwrap_or_else(|e| panic!("{e}"));
        assert!(names.insert(c.name.clone()), "duplicate tenant name {}", c.name);
        let t = Tenant::new(c.clone(), dynamic);
        t.seed(day.get(&c.name).copied().unwrap_or(0.0), month.get(&c.name).copied().unwrap_or(0.0));
        assert!(map.insert(c.key_hash.clone(), Arc::new(t)).is_none(), "duplicate key_hash for tenant {}", c.name);
    }
    map
}

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("keygen") {
        return keygen();
    }
    // .env is optional, but a .env that exists and is malformed must stop startup.
    // Variables already in the environment take precedence over the file.
    match dotenvy::dotenv() {
        Err(e) if !e.not_found() => panic!("invalid .env: {e}"),
        _ => {}
    }
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    // Real histogram buckets (aggregatable across instances), not per-process quantiles.
    let prom = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets(&[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0])
        .unwrap()
        .install_recorder()
        .expect("metrics recorder");

    let cfg = config::Config::from_env();
    let ledger = ledger::spawn(&cfg.ledger_path); // creates the ledger schema
    let db = Connection::open(&cfg.ledger_path).expect("database");
    db.execute_batch(admin::SCHEMA).expect("tenants schema");
    let tenants = load_tenants(&cfg, &db);
    let (listen, admin_listen, limit) = (cfg.listen.clone(), cfg.admin_listen.clone(), cfg.max_body_bytes);
    let app = Arc::new(App { up: upstream::Upstream::new(&cfg), ledger, tenants: RwLock::new(tenants), db: Mutex::new(db), cfg });

    let data = Router::new()
        .route("/v1/chat/completions", post(|State(a): State<Arc<App>>, h: HeaderMap, b: Bytes| proxy(a, h, b, "/chat/completions")))
        .route("/v1/embeddings", post(|State(a): State<Arc<App>>, h: HeaderMap, b: Bytes| proxy(a, h, b, "/embeddings")))
        .route("/v1/models", get(models))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(app.clone());
    let ctl = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(|State(p): State<metrics_exporter_prometheus::PrometheusHandle>| async move { p.render() }))
        .with_state(prom)
        .merge(admin::routes(app));

    let serve = |addr: String, r: Router| async move {
        let l = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|e| panic!("bind {addr}: {e}"));
        // Without NODELAY, header+body writes of a streamed reply hit Nagle + delayed-ACK: a flat ~40 ms penalty.
        let l = axum::serve::ListenerExt::tap_io(l, |tcp| {
            let _ = tcp.set_nodelay(true);
        });
        tracing::info!(%addr, "listening");
        axum::serve(l, r).with_graceful_shutdown(stop()).await.unwrap();
    };
    tokio::join!(serve(listen, data), serve(admin_listen, ctl));
}
