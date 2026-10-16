//! End-to-end tests: the real binary, a mock OpenAI-compatible upstream (axum), a real HTTP client.
//! Each test starts its own gateway on free ports with its own SQLite file.

use axum::{body::Bytes, extract::State, http::{header, HeaderMap, StatusCode}, response::IntoResponse, routing::post, Router};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

const KEY: &str = "nx_live_test_key";
type Seen = Arc<Mutex<Vec<(String, Value)>>>; // (authorization header, request body) as received upstream

struct Mock {
    port: u16,
    seen: Seen,
}

/// /broken/* always 500. /good/* answers JSON (usage 5/3) or SSE (usage 11/7); model "slow" sleeps 400 ms.
async fn mock() -> Mock {
    async fn broken() -> impl IntoResponse {
        (StatusCode::INTERNAL_SERVER_ERROR, "boom")
    }
    async fn good(State(seen): State<Seen>, h: HeaderMap, b: Bytes) -> axum::response::Response {
        let v: Value = serde_json::from_slice(&b).unwrap();
        let auth = h.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or("").to_owned();
        seen.lock().unwrap().push((auth, v.clone()));
        if v["model"] == "slow" {
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        if v["stream"] == true {
            let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\n\
                       data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7}}\n\n\
                       data: [DONE]\n\n";
            return ([(header::CONTENT_TYPE, "text/event-stream")], sse).into_response();
        }
        axum::Json(json!({"choices": [{"message": {"content": "hi"}}], "usage": {"prompt_tokens": 5, "completion_tokens": 3}})).into_response()
    }
    let seen: Seen = Default::default();
    let app = Router::new().route("/broken/chat/completions", post(broken)).route("/good/chat/completions", post(good)).with_state(seen.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    Mock { port, seen }
}

struct Gw {
    child: Child,
    port: u16,
    admin: u16,
    db: std::path::PathBuf,
    http: reqwest::Client,
}

impl Drop for Gw {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn command(mock: &Mock, port: u16, admin: u16, db: &std::path::Path, extra: &[(&str, &str)]) -> Command {
    let dir = db.parent().unwrap();
    let mut c = Command::new(env!("CARGO_BIN_EXE_nexora"));
    c.current_dir(dir).stdout(Stdio::null()).stderr(Stdio::piped());
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("NEXORA_")) {
        c.env_remove(k);
    }
    let hash = hex::encode(Sha256::digest(KEY.as_bytes()));
    let m = mock.port;
    let base = [
        ("NEXORA_LISTEN", format!("127.0.0.1:{port}")),
        ("NEXORA_ADMIN_LISTEN", format!("127.0.0.1:{admin}")),
        ("NEXORA_LEDGER_PATH", db.display().to_string()),
        ("NEXORA_ADMIN_TOKEN", "admintok".into()),
        ("NEXORA_PROVIDER_BROKEN_URL", format!("http://127.0.0.1:{m}/broken")),
        ("NEXORA_PROVIDER_GOOD_URL", format!("http://127.0.0.1:{m}/good")),
        ("NEXORA_PROVIDER_GOOD_KEY", "sk-test".into()),
        ("NEXORA_ALIAS_SMART", "broken/m1, good/m2".into()),
        ("NEXORA_ALIAS_DEAD", "broken/m1".into()),
        ("NEXORA_ALIAS_SLOW", "good/slow".into()),
        ("NEXORA_PRICES", "good/m2=1:2, good/slow=1:2, broken/m1=0:0".into()),
        ("NEXORA_TENANT_T1_KEY_HASH", hash),
    ];
    for (k, v) in base {
        c.env(k, v);
    }
    for (k, v) in extra {
        c.env(k, v); // later wins: tests override defaults here
    }
    c
}

async fn start(mock: &Mock, extra: &[(&str, &str)]) -> Gw {
    let (port, admin) = (free_port(), free_port());
    let dir = std::env::temp_dir().join(format!("nexora-e2e-{port}"));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("ledger.db");
    let child = command(mock, port, admin, &db, extra).spawn().unwrap();
    let gw = Gw { child, port, admin, db, http: reqwest::Client::new() };
    for _ in 0..100 {
        if gw.http.get(format!("http://127.0.0.1:{admin}/healthz")).send().await.is_ok() {
            return gw;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("gateway did not become ready");
}

impl Gw {
    async fn chat(&self, key: &str, body: Value) -> reqwest::Response {
        self.http.post(format!("http://127.0.0.1:{}/v1/chat/completions", self.port)).bearer_auth(key).json(&body).send().await.unwrap()
    }
    /// Status + fully read body. Always read bodies: an abandoned response looks like a client
    /// cancellation to the gateway (no usage, concurrency slot held until the stream is dropped).
    async fn call(&self, key: &str, body: Value) -> (u16, String) {
        let r = self.chat(key, body).await;
        (r.status().as_u16(), r.text().await.unwrap())
    }
    fn admin(&self, m: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http.request(m, format!("http://127.0.0.1:{}{path}", self.admin)).bearer_auth("admintok")
    }
    /// The ledger is written asynchronously: poll until `n` rows exist.
    async fn ledger(&self, n: usize) -> Vec<(String, u64, u64, f64, u16, u32)> {
        for _ in 0..50 {
            let db = rusqlite::Connection::open(&self.db).unwrap();
            let rows: Vec<_> = db
                .prepare("SELECT alias,prompt_tokens,completion_tokens,cost_usd,status,attempts FROM ledger ORDER BY rowid")
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            if rows.len() >= n {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("ledger never reached {n} rows");
    }
}

fn msg() -> Value {
    json!({"model": "smart", "messages": []})
}

#[tokio::test]
async fn auth_and_model_visibility() {
    let m = mock().await;
    let gw = start(&m, &[("NEXORA_TENANT_T1_MODELS", "smart")]).await;
    assert_eq!(gw.chat("wrong", msg()).await.status(), 401);
    assert_eq!(gw.chat(KEY, json!({"model": "dead", "messages": []})).await.status(), 404, "not allowed looks like unknown");
    let models: Value = gw.http.get(format!("http://127.0.0.1:{}/v1/models", gw.port)).bearer_auth(KEY).send().await.unwrap().json().await.unwrap();
    assert_eq!(models["data"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn fallback_streaming_and_exact_accounting() {
    let m = mock().await;
    let gw = start(&m, &[]).await;

    let r = gw.chat(KEY, msg()).await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-nexora-provider"], "good");
    assert_eq!(r.headers()["x-nexora-attempts"], "2", "broken provider tried first, then fallback");
    r.bytes().await.unwrap();

    let (status, body) = gw.call(KEY, json!({"model": "smart", "stream": true, "messages": []})).await;
    assert!(status == 200 && body.contains("[DONE]"));

    let seen = m.seen.lock().unwrap().clone();
    assert_eq!(seen[0].0, "Bearer sk-test", "provider key from env, never the client's key");
    assert_eq!(seen[0].1["model"], "m2", "alias rewritten to the real model");
    assert_eq!(seen[1].1["stream_options"]["include_usage"], true, "usage requested so streams are billed exactly");

    let rows = gw.ledger(2).await;
    assert_eq!((rows[0].1, rows[0].2), (5, 3));
    assert_eq!((rows[1].1, rows[1].2), (11, 7));
    assert!((rows[0].3 - 11e-6).abs() < 1e-12 && (rows[1].3 - 25e-6).abs() < 1e-12, "cost = tokens x price: {rows:?}");
}

#[tokio::test]
async fn all_upstreams_down_is_503_with_retry_after() {
    let m = mock().await;
    let gw = start(&m, &[]).await;
    let r = gw.chat(KEY, json!({"model": "dead", "messages": []})).await;
    assert_eq!(r.status(), 503);
    assert!(r.headers().contains_key("retry-after"));
    assert_eq!(r.json::<Value>().await.unwrap()["error"]["type"], "upstream_unavailable");
}

#[tokio::test]
async fn rate_limit_returns_429_with_retry_after() {
    let m = mock().await;
    let gw = start(&m, &[("NEXORA_TENANT_T1_RPM", "2")]).await;
    assert_eq!(gw.call(KEY, msg()).await.0, 200);
    assert_eq!(gw.call(KEY, msg()).await.0, 200);
    let r = gw.chat(KEY, msg()).await;
    assert_eq!(r.status(), 429);
    assert!(r.headers().contains_key("retry-after"));
}

#[tokio::test]
async fn budget_blocks_once_exhausted() {
    let m = mock().await;
    // First call costs 11 micro-USD, second 11: the 20 micro-USD daily budget is gone after two.
    let gw = start(&m, &[("NEXORA_TENANT_T1_DAILY_BUDGET_USD", "0.00002")]).await;
    for _ in 0..2 {
        assert_eq!(gw.call(KEY, msg()).await.0, 200);
        tokio::time::sleep(Duration::from_millis(150)).await; // spend is booked when the response stream is dropped
    }
    let r = gw.chat(KEY, msg()).await;
    assert_eq!(r.status(), 429);
    assert_eq!(r.json::<Value>().await.unwrap()["error"]["type"], "budget_exceeded");
}

#[tokio::test]
async fn budget_survives_restart() {
    let m = mock().await;
    let gw = start(&m, &[("NEXORA_TENANT_T1_DAILY_BUDGET_USD", "0.00001")]).await;
    assert_eq!(gw.call(KEY, msg()).await.0, 200); // 11 micro-USD > 10: exhausted
    gw.ledger(1).await;
    let (port, admin, db) = (free_port(), free_port(), gw.db.clone());
    drop(gw);
    let child = command(&m, port, admin, &db, &[("NEXORA_TENANT_T1_DAILY_BUDGET_USD", "0.00001")]).spawn().unwrap();
    let gw2 = Gw { child, port, admin, db, http: reqwest::Client::new() };
    for _ in 0..100 {
        if gw2.http.get(format!("http://127.0.0.1:{admin}/healthz")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(gw2.chat(KEY, msg()).await.status(), 429, "spend restored from the ledger");
}

#[tokio::test]
async fn budget_requires_priced_models() {
    let m = mock().await;
    let (port, admin) = (free_port(), free_port());
    let dir = std::env::temp_dir().join(format!("nexora-e2e-{port}"));
    std::fs::create_dir_all(&dir).unwrap();
    let out = command(&m, port, admin, &dir.join("l.db"), &[("NEXORA_TENANT_T1_DAILY_BUDGET_USD", "1"), ("NEXORA_PRICES", "good/m2=1:2")]).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("has no price"));
}

#[tokio::test]
async fn concurrency_cap() {
    let m = mock().await;
    let gw = start(&m, &[("NEXORA_TENANT_T1_MAX_CONCURRENT", "1")]).await;
    let slow = json!({"model": "slow", "messages": []});
    let (a, b) = tokio::join!(gw.call(KEY, slow.clone()), gw.call(KEY, slow.clone()));
    let mut s = [a.0, b.0];
    s.sort();
    assert_eq!(s, [200, 429]);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(gw.call(KEY, slow).await.0, 200, "slot released afterwards");
}

#[tokio::test]
async fn admin_api_key_lifecycle() {
    let m = mock().await;
    let gw = start(&m, &[]).await;
    assert_eq!(gw.http.get(format!("http://127.0.0.1:{}/admin/tenants", gw.admin)).send().await.unwrap().status(), 401);

    let r = gw.admin(reqwest::Method::POST, "/admin/tenants").json(&json!({"name": "acme", "rpm": 100, "daily_budget_usd": 5.0, "models": ["smart"]})).send().await.unwrap();
    assert_eq!(r.status(), 201);
    let key = r.json::<Value>().await.unwrap()["api_key"].as_str().unwrap().to_owned();
    assert_eq!(gw.chat(&key, msg()).await.status(), 200, "new key works immediately, no restart");

    let dup = gw.admin(reqwest::Method::POST, "/admin/tenants").json(&json!({"name": "acme"})).send().await.unwrap();
    assert_eq!(dup.status(), 409);
    let bad = gw.admin(reqwest::Method::POST, "/admin/tenants").json(&json!({"name": "x", "models": ["nope"]})).send().await.unwrap();
    assert_eq!(bad.status(), 400);

    let list = gw.admin(reqwest::Method::GET, "/admin/tenants").send().await.unwrap().text().await.unwrap();
    assert!(list.contains("acme") && !list.contains(&key) && !list.contains("key_hash"), "secrets never listed");

    assert_eq!(gw.admin(reqwest::Method::DELETE, "/admin/tenants/t1").send().await.unwrap().status(), 409, "env tenants are not revocable via the API");
    assert_eq!(gw.admin(reqwest::Method::DELETE, "/admin/tenants/acme").send().await.unwrap().status(), 204);
    assert_eq!(gw.chat(&key, msg()).await.status(), 401, "revocation is immediate");
}

#[tokio::test]
async fn api_tenants_persist_across_restart() {
    let m = mock().await;
    let gw = start(&m, &[]).await;
    let r = gw.admin(reqwest::Method::POST, "/admin/tenants").json(&json!({"name": "acme"})).send().await.unwrap();
    let key = r.json::<Value>().await.unwrap()["api_key"].as_str().unwrap().to_owned();
    let (port, admin, db) = (free_port(), free_port(), gw.db.clone());
    drop(gw);
    let child = command(&m, port, admin, &db, &[]).spawn().unwrap();
    let gw2 = Gw { child, port, admin, db, http: reqwest::Client::new() };
    for _ in 0..100 {
        if gw2.http.get(format!("http://127.0.0.1:{admin}/healthz")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(gw2.chat(&key, msg()).await.status(), 200);
}

#[tokio::test]
async fn admin_api_disabled_without_token() {
    let m = mock().await;
    let gw = start(&m, &[("NEXORA_ADMIN_TOKEN", "")]).await;
    let r = gw.http.get(format!("http://127.0.0.1:{}/admin/tenants", gw.admin)).bearer_auth("admintok").send().await.unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn invalid_config_stops_startup() {
    let m = mock().await;
    let (port, admin) = (free_port(), free_port());
    let dir = std::env::temp_dir().join(format!("nexora-e2e-{port}"));
    std::fs::create_dir_all(&dir).unwrap();
    let out = command(&m, port, admin, &dir.join("l.db"), &[("NEXORA_TENANT_T1_KEY_HASH", "zzz")]).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("key_hash must be sha256 hex"));
}
