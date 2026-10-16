//! Tenant & API-key management, on the control plane only (separate port, loopback by default).
//! Disabled (404) unless NEXORA_ADMIN_TOKEN is set. Keys are shown once and stored hashed.
//! Revocation is immediate: the tenant leaves the in-memory map before the response returns.

use crate::{config::TenantCfg, gate::Tenant, App};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get},
    Json, Router,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS tenants(
  name TEXT PRIMARY KEY, key_hash TEXT UNIQUE NOT NULL, rpm REAL NOT NULL, tpm REAL NOT NULL,
  max_concurrent INTEGER NOT NULL, daily_usd REAL, monthly_usd REAL, models TEXT NOT NULL DEFAULT '')";

/// (API key shown once, sha256 hex to store).
pub fn new_key() -> (String, String) {
    let key = format!("nx_live_{}", hex::encode(rand::random::<[u8; 32]>()));
    let hash = hex::encode(Sha256::digest(key.as_bytes()));
    (key, hash)
}

pub fn load(db: &Connection) -> Vec<TenantCfg> {
    let mut st = db.prepare("SELECT name,key_hash,rpm,tpm,max_concurrent,daily_usd,monthly_usd,models FROM tenants").unwrap();
    st.query_map([], |r| {
        Ok(TenantCfg {
            name: r.get(0)?,
            key_hash: r.get(1)?,
            rpm: r.get(2)?,
            tpm: r.get(3)?,
            max_concurrent: r.get(4)?,
            daily_usd: r.get(5)?,
            monthly_usd: r.get(6)?,
            models: r.get::<_, String>(7)?.split(',').filter(|s| !s.is_empty()).map(str::to_owned).collect(),
        })
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

pub fn routes(app: Arc<App>) -> Router {
    Router::new().route("/admin/tenants", get(list).post(create)).route("/admin/tenants/{name}", delete(revoke)).with_state(app)
}

fn err(status: u16, msg: &str) -> Response {
    (StatusCode::from_u16(status).unwrap(), Json(json!({"error": {"type": "admin_error", "message": msg}}))).into_response()
}

/// Err((status, message)) is small on purpose: a whole `Response` as the error type is 128+ bytes.
fn guard(app: &App, h: &HeaderMap) -> Result<(), (u16, &'static str)> {
    let Some(token) = &app.cfg.admin_token else { return Err((404, "not found")) };
    let given = h.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("");
    let (a, b) = (Sha256::digest(token.as_bytes()), Sha256::digest(given.as_bytes()));
    // Constant-time comparison of fixed-size digests.
    if a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0 {
        Ok(())
    } else {
        Err((401, "invalid admin token"))
    }
}

async fn create(State(app): State<Arc<App>>, h: HeaderMap, raw: Bytes) -> Response {
    if let Err((s, m)) = guard(&app, &h) {
        return err(s, m);
    }
    let Ok(v) = serde_json::from_slice::<Value>(&raw) else { return err(400, "body must be JSON") };
    let name = v["name"].as_str().unwrap_or("").to_owned();
    if !(1..=32).contains(&name.len()) || !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_') {
        return err(400, "name must match [a-z0-9_]{1,32}");
    }
    let models: Vec<String> =
        v["models"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_owned)).collect()).unwrap_or_default();
    let (key, hash) = new_key();
    let cfg = TenantCfg {
        name: name.clone(),
        key_hash: hash.clone(),
        rpm: v["rpm"].as_f64().unwrap_or(60.0),
        tpm: v["tpm"].as_f64().unwrap_or(100_000.0),
        max_concurrent: v["max_concurrent"].as_u64().unwrap_or(64) as u32,
        daily_usd: v["daily_budget_usd"].as_f64(),
        monthly_usd: v["monthly_budget_usd"].as_f64(),
        models,
    };
    if let Err(e) = app.cfg.check_tenant(&cfg) {
        return err(400, &e);
    }
    if app.tenants.read().unwrap().values().any(|t| t.name() == name) {
        return err(409, "tenant already exists");
    }
    let inserted = app.db.lock().unwrap().execute(
        "INSERT INTO tenants VALUES(?,?,?,?,?,?,?,?)",
        params![cfg.name, cfg.key_hash, cfg.rpm, cfg.tpm, cfg.max_concurrent, cfg.daily_usd, cfg.monthly_usd, cfg.models.join(",")],
    );
    if let Err(e) = inserted {
        return err(500, &e.to_string());
    }
    app.tenants.write().unwrap().insert(hash, Arc::new(Tenant::new(cfg, true)));
    (StatusCode::CREATED, Json(json!({"name": name, "api_key": key}))).into_response()
}

async fn list(State(app): State<Arc<App>>, h: HeaderMap) -> Response {
    if let Err((s, m)) = guard(&app, &h) {
        return err(s, m);
    }
    let data: Vec<Value> = app
        .tenants
        .read()
        .unwrap()
        .values()
        .map(|t| {
            let (day, month) = t.spent();
            json!({
                "name": t.name(), "source": if t.dynamic { "api" } else { "env" },
                "rpm": t.cfg.rpm, "tpm": t.cfg.tpm, "max_concurrent": t.cfg.max_concurrent,
                "daily_budget_usd": t.cfg.daily_usd, "monthly_budget_usd": t.cfg.monthly_usd,
                "spent_today_usd": day, "spent_month_usd": month, "models": t.cfg.models,
            })
        })
        .collect();
    Json(json!({"data": data})).into_response()
}

async fn revoke(State(app): State<Arc<App>>, h: HeaderMap, Path(name): Path<String>) -> Response {
    if let Err((s, m)) = guard(&app, &h) {
        return err(s, m);
    }
    let mut tenants = app.tenants.write().unwrap();
    let Some((hash, t)) = tenants.iter().find(|(_, t)| t.name() == name).map(|(k, t)| (k.clone(), t.clone())) else {
        return err(404, "no such tenant");
    };
    if !t.dynamic {
        return err(409, "tenant is defined in the environment: remove it there and restart");
    }
    if let Err(e) = app.db.lock().unwrap().execute("DELETE FROM tenants WHERE name=?", params![name]) {
        return err(500, &e.to_string());
    }
    tenants.remove(&hash); // in-flight streams keep their Arc and finish; every new request gets 401
    StatusCode::NO_CONTENT.into_response()
}
