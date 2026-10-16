//! Provider selection: ordered fallback + circuit breaker.
//! Fallback only happens BEFORE the first byte reaches the client; once a stream has
//! started, switching providers would silently corrupt the answer.

use crate::config::{Config, Target};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed},
    time::{Duration, Instant},
};

const TRIP_AFTER: u32 = 5; // consecutive failures
const COOLDOWN_MS: u64 = 30_000;

struct Rt {
    url: String,
    key: Option<String>,
    timeout: Duration,
    fails: AtomicU32,
    open_until: AtomicU64,
}

pub struct Upstream {
    http: Client,
    rt: HashMap<String, Rt>,
    t0: Instant,
}

impl Upstream {
    pub fn new(cfg: &Config) -> Self {
        let rt = cfg
            .providers
            .iter()
            .map(|(name, p)| {
                let rt = Rt {
                    url: p.base_url.trim_end_matches('/').to_owned(),
                    key: p.api_key.clone(),
                    timeout: Duration::from_millis(p.timeout_ms),
                    fails: AtomicU32::new(0),
                    open_until: AtomicU64::new(0),
                };
                (name.clone(), rt)
            })
            .collect();
        let http = Client::builder().connect_timeout(Duration::from_secs(3)).build().unwrap();
        Upstream { http, rt, t0: Instant::now() }
    }

    /// Returns (response, index of the target used, attempts) or Err(attempts) if all failed.
    pub async fn forward(&self, path: &str, targets: &[Target], body: &mut Value) -> Result<(Response, usize, u32), u32> {
        if body["stream"].as_bool() == Some(true) {
            // Ask for the final usage chunk so streamed requests are billed exactly.
            if let Some(o) = body.as_object_mut() {
                let so = o.entry("stream_options").or_insert_with(|| json!({}));
                if let Some(so) = so.as_object_mut() {
                    so.insert("include_usage".into(), true.into());
                }
            }
        }
        let mut attempts = 0;
        for (i, t) in targets.iter().enumerate() {
            let rt = &self.rt[&t.provider];
            let now = self.t0.elapsed().as_millis() as u64;
            // Open circuit: skip. After the cooldown exactly ONE request wins the CAS and probes;
            // the others keep skipping until the probe succeeds (reset below) or fails (re-opens).
            let open = rt.open_until.load(Relaxed);
            if open > now || (open != 0 && rt.open_until.compare_exchange(open, now + COOLDOWN_MS, Relaxed, Relaxed).is_err()) {
                continue;
            }
            attempts += 1;
            body["model"] = t.model.clone().into();
            let mut rq = self.http.post(format!("{}{}", rt.url, path)).json(&*body);
            if let Some(k) = &rt.key {
                rq = rq.bearer_auth(k);
            }
            // 401/403 from a provider means OUR credentials are broken: fall back, never blame the client.
            let reason = match tokio::time::timeout(rt.timeout, rq.send()).await {
                Ok(Ok(r)) if !matches!(r.status().as_u16(), 401 | 403 | 429 | 500..=599) => {
                    rt.fails.store(0, Relaxed);
                    rt.open_until.store(0, Relaxed);
                    return Ok((r, i, attempts));
                }
                Ok(Ok(r)) => {
                    if matches!(r.status().as_u16(), 401 | 403) {
                        tracing::error!(provider = %t.provider, status = r.status().as_u16(), "provider rejected OUR credentials: check its API key");
                    }
                    format!("http_{}", r.status().as_u16())
                }
                Ok(Err(_)) => "network".to_string(),
                Err(_) => "timeout".to_string(),
            };
            if rt.fails.fetch_add(1, Relaxed) + 1 >= TRIP_AFTER {
                rt.open_until.store(now + COOLDOWN_MS, Relaxed);
            }
            metrics::counter!("nexora_upstream_failures_total", "provider" => t.provider.clone(), "reason" => reason).increment(1);
        }
        Err(attempts)
    }
}
