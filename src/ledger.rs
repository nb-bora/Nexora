//! Append-only usage ledger. Written off the hot path: a full queue drops rows
//! (and counts them) rather than ever blocking a request.

use rusqlite::{params, Connection};
use std::{
    collections::HashMap,
    sync::mpsc::{sync_channel, SyncSender},
};

pub struct Row {
    pub id: String,
    pub tenant: String,
    pub alias: String,
    pub provider: String,
    pub model: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cost_usd: f64,
    pub latency_ms: u64,
    pub status: u16,
    pub attempts: u32,
    pub estimated: bool,
}

const SCHEMA: &str = "
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS ledger(
  ts INTEGER NOT NULL DEFAULT (unixepoch()), id TEXT, tenant TEXT, alias TEXT, provider TEXT, model TEXT,
  prompt_tokens INTEGER, completion_tokens INTEGER, cost_usd REAL, latency_ms INTEGER,
  status INTEGER, attempts INTEGER, estimated INTEGER);
CREATE INDEX IF NOT EXISTS ledger_ts ON ledger(ts);
CREATE TRIGGER IF NOT EXISTS ledger_no_update BEFORE UPDATE ON ledger BEGIN SELECT RAISE(ABORT,'ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS ledger_no_delete BEFORE DELETE ON ledger BEGIN SELECT RAISE(ABORT,'ledger is append-only'); END;";

/// Spend per tenant since `since` (unix seconds): restores budgets after a restart.
/// Call after `spawn`, which creates the schema.
pub fn spent_since(path: &str, since: i64) -> HashMap<String, f64> {
    let db = Connection::open(path).expect("ledger");
    let mut st = db.prepare("SELECT tenant, SUM(cost_usd) FROM ledger WHERE ts >= ? GROUP BY tenant").unwrap();
    st.query_map([since], |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?))).unwrap().filter_map(Result::ok).collect()
}

pub fn spawn(path: &str) -> SyncSender<Row> {
    let mut db = Connection::open(path).unwrap_or_else(|e| panic!("ledger {path}: {e}"));
    db.execute_batch(SCHEMA).expect("ledger schema");
    let (tx, rx) = sync_channel::<Row>(10_000);
    std::thread::spawn(move || {
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            batch.extend(rx.try_iter().take(999));
            let res = (|| -> rusqlite::Result<()> {
                let t = db.transaction()?;
                {
                    let mut st = t.prepare_cached("INSERT INTO ledger(id,tenant,alias,provider,model,prompt_tokens,completion_tokens,cost_usd,latency_ms,status,attempts,estimated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")?;
                    for r in &batch {
                        st.execute(params![r.id, r.tenant, r.alias, r.provider, r.model, r.prompt_tokens, r.completion_tokens, r.cost_usd, r.latency_ms, r.status, r.attempts, r.estimated])?;
                    }
                }
                t.commit()
            })();
            if let Err(e) = res {
                tracing::error!(error = %e, rows = batch.len(), "ledger write failed");
                metrics::counter!("nexora_ledger_errors_total").increment(batch.len() as u64);
            }
        }
    });
    tx
}
