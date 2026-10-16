//! Per-tenant admission: RPM + TPM token buckets with reserve-then-settle accounting,
//! a concurrency cap, and calendar-day / calendar-month spending budgets.
//!
//! Output size is unknown up front, so we reserve (input + max_tokens) and refund the
//! difference once real usage is known. Without this, TPM limits are bypassable by bursts.

use crate::{config::TenantCfg, sync::MutexExt};
use std::{
    sync::{atomic::AtomicU32, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

struct Bucket {
    level: f64,
    cap: f64,
    at: Instant,
}

impl Bucket {
    fn new(cap: f64) -> Self {
        Bucket { level: cap, cap, at: Instant::now() }
    }
    fn refill(&mut self) {
        let now = Instant::now();
        self.level = (self.level + now.duration_since(self.at).as_secs_f64() * self.cap / 60.0).min(self.cap);
        self.at = now;
    }
    fn wait(&self, need: f64) -> f64 {
        ((need - self.level) * 60.0 / self.cap).max(0.0)
    }
}

pub struct Limiter(Mutex<(Bucket, Bucket)>);

impl Limiter {
    pub fn new(rpm: f64, tpm: f64) -> Self {
        Limiter(Mutex::new((Bucket::new(rpm), Bucket::new(tpm))))
    }

    /// Ok(reserved tokens) or Err(seconds to wait).
    pub fn admit(&self, est: f64) -> Result<f64, f64> {
        let mut g = self.0.guard();
        let (r, t) = &mut *g;
        r.refill();
        t.refill();
        // The estimate derives from client input (`max_tokens`): a negative or NaN value must neither
        // inflate the bucket nor drain it on settlement. An oversized one must not block forever.
        let est = if est.is_nan() { 0.0 } else { est.clamp(0.0, t.cap) };
        let wait = r.wait(1.0).max(t.wait(est));
        if wait > 0.0 {
            return Err(wait);
        }
        r.level -= 1.0;
        t.level -= est;
        Ok(est)
    }

    pub fn settle(&self, reserved: f64, actual: f64) {
        let mut g = self.0.guard();
        let t = &mut g.1;
        t.level = (t.level + reserved - actual).min(t.cap);
    }
}

fn now_secs() -> u64 {
    // A clock set before 1970 yields day 0 instead of a panic on every request.
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Days since 1970-01-01 (UTC).
pub fn now_days() -> i64 {
    (now_secs() / 86_400) as i64
}

/// Calendar month number (year*12 + month0) of a day count; Hinnant's civil-from-days.
pub fn month_id(days: i64) -> i64 {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    y * 12 + (m - 1)
}

/// First day (as a day count) of the month containing `days`.
pub fn month_start(days: i64) -> i64 {
    let m = month_id(days);
    let mut d = days;
    while month_id(d - 1) == m {
        d -= 1;
    }
    d
}

struct Spend {
    day: i64,
    month: i64,
    day_usd: f64,
    month_usd: f64,
}

pub struct Tenant {
    pub cfg: TenantCfg,
    /// true if created through the admin API (and therefore revocable through it).
    pub dynamic: bool,
    pub limiter: Limiter,
    pub inflight: AtomicU32,
    spend: Mutex<Spend>,
}

impl Tenant {
    pub fn new(cfg: TenantCfg, dynamic: bool) -> Self {
        let d = now_days();
        Tenant {
            limiter: Limiter::new(cfg.rpm, cfg.tpm),
            inflight: AtomicU32::new(0),
            spend: Mutex::new(Spend { day: d, month: month_id(d), day_usd: 0.0, month_usd: 0.0 }),
            dynamic,
            cfg,
        }
    }

    pub fn name(&self) -> &str {
        &self.cfg.name
    }

    pub fn allows(&self, alias: &str) -> bool {
        self.cfg.models.is_empty() || self.cfg.models.iter().any(|m| m == alias)
    }

    fn roll(s: &mut Spend) {
        let d = now_days();
        if s.day != d {
            s.day = d;
            s.day_usd = 0.0;
        }
        if s.month != month_id(d) {
            s.month = month_id(d);
            s.month_usd = 0.0;
        }
    }

    /// Restore spend from the ledger after a restart.
    pub fn seed(&self, day_usd: f64, month_usd: f64) {
        let mut s = self.spend.guard();
        s.day_usd = day_usd;
        s.month_usd = month_usd;
    }

    pub fn add_spend(&self, usd: f64) {
        let mut s = self.spend.guard();
        Self::roll(&mut s);
        s.day_usd += usd;
        s.month_usd += usd;
    }

    pub fn spent(&self) -> (f64, f64) {
        let mut s = self.spend.guard();
        Self::roll(&mut s);
        (s.day_usd, s.month_usd)
    }

    /// Some(seconds until the budget resets) when a budget is exhausted.
    /// The cap is checked at admission, so requests already in flight can overshoot it slightly.
    pub fn over_budget(&self) -> Option<u64> {
        let (day, month) = self.spent();
        let into_day = now_secs() % 86_400;
        if self.cfg.daily_usd.is_some_and(|l| day >= l) {
            return Some(86_400 - into_day);
        }
        if self.cfg.monthly_usd.is_some_and(|l| month >= l) {
            let (d, m) = (now_days(), month_id(now_days()));
            let mut e = d;
            while month_id(e) == m {
                e += 1;
            }
            return Some((e - d) as u64 * 86_400 - into_day);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpm_blocks_then_tpm_settles() {
        let l = Limiter::new(2.0, 1000.0);
        let r = l.admit(600.0).unwrap();
        assert!(l.admit(600.0).is_err(), "second reservation exceeds tpm");
        l.settle(r, 100.0); // real usage was small: refund
        assert!(l.admit(600.0).is_ok());
        assert!(l.admit(1.0).is_err(), "rpm exhausted");
    }

    #[test]
    fn hostile_estimates_cannot_inflate_or_drain_the_bucket() {
        // `max_tokens` comes from the client: negative, NaN or infinite values must not corrupt the bucket.
        let l = Limiter::new(100.0, 1000.0);
        for est in [-1e9, f64::NAN, f64::NEG_INFINITY] {
            let r = l.admit(est).expect("admitted");
            assert!(r.is_finite() && r >= 0.0, "reservation must be sane for {est}, got {r}");
            l.settle(r, 0.0);
        }
        assert!(l.admit(1000.0).is_ok(), "the bucket must not have been drained");
        assert!(l.admit(1.0).is_err(), "and must not have been inflated beyond its capacity");
    }

    #[test]
    fn calendar_months() {
        assert_eq!(month_id(11_017) - month_id(11_016), 1); // 2000-03-01 vs 2000-02-29
        assert_eq!(month_start(11_017 + 14), 11_017);
        assert_eq!(month_id(0), 1970 * 12);
    }

    #[test]
    fn budget_trips_and_reports_retry() {
        let cfg = TenantCfg {
            name: "t".into(),
            key_hash: String::new(),
            rpm: 60.0,
            tpm: 1e5,
            max_concurrent: 4,
            daily_usd: Some(1.0),
            monthly_usd: None,
            models: vec![],
        };
        let t = Tenant::new(cfg, false);
        assert!(t.over_budget().is_none());
        t.add_spend(1.0);
        let wait = t.over_budget().expect("daily budget exhausted");
        assert!(wait > 0 && wait <= 86_400);
    }
}
