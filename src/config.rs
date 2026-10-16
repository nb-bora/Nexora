//! Configuration comes from the environment (a `.env` file is loaded first; real env vars win).
//! Names are written UPPERCASE in the environment and used lowercase inside the gateway.

use std::{collections::HashMap, env, str::FromStr};

pub struct Config {
    pub listen: String,
    /// Control plane (metrics, health, admin API): separate port, loopback by default.
    pub admin_listen: String,
    /// Admin API is disabled (404) unless this is set.
    pub admin_token: Option<String>,
    pub max_body_bytes: usize,
    /// SQLite file holding the usage ledger and API-managed tenants.
    pub ledger_path: String,
    pub providers: HashMap<String, Provider>,
    /// alias -> ordered targets (first healthy wins, the rest are fallbacks)
    pub aliases: HashMap<String, Vec<Target>>,
    pub tenants: Vec<TenantCfg>,
    /// "provider/model" -> USD per 1M tokens
    pub prices: HashMap<String, Price>,
}

pub struct Provider {
    /// Includes the version prefix, e.g. https://api.openai.com/v1
    pub base_url: String,
    pub api_key: Option<String>,
    pub timeout_ms: u64,
}

pub struct Target {
    pub provider: String,
    pub model: String,
}

#[derive(Clone)]
pub struct TenantCfg {
    pub name: String,
    /// sha256 hex of the API key (see `nexora keygen`); the key itself is never stored.
    pub key_hash: String,
    pub rpm: f64,
    pub tpm: f64,
    pub max_concurrent: u32,
    pub daily_usd: Option<f64>,
    pub monthly_usd: Option<f64>,
    /// Allowed aliases; empty = all.
    pub models: Vec<String>,
}

pub struct Price {
    pub input: f64,
    pub output: f64,
}

fn var(k: &str) -> Option<String> {
    env::var(k).ok().map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())
}

/// Parse an optional numeric variable; a malformed value stops the process instead of silently using a default.
fn opt<T: FromStr>(k: &str) -> Option<T> {
    var(k).map(|v| v.parse().unwrap_or_else(|_| panic!("{k}: invalid value `{v}`")))
}

fn num<T: FromStr>(k: &str, default: T) -> T {
    opt(k).unwrap_or(default)
}

/// Names X for which `NEXORA_<prefix>X<suffix>` is set.
fn names(prefix: &str, suffix: &str) -> Vec<String> {
    let p = format!("NEXORA_{prefix}");
    env::vars().filter_map(|(k, _)| k.strip_prefix(&p)?.strip_suffix(suffix).map(str::to_lowercase)).collect()
}

fn list(s: &str) -> Vec<String> {
    s.split(',').map(str::trim).filter(|x| !x.is_empty()).map(str::to_owned).collect()
}

/// "provider/model,provider/model" (the model may itself contain `/`).
fn parse_targets(alias: &str, s: &str) -> Vec<Target> {
    list(s)
        .iter()
        .map(|t| {
            let (p, m) = t.split_once('/').unwrap_or_else(|| panic!("alias {alias}: `{t}` must be provider/model"));
            Target { provider: p.to_lowercase(), model: m.to_owned() }
        })
        .collect()
}

/// "provider/model=in:out,..." in USD per 1M tokens. Keys are normalised like alias targets.
fn parse_prices(s: &str) -> HashMap<String, Price> {
    list(s)
        .iter()
        .map(|e| {
            let bad = |_: ()| -> ! { panic!("NEXORA_PRICES: `{e}` must be provider/model=input:output") };
            let (k, v) = e.split_once('=').unwrap_or_else(|| bad(()));
            let (p, m) = k.split_once('/').unwrap_or_else(|| bad(()));
            let (i, o) = v.split_once(':').unwrap_or_else(|| bad(()));
            let f = |x: &str| x.trim().parse::<f64>().unwrap_or_else(|_| bad(()));
            (format!("{}/{}", p.trim().to_lowercase(), m.trim()), Price { input: f(i), output: f(o) })
        })
        .collect()
}

impl Config {
    /// First target of the selected aliases (all aliases if `models` is empty) that has no price.
    /// A budget is only meaningful if every model it can spend on is priced.
    pub fn unpriced(&self, models: &[String]) -> Option<String> {
        let sel: Vec<&String> = if models.is_empty() { self.aliases.keys().collect() } else { models.iter().collect() };
        sel.iter()
            .filter_map(|a| self.aliases.get(*a))
            .flatten()
            .map(|t| format!("{}/{}", t.provider, t.model))
            .find(|k| !self.prices.contains_key(k))
    }

    /// Fail fast: a bad config must stop the process, never degrade silently.
    pub fn from_env() -> Config {
        let providers: HashMap<_, _> = names("PROVIDER_", "_URL")
            .into_iter()
            .map(|n| {
                let u = n.to_uppercase();
                let p = Provider {
                    base_url: var(&format!("NEXORA_PROVIDER_{u}_URL")).unwrap(),
                    api_key: var(&format!("NEXORA_PROVIDER_{u}_KEY")),
                    timeout_ms: num(&format!("NEXORA_PROVIDER_{u}_TIMEOUT_MS"), 60_000),
                };
                (n, p)
            })
            .collect();
        let aliases: HashMap<_, _> = names("ALIAS_", "")
            .into_iter()
            .map(|n| {
                let v = var(&format!("NEXORA_ALIAS_{}", n.to_uppercase())).unwrap_or_default();
                let t = parse_targets(&n, &v);
                (n, t)
            })
            .collect();
        let tenants: Vec<_> = names("TENANT_", "_KEY_HASH")
            .into_iter()
            .map(|n| {
                let u = n.to_uppercase();
                TenantCfg {
                    key_hash: var(&format!("NEXORA_TENANT_{u}_KEY_HASH")).unwrap().to_lowercase(),
                    rpm: num(&format!("NEXORA_TENANT_{u}_RPM"), 60.0),
                    tpm: num(&format!("NEXORA_TENANT_{u}_TPM"), 100_000.0),
                    max_concurrent: num(&format!("NEXORA_TENANT_{u}_MAX_CONCURRENT"), 64),
                    daily_usd: opt(&format!("NEXORA_TENANT_{u}_DAILY_BUDGET_USD")),
                    monthly_usd: opt(&format!("NEXORA_TENANT_{u}_MONTHLY_BUDGET_USD")),
                    models: list(&var(&format!("NEXORA_TENANT_{u}_MODELS")).unwrap_or_default()),
                    name: n,
                }
            })
            .collect();

        assert!(!providers.is_empty(), "no provider configured (NEXORA_PROVIDER_<NAME>_URL)");
        for (alias, targets) in &aliases {
            assert!(!targets.is_empty(), "alias {alias} has no target");
            for t in targets {
                assert!(providers.contains_key(&t.provider), "alias {alias}: unknown provider {}", t.provider);
            }
        }
        let cfg = Config {
            listen: var("NEXORA_LISTEN").unwrap_or_else(|| "0.0.0.0:8080".into()),
            admin_listen: var("NEXORA_ADMIN_LISTEN").unwrap_or_else(|| "127.0.0.1:9090".into()),
            admin_token: var("NEXORA_ADMIN_TOKEN"),
            max_body_bytes: num("NEXORA_MAX_BODY_BYTES", 1 << 20),
            ledger_path: var("NEXORA_LEDGER_PATH").unwrap_or_else(|| "nexora-ledger.db".into()),
            prices: parse_prices(&var("NEXORA_PRICES").unwrap_or_default()),
            providers,
            aliases,
            tenants,
        };
        // Tenants may also come from the admin API, so "no tenant" is only an error if the API is off.
        assert!(
            !cfg.tenants.is_empty() || cfg.admin_token.is_some(),
            "no tenant configured (NEXORA_TENANT_<NAME>_KEY_HASH) and admin API disabled (NEXORA_ADMIN_TOKEN)"
        );
        for t in &cfg.tenants {
            cfg.check_tenant(t).unwrap_or_else(|e| panic!("{e}"));
        }
        cfg
    }

    /// Invariants shared by env tenants, API-created tenants and API tenants reloaded at startup.
    pub fn check_tenant(&self, t: &TenantCfg) -> Result<(), String> {
        let n = &t.name;
        if t.key_hash.len() != 64 || !t.key_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("tenant {n}: key_hash must be sha256 hex"));
        }
        if !(t.max_concurrent > 0 && t.rpm > 0.0 && t.tpm > 0.0) {
            return Err(format!("tenant {n}: rpm, tpm and max_concurrent must be > 0"));
        }
        if let Some(m) = t.models.iter().find(|m| !self.aliases.contains_key(*m)) {
            return Err(format!("tenant {n}: unknown alias {m}"));
        }
        if t.daily_usd.is_some() || t.monthly_usd.is_some() {
            if let Some(k) = self.unpriced(&t.models) {
                return Err(format!("tenant {n}: a budget is set but {k} has no price in NEXORA_PRICES (it would never be enforced)"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fallback_chain_and_slashes_in_model() {
        let t = parse_targets("smart", "OpenAI/gpt-x, local/meta/llama-3");
        assert_eq!((t[0].provider.as_str(), t[0].model.as_str()), ("openai", "gpt-x"));
        assert_eq!((t[1].provider.as_str(), t[1].model.as_str()), ("local", "meta/llama-3"));
    }

    #[test]
    fn parses_prices_with_normalised_provider() {
        let p = parse_prices("OpenAI/gpt-x=0.15:0.6, local/m=0:0");
        assert_eq!((p["openai/gpt-x"].input, p["openai/gpt-x"].output), (0.15, 0.6));
        assert_eq!(p["local/m"].output, 0.0);
    }
}
