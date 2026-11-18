//! The browser's own settings, from the environment: which event store it reads, the bounds of
//! ADR 0025 (mapped onto the library's [`BrowseSettings`]), the lag metric's period, the health
//! listener and the OTLP endpoint. The hub connection is the library's [`snodra_messaging::Config`]
//! (`SNODRA_SERVER_ADDRESS`, credentials, TLS), the event store's connection its add-on's settings
//! ([`crate::store`]). Every value is checked at startup; a mistake names the variable.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use snodra_messaging::event_store::browse::BrowseSettings;

use crate::store::StoreSettings;

/// The service name the browser registers with (the server's ACL entry names it).
pub const SERVICE: &str = "snodra-event-browser";

/// The largest payload a record answer carries (`SNODRA_EVENT_BROWSER_MAX_PAYLOAD_BYTES` at
/// most): JSON-escaped or base64, it still fits the server's answer bound (2 MiB).
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
/// The longest source name (the server asks for none longer).
pub const MAX_SOURCE: usize = 1024;
/// The lag metric's period by default (`SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS`).
pub const DEFAULT_LAG_INTERVAL: Duration = Duration::from_secs(30);
/// The lag metric's shortest period (each refresh lists every subscription of every source).
pub const MIN_LAG_INTERVAL: Duration = Duration::from_secs(1);
/// The prefix of the browser's own variables.
const PREFIX: &str = "SNODRA_EVENT_BROWSER_";
/// The browser's own variables besides the bounds.
const OTHERS: [&str; 2] = [
    "SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS",
    "SNODRA_EVENT_BROWSER_HEALTH_LISTEN",
];
/// Where the gRPC health service listens by default (`SNODRA_EVENT_BROWSER_HEALTH_LISTEN`).
pub const DEFAULT_HEALTH_LISTEN: &str = "0.0.0.0:7010";

/// The bounds' variables and the [`BrowseSettings`] field each sets (named in the library's
/// validation messages).
const BOUNDS: [(&str, &str); 9] = [
    ("sources", "SNODRA_EVENT_BROWSER_SOURCES"),
    ("max_scanned", "SNODRA_EVENT_BROWSER_MAX_SCANNED"),
    ("max_results", "SNODRA_EVENT_BROWSER_MAX_RESULTS"),
    ("timeout", "SNODRA_EVENT_BROWSER_TIMEOUT_MS"),
    ("max_searches", "SNODRA_EVENT_BROWSER_MAX_SEARCHES"),
    ("max_tails", "SNODRA_EVENT_BROWSER_MAX_TAILS"),
    ("tail_duration", "SNODRA_EVENT_BROWSER_TAIL_DURATION_MS"),
    ("tail_buffer", "SNODRA_EVENT_BROWSER_TAIL_BUFFER"),
    ("max_payload", "SNODRA_EVENT_BROWSER_MAX_PAYLOAD_BYTES"),
];

/// Everything the browser reads from the environment besides the hub connection.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// The event store read and its connection (`SNODRA_EVENT_STORE` and the add-on's settings).
    pub store: StoreSettings,
    /// The sources and bounds of every call.
    pub browse: BrowseSettings,
    /// The lag metric's period; `None` when off (`SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS=0`).
    /// The metric also needs an OTLP endpoint ([`Settings::lag_metric`]).
    pub lag_interval: Option<Duration>,
    /// The gRPC health service's address (`SNODRA_EVENT_BROWSER_HEALTH_LISTEN`: unset or blank
    /// takes the default, `off` gives `None`).
    pub health_listen: Option<SocketAddr>,
    /// Where metrics are pushed (OTLP gRPC, `SNODRA_OTLP_ENDPOINT`); `None`: not exported.
    pub otlp_endpoint: Option<String>,
    /// Variables named `SNODRA_EVENT_BROWSER_*` that the browser does not know (a typo, a
    /// removed setting): each is a startup warning, by name only.
    pub unknown: Vec<String>,
}

impl Settings {
    /// The settings of this process's environment (variables that are not UTF-8 are ignored).
    pub fn from_env() -> Result<Settings, String> {
        Self::from_vars(
            &std::env::vars_os()
                .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
                .collect(),
        )
    }

    /// The settings of the variables `vars` (an unset or blank variable takes its default).
    pub fn from_vars(vars: &BTreeMap<String, String>) -> Result<Settings, String> {
        let get = |name: &str| {
            vars.get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let defaults = BrowseSettings::default();
        let sources = match get("SNODRA_EVENT_BROWSER_SOURCES") {
            None => defaults.sources.clone(),
            Some(list) => list.split(',').map(|s| s.trim().to_string()).collect(),
        };
        if let Some(long) = sources.iter().find(|s| s.len() > MAX_SOURCE) {
            return Err(format!(
                "SNODRA_EVENT_BROWSER_SOURCES: a source of {} bytes; at most {MAX_SOURCE}",
                long.len()
            ));
        }
        let number = |name: &str, default: u64| -> Result<u64, String> {
            match get(name) {
                None => Ok(default),
                Some(v) => v
                    .parse::<u64>()
                    .map_err(|_| format!("{name} must be a whole number, not {v:?}")),
            }
        };
        let count = |name: &str, default: usize| -> Result<usize, String> {
            number(name, default as u64).map(|n| usize::try_from(n).unwrap_or(usize::MAX))
        };
        let millis = |name: &str, default: Duration| -> Result<Duration, String> {
            number(name, u64::try_from(default.as_millis()).unwrap_or(u64::MAX))
                .map(Duration::from_millis)
        };
        let browse = BrowseSettings {
            sources,
            max_scanned: number("SNODRA_EVENT_BROWSER_MAX_SCANNED", defaults.max_scanned)?,
            max_results: count("SNODRA_EVENT_BROWSER_MAX_RESULTS", defaults.max_results)?,
            timeout: millis("SNODRA_EVENT_BROWSER_TIMEOUT_MS", defaults.timeout)?,
            max_searches: count("SNODRA_EVENT_BROWSER_MAX_SEARCHES", defaults.max_searches)?,
            max_tails: count("SNODRA_EVENT_BROWSER_MAX_TAILS", defaults.max_tails)?,
            tail_duration: millis(
                "SNODRA_EVENT_BROWSER_TAIL_DURATION_MS",
                defaults.tail_duration,
            )?,
            tail_buffer: count("SNODRA_EVENT_BROWSER_TAIL_BUFFER", defaults.tail_buffer)?,
            max_payload: count(
                "SNODRA_EVENT_BROWSER_MAX_PAYLOAD_BYTES",
                defaults.max_payload,
            )?,
        };
        browse.validate().map_err(|e| named(&e))?;
        if browse.max_payload > MAX_PAYLOAD_BYTES {
            return Err(format!(
                "SNODRA_EVENT_BROWSER_MAX_PAYLOAD_BYTES is at most {MAX_PAYLOAD_BYTES} (1 MiB): a \
                 larger payload would not fit the hub's answers"
            ));
        }
        let lag_interval = millis("SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS", DEFAULT_LAG_INTERVAL)?;
        if !lag_interval.is_zero() && lag_interval < MIN_LAG_INTERVAL {
            return Err(format!(
                "SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS is 0 (off) or at least {} ms",
                MIN_LAG_INTERVAL.as_millis()
            ));
        }
        let health_listen = match get("SNODRA_EVENT_BROWSER_HEALTH_LISTEN").as_deref() {
            Some("off") => None,
            other => {
                let text = other.unwrap_or(DEFAULT_HEALTH_LISTEN);
                Some(text.parse::<SocketAddr>().map_err(|_| {
                    format!(
                        "SNODRA_EVENT_BROWSER_HEALTH_LISTEN must be host:port or off, not {text:?}"
                    )
                })?)
            }
        };
        Ok(Settings {
            store: StoreSettings::from_vars(vars, &get)?,
            browse,
            lag_interval: (!lag_interval.is_zero()).then_some(lag_interval),
            health_listen,
            otlp_endpoint: get("SNODRA_OTLP_ENDPOINT"),
            unknown: vars
                .keys()
                .filter(|k| k.starts_with(PREFIX))
                .filter(|k| !BOUNDS.iter().any(|(_, v)| v == k) && !OTHERS.contains(&k.as_str()))
                .cloned()
                .collect(),
        })
    }

    /// The lag metric's period when it runs: an interval is set and the metrics go somewhere
    /// (`SNODRA_OTLP_ENDPOINT`); without an endpoint nothing would read the gauge.
    pub fn lag_metric(&self) -> Option<Duration> {
        self.lag_interval.filter(|_| self.otlp_endpoint.is_some())
    }

    /// The bounds of the lag metric's own browser: the same sources and time budget, one read at
    /// a time, so the metric never takes a slot the dashboard's calls would get.
    pub fn lag_browse(&self) -> BrowseSettings {
        BrowseSettings {
            max_searches: 1,
            max_tails: 1,
            ..self.browse.clone()
        }
    }
}

/// The library's validation message with the variable instead of the field.
fn named(message: &str) -> String {
    let mut out = message.to_string();
    for (field, var) in BOUNDS {
        let field = format!("the browser's {field} ");
        if out.contains(&field) {
            out = out.replace(&field, &format!("{var} "));
        }
    }
    if out.contains("at least one source") {
        out = format!("SNODRA_EVENT_BROWSER_SOURCES: {out}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(vars: &[(&str, &str)]) -> Result<Settings, String> {
        Settings::from_vars(
            &vars
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn defaults_are_adr_0025s_bounds_on_the_default_source() {
        let s = settings(&[]).unwrap();
        assert_eq!(s.browse, BrowseSettings::default());
        assert_eq!(s.lag_interval, Some(Duration::from_secs(30)));
        assert_eq!(s.health_listen, Some("0.0.0.0:7010".parse().unwrap()));
        assert_eq!(s.otlp_endpoint, None);
        assert_eq!(s.lag_metric(), None, "no metric without an endpoint");
        assert!(s.unknown.is_empty());
        assert_eq!(s.lag_browse().max_searches, 1);
        assert_eq!(s.lag_browse().max_tails, 1);
        assert_eq!(s.lag_browse().sources, s.browse.sources);
    }

    #[test]
    fn every_bound_is_read_from_its_variable() {
        let s = settings(&[
            (
                "SNODRA_EVENT_BROWSER_SOURCES",
                " kraken.events , kraken.events.dlt ",
            ),
            ("SNODRA_EVENT_BROWSER_MAX_SCANNED", "1000"),
            ("SNODRA_EVENT_BROWSER_MAX_RESULTS", "20"),
            ("SNODRA_EVENT_BROWSER_TIMEOUT_MS", "2500"),
            ("SNODRA_EVENT_BROWSER_MAX_SEARCHES", "2"),
            ("SNODRA_EVENT_BROWSER_MAX_TAILS", "3"),
            ("SNODRA_EVENT_BROWSER_TAIL_DURATION_MS", "60000"),
            ("SNODRA_EVENT_BROWSER_TAIL_BUFFER", "16"),
            ("SNODRA_EVENT_BROWSER_MAX_PAYLOAD_BYTES", "1048576"),
            ("SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS", "0"),
            ("SNODRA_EVENT_BROWSER_HEALTH_LISTEN", "off"),
            ("SNODRA_OTLP_ENDPOINT", "http://otel:4317"),
        ])
        .unwrap();
        assert_eq!(
            s.browse,
            BrowseSettings {
                sources: vec!["kraken.events".into(), "kraken.events.dlt".into()],
                max_scanned: 1000,
                max_results: 20,
                timeout: Duration::from_millis(2500),
                max_searches: 2,
                max_tails: 3,
                tail_duration: Duration::from_secs(60),
                tail_buffer: 16,
                max_payload: 1024 * 1024,
            }
        );
        assert_eq!(s.lag_interval, None, "0 turns the lag metric off");
        assert_eq!(s.lag_metric(), None);
        let on = settings(&[
            ("SNODRA_OTLP_ENDPOINT", "http://otel:4317"),
            ("SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS", "1000"),
        ])
        .unwrap();
        assert_eq!(on.lag_metric(), Some(Duration::from_secs(1)));
        assert_eq!(s.health_listen, None);
        assert_eq!(s.otlp_endpoint.as_deref(), Some("http://otel:4317"));
    }

    #[test]
    fn mistakes_name_their_variable() {
        for (var, value, says) in [
            ("SNODRA_EVENT_BROWSER_MAX_RESULTS", "ten", "whole number"),
            ("SNODRA_EVENT_BROWSER_MAX_RESULTS", "0", "must be positive"),
            ("SNODRA_EVENT_BROWSER_MAX_RESULTS", "1001", "at most 1000"),
            ("SNODRA_EVENT_BROWSER_MAX_SCANNED", "0", "must be positive"),
            ("SNODRA_EVENT_BROWSER_TIMEOUT_MS", "-5", "whole number"),
            ("SNODRA_EVENT_BROWSER_TIMEOUT_MS", "600000", "at most"),
            ("SNODRA_EVENT_BROWSER_MAX_SEARCHES", "1025", "at most 1024"),
            ("SNODRA_EVENT_BROWSER_MAX_TAILS", "0", "must be positive"),
            (
                "SNODRA_EVENT_BROWSER_TAIL_DURATION_MS",
                "10",
                "at least 1 s",
            ),
            ("SNODRA_EVENT_BROWSER_TAIL_BUFFER", "65537", "at most 65536"),
            (
                "SNODRA_EVENT_BROWSER_MAX_PAYLOAD_BYTES",
                "1048577",
                "at most 1048576",
            ),
            (
                "SNODRA_EVENT_BROWSER_MAX_PAYLOAD_BYTES",
                "0",
                "must be positive",
            ),
            ("SNODRA_EVENT_BROWSER_SOURCES", "a,,b", "none of them empty"),
            (
                "SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS",
                "soon",
                "whole number",
            ),
            (
                "SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS",
                "999",
                "at least 1000 ms",
            ),
            (
                "SNODRA_EVENT_BROWSER_HEALTH_LISTEN",
                "nowhere",
                "host:port or off",
            ),
        ] {
            let err = settings(&[(var, value)]).expect_err(&format!("{var}={value}"));
            assert!(err.contains(var), "{var}={value}: {err}");
            assert!(err.contains(says), "{var}={value}: {err}");
        }
        let long = "s".repeat(MAX_SOURCE + 1);
        let err = settings(&[("SNODRA_EVENT_BROWSER_SOURCES", &long)]).unwrap_err();
        assert!(err.contains("SNODRA_EVENT_BROWSER_SOURCES"), "{err}");
        assert!(!err.contains(&long), "the value is not echoed: {err}");
    }
}

#[cfg(test)]
mod unknown_tests {
    use super::*;

    #[test]
    fn unknown_browser_variables_are_named_never_their_values() {
        let vars: BTreeMap<String, String> = [
            ("SNODRA_EVENT_BROWSER_MAX_RESULT", "secret-ish"),
            ("SNODRA_EVENT_BROWSER_MAX_RESULTS", "10"),
            ("SNODRA_EVENT_BROWSER_HEALTH_LISTEN", "off"),
            ("SNODRA_EVENT_BROWSERS", "x"),
            ("SNODRA_SERVER_ADDRESS", "static://h:7000"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let s = Settings::from_vars(&vars).unwrap();
        assert_eq!(s.unknown, ["SNODRA_EVENT_BROWSER_MAX_RESULT"]);
    }
}
