//! The event store the browser reads, chosen by `KRAKEN_EVENT_STORE` (ADR 0027: the browser is
//! built on the library's event-store add-ons and reads each of them). Only this module knows the
//! add-ons and their settings; everything else sees an [`EventBrowser`].
//!
//! - `kafka` (the default, and the only one today): the Kafka add-on's browser alone
//!   ([`KafkaBrowser::open`]: no producer), with the add-on's own settings as
//!   [`KafkaConfig::from_vars`] reads them: `KRAKEN_KAFKA_BOOTSTRAP` (default `kafka:19092`) and
//!   any librdkafka property for every client as `KRAKEN_KAFKA_PROPERTY_<NAME>`, the name in
//!   capitals with `_` for `.` and `__` for `_` (`KRAKEN_KAFKA_PROPERTY_SECURITY_PROTOCOL=SASL_PLAINTEXT`
//!   sets `security.protocol`). The add-on refuses the properties it fixes itself (`group.id`, ...).

use std::collections::BTreeMap;
use std::sync::Arc;

use kraken_messaging::event_store::browse::{BrowseSettings, EventBrowser};
use kraken_messaging_kafka::{KafkaBrowser, KafkaConfig};

/// The event stores this browser reads (`KRAKEN_EVENT_STORE`).
pub const KNOWN: [&str; 1] = [kraken_messaging_kafka::NAME];

/// The event store chosen and its connection. Its `Debug` never prints a property's value (a
/// password, for example): the add-on's configuration shows the names only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreSettings {
    Kafka(KafkaConfig),
}

impl StoreSettings {
    /// `KRAKEN_EVENT_STORE` and its add-on's settings, of the variables `vars`; `get` gives one
    /// trimmed, `None` when unset or blank.
    pub(crate) fn from_vars(
        vars: &BTreeMap<String, String>,
        get: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let name = get("KRAKEN_EVENT_STORE").unwrap_or_else(|| KNOWN[0].to_string());
        match name.as_str() {
            kraken_messaging_kafka::NAME => Ok(StoreSettings::Kafka(KafkaConfig::from_vars(
                vars.iter().map(|(k, v)| (k.clone(), v.clone())),
            ))),
            other => Err(format!(
                "KRAKEN_EVENT_STORE={} is not an event store this browser reads; known: {}",
                kraken_messaging::event_store::browse::quoted(other),
                KNOWN.join(", ")
            )),
        }
    }

    /// The name of the event store (`KRAKEN_EVENT_STORE`).
    pub fn name(&self) -> &'static str {
        match self {
            StoreSettings::Kafka(_) => kraken_messaging_kafka::NAME,
        }
    }

    /// The event store's browser under `browse`. Connects nothing yet; a setting the add-on
    /// refuses is an error naming it.
    pub fn open(&self, browse: BrowseSettings) -> Result<Arc<dyn EventBrowser>, String> {
        match self {
            StoreSettings::Kafka(config) => {
                let browser = KafkaBrowser::open(&config.clone().browse(browse))
                    .map_err(|e| e.to_string())?;
                Ok(Arc::new(browser))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(vars: &[(&str, &str)]) -> Result<StoreSettings, String> {
        let vars: BTreeMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let get = |name: &str| vars.get(name).cloned().filter(|v| !v.trim().is_empty());
        StoreSettings::from_vars(&vars, &get)
    }

    fn kafka(settings: &StoreSettings) -> &KafkaConfig {
        match settings {
            StoreSettings::Kafka(config) => config,
        }
    }

    fn properties(settings: &StoreSettings) -> Vec<(&str, &str)> {
        let mut all: Vec<(&str, &str)> = kafka(settings)
            .properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        all.sort_unstable();
        all
    }

    #[test]
    fn kafka_is_the_default_with_the_add_ons_default_servers() {
        let s = store(&[]).unwrap();
        assert_eq!(s.name(), "kafka");
        assert_eq!(kafka(&s).bootstrap_servers, "kafka:19092");
        let s = store(&[
            ("KRAKEN_EVENT_STORE", "kafka"),
            ("KRAKEN_KAFKA_BOOTSTRAP", "k1:9092,k2:9092"),
        ])
        .unwrap();
        assert_eq!(kafka(&s).bootstrap_servers, "k1:9092,k2:9092");
    }

    #[test]
    fn an_unknown_event_store_names_the_known_ones() {
        let err = store(&[("KRAKEN_EVENT_STORE", "rabbitmq-streams")]).unwrap_err();
        assert_eq!(
            err,
            "KRAKEN_EVENT_STORE=\"rabbitmq-streams\" is not an event store this browser reads; known: kafka"
        );
        let long = "x".repeat(500);
        let err = store(&[("KRAKEN_EVENT_STORE", &long)]).unwrap_err();
        assert!(err.len() < 200, "the value is cut: {err}");
    }

    #[test]
    fn property_variables_become_the_add_ons_properties() {
        let s = store(&[
            ("KRAKEN_KAFKA_PROPERTY_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            ("KRAKEN_KAFKA_PROPERTY_SASL_MECHANISMS", "PLAIN"),
            ("KRAKEN_KAFKA_PROPERTIES", "not a property"),
        ])
        .unwrap();
        assert_eq!(
            properties(&s),
            [
                ("sasl.mechanisms", "PLAIN"),
                ("security.protocol", "SASL_PLAINTEXT"),
            ]
        );
    }

    #[test]
    fn property_variables_are_read_as_the_add_on_reads_them() {
        // The add-on's own reading (KafkaConfig::from_vars): `__` is a literal `_`.
        let s = store(&[("KRAKEN_KAFKA_PROPERTY_LOG__LEVEL", "6")]).unwrap();
        assert_eq!(properties(&s), [("log_level", "6")]);
    }

    #[test]
    fn the_add_on_checks_its_settings_when_the_store_opens() {
        let ok = store(&[
            ("KRAKEN_KAFKA_BOOTSTRAP", "127.0.0.1:1"),
            ("KRAKEN_KAFKA_PROPERTY_SECURITY_PROTOCOL", "plaintext"),
        ])
        .unwrap();
        assert!(ok.open(BrowseSettings::default()).is_ok());
        let fixed = store(&[("KRAKEN_KAFKA_PROPERTY_GROUP_ID", "mine")]).unwrap();
        let err = fixed.open(BrowseSettings::default()).err().unwrap();
        assert!(err.contains("group.id"), "{err}");
        let bad = BrowseSettings {
            sources: Vec::new(),
            ..BrowseSettings::default()
        };
        assert!(ok.open(bad).is_err(), "the browse settings are checked too");
        let secret = store(&[("KRAKEN_KAFKA_PROPERTY_SASL_PASSWORD", "hunter2")]).unwrap();
        assert!(!format!("{secret:?}").contains("hunter2"));
    }
}
