//! The lag metric (ADR 0027): every `SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS` the lag of every
//! subscription of every source, live instance or not, read through a browser of its own (one
//! read at a time: it never takes a slot of the dashboard's calls), exported as the gauge
//! `snodra_event_browser_subscription_lag{subscription, source}`.
//!
//! Cardinality: at most [`MAX_SUBSCRIPTIONS`] series per source, the event store's own bound of a
//! lag listing. A listing that comes back `truncated` (more subscriptions than that, or out of
//! time: it does not say which; logged once per source) updates the subscriptions it reached and
//! keeps the last values of the others; a subscription leaves the gauge when a complete listing
//! lacks it. A refresh that fails keeps the last values and counts
//! `snodra_event_browser_lag_refreshes_total{outcome}`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter, ObservableGauge};
use snodra_messaging::event_store::browse::{EventBrowser, LagRequest};
use tokio_util::sync::CancellationToken;

/// The gauge's name.
pub const LAG_GAUGE: &str = "snodra_event_browser_subscription_lag";
/// The refresh counter's name.
pub const REFRESHES: &str = "snodra_event_browser_lag_refreshes_total";
/// Series per source at most (the event store's bound on a lag listing).
pub const MAX_SUBSCRIPTIONS: usize = 500;

/// The last lag known, by source then subscription.
type Known = BTreeMap<String, BTreeMap<String, u64>>;

pub struct LagMetric {
    known: Arc<Mutex<Known>>,
    refreshes: Counter<u64>,
    /// Sources whose listing was truncated (logged once each) or failed (logged once per streak).
    truncated: Mutex<BTreeSet<String>>,
    failing: Mutex<BTreeSet<String>>,
    _gauge: ObservableGauge<u64>,
}

impl LagMetric {
    /// The gauge and the counter on `meter`.
    pub fn new(meter: &Meter) -> Self {
        let known: Arc<Mutex<Known>> = Arc::default();
        let observed = Arc::clone(&known);
        let gauge = meter
            .u64_observable_gauge(LAG_GAUGE)
            .with_description(
                "Events not yet processed by a subscription on a source, live instances or not: the sum over the segments of head minus committed position",
            )
            .with_callback(move |observer| {
                let known = observed.lock().unwrap_or_else(|e| e.into_inner());
                for (source, subscriptions) in known.iter() {
                    for (subscription, lag) in subscriptions {
                        observer.observe(
                            *lag,
                            &[
                                KeyValue::new("subscription", subscription.clone()),
                                KeyValue::new("source", source.clone()),
                            ],
                        );
                    }
                }
            })
            .build();
        LagMetric {
            known,
            refreshes: meter
                .u64_counter(REFRESHES)
                .with_description(
                    "Refreshes of the subscription lag gauge, by outcome (ok, failed)",
                )
                .build(),
            truncated: Mutex::default(),
            failing: Mutex::default(),
            _gauge: gauge,
        }
    }

    /// One refresh of every source in `sources`.
    pub async fn refresh(&self, browser: &dyn EventBrowser, sources: &[String]) {
        for source in sources {
            let request = LagRequest {
                source: Some(source.clone()),
            };
            match browser.lag(request).await {
                Ok(view) => {
                    if self
                        .failing
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(source)
                    {
                        tracing::info!(source = %source, "subscription lag readable again");
                    }
                    let mut truncated = self.truncated.lock().unwrap_or_else(|e| e.into_inner());
                    let over = view.truncated || view.subscriptions.len() > MAX_SUBSCRIPTIONS;
                    if over && truncated.insert(source.clone()) {
                        // The listing does not say which: the event store stops at its bound
                        // of subscriptions or at the end of its time budget.
                        tracing::warn!(
                            source = %source,
                            max = MAX_SUBSCRIPTIONS,
                            "subscription lag listing truncated (more than 500 subscriptions or out of time): subscriptions it did not reach keep their last values"
                        );
                    } else if !over {
                        truncated.remove(source);
                    }
                    let seen: BTreeMap<String, u64> = view
                        .subscriptions
                        .into_iter()
                        .take(MAX_SUBSCRIPTIONS)
                        .map(|s| (s.subscription, s.lag))
                        .collect();
                    let mut known = self.known.lock().unwrap_or_else(|e| e.into_inner());
                    let lags = if over {
                        // A partial listing updates what it reached; a subscription it did not
                        // reach keeps its last value until a complete listing lacks it. Still
                        // at most MAX_SUBSCRIPTIONS series: the ones reached first.
                        let mut merged = seen.clone();
                        for (name, lag) in known.get(source).into_iter().flatten() {
                            if merged.len() >= MAX_SUBSCRIPTIONS {
                                break;
                            }
                            merged.entry(name.clone()).or_insert(*lag);
                        }
                        merged
                    } else {
                        seen
                    };
                    known.insert(source.clone(), lags);
                    self.refreshes.add(1, &[KeyValue::new("outcome", "ok")]);
                }
                Err(e) => {
                    if self
                        .failing
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(source.clone())
                    {
                        tracing::warn!(source = %source, error = %e, "subscription lag not readable: the lag metric keeps its last values");
                    }
                    self.refreshes.add(1, &[KeyValue::new("outcome", "failed")]);
                }
            }
        }
    }

    /// The lag known now of `subscription` on `source`.
    pub fn lag(&self, source: &str, subscription: &str) -> Option<u64> {
        self.known
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(source)
            .and_then(|s| s.get(subscription))
            .copied()
    }
}

/// Refreshes `metric` from `browser` every `interval` (the first at once) until `stop`.
pub async fn run(
    metric: Arc<LagMetric>,
    browser: Arc<dyn EventBrowser>,
    sources: Vec<String>,
    interval: Duration,
    stop: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = ticker.tick() => {}
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = metric.refresh(&*browser, &sources) => {}
        }
    }
}
