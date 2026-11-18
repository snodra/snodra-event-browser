//! The browser's answers over an in-process event store double: every answer fits what the hub
//! takes (a search page asks again for fewer results and keeps a cursor to the rest, a record's
//! payload is cut, a lag view drops the least lagging subscriptions, anything else that would not
//! fit is `unavailable`), and the lag metric's gauge.

mod common;

use std::time::Duration;

use common::{Double, SOURCE, summary};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use snodra_event_browser::answers::{MAX_ANSWER, size};
use snodra_event_browser::lag::{LAG_GAUGE, LagMetric, MAX_SUBSCRIPTIONS, REFRESHES};
use snodra_event_browser::{Handlers, Tails};
use snodra_messaging::event_store::browse::{
    BrowseError, GetRequest, LagRequest, LagView, SearchRequest, SegmentBounds, SourceView,
    SourcesRequest, SourcesView, SubscriptionLag,
};

fn handlers(double: &Double) -> Handlers {
    Handlers::new(double.browser(), Duration::from_secs(10), Tails::new())
}

/// 300 events of some 12 KiB each: a page of 300 is some 3.6 MiB.
fn big_events() -> Vec<snodra_messaging::event_store::browse::EventSummary> {
    (0..300)
        .map(|i| summary(i, "orders.Placed", 12_000))
        .collect()
}

#[tokio::test]
async fn an_oversized_search_page_comes_with_fewer_results_and_a_cursor_to_the_rest() {
    let double = Double::new(1).with_events(big_events());
    let h = handlers(&double);
    let request = SearchRequest {
        limit: Some(300),
        timeout_ms: Some(5000),
        ..SearchRequest::default()
    };
    let mut seen = Vec::new();
    let mut page = h.search(request.clone()).await.unwrap();
    let mut pages = 1;
    loop {
        assert!(
            size(&page) <= MAX_ANSWER,
            "page {pages}: {} bytes",
            size(&page)
        );
        seen.extend(page.results.iter().map(|s| s.position.clone()));
        let Some(cursor) = page.cursor.clone() else {
            break;
        };
        page = h
            .search(SearchRequest {
                cursor: Some(cursor),
                ..request.clone()
            })
            .await
            .unwrap();
        pages += 1;
        assert!(pages < 10, "the pages move on");
    }
    assert!(pages > 1, "one page could not hold them all");
    let expected: Vec<String> = (0..300).rev().map(|i| i.to_string()).collect();
    assert_eq!(seen, expected, "every event once, newest first");
    // The first page was asked again, for fewer results, within the time budget left.
    let searches = double.searches();
    assert_eq!(searches[0].limit, Some(300));
    assert!(searches[1].limit.unwrap() < 300);
    assert!(searches[1].timeout_ms.unwrap() <= 5000);
    assert_eq!(searches[1].cursor, None, "asked again from the same place");
}

/// A re-ask never runs on a spent budget: it gets at least 250 ms (or a tenth of the budget).
#[tokio::test]
async fn a_search_asked_again_gets_a_minimum_budget() {
    let double = Double::new(1).with_events(big_events());
    double
        .state
        .lock()
        .unwrap()
        .search_delays
        .push_back(Duration::from_millis(990));
    let page = handlers(&double)
        .search(SearchRequest {
            limit: Some(300),
            timeout_ms: Some(1000),
            ..SearchRequest::default()
        })
        .await
        .unwrap();
    assert!(!page.results.is_empty() && page.cursor.is_some());
    let searches = double.searches();
    assert!(
        searches[1].timeout_ms.unwrap() >= 250,
        "{:?}",
        searches[1].timeout_ms
    );
    // A tenth of a long budget, if more.
    let double = Double::new(1).with_events(big_events());
    double
        .state
        .lock()
        .unwrap()
        .search_delays
        .push_back(Duration::from_millis(5000));
    tokio::time::pause();
    let h = Handlers::new(double.browser(), Duration::from_secs(10), Tails::new());
    let _ = h
        .search(SearchRequest {
            limit: Some(300),
            timeout_ms: Some(5000),
            ..SearchRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(double.searches()[1].timeout_ms, Some(500));
}

/// A re-ask that finds nothing in its time is an error, not an empty first page.
#[tokio::test]
async fn a_search_asked_again_that_finds_nothing_is_unavailable() {
    let double = Double::new(1).with_events(big_events());
    double.state.lock().unwrap().results_for = Some(1);
    let err = handlers(&double)
        .search(SearchRequest {
            limit: Some(300),
            ..SearchRequest::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(&err, BrowseError::Unavailable(m) if m.contains("fewer")),
        "{err:?}"
    );
    assert_eq!(double.searches().len(), 2);
}

#[tokio::test]
async fn a_page_that_fits_is_asked_once() {
    let double = Double::new(1).with_events((0..10).map(|i| summary(i, "a.B", 0)).collect());
    let page = handlers(&double)
        .search(SearchRequest::default())
        .await
        .unwrap();
    assert_eq!(page.results.len(), 10);
    assert_eq!(double.searches().len(), 1);
}

#[tokio::test]
async fn a_record_payload_is_cut_to_fit_whatever_the_event_store_sends() {
    let double = Double::new(1).with_events(vec![summary(0, "a.B", 0)]);
    // 1 MiB of characters JSON writes six-fold.
    double.state.lock().unwrap().payload = Some("\u{1}".repeat(1024 * 1024));
    let record = handlers(&double)
        .get(GetRequest {
            source: None,
            segment: "0".into(),
            position: "0".into(),
            payload: true,
        })
        .await
        .unwrap();
    assert!(size(&record) <= MAX_ANSWER);
    assert!(record.payload_truncated);
    // Not asked: no payload at all.
    let record = handlers(&double)
        .get(GetRequest {
            source: None,
            segment: "0".into(),
            position: "0".into(),
            payload: false,
        })
        .await
        .unwrap();
    assert_eq!(record.payload, None);
}

fn lagging(name: &str, lag: u64, segments: usize) -> SubscriptionLag {
    serde_json::from_value(serde_json::json!({
        "subscription": name, "state": "Empty", "members": 0, "lag": lag,
        "segments": (0..segments).map(|i| serde_json::json!({
            "segment": i.to_string(), "committed": "100000", "head": "200000", "lag": 100000
        })).collect::<Vec<_>>()
    }))
    .unwrap()
}

#[tokio::test]
async fn a_lag_view_over_the_bound_keeps_the_most_lagging_subscriptions() {
    let double = Double::new(1);
    double.state.lock().unwrap().lag = Some(LagView {
        source: SOURCE.into(),
        missing: false,
        subscriptions: (0..400)
            .map(|i| lagging(&format!("svc-{i:03}.default"), 10_000 - i, 500))
            .collect(),
        truncated: false,
    });
    let view = handlers(&double).lag(LagRequest::default()).await.unwrap();
    assert!(size(&view) <= MAX_ANSWER);
    assert!(view.truncated);
    assert!(view.subscriptions.len() < 400);
    assert_eq!(view.subscriptions[0].subscription, "svc-000.default");
}

#[tokio::test]
async fn a_sources_answer_over_the_bound_is_unavailable_not_sent() {
    let double = Double::new(1);
    double.state.lock().unwrap().sources = Some(SourcesView {
        sources: vec![SourceView {
            source: SOURCE.into(),
            missing: false,
            segments: (0..100_000)
                .map(|i| SegmentBounds {
                    segment: i.to_string(),
                    first: "0".into(),
                    head: "1000000000".into(),
                })
                .collect(),
        }],
    });
    let err = handlers(&double)
        .sources(SourcesRequest {})
        .await
        .unwrap_err();
    assert!(matches!(err, BrowseError::Unavailable(_)), "{err:?}");
}

#[tokio::test]
async fn the_event_stores_errors_pass_as_they_are() {
    let double = Double::new(1);
    double.state.lock().unwrap().failing = Some(BrowseError::Busy("4 searches".into()));
    let h = handlers(&double);
    assert_eq!(
        h.search(SearchRequest::default()).await.unwrap_err(),
        BrowseError::Busy("4 searches".into())
    );
    assert!(h.lag(LagRequest::default()).await.is_err());
}

// ---- the lag metric -------------------------------------------------------------------------

struct Exported {
    exporter: InMemoryMetricExporter,
    provider: SdkMeterProvider,
}

impl Exported {
    fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        Exported { exporter, provider }
    }

    /// Every point of the u64 gauge `name`: its attributes, sorted, and its value.
    fn gauge(&self, name: &str) -> Vec<(Vec<(String, String)>, u64)> {
        self.exporter.reset();
        self.provider.force_flush().unwrap();
        let finished = self.exporter.get_finished_metrics().unwrap();
        let Some(metric) = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == name)
        else {
            return vec![];
        };
        let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
            panic!("{name} is not a u64 gauge");
        };
        let mut points: Vec<_> = gauge
            .data_points()
            .map(|p| {
                let mut attrs: Vec<(String, String)> = p
                    .attributes()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect();
                attrs.sort();
                (attrs, p.value())
            })
            .collect();
        points.sort();
        points
    }

    fn counter(&self, name: &str, outcome: &str) -> u64 {
        self.exporter.reset();
        self.provider.force_flush().unwrap();
        let finished = self.exporter.get_finished_metrics().unwrap();
        let Some(metric) = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == name)
        else {
            return 0;
        };
        let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
            panic!("{name} is not a u64 counter");
        };
        sum.data_points()
            .filter(|p| p.attributes().any(|kv| kv.value.as_str() == outcome))
            .map(|p| p.value())
            .sum()
    }
}

fn point(subscription: &str, source: &str, lag: u64) -> (Vec<(String, String)>, u64) {
    (
        vec![
            ("source".to_string(), source.to_string()),
            ("subscription".to_string(), subscription.to_string()),
        ],
        lag,
    )
}

#[tokio::test]
async fn the_lag_gauge_shows_every_subscription_of_every_source_and_forgets_the_gone() {
    let exported = Exported::new();
    let metric = LagMetric::new(&exported.provider.meter("test"));
    let double = Double::new(1);
    double.state.lock().unwrap().lag = Some(LagView {
        source: SOURCE.into(),
        missing: false,
        subscriptions: vec![
            lagging("orders.projector", 6, 1),
            lagging("audit.default", 0, 1),
        ],
        truncated: false,
    });
    let sources = vec![SOURCE.to_string(), "snodra.events.dlt".to_string()];
    metric.refresh(&*double.browser(), &sources).await;
    assert_eq!(
        exported.gauge(LAG_GAUGE),
        [
            point("audit.default", "snodra.events", 0),
            point("orders.projector", "snodra.events", 6),
            point("audit.default", "snodra.events.dlt", 0),
            point("orders.projector", "snodra.events.dlt", 6),
        ]
    );
    // Each source asked by name.
    let asked: Vec<_> = double
        .state
        .lock()
        .unwrap()
        .lags
        .iter()
        .map(|r| r.source.clone())
        .collect();
    assert_eq!(
        asked,
        [Some(SOURCE.into()), Some("snodra.events.dlt".into())]
    );
    assert_eq!(exported.counter(REFRESHES, "ok"), 2);

    // A subscription that is gone leaves the gauge.
    double.state.lock().unwrap().lag = Some(LagView {
        source: SOURCE.into(),
        missing: false,
        subscriptions: vec![lagging("orders.projector", 9, 1)],
        truncated: false,
    });
    metric.refresh(&*double.browser(), &sources[..1]).await;
    assert_eq!(metric.lag(SOURCE, "orders.projector"), Some(9));
    assert_eq!(metric.lag(SOURCE, "audit.default"), None);

    // A failed refresh keeps the last values and is counted.
    double.state.lock().unwrap().failing = Some(BrowseError::Unavailable("down".into()));
    metric.refresh(&*double.browser(), &sources[..1]).await;
    assert_eq!(metric.lag(SOURCE, "orders.projector"), Some(9));
    assert_eq!(exported.counter(REFRESHES, "failed"), 1);
}

#[tokio::test]
async fn the_lag_gauge_has_at_most_one_series_per_subscription_the_listing_holds() {
    let exported = Exported::new();
    let metric = LagMetric::new(&exported.provider.meter("test"));
    let double = Double::new(1);
    double.state.lock().unwrap().lag = Some(LagView {
        source: SOURCE.into(),
        missing: false,
        subscriptions: (0..MAX_SUBSCRIPTIONS + 50)
            .map(|i| lagging(&format!("s-{i:04}"), 1, 1))
            .collect(),
        truncated: true,
    });
    metric
        .refresh(&*double.browser(), &[SOURCE.to_string()])
        .await;
    assert_eq!(exported.gauge(LAG_GAUGE).len(), MAX_SUBSCRIPTIONS);
}

/// A truncated listing (out of time, or more subscriptions than it reads) updates what it
/// reached and keeps the others; only a complete listing that lacks a subscription drops it.
#[tokio::test]
async fn a_truncated_lag_listing_keeps_the_subscriptions_it_did_not_reach() {
    let exported = Exported::new();
    let metric = LagMetric::new(&exported.provider.meter("test"));
    let double = Double::new(1);
    let set = |subscriptions: Vec<SubscriptionLag>, truncated: bool| {
        double.state.lock().unwrap().lag = Some(LagView {
            source: SOURCE.into(),
            missing: false,
            subscriptions,
            truncated,
        });
    };
    let sources = [SOURCE.to_string()];
    set(
        vec![lagging("a.default", 5, 1), lagging("b.default", 3, 1)],
        false,
    );
    metric.refresh(&*double.browser(), &sources).await;
    // Out of time after `a`: `b` keeps its value.
    set(vec![lagging("a.default", 7, 1)], true);
    metric.refresh(&*double.browser(), &sources).await;
    assert_eq!(metric.lag(SOURCE, "a.default"), Some(7));
    assert_eq!(metric.lag(SOURCE, "b.default"), Some(3));
    assert_eq!(exported.gauge(LAG_GAUGE).len(), 2);
    // A complete listing without `b`: gone.
    set(vec![lagging("a.default", 8, 1)], false);
    metric.refresh(&*double.browser(), &sources).await;
    assert_eq!(metric.lag(SOURCE, "b.default"), None);
    assert_eq!(exported.gauge(LAG_GAUGE).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn the_lag_task_refreshes_on_its_interval_until_stopped() {
    let exported = Exported::new();
    let metric = std::sync::Arc::new(LagMetric::new(&exported.provider.meter("test")));
    let double = Double::new(1);
    let stop = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(snodra_event_browser::lag::run(
        metric,
        double.browser(),
        vec![SOURCE.to_string()],
        Duration::from_secs(30),
        stop.clone(),
    ));
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(
        double.state.lock().unwrap().lags.len(),
        3,
        "at 0, 30 and 60 s"
    );
    stop.cancel();
    task.await.unwrap();
}
