//! End to end on Docker: a Kafka container, a hub container (the hub's image, see
//! `hub_container`), and the browser in process on the Kafka add-on, as `kraken-event-browser`
//! runs it (`run`, settings from variables). Events are published with the add-on; everything is
//! read through the hub's HTTP API: search (filters, paging), a record with and without its
//! payload under the hub's payload rules, the sources, the lag of a subscription without a live
//! instance, a tail that delivers a new event and ends on time, a busy browser as 429, and 404 on
//! every route once the browser stopped.

mod hub_container;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::StreamExt;
use kraken_event_browser::Settings;
use kraken_messaging::event_store::{
    EventStore, OutgoingEvent, SubscriptionEvent, SubscriptionSpec,
};
use kraken_messaging_kafka::{KafkaConfig, KafkaEventStore};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use serde_json::Value;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka};
use tokio::sync::oneshot;

const SOURCE: &str = "kraken.events";
const TOKEN: &str = "a-dashboard-token-0123456789";

async fn create_topic(bootstrap: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    for r in admin
        .create_topics(
            &[NewTopic::new(
                SOURCE,
                partitions,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(20))),
        )
        .await
        .unwrap()
    {
        r.unwrap();
    }
}

fn event(id: &str, type_name: &str, key: &str, correlation: &str, payload: &str) -> OutgoingEvent {
    let headers = HashMap::from([
        ("type".to_string(), type_name.to_string()),
        ("id".to_string(), id.to_string()),
        ("correlationId".to_string(), correlation.to_string()),
        ("timestamp".to_string(), "2026-11-14T10:00:00Z".to_string()),
        ("source".to_string(), "orders-service".to_string()),
    ]);
    OutgoingEvent {
        source: SOURCE.to_string(),
        id: id.to_string(),
        type_name: type_name.to_string(),
        key: Some(key.to_string()),
        headers,
        payload: payload.as_bytes().to_vec(),
    }
}

async fn hub() -> hub_container::Hub {
    hub_container::hub_with(
        &[
            // Reads are open; a payload is served to a request presenting the write token.
            ("KRAKEN_HUB_DASHBOARD_WRITE_TOKEN", TOKEN),
            ("KRAKEN_HUB_EVENT_BROWSER_PAYLOADS", "true"),
            // Tails end on time within the test: the hub asks 2 s under this.
            ("KRAKEN_HUB_MAX_STREAM_DURATION", "6s"),
        ],
        &[],
    )
    .await
}

async fn get(http: SocketAddr, path: &str, token: Option<&str>) -> (u16, Value) {
    let mut request = reqwest::Client::new().get(format!("http://{http}{path}"));
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let r = request.send().await.unwrap();
    let status = r.status().as_u16();
    let text = r.text().await.unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// `GET path` until `done` holds (60 s at most).
async fn until(
    http: SocketAddr,
    path: &str,
    what: &str,
    done: impl Fn(u16, &Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = get(http, path, Some(TOKEN)).await;
        if done(status, &body) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "{what} within 60 s: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn ids(page: &Value) -> Vec<String> {
    page["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["headers"]["id"].as_str().unwrap().to_string())
        .collect()
}

/// A server-sent events reader.
struct Sse {
    status: u16,
    body: futures::stream::BoxStream<'static, reqwest::Result<bytes::Bytes>>,
    buffer: String,
}

async fn sse(http: SocketAddr, path: &str) -> Sse {
    let r = reqwest::Client::new()
        .get(format!("http://{http}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .unwrap();
    Sse {
        status: r.status().as_u16(),
        body: r.bytes_stream().boxed(),
        buffer: String::new(),
    }
}

impl Sse {
    /// The next event other than a heartbeat, `None` when the stream ended or nothing came.
    async fn next(&mut self, within: Duration) -> Option<(String, String)> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                let (mut name, mut data) = (String::new(), String::new());
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        name = v.trim().to_string();
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push_str(v.trim_start());
                    }
                }
                if name.is_empty() || name == "heartbeat" {
                    continue;
                }
                return Some((name, data));
            }
            match tokio::time::timeout_at(deadline, self.body.next()).await {
                Ok(Some(Ok(bytes))) => self.buffer.push_str(&String::from_utf8_lossy(&bytes)),
                _ => return None,
            }
        }
    }
}

#[tokio::test]
async fn the_dashboard_reads_kafka_through_the_hub_and_the_browser() {
    let kafka = Kafka::default().start().await.expect("kafka container");
    let port = kafka.get_host_port_ipv4(apache::KAFKA_PORT).await.unwrap();
    let bootstrap = format!("127.0.0.1:{port}");
    create_topic(&bootstrap, 3).await;

    // Six events, published with the add-on.
    let store = KafkaEventStore::new(KafkaConfig::new(&bootstrap)).unwrap();
    let published = [
        ("e-1", "orders.Placed", "order-1", "c-1"),
        ("e-2", "orders.Placed", "order-2", "c-2"),
        ("e-3", "orders.Shipped", "order-1", "c-1"),
        ("e-4", "orders.Placed", "order-3", "c-3"),
        ("e-5", "billing.Paid", "order-2", "c-2"),
        ("e-6", "orders.Placed", "order-4", "c-4"),
    ];
    for (id, type_name, key, correlation) in published {
        let payload = format!("{{\"id\":\"{id}\",\"total\":42}}");
        store
            .publish(&event(id, type_name, key, correlation, &payload))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A subscription reads two events, acknowledges them and leaves: no live instance.
    let mut subscription = store
        .subscribe(SubscriptionSpec {
            name: "orders.projector".into(),
            source: SOURCE.into(),
        })
        .await
        .unwrap();
    let mut acked = 0;
    let deadline = Instant::now() + Duration::from_secs(60);
    while acked < 2 {
        assert!(Instant::now() < deadline, "deliveries within 60 s");
        if let Ok(Ok(SubscriptionEvent::Delivery(d))) =
            tokio::time::timeout(Duration::from_secs(5), subscription.next()).await
        {
            subscription.ack(&d.segment, &d.position).unwrap();
            acked += 1;
        }
    }
    subscription.close().await;

    let hub = hub().await;
    let (grpc, http) = (hub.grpc, hub.http);
    let vars: BTreeMap<String, String> = [
        ("KRAKEN_KAFKA_BOOTSTRAP", bootstrap.as_str()),
        ("KRAKEN_EVENT_BROWSER_MAX_TAILS", "1"),
        ("KRAKEN_EVENT_BROWSER_LAG_INTERVAL_MS", "1000"),
        // The lag metric runs only with an endpoint (nothing is exported in the test: no meter
        // provider is installed).
        ("KRAKEN_OTLP_ENDPOINT", "http://127.0.0.1:1"),
        ("KRAKEN_EVENT_BROWSER_HEALTH_LISTEN", "off"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let settings = Settings::from_vars(&vars).unwrap();
    let config =
        kraken_messaging::Config::new(kraken_event_browser::SERVICE, &format!("static://{grpc}"));
    let (stop, stopped) = oneshot::channel::<()>();
    let browser = tokio::spawn(kraken_event_browser::run(config, settings, async {
        let _ = stopped.await;
    }));
    until(http, "/api/v1/version", "the browser installed", |_, v| {
        v["eventBrowser"]["installed"] == true
    })
    .await;

    // Search: a type, newest first, two by two with the cursor.
    let page = until(
        http,
        "/api/v1/events?type=orders.Placed&limit=2",
        "a page",
        |s, v| s == 200 && v["results"].as_array().is_some_and(|r| r.len() == 2),
    )
    .await;
    assert_eq!(ids(&page), ["e-6", "e-4"]);
    let cursor = page["cursor"].as_str().expect("a cursor").to_string();
    let (status, next) = get(
        http,
        &format!("/api/v1/events?type=orders.Placed&limit=2&cursor={cursor}"),
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, 200, "{next}");
    assert_eq!(ids(&next), ["e-2", "e-1"]);
    // The other filters: key, correlation id, publishing service, a type prefix.
    let (_, by_key) = get(http, "/api/v1/events?key=order-2", Some(TOKEN)).await;
    assert_eq!(ids(&by_key), ["e-5", "e-2"]);
    let (_, by_correlation) = get(http, "/api/v1/events?correlationId=c-1", Some(TOKEN)).await;
    assert_eq!(ids(&by_correlation), ["e-3", "e-1"]);
    let (_, by_service) = get(
        http,
        "/api/v1/events?service=orders-service&type=billing.*",
        Some(TOKEN),
    )
    .await;
    assert_eq!(ids(&by_service), ["e-5"]);
    let summary = &by_service["results"][0];
    assert!(
        summary.get("payload").is_none(),
        "a summary never carries a payload"
    );

    // A record, with its payload for the write token, hidden without a token.
    let (segment, position) = (
        summary["segment"].as_str().unwrap().to_string(),
        summary["position"].as_str().unwrap().to_string(),
    );
    let path = format!("/api/v1/events/{SOURCE}/{segment}/{position}");
    let (status, record) = get(http, &path, Some(TOKEN)).await;
    assert_eq!(status, 200, "{record}");
    assert_eq!(record["payload"], "{\"id\":\"e-5\",\"total\":42}");
    assert_eq!(record["headers"]["type"], "billing.Paid");
    assert!(record["allHeaders"].as_array().unwrap().len() >= 5);
    let (status, hidden) = get(http, &path, None).await;
    assert_eq!(status, 200);
    assert_eq!(hidden["payload"]["hidden"], "no-token-presented");
    // A payload search under the rules.
    let (_, q) = get(http, "/api/v1/events?q=%22e-4%22", Some(TOKEN)).await;
    assert_eq!(ids(&q), ["e-4"]);
    let (status, _) = get(
        http,
        &format!("/api/v1/events/{SOURCE}/{segment}/999999"),
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, 404);

    // The sources: three segments holding six events.
    let (status, sources) = get(http, "/api/v1/events/sources", Some(TOKEN)).await;
    assert_eq!(status, 200, "{sources}");
    let segments = sources["sources"][0]["segments"].as_array().unwrap();
    assert_eq!(segments.len(), 3);

    // The lag of the subscription without a live instance.
    let lag = until(
        http,
        "/api/v1/events/lag",
        "the subscription's lag",
        |s, v| {
            s == 200
                && v["subscriptions"].as_array().is_some_and(|subs| {
                    subs.iter().any(|x| x["subscription"] == "orders.projector")
                })
        },
    )
    .await;
    let projector = lag["subscriptions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["subscription"] == "orders.projector")
        .unwrap();
    assert_eq!(projector["members"], 0);
    // Two of six acknowledged: its lag is what is left behind its committed positions, on the
    // segments it committed on (the others have no committed position).
    let segments = projector["segments"].as_array().unwrap();
    assert_eq!(segments.len(), 3, "{projector}");
    let mut committed = 0;
    let mut behind = 0;
    for s in segments {
        if let Some(position) = s["committed"].as_str() {
            let head: u64 = s["head"].as_str().unwrap().parse().unwrap();
            let position: u64 = position.parse().unwrap();
            committed += position;
            behind += head - position;
            assert_eq!(s["lag"], head - position, "{projector}");
        } else {
            assert!(s["lag"].is_null(), "{projector}");
        }
    }
    assert_eq!(committed, 2, "the two acknowledged: {projector}");
    assert_eq!(projector["lag"], behind, "{projector}");

    // A tail: start, the event published after it, and its end on time.
    let mut tail = sse(http, "/api/v1/events/tail?type=orders.*").await;
    assert_eq!(tail.status, 200);
    assert_eq!(tail.next(Duration::from_secs(10)).await.unwrap().0, "start");
    // The browser's only tail slot is taken: a second tail is 429.
    let (status, busy) = get(http, "/api/v1/events/tail", Some(TOKEN)).await;
    assert_eq!(status, 429, "{busy}");
    store
        .publish(&event("e-7", "orders.Cancelled", "order-9", "c-9", "{}"))
        .await
        .unwrap();
    let (name, data) = tail
        .next(Duration::from_secs(10))
        .await
        .expect("the new event");
    assert_eq!(name, "record", "{data}");
    let record: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(record["headers"]["id"], "e-7");
    let (name, data) = tail.next(Duration::from_secs(15)).await.expect("the end");
    assert_eq!(
        (name.as_str(), data.as_str()),
        ("end", "{\"reason\":\"maxDuration\"}")
    );
    assert_eq!(
        tail.next(Duration::from_secs(5)).await,
        None,
        "nothing after the end"
    );

    // The browser stops: every route is 404.
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(30), browser)
        .await
        .expect("the browser stops")
        .unwrap()
        .unwrap();
    for route in [
        "/api/v1/events",
        "/api/v1/events/sources",
        "/api/v1/events/lag",
        "/api/v1/events/tail",
        path.as_str(),
    ] {
        let (status, body) = get(http, route, Some(TOKEN)).await;
        assert_eq!(
            (status, body["error"].as_str()),
            (404, Some("event browser not installed")),
            "{route}"
        );
    }
}
