//! The browser behind a real hub (the hub's image, see `hub_container`), on an event store
//! double: it registers as `snodra-event-browser` with the five types, the hub's
//! `/api/v1/events*` routes answer through it, a tail streams `start`, records and heartbeats and
//! ends `shutdown` when the browser stops, a reader that leaves frees the browser's tail slot, a
//! busy browser is a 429, and the health service follows the hub connection. Needs Docker for the
//! hub.

mod common;
mod hub_container;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::{Double, summary};
use futures::StreamExt;
use hub_container::{Hub, hub, hub_with};
use serde_json::Value;
use sha2::{Digest, Sha256};
use snodra_event_browser::{Browsers, Settings};
use snodra_messaging::event_store::browse::LagView;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct Running {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), snodra_messaging::MessagingError>>,
}

impl Running {
    /// Stops the browser and waits until it stopped.
    pub async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("the browser stops in time")
            .unwrap()
            .unwrap();
    }
}

fn settings(health: Option<SocketAddr>) -> Settings {
    let mut s = Settings::from_vars(&Default::default()).unwrap();
    s.health_listen = health;
    s.lag_interval = None;
    s
}

pub fn start_browser(hub: &Hub, double: &Double, settings: Settings) -> Running {
    let config = snodra_messaging::Config::new(
        snodra_event_browser::SERVICE,
        &format!("static://{}", hub.grpc),
    );
    start_browser_with(config, double, settings)
}

pub fn start_browser_with(
    config: snodra_messaging::Config,
    double: &Double,
    settings: Settings,
) -> Running {
    let browsers = Browsers {
        calls: double.browser(),
        lag: None,
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(snodra_event_browser::serve(
        config,
        settings,
        browsers,
        async {
            let _ = stopped.await;
        },
    ));
    Running {
        stop: Some(stop),
        task,
    }
}

pub async fn get(http: SocketAddr, path: &str) -> (u16, Value) {
    let r = reqwest::get(format!("http://{http}{path}")).await.unwrap();
    let status = r.status().as_u16();
    let body = r.text().await.unwrap();
    (status, serde_json::from_str(&body).unwrap_or(Value::Null))
}

pub async fn until_installed(http: SocketAddr, installed: bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, v) = get(http, "/api/v1/version").await;
        if v["eventBrowser"]["installed"] == installed {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "installed never became {installed}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A server-sent events reader.
pub struct Sse {
    pub status: u16,
    body: futures::stream::BoxStream<'static, reqwest::Result<bytes::Bytes>>,
    buffer: String,
}

pub async fn sse(http: SocketAddr, path: &str) -> Sse {
    let r = reqwest::get(format!("http://{http}{path}")).await.unwrap();
    Sse {
        status: r.status().as_u16(),
        body: r.bytes_stream().boxed(),
        buffer: String::new(),
    }
}

impl Sse {
    /// The next event `(name, data)`, `None` when the stream ended or nothing came in `within`.
    pub async fn next(&mut self, within: Duration) -> Option<(String, String)> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                let (mut name, mut data) = (String::from("message"), String::new());
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        name = v.trim().to_string();
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push_str(v.trim_start());
                    }
                }
                if data.is_empty() && name == "message" {
                    continue; // a comment (keep-alive)
                }
                return Some((name, data));
            }
            match tokio::time::timeout_at(deadline, self.body.next()).await {
                Ok(Some(Ok(bytes))) => self.buffer.push_str(&String::from_utf8_lossy(&bytes)),
                _ => return None,
            }
        }
    }

    /// The next event other than a heartbeat.
    pub async fn next_not_heartbeat(&mut self, within: Duration) -> Option<(String, String)> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(left).await {
                Some((name, _)) if name == "heartbeat" => continue,
                other => return other,
            }
        }
    }
}

const ROUTES: [&str; 5] = [
    "/api/v1/events",
    "/api/v1/events/snodra.events/0/1",
    "/api/v1/events/sources",
    "/api/v1/events/lag",
    "/api/v1/events/tail",
];

#[tokio::test]
async fn the_hubs_events_api_answers_through_the_browser_until_it_stops() {
    let hub = hub().await;
    let double =
        Double::new(4).with_events((0..5).map(|i| summary(i, "orders.Placed", 0)).collect());
    double.state.lock().unwrap().lag = Some(
        serde_json::from_value::<LagView>(serde_json::json!({
            "source": "snodra.events",
            "subscriptions": [{"subscription": "orders.projector", "state": "Empty", "members": 0,
                "segments": [{"segment": "0", "committed": "2", "head": "5", "lag": 3}], "lag": 3}],
            "truncated": false
        }))
        .unwrap(),
    );
    let browser = start_browser(&hub, &double, settings(None));
    until_installed(hub.http, true).await;

    let (status, page) = get(hub.http, "/api/v1/events?type=orders.Placed&limit=2").await;
    assert_eq!(status, 200, "{page}");
    let positions: Vec<&str> = page["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["position"].as_str().unwrap())
        .collect();
    assert_eq!(positions, ["4", "3"]);
    let cursor = page["cursor"].as_str().unwrap().to_string();
    let (_, next) = get(
        hub.http,
        &format!("/api/v1/events?type=orders.Placed&limit=2&cursor={cursor}"),
    )
    .await;
    assert_eq!(next["results"][0]["position"], "2");
    let (status, record) = get(hub.http, "/api/v1/events/snodra.events/0/1").await;
    assert_eq!(status, 200, "{record}");
    assert_eq!(record["payload"]["hidden"], "payloads-off");
    let (status, sources) = get(hub.http, "/api/v1/events/sources").await;
    assert_eq!(
        (status, &sources["sources"][0]["source"]),
        (200, &Value::from("snodra.events"))
    );
    let (status, lag) = get(hub.http, "/api/v1/events/lag").await;
    assert_eq!(status, 200);
    assert_eq!(lag["subscriptions"][0]["subscription"], "orders.projector");
    assert_eq!(lag["subscriptions"][0]["lag"], 3);
    let (status, missing) = get(hub.http, "/api/v1/events/snodra.events/0/99").await;
    assert_eq!(
        (status, missing["error"].as_str()),
        (404, Some("no event there"))
    );

    // A tail: start, a record, heartbeats while quiet (the hub asks 2 s), and `shutdown` when
    // the browser stops, before its hub connection closes.
    let mut tail = sse(hub.http, "/api/v1/events/tail?type=orders.Placed").await;
    assert_eq!(tail.status, 200);
    assert_eq!(tail.next(Duration::from_secs(5)).await.unwrap().0, "start");
    double.publish(summary(5, "orders.Placed", 0));
    let (name, data) = tail
        .next_not_heartbeat(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(name, "record", "{data}");
    assert_eq!(
        serde_json::from_str::<Value>(&data).unwrap()["position"],
        "5"
    );
    assert_eq!(
        tail.next(Duration::from_secs(5)).await.unwrap().0,
        "heartbeat"
    );
    browser.stop().await;
    let (name, data) = tail
        .next_not_heartbeat(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        (name.as_str(), data.as_str()),
        ("end", "{\"reason\":\"shutdown\"}")
    );
    assert_eq!(tail.next(Duration::from_secs(5)).await, None);

    // Gone: every route is 404 again (the hub keeps the types' owner for its owner grace, but
    // no instance answers).
    for route in ROUTES {
        let (status, body) = get(hub.http, route).await;
        assert_eq!(
            (status, body["error"].as_str()),
            (404, Some("event browser not installed")),
            "{route}"
        );
    }
}

#[tokio::test]
async fn a_reader_that_leaves_frees_the_tail_slot_and_a_busy_browser_is_429() {
    let hub = hub().await;
    let double = Double::new(1);
    let browser = start_browser(&hub, &double, settings(None));
    until_installed(hub.http, true).await;
    let mut first = sse(hub.http, "/api/v1/events/tail").await;
    assert_eq!(first.next(Duration::from_secs(5)).await.unwrap().0, "start");
    assert_eq!(double.free_tails(), 0);
    // The browser's only slot is taken: busy, a 429 with Retry-After.
    let r = reqwest::get(format!("http://{}/api/v1/events/tail", hub.http))
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 429);
    assert_eq!(r.headers()["retry-after"], "1");
    // The reader leaves: the hub cancels, the browser drops the tail and frees the slot.
    drop(first);
    let deadline = Instant::now() + Duration::from_secs(15);
    while double.free_tails() == 0 {
        assert!(Instant::now() < deadline, "the slot was never freed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut again = sse(hub.http, "/api/v1/events/tail").await;
    assert_eq!(again.status, 200);
    assert_eq!(again.next(Duration::from_secs(5)).await.unwrap().0, "start");
    drop(again);
    browser.stop().await;
}

fn free_port() -> SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    probe.local_addr().unwrap()
}

/// Polls the health service until it reports `serving` (15 s at most).
async fn until_health(health: SocketAddr, serving: bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if snodra_event_browser::health::probe(health).await == Ok(serving) {
            return;
        }
        assert!(Instant::now() < deadline, "health never became {serving}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// SERVING while a hub has the browser registered, NOT_SERVING (from the health service still
/// up) once the hub is gone.
#[tokio::test]
async fn health_follows_the_hub_connection() {
    let health = free_port();
    let hub = hub().await;
    let double = Double::new(1);
    let browser = start_browser(&hub, &double, settings(Some(health)));
    until_health(health, true).await;
    hub.stop().await;
    until_health(health, false).await;
    browser.stop().await;
    assert!(
        snodra_event_browser::health::probe(health).await.is_err(),
        "the health service stops with the browser"
    );
}

/// A hub that refuses the browser's Register (its credential lacks the `event-browser` right):
/// health stays NOT_SERVING, and the library retries at its longest backoff, not in a loop.
#[tokio::test]
async fn a_refused_browser_is_not_serving_and_retries_slowly() {
    let token = "a-browser-token-without-the-right";
    let digest: String = Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let acl =
        format!("[[token]]\ntoken_sha256 = \"{digest}\"\nservice = \"snodra-event-browser\"\n");
    let hub = hub_with(
        &[
            ("SNODRA_SERVER_AUTH_MODE", "enforce"),
            ("SNODRA_SERVER_ACL_FILE", "/app/acl.toml"),
        ],
        &[("/app/acl.toml", &acl)],
    )
    .await;
    // A proxy in front of the hub counts the browser's connection attempts.
    let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    {
        let (attempts, target) = (attempts.clone(), hub.grpc);
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = proxy.accept().await {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
    }
    let mut config = snodra_messaging::Config::new(
        snodra_event_browser::SERVICE,
        &format!("static://{proxy_addr}"),
    );
    config.hub_token = Some(snodra_messaging::SecretString::new(token));
    let health = free_port();
    let double = Double::new(1);
    let browser = start_browser_with(config, &double, settings(Some(health)));
    let watched = Instant::now();
    while watched.elapsed() < Duration::from_secs(7) {
        let probe = snodra_event_browser::health::probe(health).await;
        assert!(probe != Ok(true), "a refused browser is never SERVING");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let n = attempts.load(std::sync::atomic::Ordering::SeqCst);
    assert!((1..=3).contains(&n), "{n} attempts in 7 s");
    let (status, body) = get(hub.http, "/api/v1/events").await;
    assert_eq!(
        (status, body["error"].as_str()),
        (404, Some("event browser not installed"))
    );
    browser.stop().await;
}

#[tokio::test]
async fn a_browser_under_another_service_name_does_not_start() {
    let hub = hub().await;
    let double = Double::new(1);
    let config = snodra_messaging::Config::new("orders", &format!("static://{}", hub.grpc));
    let err = snodra_event_browser::serve(
        config,
        settings(None),
        Browsers {
            calls: double.browser(),
            lag: None,
        },
        std::future::pending(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("snodra-event-browser"), "{err}");
}

/// A caller that leaves a search: the hub forgets the request at once (the next search is
/// answered), but a point query has no `Cancel` in the protocol (only streams and
/// subscriptions do), so the browser's search runs to its own end, bounded by its time budget,
/// and its late answer goes nowhere.
#[tokio::test]
async fn a_search_whose_caller_leaves_is_forgotten_by_the_hub_and_ends_at_the_browser() {
    let hub = hub().await;
    let double = Double::new(1).with_events(vec![summary(0, "a.B", 0)]);
    double
        .state
        .lock()
        .unwrap()
        .search_delays
        .push_back(Duration::from_secs(2));
    let browser = start_browser(&hub, &double, settings(None));
    until_installed(hub.http, true).await;
    let left = reqwest::Client::new()
        .get(format!("http://{}/api/v1/events", hub.http))
        .timeout(Duration::from_millis(300))
        .send()
        .await;
    assert!(left.is_err(), "the caller gave up");
    // The hub forgot the request: the next search is answered while the first still runs.
    let (status, page) = get(hub.http, "/api/v1/events").await;
    assert_eq!(status, 200, "{page}");
    assert_eq!(
        double.searches().len(),
        1,
        "only the second search answered so far"
    );
    // The first one ends on its own, never dropped half-way.
    let deadline = Instant::now() + Duration::from_secs(10);
    while double.searches().len() < 2 {
        assert!(Instant::now() < deadline, "the first search never ended");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(double.state.lock().unwrap().searches_dropped, 0);
    browser.stop().await;
}
