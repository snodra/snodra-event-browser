//! `kraken-event-browser`: the hub's event browser add-on (ADR 0027). It connects to every hub
//! like any service (the library's hub client, service `kraken-event-browser`), handles the
//! reserved query types `kraken.events.Search`, `Get`, `Sources`, `Lag` (point queries) and
//! `Tail` (a streaming query), and answers them from the browser of the event store add-on
//! chosen by `KRAKEN_EVENT_STORE` (an [`EventBrowser`] opened alone, without the rest of the
//! store: [`store`]). The hub keeps the dashboard, the payload rules and the
//! audit; the browser keeps the bounds of the reads ([`settings`]), fits every answer to what the
//! hub takes ([`answers`]), ends its tails with `shutdown` when it stops ([`tails`]) and exports
//! the subscriptions' lag ([`lag`]).

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use kraken_messaging::event_store::browse::{
    BrowseError, EventBrowser, EventRecord, GetRequest, LagRequest, LagView, SearchPage,
    SearchRequest, SourcesRequest, SourcesView, TailEvent, TailRequest,
};
use kraken_messaging::{Config, KrakenMessaging, MessageContext, MessagingError};
use tokio_util::sync::CancellationToken;

pub mod answers;
pub mod health;
pub mod lag;
pub mod settings;
pub mod store;
pub mod tails;
pub mod telemetry;

pub use settings::{SERVICE, Settings};
pub use tails::Tails;

/// How long a stopping browser waits for its tails to hand their `end` to the library.
pub const TAIL_END_GRACE: Duration = Duration::from_secs(5);
/// How often the health service's status follows the hub connections.
const READINESS_PERIOD: Duration = Duration::from_millis(500);
/// The longest header value a log line carries.
const MAX_LOGGED: usize = 128;

/// The browser's answers to the five queries, each fitted to what the hub takes.
#[derive(Clone)]
pub struct Handlers {
    browser: Arc<dyn EventBrowser>,
    /// A search's time budget when the request names none (`KRAKEN_EVENT_BROWSER_TIMEOUT_MS`).
    timeout: Duration,
    tails: Tails,
}

impl Handlers {
    pub fn new(browser: Arc<dyn EventBrowser>, timeout: Duration, tails: Tails) -> Self {
        Handlers {
            browser,
            timeout,
            tails,
        }
    }

    pub async fn search(&self, request: SearchRequest) -> Result<SearchPage, BrowseError> {
        let page =
            answers::search(&*self.browser, request, self.timeout, answers::MAX_ANSWER).await?;
        answers::bounded(page, answers::MAX_ANSWER)
    }

    pub async fn get(&self, request: GetRequest) -> Result<EventRecord, BrowseError> {
        let record = self.browser.get(request).await?;
        answers::bounded(
            answers::fit_record(record, answers::MAX_ANSWER),
            answers::MAX_ANSWER,
        )
    }

    pub async fn sources(&self, request: SourcesRequest) -> Result<SourcesView, BrowseError> {
        answers::bounded(self.browser.sources(request).await?, answers::MAX_ANSWER)
    }

    pub async fn lag(&self, request: LagRequest) -> Result<LagView, BrowseError> {
        let view = self.browser.lag(request).await?;
        answers::bounded(
            answers::fit_lag(view, answers::MAX_ANSWER),
            answers::MAX_ANSWER,
        )
    }

    pub async fn tail(
        &self,
        request: TailRequest,
    ) -> Result<BoxStream<'static, TailEvent>, BrowseError> {
        let stream = self.browser.tail(request).await?;
        Ok(self.tails.watch(stream))
    }
}

/// `s` cut to [`MAX_LOGGED`] characters (a header is the sender's choice).
fn clip(s: &str) -> String {
    match s.char_indices().nth(MAX_LOGGED) {
        Some((end, _)) => s[..end].to_string(),
        None => s.to_string(),
    }
}

/// One line per call: the query, who the hub says asked (`userId`) and the correlation id, the
/// outcome. Informational: any sender allowed to send the types can set those headers.
fn logged<T>(kind: &'static str, ctx: &MessageContext, result: &Result<T, BrowseError>) {
    let user = ctx.headers.get("userId").map(|u| clip(u));
    let correlation_id = clip(&ctx.correlation_id);
    match result {
        Ok(_) => tracing::info!(
            kind,
            user_id = user.as_deref(),
            correlation_id,
            "event browser call answered"
        ),
        Err(e) => tracing::info!(
            kind,
            user_id = user.as_deref(),
            correlation_id,
            error = e.code(),
            "event browser call refused"
        ),
    }
}

/// The library's handler error (`HANDLER_ERROR` with the [`BrowseError`] JSON as its details).
fn answered<T>(
    kind: &'static str,
    ctx: &MessageContext,
    result: Result<T, BrowseError>,
) -> Result<T, MessagingError> {
    logged(kind, ctx, &result);
    result.map_err(MessagingError::from)
}

/// The event stores' browsers the process runs on: one for the hub's calls and, when the lag
/// metric is on, one of its own.
pub struct Browsers {
    pub calls: Arc<dyn EventBrowser>,
    pub lag: Option<Arc<dyn EventBrowser>>,
}

impl Browsers {
    /// The browsers of the event store `settings` choose.
    pub fn open(settings: &Settings) -> Result<Browsers, String> {
        Ok(Browsers {
            calls: settings.store.open(settings.browse.clone())?,
            lag: match settings.lag_metric() {
                Some(_) => Some(settings.store.open(settings.lag_browse())?),
                None => None,
            },
        })
    }
}

/// Runs the browser until `shutdown`: registers the five query types with every hub of
/// `config` (the library's hub client), serves the health service, refreshes the lag metric.
/// When `shutdown` completes: health goes `NOT_SERVING`, every open tail ends with `shutdown`
/// (at most [`TAIL_END_GRACE`]), then the library closes the hub connections.
pub async fn serve(
    config: Config,
    settings: Settings,
    browsers: Browsers,
    shutdown: impl Future<Output = ()>,
) -> Result<(), MessagingError> {
    if config.service_name != SERVICE {
        return Err(MessagingError::InvalidMessage(format!(
            "the event browser registers as {SERVICE}, not {}",
            clip(&config.service_name)
        )));
    }
    for name in &settings.unknown {
        tracing::warn!(
            variable = %clip(name),
            "unknown KRAKEN_EVENT_BROWSER_* variable: not read"
        );
    }
    let tails = Tails::new();
    let handlers = Handlers::new(browsers.calls, settings.browse.timeout, tails.clone());
    let stop = CancellationToken::new();
    let health = match settings.health_listen {
        Some(addr) => Some(health::serve(addr, stop.clone()).await?),
        None => None,
    };
    let (search, get, sources, lag, tail) = (
        handlers.clone(),
        handlers.clone(),
        handlers.clone(),
        handlers.clone(),
        handlers,
    );
    let started = async {
        KrakenMessaging::builder(config)
            .await?
            .query::<SearchRequest, _, _>(move |request, ctx| {
                let h = search.clone();
                async move { answered("search", &ctx, h.search(request).await) }
            })
            .query::<GetRequest, _, _>(move |request, ctx| {
                let h = get.clone();
                async move { answered("get", &ctx, h.get(request).await) }
            })
            .query::<SourcesRequest, _, _>(move |request, ctx| {
                let h = sources.clone();
                async move { answered("sources", &ctx, h.sources(request).await) }
            })
            .query::<LagRequest, _, _>(move |request, ctx| {
                let h = lag.clone();
                async move { answered("lag", &ctx, h.lag(request).await) }
            })
            .streaming_query::<TailRequest, _, _, _>(move |request, ctx| {
                let h = tail.clone();
                async move {
                    answered("tail", &ctx, h.tail(request).await)
                        .map(|parts| parts.map(Ok::<_, MessagingError>))
                }
            })
            .start()
            .await
    };
    let app = match started.await {
        Ok(app) => app,
        Err(e) => {
            // The health service stops with the browser that did not start.
            stop.cancel();
            return Err(e);
        }
    };
    tracing::info!(
        instance = app.instance_id(),
        sources = ?settings.browse.sources,
        event_store = settings.store.name(),
        "event browser started"
    );
    let mut tasks = Vec::new();
    match (settings.lag_interval, &settings.otlp_endpoint) {
        (None, _) => tracing::info!("lag metric off (KRAKEN_EVENT_BROWSER_LAG_INTERVAL_MS=0)"),
        (Some(_), None) => tracing::info!(
            "lag metric off: no KRAKEN_OTLP_ENDPOINT to export it to (no second event store connection, no periodic listing)"
        ),
        (Some(_), Some(_)) => {}
    }
    if let (Some(interval), Some(browser)) = (settings.lag_metric(), browsers.lag) {
        let metric = Arc::new(lag::LagMetric::new(&opentelemetry::global::meter(SERVICE)));
        tasks.push(tokio::spawn(lag::run(
            metric,
            browser,
            settings.browse.sources.clone(),
            interval,
            stop.clone(),
        )));
    }
    let app = Arc::new(app);
    if let Some(health) = &health {
        let (app, health, stop) = (Arc::clone(&app), health.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(READINESS_PERIOD);
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => return,
                    _ = ticker.tick() => health.set(!app.hub_ids().is_empty()).await,
                }
            }
        }));
    }
    shutdown.await;
    tracing::info!(open_tails = tails.open(), "event browser stopping");
    stop.cancel();
    if let Some(health) = &health {
        health.set(false).await;
    }
    if !tails.end_all(TAIL_END_GRACE).await {
        tracing::warn!(
            open = tails.open(),
            "tails still open after their shutdown end; closing the hub connections anyway"
        );
    }
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    match Arc::try_unwrap(app) {
        Ok(app) => app.shutdown().await,
        Err(_) => tracing::warn!("the messaging is still shared at shutdown"),
    }
    if let Some(health) = health {
        health.stopped().await;
    }
    tracing::info!("event browser stopped");
    Ok(())
}

/// `serve` on the event store `settings` choose.
pub async fn run(
    config: Config,
    settings: Settings,
    shutdown: impl Future<Output = ()>,
) -> Result<(), MessagingError> {
    let browsers = Browsers::open(&settings).map_err(MessagingError::InvalidMessage)?;
    serve(config, settings, browsers, shutdown).await
}

/// Ctrl-C or SIGTERM.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    futures::future::select(ctrl_c.boxed(), terminate.boxed()).await;
}

/// The address the `--health-check` probe asks: the health listener's port on the loopback.
pub fn probe_address(listen: SocketAddr) -> SocketAddr {
    let ip = if listen.ip().is_unspecified() {
        match listen {
            SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
            SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        }
    } else {
        listen.ip()
    };
    SocketAddr::new(ip, listen.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(vars: &[(&str, &str)]) -> Settings {
        Settings::from_vars(
            &vars
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
        .unwrap()
    }

    /// Without an endpoint nothing reads the lag gauge: no second event store connection.
    #[test]
    fn the_lag_metric_has_its_own_browser_only_when_metrics_are_exported() {
        let off = Browsers::open(&settings(&[("KRAKEN_KAFKA_BOOTSTRAP", "127.0.0.1:1")])).unwrap();
        assert!(off.lag.is_none());
        let on = Browsers::open(&settings(&[
            ("KRAKEN_KAFKA_BOOTSTRAP", "127.0.0.1:1"),
            ("KRAKEN_OTLP_ENDPOINT", "http://127.0.0.1:4317"),
        ]))
        .unwrap();
        assert!(on.lag.is_some());
        let zero = Browsers::open(&settings(&[
            ("KRAKEN_KAFKA_BOOTSTRAP", "127.0.0.1:1"),
            ("KRAKEN_OTLP_ENDPOINT", "http://127.0.0.1:4317"),
            ("KRAKEN_EVENT_BROWSER_LAG_INTERVAL_MS", "0"),
        ]))
        .unwrap();
        assert!(zero.lag.is_none());
    }
}
