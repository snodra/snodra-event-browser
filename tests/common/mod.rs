//! An in-process [`EventBrowser`] double: a list of events of one source, newest last, that a
//! search pages newest first (with its own cursor: the index to go on below), records of any
//! size, a canned sources and lag answer, and tails fed by hand, under the bounds of its
//! settings (a tail slot is held until the tail's stream is dropped). Every request is recorded.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use snodra_messaging::event_store::browse::{
    BrowseError, EndReason, EventBrowser, EventRecord, EventSummary, GetRequest, LagRequest,
    LagView, SearchPage, SearchRequest, SegmentBounds, SourceView, SourcesRequest, SourcesView,
    StoppedBy, TailFeed, TailRequest, TailStart, TailStream, tail_channel,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub const SOURCE: &str = "kraken.events";

/// A summary at `position` of segment `0` with `type` and a header of `pad` bytes.
pub fn summary(position: usize, type_name: &str, pad: usize) -> EventSummary {
    let mut headers = BTreeMap::new();
    headers.insert("type".to_string(), type_name.to_string());
    headers.insert("id".to_string(), format!("e-{position}"));
    if pad > 0 {
        headers.insert("traceparent".to_string(), "t".repeat(pad));
    }
    EventSummary {
        source: SOURCE.to_string(),
        segment: "0".to_string(),
        position: position.to_string(),
        timestamp: Some(1_700_000_000_000 + position as i64),
        key: Some(format!("k-{position}")),
        key_truncated: false,
        headers,
        size: 17,
    }
}

#[derive(Default)]
pub struct State {
    pub events: Vec<EventSummary>,
    pub payload: Option<String>,
    pub lag: Option<LagView>,
    pub sources: Option<SourcesView>,
    pub searches: Vec<SearchRequest>,
    pub lags: Vec<LagRequest>,
    pub tails: Vec<TailFeed>,
    /// Answered by every call while set.
    pub failing: Option<BrowseError>,
    /// Each search waits the next of these first.
    pub search_delays: std::collections::VecDeque<Duration>,
    /// After this many searches, a search finds nothing in its time (`timeBudget`, a cursor).
    pub results_for: Option<usize>,
    /// Searches dropped before they answered (their caller cancelled).
    pub searches_dropped: usize,
}

/// Counts a search dropped before it answered.
struct Unanswered {
    state: Arc<Mutex<State>>,
    answered: bool,
}

impl Drop for Unanswered {
    fn drop(&mut self) {
        if !self.answered {
            self.state.lock().unwrap().searches_dropped += 1;
        }
    }
}

#[derive(Clone)]
pub struct Double {
    pub state: Arc<Mutex<State>>,
    tails: Arc<Semaphore>,
    max_tails: usize,
}

impl Double {
    pub fn new(max_tails: usize) -> Self {
        Double {
            state: Arc::default(),
            tails: Arc::new(Semaphore::new(max_tails)),
            max_tails,
        }
    }

    pub fn with_events(self, events: Vec<EventSummary>) -> Self {
        self.state.lock().unwrap().events = events;
        self
    }

    pub fn browser(&self) -> Arc<dyn EventBrowser> {
        Arc::new(self.clone())
    }

    pub fn searches(&self) -> Vec<SearchRequest> {
        self.state.lock().unwrap().searches.clone()
    }

    /// Feeds `summary` to every open tail.
    pub fn publish(&self, summary: EventSummary) {
        let mut state = self.state.lock().unwrap();
        state.events.push(summary.clone());
        for feed in &mut state.tails {
            feed.record(summary.clone());
        }
    }

    /// Ends every open tail.
    pub fn end_tails(&self, reason: EndReason) {
        for feed in &mut self.state.lock().unwrap().tails {
            feed.end(reason);
        }
    }

    /// Tail slots free now.
    pub fn free_tails(&self) -> usize {
        self.tails.available_permits()
    }

    pub fn max_tails(&self) -> usize {
        self.max_tails
    }

    fn failing(&self) -> Result<(), BrowseError> {
        match &self.state.lock().unwrap().failing {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}

/// Holds a tail slot while the tail's stream lives.
struct Slot<S> {
    inner: S,
    _permit: OwnedSemaphorePermit,
}

impl<S: futures::Stream + Unpin> futures::Stream for Slot<S> {
    type Item = S::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl EventBrowser for Double {
    fn search(&self, request: SearchRequest) -> BoxFuture<'_, Result<SearchPage, BrowseError>> {
        Box::pin(async move {
            self.failing()?;
            let mut unanswered = Unanswered {
                state: Arc::clone(&self.state),
                answered: false,
            };
            let delay = self.state.lock().unwrap().search_delays.pop_front();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            unanswered.answered = true;
            let mut state = self.state.lock().unwrap();
            state.searches.push(request.clone());
            if state.results_for.is_some_and(|n| state.searches.len() > n) {
                return Ok(SearchPage {
                    source: SOURCE.to_string(),
                    missing: false,
                    results: vec![],
                    scanned: 1,
                    exhausted: false,
                    stopped_by: Some(StoppedBy::TimeBudget),
                    cursor: Some(state.events.len().to_string()),
                });
            }
            let upper = match &request.cursor {
                Some(c) => c
                    .parse::<usize>()
                    .map_err(|_| BrowseError::Invalid("the cursor is malformed".into()))?,
                None => state.events.len(),
            };
            let limit = request.limit.unwrap_or(50);
            if limit == 0 {
                return Err(BrowseError::Invalid("limit must be at least 1".into()));
            }
            let wanted = |e: &&EventSummary| match &request.filters.type_name {
                Some(t) => e.headers.get("type") == Some(t),
                None => true,
            };
            let below: Vec<(usize, &EventSummary)> = state.events[..upper.min(state.events.len())]
                .iter()
                .enumerate()
                .rev()
                .filter(|(_, e)| wanted(e))
                .collect();
            let results: Vec<EventSummary> = below
                .iter()
                .take(limit)
                .map(|(_, e)| (*e).clone())
                .collect();
            let more = below.len() > limit;
            let cursor = more.then(|| below[limit - 1].0.to_string());
            Ok(SearchPage {
                source: SOURCE.to_string(),
                missing: false,
                scanned: upper as u64,
                exhausted: !more,
                stopped_by: more.then_some(StoppedBy::Limit),
                cursor,
                results,
            })
        })
    }

    fn get(&self, request: GetRequest) -> BoxFuture<'_, Result<EventRecord, BrowseError>> {
        Box::pin(async move {
            self.failing()?;
            let state = self.state.lock().unwrap();
            let position: usize = request
                .position
                .parse()
                .map_err(|_| BrowseError::Invalid("not a position".into()))?;
            let summary = state
                .events
                .get(position)
                .cloned()
                .ok_or_else(|| BrowseError::NotFound("no event there".into()))?;
            Ok(EventRecord {
                summary,
                all_headers: vec![],
                headers_omitted: 0,
                payload: request.payload.then(|| state.payload.clone()),
                payload_base64: None,
                payload_truncated: false,
            })
        })
    }

    fn sources(&self, _: SourcesRequest) -> BoxFuture<'_, Result<SourcesView, BrowseError>> {
        Box::pin(async move {
            self.failing()?;
            let state = self.state.lock().unwrap();
            Ok(state.sources.clone().unwrap_or_else(|| SourcesView {
                sources: vec![SourceView {
                    source: SOURCE.to_string(),
                    missing: false,
                    segments: vec![SegmentBounds {
                        segment: "0".into(),
                        first: "0".into(),
                        head: state.events.len().to_string(),
                    }],
                }],
            }))
        })
    }

    fn lag(&self, request: LagRequest) -> BoxFuture<'_, Result<LagView, BrowseError>> {
        Box::pin(async move {
            self.failing()?;
            let mut state = self.state.lock().unwrap();
            state.lags.push(request.clone());
            Ok(state.lag.clone().unwrap_or_else(|| LagView {
                source: request.source.unwrap_or_else(|| SOURCE.to_string()),
                missing: false,
                subscriptions: vec![],
                truncated: false,
            }))
        })
    }

    fn tail(&self, request: TailRequest) -> BoxFuture<'_, Result<TailStream, BrowseError>> {
        Box::pin(async move {
            self.failing()?;
            if request.filters.from.is_some() {
                return Err(BrowseError::Invalid("a tail takes no from".into()));
            }
            let permit = Arc::clone(&self.tails)
                .try_acquire_owned()
                .map_err(|_| BrowseError::Busy(format!("{} tails are open", self.max_tails)))?;
            let heartbeat = Duration::from_millis(request.heartbeat_ms.unwrap_or(15_000));
            let (mut feed, stream) = tail_channel(16, heartbeat);
            feed.start(TailStart {
                source: SOURCE.to_string(),
                segments: 1,
                ends_at_ms: 1_900_000_000_000,
            });
            self.state.lock().unwrap().tails.push(feed);
            let stream: TailStream = Box::pin(Slot {
                inner: stream,
                _permit: permit,
            });
            Ok(stream)
        })
    }
}
