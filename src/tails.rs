//! The open tails: each one the event store's tail stream as it is, fitted to the hub's part
//! bound, that ends with `shutdown` when the browser stops ([`Tails::end_all`]), after its
//! `start` and before the hub connection closes. A tail whose request the hub cancelled is
//! dropped by the library, which drops the event store's stream and frees its slot.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use kraken_messaging::event_store::browse::{EndReason, TailEnd, TailEvent, TailStream};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::answers;

/// Every tail open on this browser.
#[derive(Clone, Default)]
pub struct Tails {
    stop: CancellationToken,
    open: Arc<Open>,
}

#[derive(Default)]
struct Open {
    count: AtomicUsize,
    changed: Notify,
}

/// Counts one open tail until dropped.
struct Counted(Arc<Open>);

impl Counted {
    fn new(open: &Arc<Open>) -> Self {
        open.count.fetch_add(1, Ordering::SeqCst);
        Counted(Arc::clone(open))
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}

/// Where a watched tail is.
struct Watched {
    stream: TailStream,
    stop: CancellationToken,
    started: bool,
    ended: bool,
    _counted: Counted,
}

impl Tails {
    pub fn new() -> Self {
        Self::default()
    }

    /// Tails open now.
    pub fn open(&self) -> usize {
        self.open.count.load(Ordering::SeqCst)
    }

    /// `stream` as the hub gets it: its parts fitted to the hub's bound, its `start` first,
    /// `end` `shutdown` once [`end_all`](Self::end_all) is called, nothing after an `end`.
    pub fn watch(&self, stream: TailStream) -> BoxStream<'static, TailEvent> {
        let state = Watched {
            stream,
            stop: self.stop.clone(),
            started: false,
            ended: false,
            _counted: Counted::new(&self.open),
        };
        futures::stream::unfold(state, |mut state| async move {
            loop {
                if state.ended {
                    return None;
                }
                let next = if state.started {
                    tokio::select! {
                        biased;
                        _ = state.stop.cancelled() => Some(TailEvent::End(TailEnd {
                            reason: EndReason::Shutdown,
                        })),
                        part = state.stream.next() => part,
                    }
                } else {
                    // The start is already there: it goes first, even when stopping.
                    state.stream.next().await
                };
                let part = next?;
                state.started = true;
                state.ended = matches!(part, TailEvent::End(_));
                if let Some(part) = answers::fit_part(part) {
                    return Some((part, state));
                }
            }
        })
        .boxed()
    }

    /// Ends every open tail with `shutdown` and waits (at most `within`) until each has been
    /// handed back to the library; `false` when some were still open.
    pub async fn end_all(&self, within: Duration) -> bool {
        self.stop.cancel();
        let wait = async {
            loop {
                let changed = self.open.changed.notified();
                if self.open() == 0 {
                    return;
                }
                changed.await;
            }
        };
        tokio::time::timeout(within, wait).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use kraken_messaging::event_store::browse::{Heartbeat, TailStart, tail_channel};

    use super::*;

    fn start() -> TailStart {
        TailStart {
            source: "kraken.events".into(),
            segments: 3,
            ends_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn a_tail_ends_with_shutdown_after_its_start_and_frees_its_count() {
        let tails = Tails::new();
        let (mut feed, stream) = tail_channel(16, Duration::from_secs(3600));
        feed.start(start());
        let mut watched = tails.watch(stream);
        assert_eq!(tails.open(), 1);
        // Stopping before the start was read: the start still comes first.
        let ending = {
            let tails = tails.clone();
            tokio::spawn(async move { tails.end_all(Duration::from_secs(5)).await })
        };
        assert!(matches!(watched.next().await, Some(TailEvent::Start(_))));
        assert_eq!(
            watched.next().await,
            Some(TailEvent::End(TailEnd {
                reason: EndReason::Shutdown
            }))
        );
        assert_eq!(watched.next().await, None, "nothing after the end");
        drop(watched);
        assert!(ending.await.unwrap(), "every tail handed back");
        assert_eq!(tails.open(), 0);
        drop(feed);
    }

    #[tokio::test]
    async fn the_event_stores_parts_pass_and_its_end_is_the_last() {
        let tails = Tails::new();
        let (mut feed, stream) = tail_channel(16, Duration::from_secs(3600));
        feed.start(start());
        let mut watched = tails.watch(stream);
        assert!(matches!(watched.next().await, Some(TailEvent::Start(_))));
        feed.end(EndReason::MaxDuration);
        assert_eq!(
            watched.next().await,
            Some(TailEvent::End(TailEnd {
                reason: EndReason::MaxDuration
            }))
        );
        assert_eq!(watched.next().await, None);
        let _ = Heartbeat { now_ms: 0 };
    }

    #[tokio::test]
    async fn a_dropped_tail_drops_the_event_stores_stream() {
        let tails = Tails::new();
        let (mut feed, stream) = tail_channel(16, Duration::from_secs(3600));
        feed.start(start());
        let watched = tails.watch(stream);
        assert!(!feed.is_closed());
        drop(watched);
        assert!(
            feed.is_closed(),
            "the reader left: the event store frees its slot"
        );
        assert_eq!(tails.open(), 0);
        assert!(tails.end_all(Duration::from_millis(10)).await);
    }
}
