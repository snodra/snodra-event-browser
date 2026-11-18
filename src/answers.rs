//! Size discipline towards the server: the server reads a point answer of at most [`MAX_ANSWER`]
//! bytes and a tail part of at most [`MAX_PART`] (more is a 502, and over the gRPC transport's
//! 4 MiB the browser's whole session drops). Every answer is fitted here, whatever the settings:
//!
//! - a record's payload is cut further (`payloadTruncated`) until the JSON-escaped (or base64)
//!   answer fits;
//! - a lag view drops the subscriptions with the least lag until it fits (`truncated`);
//! - a search page that would not fit is asked again for fewer results, so it comes with the
//!   event store's own cursor to the rest;
//! - anything else that still would not fit (a sources list of thousands of segments) is
//!   answered `unavailable` rather than sent;
//! - a tail record over [`MAX_PART`] (never with the library's bounds) is dropped with a warning.
//!
//! Sizes are those of the reply as the library writes it: compact `serde_json`.

use std::time::Duration;

use tokio::time::Instant;

use serde::Serialize;
use snodra_messaging::event_store::browse::{
    BrowseError, EventBrowser, EventRecord, LagView, MAX_CURSOR, SearchPage, SearchRequest,
    TailEvent,
};

/// The longest point answer the hub reads (its `MAX_ANSWER`).
pub const MAX_ANSWER: usize = 2 * 1024 * 1024;
/// The longest tail part the hub reads (its `MAX_PART`).
pub const MAX_PART: usize = 64 * 1024;
/// How many times a search is asked again for fewer results before it gives up.
const SEARCH_ATTEMPTS: usize = 4;
/// The least time a search asked again gets (or a tenth of the budget, if more): a re-ask never
/// runs on a spent budget, which would come back empty.
pub const MIN_REASK: Duration = Duration::from_millis(250);

/// The bytes of `value`'s JSON.
pub fn size<T: Serialize>(value: &T) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len())
}

/// `answer` when its JSON is at most `max` bytes, else `unavailable`.
pub fn bounded<T: Serialize>(answer: T, max: usize) -> Result<T, BrowseError> {
    let size = size(&answer);
    if size <= max {
        Ok(answer)
    } else {
        Err(BrowseError::Unavailable(format!(
            "the answer is {size} bytes, more than the hub takes ({max})"
        )))
    }
}

/// The bytes `c` takes in a JSON string.
fn escaped_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\u{08}' | '\u{0C}' | '\n' | '\r' | '\t' => 2,
        c if (c as u32) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

/// The longest prefix of `text` whose JSON string body takes at most `budget` bytes.
fn cut_escaped(text: &str, budget: usize) -> &str {
    let mut used = 0;
    for (i, c) in text.char_indices() {
        used += escaped_len(c);
        if used > budget {
            return &text[..i];
        }
    }
    text
}

/// `record` with its payload cut (`payloadTruncated`) so that its JSON is at most `max` bytes.
/// A record without a payload, or one that already fits, is unchanged.
pub fn fit_record(mut record: EventRecord, max: usize) -> EventRecord {
    let total = size(&record);
    if total <= max {
        return record;
    }
    if let Some(text) = record.payload.as_mut().and_then(Option::take) {
        record.payload = Some(Some(String::new()));
        let budget = max.saturating_sub(size(&with_truncated(&record)));
        let kept = cut_escaped(&text, budget).to_string();
        record.payload = Some(Some(kept));
        record.payload_truncated = true;
    } else if let Some(base64) = record.payload_base64.take() {
        record.payload_base64 = Some(String::new());
        let budget = max.saturating_sub(size(&with_truncated(&record)));
        // Whole quanta of 4 characters: the prefix is the base64 of a prefix of the bytes.
        let keep = (budget / 4 * 4).min(base64.len());
        record.payload_base64 = Some(base64[..keep].to_string());
        record.payload_truncated = true;
    }
    record
}

fn with_truncated(record: &EventRecord) -> EventRecord {
    let mut r = record.clone();
    r.payload_truncated = true;
    r
}

/// `view` without the subscriptions of least lag (the last ones: the view is sorted by lag,
/// descending) that make its JSON longer than `max` bytes; `truncated` when any was dropped.
pub fn fit_lag(mut view: LagView, max: usize) -> LagView {
    if size(&view) <= max {
        return view;
    }
    let subscriptions = std::mem::take(&mut view.subscriptions);
    view.truncated = true;
    let mut used = size(&view);
    let mut kept = Vec::new();
    for subscription in subscriptions {
        // One more element and its comma.
        let cost = size(&subscription) + 1;
        if used + cost > max {
            break;
        }
        used += cost;
        kept.push(subscription);
    }
    view.subscriptions = kept;
    view
}

/// How many of `page`'s first results fit in `max` bytes with a cursor of any length (at least
/// one).
fn fitting(page: &SearchPage, max: usize) -> usize {
    let mut empty = page.clone();
    empty.results.clear();
    empty.cursor = None;
    // The longest cursor, quoted, in place of `null`.
    let mut used = size(&empty) + MAX_CURSOR + 2;
    let mut count = 0;
    for result in &page.results {
        let cost = size(result) + 1;
        if used + cost > max {
            break;
        }
        used += cost;
        count += 1;
    }
    count.max(1)
}

/// One page of `request` from `browser` whose JSON is at most `max` bytes: a page that would be
/// longer is asked again for as many results as fit, within what is left of the time budget
/// (the request's `timeoutMs`, else `budget`) but never less than [`MIN_REASK`] or a tenth of
/// it, and comes with the event store's cursor to the rest.
///
/// The page in hand is not cut instead: only the event store can write a cursor that goes on
/// right after the last result kept (the binary is store-neutral). Each ask takes the store's
/// read slot anew (the browser API holds a slot per call only). A re-ask that comes back
/// without any result (out of time before it reached what the first ask found) is
/// `unavailable`, rather than an empty first page.
pub async fn search(
    browser: &dyn EventBrowser,
    mut request: SearchRequest,
    budget: Duration,
    max: usize,
) -> Result<SearchPage, BrowseError> {
    let started = Instant::now();
    let budget = request
        .timeout_ms
        .map_or(budget, |ms| Duration::from_millis(ms).min(budget));
    let minimum = MIN_REASK.max(budget / 10);
    for attempt in 0..SEARCH_ATTEMPTS {
        let page = browser.search(request.clone()).await?;
        if attempt > 0 && page.results.is_empty() {
            return Err(BrowseError::Unavailable(
                "a page of these events is larger than the hub takes and no time was left to read a smaller one: ask for fewer (limit)"
                    .to_string(),
            ));
        }
        if size(&page) <= max {
            return Ok(page);
        }
        let fit = fitting(&page, max);
        tracing::debug!(
            results = page.results.len(),
            fit,
            "a search page is larger than the hub takes: asking for fewer results"
        );
        request.limit = Some(fit.min(page.results.len().saturating_sub(1)).max(1));
        let left = budget.saturating_sub(started.elapsed()).max(minimum);
        request.timeout_ms = Some(u64::try_from(left.as_millis()).unwrap_or(u64::MAX));
    }
    Err(BrowseError::Unavailable(
        "a page of these events is larger than the hub takes".to_string(),
    ))
}

/// `part` when its JSON is at most [`MAX_PART`] bytes; a larger record is dropped (`None`)
/// with a warning. A start, heartbeat or end is always small.
pub fn fit_part(part: TailEvent) -> Option<TailEvent> {
    if let TailEvent::Record(summary) = &part {
        let size = size(&part);
        if size > MAX_PART {
            tracing::warn!(
                size,
                segment = %snodra_messaging::event_store::browse::quoted(&summary.segment),
                position = %snodra_messaging::event_store::browse::quoted(&summary.position),
                "a tail record is larger than the hub takes: dropped"
            );
            return None;
        }
    }
    Some(part)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use snodra_messaging::event_store::browse::{
        EventSummary, HeaderView, MAX_HEADER_NAME, MAX_HEADER_VALUE, MAX_HEADERS, MAX_KEY,
        MAX_SUMMARY_HEADER, SegmentLag, SubscriptionLag,
    };

    use super::*;

    /// The worst summary the library writes: every field at its bound, every character one
    /// JSON writes as `\u00XX`.
    pub(crate) fn worst_summary(source: &str) -> EventSummary {
        let control = |n: usize| "\u{1}".repeat(n);
        EventSummary {
            source: source.to_string(),
            segment: control(128),
            position: control(128),
            timestamp: Some(i64::MIN),
            key: Some(control(MAX_KEY)),
            key_truncated: true,
            headers: snodra_messaging::event_store::headers::ALL
                .iter()
                .map(|h| (h.to_string(), control(MAX_SUMMARY_HEADER)))
                .collect::<BTreeMap<_, _>>(),
            size: u64::MAX,
        }
    }

    fn record(payload: Option<Option<String>>, base64: Option<String>) -> EventRecord {
        EventRecord {
            summary: worst_summary("snodra.events"),
            all_headers: (0..MAX_HEADERS)
                .map(|i| HeaderView {
                    name: format!("{i}{}", "\u{1}".repeat(MAX_HEADER_NAME - 3)),
                    value: Some("\u{1}".repeat(MAX_HEADER_VALUE)),
                    value_hex: None,
                    truncated: true,
                })
                .collect(),
            headers_omitted: 7,
            payload,
            payload_base64: base64,
            payload_truncated: false,
        }
    }

    #[test]
    fn a_tail_part_of_the_largest_summary_fits() {
        let part = TailEvent::Record(worst_summary(&"s".repeat(crate::settings::MAX_SOURCE)));
        assert!(size(&part) < MAX_PART, "{} bytes", size(&part));
        assert!(fit_part(part).is_some());
    }

    #[test]
    fn an_oversized_tail_record_is_dropped() {
        let part = TailEvent::Record(worst_summary(&"s".repeat(MAX_PART)));
        assert!(fit_part(part).is_none());
    }

    /// The largest payload the settings allow, every character escaped six-fold, with every
    /// header at its bound: cut to fit, flagged, still valid JSON of the same shape.
    #[test]
    fn a_record_with_the_largest_escaped_payload_is_cut_to_fit() {
        let payload = "\u{1}".repeat(crate::settings::MAX_PAYLOAD_BYTES);
        let r = record(Some(Some(payload.clone())), None);
        assert!(size(&r) > MAX_ANSWER, "the test needs an oversized record");
        let fitted = fit_record(r, MAX_ANSWER);
        assert!(size(&fitted) <= MAX_ANSWER, "{}", size(&fitted));
        assert!(size(&fitted) > MAX_ANSWER - 16, "cut no more than needed");
        assert!(fitted.payload_truncated);
        let text = fitted.payload.clone().unwrap().unwrap();
        assert!(payload.starts_with(&text) && !text.is_empty());
        let back: EventRecord =
            serde_json::from_slice(&serde_json::to_vec(&fitted).unwrap()).unwrap();
        assert_eq!(back, fitted);
    }

    #[test]
    fn a_base64_payload_is_cut_in_whole_quanta() {
        let base64 = "AAAA".repeat(MAX_ANSWER / 4);
        let fitted = fit_record(record(None, Some(base64)), MAX_ANSWER);
        assert!(size(&fitted) <= MAX_ANSWER);
        assert!(fitted.payload_truncated);
        let kept = fitted.payload_base64.unwrap();
        assert_eq!(kept.len() % 4, 0);
        assert!(!kept.is_empty());
    }

    #[test]
    fn a_record_that_fits_is_left_alone() {
        let r = record(Some(Some("{\"total\":42}".into())), None);
        assert_eq!(fit_record(r.clone(), MAX_ANSWER), r);
        let tombstone = record(Some(None), None);
        assert_eq!(fit_record(tombstone.clone(), 10), tombstone);
    }

    fn subscription(name: &str, lag: u64, segments: usize) -> SubscriptionLag {
        SubscriptionLag {
            subscription: name.to_string(),
            state: "Empty".into(),
            members: 0,
            segments: (0..segments)
                .map(|i| SegmentLag {
                    segment: i.to_string(),
                    committed: Some("1000000".into()),
                    head: "2000000".into(),
                    lag: Some(1_000_000),
                })
                .collect(),
            lag,
        }
    }

    #[test]
    fn a_lag_view_drops_the_least_lagging_subscriptions_to_fit() {
        // 500 subscriptions of 1000 segments: some 40 MB.
        let view = LagView {
            source: "snodra.events".into(),
            missing: false,
            subscriptions: (0..500)
                .map(|i| subscription(&format!("s-{i:03}"), 1_000_000 - i, 1000))
                .collect(),
            truncated: false,
        };
        let fitted = fit_lag(view, MAX_ANSWER);
        assert!(size(&fitted) <= MAX_ANSWER, "{}", size(&fitted));
        assert!(fitted.truncated);
        assert!(!fitted.subscriptions.is_empty());
        // The most lagging ones stay, in order.
        assert_eq!(fitted.subscriptions[0].subscription, "s-000");
        let lags: Vec<u64> = fitted.subscriptions.iter().map(|s| s.lag).collect();
        assert!(lags.windows(2).all(|w| w[0] >= w[1]));
        // A view that fits is unchanged.
        let small = LagView {
            source: "snodra.events".into(),
            missing: false,
            subscriptions: vec![subscription("a", 3, 2)],
            truncated: false,
        };
        assert_eq!(fit_lag(small.clone(), MAX_ANSWER), small);
    }

    #[test]
    fn an_answer_over_the_bound_is_unavailable() {
        assert_eq!(bounded(vec![1, 2], 10), Ok(vec![1, 2]));
        assert!(matches!(
            bounded("x".repeat(20), 10),
            Err(BrowseError::Unavailable(m)) if m.contains("more than the hub takes")
        ));
    }

    #[test]
    fn escaping_is_counted_as_json_writes_it() {
        for text in [
            "plain",
            "quote\" back\\slash",
            "\n\t\r\u{8}\u{c}",
            "\u{1}\u{1f}",
            "é€😀",
            "\u{7f}/",
        ] {
            let json = serde_json::to_string(text).unwrap();
            let counted: usize = text.chars().map(escaped_len).sum();
            assert_eq!(counted + 2, json.len(), "{text:?}");
        }
        assert_eq!(cut_escaped("ab\"c", 3), "ab");
        assert_eq!(cut_escaped("ab\"c", 4), "ab\"");
        assert_eq!(cut_escaped("é", 1), "");
    }
}
