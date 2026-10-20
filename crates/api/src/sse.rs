// SPDX-License-Identifier: AGPL-3.0-only

//! `GET /v1/events` — one stream, two event types, and an honest resync.
//!
//! **The daemon has no durable event log and this does not pretend otherwise.** Ids are
//! `{epoch}:{seq}` — an epoch minted at process start plus a monotonic counter — and a bounded
//! ring holds the recent tail. A client reconnecting past the ring, or from a prior epoch, gets
//! `resync` rather than a replay that silently skips events.
//!
//! That is the refusal grammar applied to a stream: the server never claims to have replayed what
//! it does not hold. It costs nothing, because **every event is a hint to refetch and refetching
//! is always correct** — the API is read-only, so a client that resyncs loses no state.

use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// How many events the tail holds. Enough that an ordinary reconnect — a laptop lid, a proxy
/// blip — replays rather than resyncs, and small enough that the buffer is not a second store.
/// A resync is cheap by construction, so erring small costs a refetch and never correctness.
const RING: usize = 256;

/// How many events a subscriber may fall behind before the broadcast drops it. Independent of
/// [`RING`]: this bounds a slow CONSUMER, that bounds reconnect history.
const LAG: usize = 64;

/// How often an idle stream emits a comment frame so a proxy does not reap it.
///
/// Chosen against the smallest default in the path rather than against taste: nginx's
/// `proxy_read_timeout` is 60 seconds, and a third of that survives one missed beat. Not on the
/// re-derivation list, by that list's own rule — no fleet has an opinion about it, and what moves
/// it is a proxy's configuration rather than an observation.
const HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(20);

/// One published event.
#[derive(Debug, Clone)]
pub struct Event {
    pub id: String,
    pub kind: &'static str,
    pub data: Value,
}

/// The publisher, and the tail a reconnect reads from.
#[derive(Clone)]
pub struct Events {
    epoch: String,
    seq: Arc<Mutex<u64>>,
    ring: Arc<Mutex<VecDeque<Event>>>,
    tx: broadcast::Sender<Event>,
}

impl Events {
    /// A fresh epoch. **Minted per process, which is what makes a daemon restart legible**: the
    /// sessions die at the same instant, so a client discovers both at once rather than resuming
    /// a stream whose history it cannot account for.
    #[must_use]
    pub fn new() -> Self {
        let epoch = analyzer_serving_clickhouse::engine::server::random_token()
            .map_or_else(|_| "0".to_string(), |t| t[..8].to_string());
        Self {
            epoch,
            seq: Arc::new(Mutex::new(0)),
            ring: Arc::new(Mutex::new(VecDeque::with_capacity(RING))),
            tx: broadcast::channel(LAG).0,
        }
    }

    /// An applied tick: the classified day, the window, and the meaning it was computed under.
    ///
    /// **The discriminator rides along** so a parameter change arriving mid-session is visible as
    /// a seam rather than as figures that quietly moved.
    pub fn tick_applied(
        &self,
        classified_day: &str,
        window: &analyzer_analysis::Window,
        discriminator: &str,
        at_ms: f64,
    ) {
        // Built from the typed payload rather than an inline `json!`, so the document's schema
        // and these bytes cannot describe different shapes.
        self.publish(
            "tick_applied",
            payload(&crate::wire::TickAppliedEvent {
                classified_day: classified_day.to_string(),
                window: crate::wire::WindowJson::of(window),
                discriminator: discriminator.to_string(),
                semantics_version: analyzer_analysis::SEMANTICS_VERSION.to_string(),
                at_ms,
            }),
        );
    }

    /// An ingested batch: the segment, its record count and its span.
    ///
    /// **Wall-clock facts, and they stay wall-clock facts**. These feed
    /// the dark status strip; no accounting figure rides this stream, and nothing here carries
    /// coverage because none of it is a measurement of the fleet.
    pub fn ingest_batch(&self, segment: &str, records: u64, span: (f64, f64), at_ms: f64) {
        self.publish(
            "ingest_batch",
            payload(&crate::wire::IngestBatchEvent {
                segment: segment.to_string(),
                records,
                span: crate::wire::SpanJson {
                    start_ms: span.0,
                    end_ms: span.1,
                    domain: "engine",
                },
                at_ms,
            }),
        );
    }

    fn publish(&self, kind: &'static str, data: Value) {
        let Ok(mut seq) = self.seq.lock() else { return };
        *seq += 1;
        let event = Event {
            id: format!("{}:{}", self.epoch, *seq),
            kind,
            data,
        };
        drop(seq);
        if let Ok(mut ring) = self.ring.lock() {
            if ring.len() == RING {
                ring.pop_front();
            }
            ring.push_back(event.clone());
        }
        // A send with no subscribers is not an error: nobody is watching, and the ring still
        // holds the event for whoever connects next.
        let _ = self.tx.send(event);
    }

    /// A live subscription, taken BEFORE the tail it will be resumed from — the only order
    /// in which the two together lose nothing, and the type admits no other.
    #[must_use = "a subscription is resumed from the tail, then streamed"]
    pub fn subscribed(&self) -> Subscribed {
        Subscribed(self.tx.subscribe())
    }

    /// What a reconnecting client gets for its `Last-Event-ID`.
    ///
    /// Three cases, each decided rather than defaulted — see the module header. Reached only
    /// through [`Subscribed::resume`]: a tail read before the subscription exists has a hole
    /// in front of it, for every event published between the two.
    fn resume(&self, last_event_id: Option<&str>) -> Resume {
        let Some(id) = last_event_id else {
            return Resume::Fresh;
        };
        let Some((epoch, seq)) = id.split_once(':') else {
            return Resume::Resync("the event id is not {epoch}:{seq}");
        };
        if epoch != self.epoch {
            return Resume::Resync(
                "the analyzer restarted, so this stream is a new epoch and its history is not \
                 continuous with yours",
            );
        }
        let Ok(seq) = seq.parse::<u64>() else {
            return Resume::Resync("the event id's sequence is not a number");
        };
        let Ok(ring) = self.ring.lock() else {
            return Resume::Resync("the event tail is unavailable");
        };
        let tail: Vec<Event> = ring
            .iter()
            .filter(|e| event_seq(e).is_some_and(|s| s > seq))
            .cloned()
            .collect();
        // Held in the ring means the OLDEST retained event is at or before the one asked for.
        // Otherwise there is a hole between them, and streaming the tail would silently skip it.
        let contiguous = ring
            .front()
            .and_then(event_seq)
            .is_some_and(|oldest| oldest <= seq + 1);
        if contiguous {
            Resume::Replay(tail)
        } else {
            Resume::Resync("the events since yours are no longer buffered")
        }
    }

    #[must_use]
    pub fn epoch(&self) -> &str {
        &self.epoch
    }
}

impl Default for Events {
    fn default() -> Self {
        Self::new()
    }
}

/// A live subscription that no tail has yet been read for. An event published from here on
/// reaches the subscription; reading the tail afterwards may find it there too, and the
/// stream renders whichever copy comes first, once.
#[derive(Debug)]
pub struct Subscribed(broadcast::Receiver<Event>);

impl Subscribed {
    /// The handoff: the tail owed to `last_event_id`, read now that this subscription exists.
    #[must_use]
    pub fn resume(self, events: &Events, last_event_id: Option<&str>) -> Handoff {
        Handoff {
            receiver: self.0,
            resume: events.resume(last_event_id),
        }
    }
}

/// A reconnect's two halves in the order that loses nothing: the subscription, then the tail.
#[derive(Debug)]
pub struct Handoff {
    receiver: broadcast::Receiver<Event>,
    resume: Resume,
}

/// What a reconnect is owed.
#[derive(Debug, Clone, PartialEq)]
pub enum Resume {
    /// No `Last-Event-ID`: stream live from here.
    Fresh,
    /// The gap is held; these are the events since.
    Replay(Vec<Event>),
    /// The gap is not held. **Say so** — the client refetches rather than assuming continuity.
    Resync(&'static str),
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.kind == other.kind && self.data == other.data
    }
}

/// A typed payload as the `data:` line's value.
///
/// A serialization failure here would be a bug in the type, not a runtime condition — so it
/// degrades to a payload that SAYS so rather than to silence, which on an event stream would
/// look like nothing happened.
fn payload<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value)
        .unwrap_or_else(|e| json!({ "error": format!("event payload failed to serialize: {e}") }))
}

fn event_seq(e: &Event) -> Option<u64> {
    e.id.split_once(':')?.1.parse().ok()
}

/// The SSE response for a subscriber, honouring its `Last-Event-ID`.
///
/// **A resync is an EVENT, not an error.** The client stays connected and refetches; disconnecting
/// it would turn a bounded gap in the buffer into an outage on a surface whose whole job is to say
/// when something changed.
#[must_use]
pub fn stream(events: &Events, last_event_id: Option<&str>) -> axum::response::Response {
    stream_from(events, events.subscribed().resume(events, last_event_id))
}

/// The response for one handoff: the tail as a prelude, then the subscription live. An
/// event in both — published after the subscription began and before the tail was read — is
/// rendered from the tail and dropped from the live half by its sequence.
fn stream_from(events: &Events, handoff: Handoff) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    use tokio_stream::StreamExt as _;

    let Handoff { receiver, resume } = handoff;
    let mut prelude = String::new();
    let mut replayed_through: Option<u64> = None;
    match resume {
        Resume::Fresh => {}
        Resume::Replay(tail) => {
            for e in &tail {
                prelude.push_str(&render(e));
            }
            replayed_through = tail.iter().filter_map(event_seq).max();
        }
        Resume::Resync(why) => {
            prelude.push_str(&render(&Event {
                id: format!("{}:0", events.epoch()),
                kind: "resync",
                data: payload(&crate::wire::ResyncEvent {
                    reason: why.to_string(),
                    epoch: events.epoch().to_string(),
                }),
            }));
        }
    }

    // The prelude first, then the live tail. A subscriber that falls behind `LAG` is dropped by
    // the broadcast with a `Lagged` error; that becomes a `resync` frame rather than a
    // disconnect, for the same reason as above — the client refetches and keeps its stream.
    let live = tokio_stream::wrappers::BroadcastStream::new(receiver).filter_map(move |r| {
        Some(Ok::<_, std::convert::Infallible>(match r {
            // Already in the prelude: the tail was read after this subscription began.
            Ok(e) if replayed_through.is_some_and(|last| event_seq(&e) <= Some(last)) => {
                return None;
            }
            Ok(e) => render(&e),
            Err(_) => render(&Event {
                id: String::from("0:0"),
                kind: "resync",
                data: payload(&crate::wire::ResyncEvent {
                    reason: "this connection fell behind the event buffer".to_string(),
                    epoch: String::new(),
                }),
            }),
        }))
    });
    // **The heartbeat.** Events are minutes apart and read-api section 6.5 puts an operator's proxy in front
    // of every production deployment. nginx's default `proxy_read_timeout` is 60 seconds, so an
    // idle stream is torn down on a schedule; the console then reconnects and replays, which the
    // reader sees as the live view going quiet and then jumping. A comment frame is ignored by
    // every `EventSource` implementation and is enough to keep the connection accounted for.
    //
    // `skip(1)` because a tokio interval fires immediately, and the prelude has just been written.
    let beat = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(HEARTBEAT))
        .skip(1)
        .map(|_| Ok::<_, std::convert::Infallible>(": keepalive\n\n".to_string()));
    let body =
        tokio_stream::once(Ok::<_, std::convert::Infallible>(prelude)).chain(live.merge(beat));

    (
        [
            (axum::http::header::CONTENT_TYPE, "text/event-stream"),
            (axum::http::header::CACHE_CONTROL, "no-store"),
            // Proxies that buffer will hold an event stream until it is useless.
            (
                axum::http::HeaderName::from_static("x-accel-buffering"),
                "no",
            ),
        ],
        axum::body::Body::from_stream(body),
    )
        .into_response()
}

fn render(e: &Event) -> String {
    format!("id: {}\nevent: {}\ndata: {}\n\n", e.id, e.kind, e.data)
}

#[cfg(test)]
mod tests {
    use super::{Events, Resume};

    /// **Asserted on a frozen clock rather than by waiting.** The beat exists because events
    /// are minutes apart and read-api section 6.5 puts an operator's proxy in front of every deployment; nginx
    /// reaps an idle upstream at 60 seconds by default, and the console sees the live view go
    /// quiet and then jump when it reconnects and replays.
    ///
    /// The two things worth pinning are both easy to get wrong and invisible if you do: that a
    /// beat arrives at all on an idle stream, and that merging it did not displace the events.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_idle_stream_beats_so_a_proxy_does_not_reap_it() {
        use tokio_stream::StreamExt as _;

        let events = Events::new();
        let response = super::stream(&events, None);
        let mut body = response.into_body().into_data_stream();

        // The prelude: a fresh subscriber has nothing to replay, so this is the empty string.
        let prelude = body.next().await.expect("prelude").expect("bytes");
        assert!(prelude.is_empty(), "a fresh connection replays nothing");

        // Nothing has been published and nothing will be. Without the beat this is where the
        // connection sits silent until the proxy kills it.
        //
        // **Bounded, because the absence of a beat is a HANG rather than a wrong value.** An
        // unbounded `next()` here waits forever when the merge is broken, and a test that hangs
        // in CI is worse than one that fails: it reports nothing, blocks the runner, and gets
        // marked flaky. The paused clock makes the bound instant — with no interval left to fire,
        // auto-advance runs straight to this deadline.
        let beat = tokio::time::timeout(super::HEARTBEAT * 3, body.next())
            .await
            .expect("an idle stream must beat; it stayed silent, so a proxy would reap it")
            .expect("a frame")
            .expect("bytes");
        let beat = String::from_utf8(beat.to_vec()).expect("utf8");
        assert_eq!(
            beat, ": keepalive\n\n",
            "an SSE comment frame: ignored by every EventSource, and enough to keep the \
             connection accounted for"
        );

        // And the beat did not take the stream's real job with it.
        events.publish("tick", serde_json::json!({"n": 1}));
        let frame = loop {
            let next = body.next().await.expect("a frame").expect("bytes");
            let next = String::from_utf8(next.to_vec()).expect("utf8");
            if !next.starts_with(':') {
                break next;
            }
        };
        assert!(
            frame.contains("event: tick"),
            "merging the heartbeat must not displace the events: {frame}"
        );
    }

    /// The next frame that is not a keepalive comment.
    async fn next_event(
        body: &mut (impl tokio_stream::Stream<Item = Result<axum::body::Bytes, axum::Error>> + Unpin),
    ) -> String {
        use tokio_stream::StreamExt as _;
        loop {
            let next = body.next().await.expect("a frame").expect("bytes");
            let next = String::from_utf8(next.to_vec()).expect("utf8");
            if !next.starts_with(':') {
                break next;
            }
        }
    }

    /// The handoff between the replayed tail and the live subscription loses nothing and
    /// repeats nothing: an event published after the handoff arrives live, once.
    #[tokio::test(flavor = "current_thread")]
    async fn a_reconnect_replays_the_tail_once_and_then_streams_live() {
        use tokio_stream::StreamExt as _;

        let events = seeded(3);
        let last = format!("{}:1", events.epoch());
        let response = super::stream(&events, Some(&last));
        let mut body = response.into_body().into_data_stream();

        let prelude = body.next().await.expect("prelude").expect("bytes");
        let prelude = String::from_utf8(prelude.to_vec()).expect("utf8");
        assert_eq!(
            prelude.matches("event: ingest_batch").count(),
            2,
            "events 2 and 3 replay: {prelude}"
        );
        events.ingest_batch("0004", 10, (0.0, 1.0), 0.0);
        let frame = next_event(&mut body).await;
        assert!(
            frame.contains(&format!("id: {}:4", events.epoch())),
            "the fourth event arrives live, once: {frame}"
        );
    }

    /// **The race itself.** An event published after the subscription began and before the
    /// tail was read is in both halves. It is rendered once, from the tail; the live half
    /// drops its copy, and the next live frame is the event after it. The two halves are
    /// taken here by hand, in the order the type allows, with the publication between them.
    #[tokio::test(flavor = "current_thread")]
    async fn an_event_landing_between_the_subscription_and_the_tail_is_rendered_once() {
        use tokio_stream::StreamExt as _;

        let events = seeded(3);
        let subscribed = events.subscribed();
        events.ingest_batch("0004", 10, (0.0, 1.0), 0.0);
        let last = format!("{}:1", events.epoch());
        let handoff = subscribed.resume(&events, Some(&last));

        let response = super::stream_from(&events, handoff);
        let mut body = response.into_body().into_data_stream();
        let prelude = body.next().await.expect("prelude").expect("bytes");
        let prelude = String::from_utf8(prelude.to_vec()).expect("utf8");
        let id4 = format!("id: {}:4", events.epoch());
        assert_eq!(
            prelude.matches("event: ingest_batch").count(),
            3,
            "events 2, 3 and 4 replay: {prelude}"
        );
        assert_eq!(prelude.matches(&id4).count(), 1, "{prelude}");

        events.ingest_batch("0005", 10, (0.0, 1.0), 0.0);
        let frame = next_event(&mut body).await;
        assert!(
            !frame.contains(&id4),
            "the fourth event's live copy is dropped, not repeated: {frame}"
        );
        assert!(
            frame.contains(&format!("id: {}:5", events.epoch())),
            "the fifth event arrives live, once: {frame}"
        );
    }

    fn seeded(n: u64) -> Events {
        let events = Events::new();
        for i in 0..n {
            events.ingest_batch(&format!("{i:04}"), 10, (0.0, 1.0), 0.0);
        }
        events
    }

    #[test]
    fn a_fresh_connection_streams_from_now() {
        assert_eq!(seeded(3).resume(None), Resume::Fresh);
    }

    #[test]
    fn a_held_gap_replays_exactly_the_events_since() {
        let events = seeded(5);
        let Resume::Replay(tail) = events.resume(Some(&format!("{}:3", events.epoch()))) else {
            panic!("a gap inside the ring must replay");
        };
        assert_eq!(tail.len(), 2, "events 4 and 5");
    }

    #[test]
    fn a_prior_epoch_resyncs_rather_than_pretending_to_replay() {
        // The daemon restarted. Its counter starts over, so seq 3 of THIS epoch is a different
        // event from seq 3 of the last one — replaying would hand the client the wrong history
        // while looking entirely successful.
        let events = seeded(5);
        assert!(matches!(
            events.resume(Some("deadbeef:3")),
            Resume::Resync(_)
        ));
    }

    #[test]
    fn a_malformed_id_resyncs_instead_of_erroring_the_client_off_the_stream() {
        let events = seeded(2);
        assert!(matches!(events.resume(Some("nonsense")), Resume::Resync(_)));
        assert!(matches!(
            events.resume(Some(&format!("{}:not-a-number", events.epoch()))),
            Resume::Resync(_)
        ));
    }

    #[test]
    fn an_event_carries_its_epoch_so_two_processes_ids_never_collide() {
        let (a, b) = (Events::new(), Events::new());
        assert_ne!(a.epoch(), b.epoch(), "epochs are per process");
    }
}
