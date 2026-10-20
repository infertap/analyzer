// SPDX-License-Identifier: AGPL-3.0-only

//! `/metrics`: the scrape endpoint the textfile rendering always anticipated.
//!
//! This module is one of the workspace's two listeners, and the static audit names it as
//! an exception (`network_use_is_confined_to_our_io_layer_and_the_engine_reaches_nothing`).
//! What keeps the exception narrow is construction, not intention:
//!
//! - it serves exactly one document — the latest self-health sample in Prometheus
//!   exposition format, the same bytes `health.prom` holds — and nothing else at any path;
//! - it never reads the request beyond discarding one line, so no request content can
//!   influence anything;
//! - it binds loopback by default; reaching it from a scraper elsewhere is the operator's
//!   explicit `--metrics-addr` decision, stated in config rather than defaulted.
//!
//! The customer's existing monitoring watches this analyzer the way everyone watches vLLM:
//! scrape `/metrics`, alarm on the names `METRIC_NAMES` pins as API. The textfile artifact
//! stays — both surfaces render the identical sample, so they cannot disagree.

use crate::Error;
use std::io::{Read as _, Write as _};
use std::net::TcpListener;
// Re-exported so the rest of the daemon can NAME an address without touching std::net —
// the network fence greps for it, and this module is the one place it belongs.
pub use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// How long one scrape may take to send its request or read the response before the
/// listener moves on: a stuck client cannot wedge the daemon, only this thread, and only
/// this long.
const SCRAPE_IO_TIMEOUT: Duration = Duration::from_secs(2);

/// How much of a request is drained and discarded. The endpoint answers every request with
/// the one document it has, so parsing would only create behaviour an input could influence.
const REQUEST_DISCARD_BYTES: usize = 4096;

/// The latest rendered sample, shared between the sampler (writer) and the listener
/// thread (reader).
///
/// It carries an age, and that is the load-bearing part. The sampler runs on its own thread
/// precisely so observation never queues behind work, which means the loop can be healthy
/// while the sampler is dead or wedged, and this document would keep being served with
/// every figure frozen. A scrape endpoint answering `200 OK` is what a monitoring system
/// reads as "the process is fine", so a stale document must stop being served.
///
/// One definition of stale, two consumers: the listener refuses to serve past it (the
/// scraper sees the target go down), and [`crate::Daemon::run`] exits past it (the init
/// system restarts through the ordered claim-and-recover path). Neither would be
/// sufficient alone — a deployment may run with no listener at all, and a scrape failure
/// nobody has alarmed on is not a recovery.
#[derive(Debug, Clone)]
pub(crate) struct Published {
    /// The rendering and when it was published; `None` until the first sample lands.
    doc: Arc<RwLock<Option<(String, Instant)>>>,
    /// When this was constructed, so "the sampler never published at all" ages out of
    /// startup the same way a sampler that stopped publishing does.
    created: Instant,
    max_age: Duration,
}

impl Published {
    /// A document that goes stale `max_age` after the last publication (or after
    /// construction, if there has never been one).
    pub(crate) fn new(max_age: Duration) -> Self {
        Self {
            doc: Arc::new(RwLock::new(None)),
            created: Instant::now(),
            max_age,
        }
    }

    /// Replace the served document with a fresh rendering.
    pub(crate) fn publish(&self, prometheus_text: String) {
        if let Ok(mut doc) = self.doc.write() {
            *doc = Some((prometheus_text, Instant::now()));
        }
    }

    /// When the document was last published, or when this was constructed if never.
    fn published_at(&self) -> Instant {
        self.doc
            .read()
            .ok()
            .and_then(|d| d.as_ref().map(|(_, at)| *at))
            .unwrap_or(self.created)
    }

    /// How long since the last publication, or since construction if there has been none.
    pub(crate) fn age(&self) -> Duration {
        self.age_at(Instant::now())
    }

    fn age_at(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.published_at())
    }

    /// Whether the sample is too old to stand behind.
    pub(crate) fn is_stale(&self) -> bool {
        self.is_stale_at(Instant::now())
    }

    fn is_stale_at(&self, now: Instant) -> bool {
        self.age_at(now) > self.max_age
    }

    /// The staleness bound: a publication older than this is refused, never served.
    pub(crate) fn max_age(&self) -> Duration {
        self.max_age
    }

    /// The document to serve, or `None` when it is too old to serve at all.
    fn snapshot_at(&self, now: Instant) -> Option<String> {
        let doc = self.doc.read().ok()?;
        let (text, at) = doc.as_ref()?;
        (now.saturating_duration_since(*at) <= self.max_age).then(|| text.clone())
    }
}

/// One HTTP/1.1 response, closed after the body.
fn http_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )
}

/// The whole response, status line included, for whatever state the sample is in.
///
/// A stale sample is a failed scrape, never a served one. Answering 200 with a frozen
/// document would report health on behalf of a sampler that has stopped reporting, with
/// every alarm the customer wrote bound to values that can no longer change. 503 makes
/// Prometheus set `up = 0` for the target, the signal every monitoring stack already
/// escalates, and it fires on the one condition the document itself cannot express.
fn response_at(published: &Published, now: Instant) -> String {
    match published.snapshot_at(now) {
        Some(body) => http_response("200 OK", "text/plain; version=0.0.4; charset=utf-8", &body),
        None => http_response(
            "503 Service Unavailable",
            "text/plain; charset=utf-8",
            &format!(
                "no sample newer than {}s: the health sampler has not published within its \
                 staleness bound, so this endpoint refuses to serve a frozen document as if it \
                 were current.\n",
                published.max_age().as_secs()
            ),
        ),
    }
}

/// Bind and serve `/metrics` on a background thread until the process ends.
///
/// Serving is deliberately sequential: a scrape is one small read every interval, and a
/// thread-per-connection design here would be capacity nobody asked for on a surface whose
/// whole job is to be boring. A stuck client cannot wedge the daemon — only this thread,
/// bounded by the write timeout — and the next accept serves the next scraper.
///
/// # Errors
/// The address cannot be bound: refused at startup, never discovered at first scrape.
pub(crate) fn serve(addr: SocketAddr, published: Published) -> Result<(), Error> {
    let listener = TcpListener::bind(addr).map_err(|source| Error::Bind { addr, source })?;
    std::thread::Builder::new()
        .name("metrics".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let _ = stream.set_read_timeout(Some(SCRAPE_IO_TIMEOUT));
                let _ = stream.set_write_timeout(Some(SCRAPE_IO_TIMEOUT));
                let mut discard = [0u8; REQUEST_DISCARD_BYTES];
                let _ = stream.read(&mut discard);
                let _ = stream.write_all(response_at(&published, Instant::now()).as_bytes());
            }
        })
        .map_err(|source| Error::Thread {
            name: "metrics",
            source,
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    /// A listener on a port the OS assigns, read back, so no fixed port collides with a
    /// parallel test run.
    fn serve_on_a_free_port(published: &Published) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);
        serve(addr, published.clone()).expect("serve");
        addr
    }

    fn scrape(addr: SocketAddr, request: &str) -> String {
        let mut s = TcpStream::connect(addr).expect("connect");
        s.write_all(request.as_bytes()).expect("request");
        let mut out = String::new();
        s.read_to_string(&mut out).expect("response");
        out
    }

    #[test]
    fn serves_the_published_document_and_only_that() {
        let published = Published::new(Duration::from_hours(1));
        published.publish("# TYPE t gauge\nt 1\n".to_string());
        let addr = serve_on_a_free_port(&published);

        let response = scrape(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("# TYPE t gauge"));

        // a fresh publish is what the next scrape sees
        published.publish("# TYPE t gauge\nt 2\n".to_string());
        assert!(scrape(addr, "GET / HTTP/1.1\r\n\r\n").contains("t 2"));

        // request content cannot influence the response: any bytes get the same document
        let response = scrape(addr, "DELETE /everything HTTP/1.1\r\nX-Evil: yes\r\n\r\n");
        assert!(response.contains("t 2"), "one document, whatever was asked");
    }

    /// The failure mode this endpoint would otherwise create: a sampler that died while
    /// the loop stayed healthy, and a cached document served forever with every figure
    /// frozen. The clock is passed in, so the bound is tested at its edge without sleeping.
    #[test]
    fn a_stale_sample_fails_the_scrape_instead_of_being_served() {
        let bound = Duration::from_millis(50);
        let published = Published::new(bound);
        let created = published.created;

        // Before the first sample there is nothing to stand behind, and the clock runs from
        // construction: a sampler that died before publishing once must not read as fresh
        // forever, which is what a bare "have we published?" check would do.
        assert!(!published.is_stale_at(created + bound));
        assert!(published.is_stale_at(created + bound + Duration::from_millis(1)));
        assert!(
            response_at(&published, created + bound * 2).starts_with("HTTP/1.1 503"),
            "a sampler that never published must not present as a healthy target"
        );

        // A fresh publication restores service.
        published.publish("# TYPE t gauge\nt 1\n".to_string());
        let at = published.published_at();
        assert!(!published.is_stale_at(at + bound));
        let response = response_at(&published, at + bound);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("t 1"));

        // Letting it age out again withdraws it: the document is unchanged, so only its
        // age can carry the failure.
        let response = response_at(&published, at + bound * 2);
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(
            !response.contains("t 1"),
            "the stale body must not be served alongside the failure status"
        );
        assert!(
            response.contains("staleness bound"),
            "the refusal says why, like every other refusal here: {response}"
        );
    }
}
