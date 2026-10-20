// SPDX-License-Identifier: AGPL-3.0-only

//! Binding and serving — `docs/read-api.md` section 6.1.
//!
//! **Why this lives here and not in `crates/cli`.** The CLI is the composition root and would be
//! the natural place to call `axum::serve`. It is also a crate with no HTTP dependency, and giving
//! it one would put axum and tokio through the dependency fence for the sake of ten lines. This
//! crate already owns both, so the shell asks for a listener and a loop and never names a runtime.
//!
//! **Binding is SEPARATE from serving, and separate is the point.** [`bind`] takes the socket on
//! the caller's thread, before anything is spawned, so a port already in use is a refusal at
//! startup with a sentence naming the address — the same shape `daemon::metrics::serve` gets by
//! binding before it spawns, and the same shape `storage::server` gets for the engine's ports. A
//! listener bound inside the serving thread would fail somewhere nobody is reading, and the
//! deployment would come up looking healthy with no console on it.

use crate::Error;
use std::net::{SocketAddr, TcpListener};

/// A bound socket, named here so the shell can hold one between the bind and the serve
/// without reaching into `std::net` itself: this crate is the one that listens.
pub type Listener = TcpListener;

/// Take the socket.
///
/// # Errors
/// The address is in use, or not one this host can bind.
pub fn bind(addr: SocketAddr) -> Result<Listener, Error> {
    let listener = TcpListener::bind(addr).map_err(|e| {
        Error::Listener(format!(
            "binding the read API to {addr}: {e}. Another process holds it, or the address is \
             not one this host has — `--api-addr off` disables the listener entirely."
        ))
    })?;
    // tokio adopts this listener and drives it with its own reactor, which requires the socket
    // not block the thread it is polled on.
    listener
        .set_nonblocking(true)
        .map_err(|e| Error::Listener(format!("preparing the read API listener {addr}: {e}")))?;
    Ok(listener)
}

/// Serve `state` on `listener` until `stop` answers true. **Blocks the calling thread**, which is
/// why the shell gives this one of its own.
///
/// **The runtime is built here and belongs to this call.** `analyzer_serving_clickhouse::engine::Engine` drives its
/// own current-thread runtime and every DAO method is synchronous, so the two must never nest —
/// the crate header carries that rule, and `ApiState`'s own read path is what enforces it for
/// handlers. This function's only obligation is to be the one place a serving runtime is created.
///
/// **Shutdown is polled, not signalled, and the daemon owns the signal.** `tokio::signal` would
/// install a SECOND SIGTERM handler beside `daemon::Shutdown`'s, and a process with two opinions
/// about how to stop has no ordered stop at all. So `stop` is the daemon's own flag, read on the
/// same coarse cadence its loop uses; the API notices within a tick and drains in-flight requests.
///
/// # Errors
/// The runtime cannot be built, or the server fails while accepting.
pub fn serve(
    listener: TcpListener,
    state: crate::ApiState,
    stop: impl Fn() -> bool + Send + 'static,
) -> Result<(), Error> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::Listener(format!("building the read API runtime: {e}")))?;

    // **This clone is load-bearing and must not be tidied away.** `ApiState` holds the engine, an
    // `Engine` drives its own current-thread runtime, and dropping one inside an async context
    // BLOCKS on that runtime's shutdown — which tokio forbids and panics on. The router owns a
    // state, so when the serve future completes the router drops inside `block_on`; if that were
    // the last reference the engine would go with it, on exactly the thread that may not take it.
    //
    // Holding a reference here, and releasing it AFTER the runtime is gone, means the last drop
    // always lands on this blocking-allowed thread. In the shipped composition the daemon's
    // observer closure happens to hold another clone, so the panic would not fire today — which
    // is the kind of accident that survives until the composition changes and then fails in a
    // deployment rather than in a test.
    let keepalive = state.clone();
    let result = runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener)
            .map_err(|e| Error::Listener(format!("adopting the read API listener: {e}")))?;
        axum::serve(listener, crate::router(state))
            .with_graceful_shutdown(async move {
                while !stop() {
                    tokio::time::sleep(SHUTDOWN_POLL).await;
                }
            })
            .await
            .map_err(|e| Error::Listener(format!("serving the read API: {e}")))
    });
    // Explicit and ORDERED, because locals drop in reverse declaration order and this pair must
    // not depend on how they happen to be declared.
    drop(runtime);
    drop(keepalive);
    result
}

/// How often the serving loop asks whether the daemon has been told to stop.
///
/// **Not on the re-derivation list, by that list's own rule**: it governs no measurement and
/// enters no keyed history. It is a shutdown latency, and a fifth of the loop's own one-second
/// tick is small enough to be invisible beside the engine's ordered stop.
const SHUTDOWN_POLL: std::time::Duration = std::time::Duration::from_millis(200);

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpListener};

    #[test]
    fn a_port_already_taken_is_a_sentence_at_startup_not_a_silent_dark_console() {
        // The failure this shape exists to prevent: bind inside the serving thread and a port
        // conflict surfaces on a thread nobody reads, leaving a deployment that looks healthy and
        // serves nothing. Binding on the caller's thread makes it a startup refusal.
        let held = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("hold a port");
        let taken = held.local_addr().expect("addr");

        let refused = super::bind(taken).expect_err("the port is held");
        assert!(
            matches!(&refused, crate::Error::Listener(m) if m.contains(&taken.to_string())),
            "the refusal must name the address an operator has to go and free: {refused}"
        );
        assert!(
            matches!(&refused, crate::Error::Listener(m) if m.contains("--api-addr off")),
            "and the way out, for a deployment that wants no console: {refused}"
        );
    }

    #[test]
    fn a_free_port_binds_nonblocking_because_tokio_will_poll_it() {
        let listener = super::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind");
        // `from_std` REJECTS a blocking socket at runtime rather than at compile time, so the
        // property is asserted here instead of discovered in a deployment.
        //
        // **It must also be called with a reactor in scope**, which is why `serve` adopts the
        // listener INSIDE `block_on` rather than beside the bind: the two halves of this module
        // split exactly where tokio requires them to.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        assert!(
            runtime
                .block_on(async { tokio::net::TcpListener::from_std(listener) })
                .is_ok(),
            "the bound listener must be adoptable by the runtime that will serve on it"
        );
    }
}
