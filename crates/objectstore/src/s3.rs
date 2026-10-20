// SPDX-License-Identifier: AGPL-3.0-only

//! The S3-API store: AWS and every S3-compatible implementation a customer might run, over the
//! process's one asynchronous runtime, with the request counters a measurement reads.

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
// `list` is on the base trait; `get` and `put` are on the extension trait. Both are imported
// anonymously so this crate's own `ObjectStore` name is the one a reader sees.
use super::{
    Conditional, Error, ObjectMeta, ObjectStore, Precondition, Version, CONNECT_TIMEOUT,
    REQUEST_TIMEOUT, RETRY_ATTEMPTS, RETRY_BUDGET,
};
use object_store::ObjectStore as _;
use object_store::ObjectStoreExt as _;

/// A deployment's own static credential, when it has configured one. `session_token`
/// carries a temporary credential's token, without which the pair is meaningless.
///
/// `Debug` prints the key id and whether a secret and a token are present, never their
/// bytes: a configuration is logged, and a logged secret is a leaked one.
#[derive(Clone)]
pub struct StaticCredentials {
    /// The static credential's access key id.
    pub access_key_id: String,
    /// The static credential's secret.
    pub secret_access_key: String,
    /// The session token of a temporary credential, when one was issued.
    pub session_token: Option<String>,
}

impl std::fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// An S3-API store: AWS, and every S3-compatible implementation a customer might run.
pub struct S3Store {
    inner: object_store::aws::AmazonS3,
    /// The spec a [`ObjectStore::sibling`] rebuilds from — retained because the built
    /// client cannot be asked for it back.
    url: String,
    creds: Option<StaticCredentials>,
    /// The process's runtime ([`runtime`]): every handle's calls, side by side.
    runtime: &'static tokio::runtime::Runtime,
}

impl std::fmt::Debug for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store")
            .field("url", &self.url)
            .field("creds", &self.creds)
            .finish_non_exhaustive()
    }
}

impl S3Store {
    /// Build a store against `url`.
    ///
    /// **`creds` is the deployment's own static pair, or `None` to resolve through the chain.**
    /// Both paths matter and neither is a fallback for the other:
    ///
    /// - A customer who configured `INFERTAP_S3_ACCESS_KEY_ID` keeps working, unchanged. Their
    ///   credential is still read from an environment and still never touches an argument list.
    /// - A deployment that configures NOTHING gets the provider chain — instance profile via
    ///   IMDS, IRSA via its projected token, `AssumeRole`, SSO. That is the posture a security
    ///   review asks for.
    ///
    /// So the absence of a credential becomes a request for identity rather than an error, which
    /// is what makes "no secret anywhere on the box" a configuration a customer can choose.
    ///
    /// # Errors
    /// A URL the builder cannot parse, or a runtime that will not start.
    pub fn open(url: &str, creds: Option<&StaticCredentials>) -> Result<Self, Error> {
        let runtime = runtime()?;
        let mut builder = AmazonS3Builder::from_env()
            .with_url(url)
            .with_retry(object_store::RetryConfig {
                max_retries: RETRY_ATTEMPTS,
                retry_timeout: RETRY_BUDGET,
                ..Default::default()
            })
            .with_client_options(
                // This REPLACES the client options `from_env` assembled, so anything env
                // grants must be re-granted here or it silently dies in the swap.
                // `AWS_ALLOW_HTTP` is one: without it here an http endpoint — the
                // airgapped deployment the docs describe — fails every request at build
                // time ("builder error", no wire traffic).
                object_store::ClientOptions::new()
                    .with_allow_http(
                        std::env::var("AWS_ALLOW_HTTP")
                            .is_ok_and(|v| v.eq_ignore_ascii_case("true") || v == "1"),
                    )
                    .with_connect_timeout(CONNECT_TIMEOUT)
                    .with_timeout(REQUEST_TIMEOUT),
            );
        if let Some(c) = creds {
            builder = builder
                .with_access_key_id(&c.access_key_id)
                .with_secret_access_key(&c.secret_access_key);
            if let Some(token) = &c.session_token {
                builder = builder.with_token(token);
            }
        }
        let inner = builder.build().map_err(|e| Error::Config {
            url: url.to_string(),
            message: e.to_string(),
        })?;

        Ok(Self {
            inner,
            url: url.to_string(),
            creds: creds.cloned(),
            runtime,
        })
    }

    /// Run one async call to completion on the process's runtime, beside any other thread's.
    fn block<F: std::future::Future>(&self, f: F) -> F::Output {
        self.runtime.block_on(f)
    }
}

/// Worker threads of the object-store runtime: the requests' I/O and their futures' wake-ups,
/// not their callers, which block on their own threads. Two carry the rig's writers and readers
/// with a core left for the fold. A labelled analytic constant (`docs/conformance.md`), pinned
/// rather than scaled with the host's cores: a constant derived on one box and scaled on another
/// is how the shipper met `TasksMax` on its first 16-core host.
pub const RUNTIME_WORKERS: usize = 2;

/// The process's one object-store runtime, started on first use.
///
/// # Errors
/// The runtime would not start.
pub(crate) fn runtime() -> Result<&'static tokio::runtime::Runtime, Error> {
    static RUNTIME: std::sync::OnceLock<Result<tokio::runtime::Runtime, String>> =
        std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(RUNTIME_WORKERS)
                .thread_name("object-store")
                .enable_all()
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|e| Error::Runtime(e.clone()))
}

/// Requests this process has sent to S3 buckets, by kind, over every handle: what a caller
/// reads a before and after of to say what an operation cost the bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Requests {
    /// `GET`s, whole or ranged.
    pub gets: u64,
    /// `PUT`s, conditional or not.
    pub puts: u64,
    /// Listings requested (one per call, however many pages it read).
    pub lists: u64,
    /// `DELETE`s.
    pub deletes: u64,
}

impl Requests {
    /// What was sent between `before` and `self`.
    #[must_use]
    pub fn since(self, before: Self) -> Self {
        Self {
            gets: self.gets.saturating_sub(before.gets),
            puts: self.puts.saturating_sub(before.puts),
            lists: self.lists.saturating_sub(before.lists),
            deletes: self.deletes.saturating_sub(before.deletes),
        }
    }
}

static S3_GETS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static S3_PUTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static S3_LISTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static S3_DELETES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn count(counter: &std::sync::atomic::AtomicU64) {
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

impl S3Store {
    /// Every request this process has sent to S3 so far.
    #[must_use]
    pub fn requests() -> Requests {
        let load = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
        Requests {
            gets: load(&S3_GETS),
            puts: load(&S3_PUTS),
            lists: load(&S3_LISTS),
            deletes: load(&S3_DELETES),
        }
    }
}

impl ObjectStore for S3Store {
    fn location(&self) -> String {
        self.url.clone()
    }

    fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>, Error> {
        count(&S3_GETS);
        let p = ObjectPath::from(key);
        self.block(async {
            let bytes =
                self.inner
                    .get_range(&p, range.clone())
                    .await
                    .map_err(|e| Error::FetchRange {
                        key: key.to_string(),
                        start: range.start,
                        end: range.end,
                        message: e.to_string(),
                    })?;
            Ok(bytes.to_vec())
        })
    }

    fn sibling(&self) -> Result<Box<dyn ObjectStore>, Error> {
        Ok(Box::new(Self::open(&self.url, self.creds.as_ref())?))
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, Error> {
        use futures_util_shim::TryStreamShim as _;
        count(&S3_LISTS);
        let p = ObjectPath::from(prefix);
        self.block(async {
            let mut out = Vec::new();
            let mut stream = self.inner.list(Some(&p));
            while let Some(meta) = stream.try_next_shim().await? {
                out.push(ObjectMeta::listed(
                    meta.location.as_ref().to_string(),
                    meta.size,
                    meta.e_tag,
                ));
            }
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        })
    }

    fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>, Error> {
        use futures_util_shim::TryStreamShim as _;
        count(&S3_LISTS);
        let p = ObjectPath::from(prefix);
        let offset = ObjectPath::from(after);
        self.block(async {
            let mut out = Vec::new();
            let mut stream = self.inner.list_with_offset(Some(&p), &offset);
            while let Some(meta) = stream.try_next_shim().await? {
                out.push(ObjectMeta::listed(
                    meta.location.as_ref().to_string(),
                    meta.size,
                    meta.e_tag,
                ));
            }
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        })
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        count(&S3_GETS);
        // The WHOLE read retries, not just the request: the client's
        // retry config covers request-level failures, but a body that dies mid-stream
        // surfaces from `bytes()` AFTER the request succeeded ("request or response body
        // error"). A GET is idempotent, so the
        // honest response to a torn body is to ask again, say so, and only then fail.
        let p = ObjectPath::from(key);
        let mut last_err: Option<Error> = None;
        for attempt in 1..=3u32 {
            let result: Result<Vec<u8>, Error> = self.block(async {
                let got = self.inner.get(&p).await.map_err(|e| Error::Object {
                    op: "fetching",
                    key: key.to_string(),
                    message: e.to_string(),
                })?;
                let bytes = got.bytes().await.map_err(|e| Error::Object {
                    op: "reading",
                    key: key.to_string(),
                    message: e.to_string(),
                })?;
                Ok(bytes.to_vec())
            });
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    if attempt < 3 {
                        log::warn!(
                            "object read failed (attempt \
                             {attempt} of 3), retrying: {e}"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(
                            250 * u64::from(attempt),
                        ));
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.expect("three attempts, three errors"))
    }

    fn put(&self, key: &str, body: &[u8]) -> Result<(), Error> {
        count(&S3_PUTS);
        let p = ObjectPath::from(key);
        let payload = bytes::Bytes::copy_from_slice(body);
        self.block(async {
            self.inner
                .put(&p, payload.into())
                .await
                .map(|_| ())
                .map_err(|e| Error::Object {
                    op: "writing",
                    key: key.to_string(),
                    message: e.to_string(),
                })
        })
    }

    fn get_versioned(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, Error> {
        count(&S3_GETS);
        let p = ObjectPath::from(key);
        let fail = |message: String| Error::Object {
            op: "reading",
            key: key.to_string(),
            message,
        };
        self.block(async {
            let got = match self.inner.get(&p).await {
                Ok(got) => got,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(fail(e.to_string())),
            };
            let etag = got
                .meta
                .e_tag
                .clone()
                .ok_or_else(|| fail("the store reported no entity tag".to_string()))?;
            let body = got.bytes().await.map_err(|e| fail(e.to_string()))?;
            Ok(Some((body.to_vec(), Version(etag))))
        })
    }

    /// `If-None-Match: *` for [`Precondition::Absent`] and `If-Match: <etag>` for
    /// [`Precondition::Matches`], through `object_store`'s `ETagMatch` mode (its default).
    ///
    /// **A retried request can report `Refused` for a write that landed.** The transport
    /// retries on a lost response; the retry then finds its own write and the precondition
    /// fails. The caller cannot tell that from a competing writer without re-reading, which
    /// the contract above already requires.
    fn put_if(
        &self,
        key: &str,
        body: &[u8],
        condition: &Precondition,
    ) -> Result<Conditional, Error> {
        count(&S3_PUTS);
        let p = ObjectPath::from(key);
        let payload = bytes::Bytes::copy_from_slice(body);
        let mode = match condition {
            Precondition::Absent => object_store::PutMode::Create,
            Precondition::Matches(v) => {
                object_store::PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some(v.0.clone()),
                    version: None,
                })
            }
        };
        self.block(async {
            match self.inner.put_opts(&p, payload.into(), mode.into()).await {
                Ok(written) => written
                    .e_tag
                    .map(|e| Conditional::Written(Version(e)))
                    .ok_or_else(|| Error::Object {
                        op: "writing",
                        key: key.to_string(),
                        message: "the store reported no entity tag for the write".to_string(),
                    }),
                Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => Ok(Conditional::Refused),
                Err(e) => Err(Error::Object {
                    op: "writing",
                    key: key.to_string(),
                    message: e.to_string(),
                }),
            }
        })
    }

    fn delete(&self, key: &str) -> Result<(), Error> {
        count(&S3_DELETES);
        let p = ObjectPath::from(key);
        self.block(async {
            match self.inner.delete(&p).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(Error::Object {
                    op: "deleting",
                    key: key.to_string(),
                    message: e.to_string(),
                }),
            }
        })
    }
}

/// A thin shim so `list`'s stream can be drained without taking a `futures` dependency for one
/// method. `object_store` returns a `BoxStream`, and draining it is the only async-stream use in
/// this workspace.
mod futures_util_shim {
    use super::Error;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    pub trait TryStreamShim {
        type Ok;
        fn try_next_shim(&mut self) -> impl Future<Output = Result<Option<Self::Ok>, Error>>;
    }

    impl<S, T> TryStreamShim for S
    where
        S: futures_core::Stream<Item = Result<T, object_store::Error>> + Unpin,
    {
        type Ok = T;
        fn try_next_shim(&mut self) -> impl Future<Output = Result<Option<T>, Error>> {
            Next(self)
        }
    }

    struct Next<'a, S: ?Sized>(&'a mut S);

    impl<S, T> Future for Next<'_, S>
    where
        S: futures_core::Stream<Item = Result<T, object_store::Error>> + Unpin + ?Sized,
    {
        type Output = Result<Option<T>, Error>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut *self.0).poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(Ok(None)),
                Poll::Ready(Some(Ok(v))) => Poll::Ready(Ok(Some(v))),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Err(Error::Listing(e.to_string()))),
            }
        }
    }
}
