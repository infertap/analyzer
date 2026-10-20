// SPDX-License-Identifier: AGPL-3.0-only

//! Auth v1 — `docs/read-api.md` section 6.
//!
//! **The users.xml pattern, applied to a viewer token.** `storage::server::Credential` already
//! mints a random secret, writes only its SHA-256 to disk and keeps the plaintext in process
//! memory until it dies. This is that shape for a credential a human carries: the plaintext is
//! printed once by `token issue`, only the digest reaches the store, and there is nothing in a
//! backup to leak.
//!
//! **Viewer is the only role, because there is nothing to authorize.** Every endpoint is a read
//! of the same fleet. A role system with one role is a schema nobody needs yet and a migration
//! everybody pays for.

use crate::Error;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, SystemTime};

/// How long a session survives WITHOUT BEING USED. Refreshed on every authenticated request.
///
/// A flat absolute lifetime with no renewal would make an operator thirteen hours into an
/// incident re-authenticate, and — the part that decides it — the token is the standing
/// credential, so every lapse means reading the token file off the analyzer host again. Grafana
/// runs 7 days idle / 30 days absolute, and these are those numbers.
///
/// **Long is cheap here, because sessions are in-memory and die with the process anyway.** Daemon
/// uptime is the real bound, so this rarely binds; what it removes is daily re-authentication on
/// a daemon that has been up for a month.
///
/// **Not on the re-derivation list, and the reason is the list's own rule**: it governs no
/// measurement and enters no keyed history. It is a security posture default, revisited when a
/// deployment states its own session policy.
// Written in hours because `Duration::from_days` is still unstable (`duration_constructors`)
// while `from_hours` is not; the arithmetic is left visible rather than folded to a raw number.
pub(crate) const SESSION_IDLE_TTL: Duration = Duration::from_hours(24 * 7);

/// How long a session survives AT ALL, however continuously it is used.
///
/// The ceiling under the sliding window above: activity may keep a session alive for a month, and
/// then it ends regardless. Same exemption from the re-derivation list, same reason.
pub(crate) const SESSION_ABSOLUTE_TTL: Duration = Duration::from_hours(24 * 30);

/// The one `Set-Cookie` builder, because create and destroy MUST agree.
///
/// A browser replaces a cookie by matching name, domain and path; a delete that spells any of them
/// differently leaves the original in place and the sign-out silently does nothing. Two format
/// strings is how that happens, so there is one.
///
/// **`Secure` is conditional.** The listener speaks plaintext and binds loopback by default, and
/// **Safari discards a `Secure` cookie on `http://localhost` entirely** — Firefox and Chrome allow
/// it, Safari does not — so an unconditional `Secure` would lock out the demo kit and the
/// SSH-tunnel topology, the most locked-down way to reach this product. Silently: `200` with a
/// `Set-Cookie` the browser drops, then every read 401s, and the sign-in screen returns with no
/// error to render. Grafana's `cookie_secure` defaults to false for the same
/// reason. `--api-cookie-secure on` is how a TLS-terminated deployment asks for it.
///
/// **`SameSite=Lax`, not `Strict`**. `Strict` withholds the cookie on any cross-site
/// top-level navigation, so a deep link pasted into a runbook or a chat — the console's own
/// stated grammar, where URL is state — always landed on the sign-in screen even for a signed-in
/// operator. `Lax` still withholds it on cross-site POST, which is the CSRF vector, and this API
/// has exactly one POST.
///
/// **No `__Host-` prefix**, deliberately: Chrome rejects that prefix on localhost even where it
/// accepts a bare `Secure` cookie, so the obvious hardening would break the demo kit on a second
/// browser. Recorded here so its absence reads as a decision.
#[must_use]
pub(crate) fn session_cookie_header(value: &str, max_age_secs: u64, secure: bool) -> String {
    let secure = if secure { " Secure;" } else { "" };
    format!(
        "infertap_session={value}; HttpOnly;{secure} SameSite=Lax; Path=/; Max-Age={max_age_secs}"
    )
}

/// One issued viewer token, as it exists at rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenRecord {
    /// The public handle: `tok-` and twelve hex of a hash OF the digest, so it reveals
    /// nothing of what `verify` compares against.
    pub id: String,
    /// The operator's name for the token, given at `token issue` and shown by `token list`.
    pub label: String,
    /// **The only form that reaches disk.** Upper-case hex of the SHA-256, the same rendering
    /// `storage::server::Credential` uses for the engine credential.
    pub sha256_hex: String,
    /// When the token was minted, wall-clock epoch ms.
    pub issued_at_ms: u64,
    /// Set when a later `token rotate` superseded this one. A rotated record is kept rather than
    /// deleted so `token list` can show that a credential existed and when it stopped working —
    /// an operator debugging a 401 needs that, and it costs one line.
    pub rotated_at_ms: Option<u64>,
}

impl TokenRecord {
    /// Whether the token still authenticates: not yet superseded by a `token rotate`.
    #[must_use]
    pub fn active(&self) -> bool {
        self.rotated_at_ms.is_none()
    }
}

/// The token digests, on disk.
#[derive(Debug, Default)]
pub struct TokenStore {
    path: PathBuf,
    records: RwLock<Vec<TokenRecord>>,
}

impl TokenStore {
    /// Read the store, or start an empty one. A missing file is not an error: a deployment that
    /// has never issued a token is the ordinary first state, and it answers 401 to everything.
    ///
    /// # Errors
    /// The file exists and cannot be read or parsed.
    pub fn open(path: &Path) -> Result<Self, Error> {
        let records = match std::fs::read_to_string(path) {
            Ok(text) => parse(&text)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => {
                return Err(Error::Auth(format!(
                    "reading token store {}: {e}",
                    path.display()
                )))
            }
        };
        Ok(Self {
            path: path.to_path_buf(),
            records: RwLock::new(records),
        })
    }

    /// Re-read the store from disk.
    ///
    /// **The daemon and `token issue|rotate` are DIFFERENT PROCESSES, and that is the whole
    /// reason this exists.** The CLI mutates its own copy and writes the file; a serving process
    /// that read the file once at startup would never learn: a token issued while the analyzer
    /// runs would be refused until restart, and — the sharp half — **`token rotate` would not
    /// revoke**, atomic within one process and not across the only split that exists in
    /// production.
    ///
    /// A missing file becomes an empty store, the same reading [`Self::open`] gives it: a
    /// deployment whose store was deleted admits nobody. Any other I/O error leaves the cached
    /// records untouched and is returned, so the caller decides — and they decide differently
    /// (see [`Self::verify`] and [`Self::is_active`]).
    fn reload(&self) -> Result<(), Error> {
        let parsed = match std::fs::read_to_string(&self.path) {
            Ok(text) => parse(&text)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => {
                return Err(Error::Auth(format!(
                    "reading token store {}: {e}",
                    self.path.display()
                )))
            }
        };
        let mut records = self
            .records
            .write()
            .map_err(|_| Error::Poisoned("token store lock poisoned"))?;
        *records = parsed;
        Ok(())
    }

    /// Does `presented` match an active token? Answers with its id.
    ///
    /// **Constant-time, and this is the one place in the workspace that needs to be.** A
    /// short-circuiting comparison leaks the length of the matching prefix through timing, which
    /// is the difference between a token nobody can guess and one somebody can walk. `subtle` is
    /// on the allowlist for exactly this call — the philosophy is buy crypto-adjacent primitives,
    /// own arithmetic ones.
    ///
    /// Every active record is compared, with no early exit on a match either, so the time taken
    /// does not reveal WHICH token matched or how many are stored. **The second pass that recovers
    /// the id is NOT constant-time and does not need to be**: it runs only after a match is
    /// established, for a caller who by then is holding the credential it would leak.
    ///
    /// **Fails CLOSED on an unreadable store.** This is the sign-in path, taken once per session
    /// and never per request, so refusing costs an operator one retry — and authenticating against
    /// a copy that may predate a revocation is the thing this function was rewritten to stop.
    #[must_use]
    pub fn verify(&self, presented: &str) -> Option<String> {
        use subtle::ConstantTimeEq;
        if self.reload().is_err() {
            return None;
        }
        let digest = sha256_hex(presented);
        let records = self.records.read().ok()?;
        let mut hit = subtle::Choice::from(0u8);
        for record in records.iter().filter(|r| r.active()) {
            hit |= digest.as_bytes().ct_eq(record.sha256_hex.as_bytes());
        }
        if !bool::from(hit) {
            return None;
        }
        records
            .iter()
            .find(|r| r.active() && r.sha256_hex == digest)
            .map(|r| r.id.clone())
    }

    /// Is `id` still an active token?
    ///
    /// **Called on every authenticated request**, because that is what makes `token rotate` kill
    /// the sessions a leaked credential already opened. Checking only at sign-in would leave those
    /// sessions alive for their full lifetime, which the 7-day idle window makes a long time to
    /// hold a door open for somebody you have already locked out.
    ///
    /// The cost is one read of a file measured in hundreds of bytes, per request, against a
    /// console with one operator. Stated rather than hidden, with the same revisit trigger as the
    /// engine mutex: more than one concurrent viewer mattering.
    ///
    /// **Fails OPEN on an unreadable store, the opposite of [`Self::verify`].** A transient I/O
    /// error must not sign an operator out mid-incident; the cached answer is at most one reload
    /// stale, and a revocation is caught on the next read that succeeds. The asymmetry is
    /// deliberate: refusing a new session costs a retry, dropping a live one costs an incident.
    #[must_use]
    pub fn is_active(&self, id: &str) -> bool {
        let _ = self.reload();
        self.records
            .read()
            .is_ok_and(|records| records.iter().any(|r| r.id == id && r.active()))
    }

    /// Mint a token, returning the plaintext ONCE. Only the digest is stored.
    ///
    /// **Re-reads first, so a second process's records are not clobbered.** `token issue` in the
    /// CLI and the serve-mode bootstrap both write this file; without the re-read, whichever
    /// flushed second would drop the other's row. This narrows the window rather than closing it —
    /// there is no file lock — and at hand-run frequency against a store touched once at install,
    /// that is the right amount of machinery.
    ///
    /// # Errors
    /// The store cannot be read or written.
    pub fn issue(&self, label: &str, now_ms: u64) -> Result<(String, TokenRecord), Error> {
        self.reload()?;
        self.append(label, now_ms)
    }

    /// Mint and flush against the records already in hand. **No re-read**, which is what lets
    /// [`Self::rotate`] supersede and mint under one load and one flush.
    fn append(&self, label: &str, now_ms: u64) -> Result<(String, TokenRecord), Error> {
        let plaintext = random_hex()?;
        let digest = sha256_hex(&plaintext);
        let record = TokenRecord {
            // **The id is a SECOND hash, not a prefix of the stored digest.** Ids travel where
            // digests do not: a ticket, a runbook, a `token list` pasted into chat. A prefix of
            // the compared value is not exploitable on its own (a preimage of the rest is still
            // infeasible), but it is a gratuitous leak from a public handle, and avoiding it
            // costs one more hash.
            //
            // Hashing the digest keeps every property the id needed (stable, derived, unique per
            // credential, no extra state) while making it reveal nothing about what `verify`
            // compares against.
            id: format!("tok-{}", sha256_hex(&digest)[..12].to_lowercase()),
            label: label.to_string(),
            sha256_hex: digest,
            issued_at_ms: now_ms,
            rotated_at_ms: None,
        };
        {
            let mut records = self
                .records
                .write()
                .map_err(|_| Error::Poisoned("token store lock poisoned"))?;
            records.push(record.clone());
        }
        self.flush()?;
        Ok((plaintext, record))
    }

    /// Supersede `id` and mint its replacement, **in one operation**.
    ///
    /// Two steps would leave a window where both work or neither does, and an operator rotating a
    /// leaked credential is precisely the person who cannot afford either.
    ///
    /// The store is re-read from disk ONCE, here, so the claim holds across the CLI/daemon
    /// split and not only within one process; the supersession then rides the same load and
    /// the same flush as the replacement. Calling [`Self::issue`] instead would re-read
    /// between the two and discard the rotation.
    ///
    /// # Errors
    /// No such active token, or the store cannot be read or written.
    pub fn rotate(&self, id: &str, now_ms: u64) -> Result<(String, TokenRecord), Error> {
        self.reload()?;
        {
            let mut records = self
                .records
                .write()
                .map_err(|_| Error::Poisoned("token store lock poisoned"))?;
            let target = records
                .iter_mut()
                .find(|r| r.id == id && r.active())
                .ok_or_else(|| Error::Auth(format!("no active token {id:?}")))?;
            target.rotated_at_ms = Some(now_ms);
        }
        let label = format!("rotated from {id}");
        self.append(&label, now_ms)
    }

    /// Every record, active and superseded.
    #[must_use]
    pub fn records(&self) -> Vec<TokenRecord> {
        self.records.read().map(|r| r.clone()).unwrap_or_default()
    }

    /// **Written to a sibling temp file and RENAMED, never written in place.**
    ///
    /// `verify` and `is_active` now read this file from another process on a live path, and
    /// `std::fs::write` truncates before it writes — so an in-place flush has a window where a
    /// concurrent reader sees an empty or half-written store and parses nothing. That reader would
    /// answer 401 to a valid credential, once, for reasons nothing could reproduce. A rename is
    /// atomic on every filesystem this ships to, so a reader sees the whole old file or the whole
    /// new one.
    ///
    /// The mode is set on the temp file BEFORE the rename, so the final path is never briefly
    /// world-readable.
    fn flush(&self) -> Result<(), Error> {
        let records = self
            .records
            .read()
            .map_err(|_| Error::Poisoned("token store lock poisoned"))?;
        let text = records.iter().fold(String::new(), |mut acc, r| {
            let _ = writeln!(
                acc,
                "{}\t{}\t{}\t{}\t{}",
                r.id,
                r.label.replace(['\t', '\n'], " "),
                r.sha256_hex,
                r.issued_at_ms,
                r.rotated_at_ms
                    .map_or_else(|| "-".into(), |v| v.to_string())
            );
            acc
        });
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Auth(format!("token store dir {}: {e}", parent.display())))?;
        }
        let staged = self.path.with_extension("tmp");
        std::fs::write(&staged, text)
            .map_err(|e| Error::Auth(format!("writing token store {}: {e}", staged.display())))?;
        restrict(&staged)?;
        std::fs::rename(&staged, &self.path).map_err(|e| {
            let _ = std::fs::remove_file(&staged);
            Error::Auth(format!(
                "replacing token store {}: {e}",
                self.path.display()
            ))
        })
    }
}

/// Mint the first viewer token if this deployment has never had one, leaving the plaintext at
/// `path` with mode `0600`. Answers `None` when a token already exists.
///
/// **The shape is k3s's rather than Consul's.** Requiring a human to run `token issue`
/// before the product shows anything is the wrong default and no comparable product does it:
/// Grafana ships `admin`/`admin`, Elasticsearch prints a generated password, k3s writes a token
/// to a `0600` file. But the print-once pattern does not survive the transplant, and the reason
/// is the OUTPUT CHANNEL rather than the idea — Consul, Elasticsearch and Jupyter print to an
/// interactive terminal on a first run somebody is watching. **This process runs under systemd,
/// so its stderr is the journal, and journals ship to Splunk or Loki.** A live viewer credential
/// there is readable by everyone with log access, which is a finding at the first enterprise
/// security review and a self-inflicted one. So the journal gets the PATH and the token id; the
/// plaintext gets a file with an owner and a mode.
///
/// **The condition is an EMPTY store, not a missing file.** "Never issued" is the honest reading
/// of first run. A deployment whose operator already ran `token issue` — an Ansible or Terraform
/// shop seeding the store before first start — has a non-empty store and never sees this path,
/// so `token issue` stays the automated route.
///
/// The file persists. Deleting it after first use is clever and wrong: an operator who loses the
/// credential should be able to read it again, not reinstall. `token rotate` is what invalidates
/// it, and doing so now ends every session it opened.
///
/// # Errors
/// The store cannot be read or written, or the plaintext file cannot be created.
pub fn bootstrap_token(
    store: &TokenStore,
    path: &Path,
    now_ms: u64,
) -> Result<Option<TokenRecord>, Error> {
    // Re-read before deciding, so the verdict is about the store ON DISK rather than about
    // whatever this handle read when it was opened. The CLI happens to open one immediately
    // before calling this, but a caller that held one for any length of time would otherwise
    // mint a SECOND credential behind a deployment that already has one — and write a file
    // telling the operator to use it.
    store.reload()?;
    if !store.records().is_empty() {
        return Ok(None);
    }
    let (plaintext, record) = store.issue("initial", now_ms)?;
    write_secret(path, &plaintext)?;
    Ok(Some(record))
}

/// Create `path` holding `secret`, **owner-readable from the instant it exists**.
///
/// `fs::write` followed by `set_permissions` is the obvious spelling and it is wrong here: it
/// leaves a window in which a plaintext viewer credential is world-readable. Creating with the
/// mode already set closes it, which is the same reasoning that puts the mode on the token
/// store's staged file BEFORE the rename rather than on the final path after it.
#[cfg(unix)]
fn write_secret(path: &Path, secret: &str) -> Result<(), Error> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Auth(format!("creating {}: {e}", parent.display())))?;
    }
    // Truncating rather than refusing an existing file: the store is empty, so whatever is here
    // names a credential that no longer verifies, and leaving it would hand an operator a token
    // that silently does not work.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| Error::Auth(format!("creating {}: {e}", path.display())))?;
    writeln!(file, "{secret}")
        .map_err(|e| Error::Auth(format!("writing {}: {e}", path.display())))?;
    // An existing file keeps its old mode through `open`, so the mode is asserted either way.
    restrict(path)
}

#[cfg(not(unix))]
fn write_secret(path: &Path, secret: &str) -> Result<(), Error> {
    std::fs::write(path, format!("{secret}\n"))
        .map_err(|e| Error::Auth(format!("writing {}: {e}", path.display())))
}

/// `0600`: the digests are not secret in the way the plaintext is, but a world-readable list of
/// credential digests is an offline attack surface for no benefit.
#[cfg(unix)]
fn restrict(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| Error::Auth(format!("restricting {}: {e}", path.display())))
}

#[cfg(not(unix))]
fn restrict(_path: &Path) -> Result<(), Error> {
    Ok(())
}

fn parse(text: &str) -> Result<Vec<TokenRecord>, Error> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let f: Vec<&str> = line.split('\t').collect();
            let [id, label, sha, issued, rotated] = f[..] else {
                return Err(Error::Auth(format!("token store: malformed line {line:?}")));
            };
            Ok(TokenRecord {
                id: id.to_string(),
                label: label.to_string(),
                sha256_hex: sha.to_string(),
                issued_at_ms: issued
                    .parse()
                    .map_err(|e| Error::Auth(format!("issued_at_ms: {e}")))?,
                rotated_at_ms: match rotated {
                    "-" => None,
                    v => Some(
                        v.parse()
                            .map_err(|e| Error::Auth(format!("rotated_at_ms: {e}")))?,
                    ),
                },
            })
        })
        .collect()
}

/// Live sessions, in memory and keyed to this process.
///
/// **A daemon restart invalidates every session, and that is correct rather than a limitation.**
/// The SSE epoch changes at the same instant (`sse::Events`), so a client discovers both at once
/// and re-authenticates into a stream it can trust rather than resuming one whose history it
/// cannot account for.
#[derive(Debug, Default)]
pub struct Sessions {
    live: Mutex<HashMap<String, Live>>,
}

/// One live session. **It remembers the token it was born from**, which is what lets
/// `token rotate` end it — see [`Sessions::token_for`].
#[derive(Debug, Clone)]
struct Live {
    token_id: String,
    /// When it was minted. Fixed, and what [`SESSION_ABSOLUTE_TTL`] is measured from.
    created_at: SystemTime,
    /// When it was last used. Slides, and what [`SESSION_IDLE_TTL`] is measured from.
    last_seen: SystemTime,
}

impl Sessions {
    /// Mint a session id against the token that authorised it. 32 random bytes from the same
    /// source the engine credential uses.
    ///
    /// # Errors
    /// The system random source is unavailable.
    pub fn create(&self, token_id: &str) -> Result<String, Error> {
        let id = random_hex()?;
        let mut live = self
            .live
            .lock()
            .map_err(|_| Error::Poisoned("session lock poisoned"))?;
        let now = SystemTime::now();
        live.insert(
            id.clone(),
            Live {
                token_id: token_id.to_string(),
                created_at: now,
                last_seen: now,
            },
        );
        Ok(id)
    }

    /// Is this session live, and which token opened it? Expired entries are dropped as found.
    ///
    /// **Returning the token id rather than a bool is what closes the third half.** A
    /// session that outlives the credential that created it means `token rotate` invalidates a
    /// leaked token for future sign-ins while leaving the attacker's existing session running to
    /// its full lifetime — the one window an operator rotating a credential is trying to shut.
    /// The caller pairs this with [`TokenStore::is_active`]; this type stays ignorant of the
    /// store, so neither has to know how the other is built.
    /// **Using a session RENEWS it**: the idle window slides on every authenticated request,
    /// while the absolute ceiling does not move. An operator working through an incident is never
    /// signed out mid-incident; one who walked away a week ago is.
    #[must_use]
    pub fn token_for(&self, id: &str) -> Option<String> {
        let mut live = self.live.lock().ok()?;
        let now = SystemTime::now();

        // Two lookups rather than one, because deciding to evict and then evicting cannot both
        // hold a borrow of the map. A `HashMap` hit is not the cost worth contorting this for.
        let verdict = live.get(id).map(|session| {
            let idle_out = now
                .duration_since(session.last_seen)
                .is_ok_and(|idle| idle >= SESSION_IDLE_TTL);
            let aged_out = now
                .duration_since(session.created_at)
                .is_ok_and(|age| age >= SESSION_ABSOLUTE_TTL);
            (idle_out || aged_out, session.token_id.clone())
        });

        match verdict {
            None => None,
            Some((true, _)) => {
                live.remove(id);
                None
            }
            Some((false, token_id)) => {
                if let Some(session) = live.get_mut(id) {
                    session.last_seen = now;
                }
                Some(token_id)
            }
        }
    }

    /// The two instants a session is judged against, for the test that asserts one slides and the
    /// other does not. Both are private state; a test that reached around them would be asserting
    /// its own arithmetic instead of this type's.
    #[must_use]
    pub fn stamps_for_test(&self, id: &str) -> Option<(SystemTime, SystemTime)> {
        let live = self.live.lock().ok()?;
        live.get(id).map(|s| (s.created_at, s.last_seen))
    }

    /// End a session — sign-out. An id nothing holds is a no-op, and so is a poisoned lock.
    pub fn destroy(&self, id: &str) {
        if let Ok(mut live) = self.live.lock() {
            live.remove(id);
        }
    }
}

/// Hex of 32 random bytes, from the same source `storage::server` mints the engine credential
/// with. Reused rather than reimplemented: one place decides what random means here.
fn random_hex() -> Result<String, Error> {
    analyzer_serving_clickhouse::engine::server::random_token().map_err(Error::from)
}

fn sha256_hex(value: &str) -> String {
    analyzer_serving_clickhouse::engine::server::sha256_hex_upper(value.as_bytes())
}

// ===========================================================================================
// The middleware. Wraps the read group as a whole, so a NEW ROUTE IS AUTHENTICATED BY DEFAULT.
// An opt-in per handler is a thing somebody eventually forgets exactly once.
// ===========================================================================================

/// Reject anything without a live session.
///
/// **Every endpoint, including `/v1/stamp`.** A stamp names the fleet, the semantics generation
/// and the analyzer version, which is reconnaissance. `/metrics` keeps its own unauthenticated
/// posture on its own listener and its own bind; nothing about that surface changes.
///
/// # Errors
/// No cookie, or a cookie naming a session this process does not hold.
pub async fn require_session(
    axum::extract::State(state): axum::extract::State<crate::ApiState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, crate::error::ApiError> {
    let Some(id) = session_cookie(request.headers()) else {
        return Err(crate::error::ApiError::new(
            crate::error::Cause::Unauthenticated,
            "no session; exchange a viewer token at POST /v1/session",
        ));
    };
    let Some(token_id) = state.sessions.token_for(&id) else {
        return Err(crate::error::ApiError::new(
            crate::error::Cause::SessionExpired,
            "the session is expired or was ended by an analyzer restart; authenticate again",
        ));
    };
    // **The token behind the session must STILL be active.** Without this, `token rotate` shuts
    // the door for the next sign-in and leaves every session the leaked credential already opened
    // running to its own expiry. Reusing `session_expired` rather than minting a cause: the closed
    // set is a contract, the honest sentence fits in the message, and to the client the required
    // action is identical — authenticate again.
    if !state.tokens.is_active(&token_id) {
        state.sessions.destroy(&id);
        return Err(crate::error::ApiError::new(
            crate::error::Cause::SessionExpired,
            "the viewer token behind this session was rotated; authenticate again with the \
             replacement",
        ));
    }
    Ok(next.run(request).await)
}

/// The session id from a `Cookie` header, if present.
#[must_use]
pub(crate) fn session_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|c| c.trim().split_once('='))
        .find(|(k, _)| *k == "infertap_session")
        .map(|(_, v)| v.to_string())
}

#[cfg(test)]
mod tests {
    use super::{Sessions, TokenStore};

    fn store(dir: &std::path::Path) -> TokenStore {
        TokenStore::open(&dir.join("tokens")).expect("open")
    }

    #[test]
    fn a_missing_store_is_an_empty_one_that_admits_nobody() {
        let dir = tempdir();
        let s = store(&dir);
        assert!(
            s.verify("anything").is_none(),
            "a deployment that never issued a token answers 401 to everything"
        );
    }

    #[test]
    fn the_plaintext_is_printed_once_and_never_reaches_disk() {
        let dir = tempdir();
        let s = store(&dir);
        let (plaintext, record) = s.issue("laptop", 1).expect("issue");

        assert_eq!(
            s.verify(&plaintext).as_deref(),
            Some(record.id.as_str()),
            "verify answers with the id of the token that matched"
        );
        let on_disk = std::fs::read_to_string(dir.join("tokens")).expect("read");
        assert!(
            !on_disk.contains(&plaintext),
            "the store must hold no plaintext; it held {on_disk:?}"
        );
        assert!(
            on_disk.contains(&record.sha256_hex),
            "the digest is what is stored"
        );
    }

    #[test]
    fn rotate_invalidates_the_prior_token_in_the_same_operation() {
        // The window two steps would leave open is exactly the one an operator rotating a leaked
        // credential cannot afford.
        let dir = tempdir();
        let s = store(&dir);
        let (old, record) = s.issue("laptop", 1).expect("issue");
        let (new, _) = s.rotate(&record.id, 2).expect("rotate");

        assert!(
            s.verify(&old).is_none(),
            "the rotated token stops working immediately"
        );
        assert!(s.verify(&new).is_some(), "its replacement works");
        assert_eq!(s.records().len(), 2, "the superseded record is kept");
    }

    // =======================================================================================
    // `token issue|rotate` runs in the CLI process and the analyzer serves from another, so a
    // store read once at startup would learn nothing. Every test below holds TWO `TokenStore`
    // values over ONE path, because that is the shape production has.
    // =======================================================================================

    #[test]
    fn a_token_issued_by_another_process_is_accepted_without_a_restart() {
        let dir = tempdir();
        let serving = store(&dir); // the daemon, already up
        let cli = store(&dir); // `infertap-analyzer token issue`

        let (plaintext, record) = cli.issue("laptop", 1).expect("issue");

        assert_eq!(
            serving.verify(&plaintext).as_deref(),
            Some(record.id.as_str()),
            "a token minted while the analyzer is running must work immediately, without a \
             daemon restart"
        );
    }

    #[test]
    fn a_rotated_token_is_refused_by_a_store_that_was_already_open() {
        let dir = tempdir();
        let cli = store(&dir);
        let (leaked, record) = cli.issue("laptop", 1).expect("issue");

        let serving = store(&dir);
        assert!(
            serving.verify(&leaked).is_some(),
            "precondition: the credential works before it is rotated"
        );

        let (replacement, _) = cli.rotate(&record.id, 2).expect("rotate");

        assert!(
            serving.verify(&leaked).is_none(),
            "ROTATION MUST REVOKE. A running analyzer that kept accepting a rotated credential \
             until restart would make `token rotate` a no-op for the person using it"
        );
        assert!(
            serving.verify(&replacement).is_some(),
            "and the replacement works, in the same operation"
        );
    }

    #[test]
    fn a_live_session_dies_with_the_token_that_opened_it() {
        // The third half: revoking future sign-ins leaves the attacker's EXISTING session running
        // to its own expiry, which is the window the operator is rotating to shut.
        let dir = tempdir();
        let cli = store(&dir);
        let (_, record) = cli.issue("laptop", 1).expect("issue");

        let serving = store(&dir);
        let sessions = Sessions::default();
        let session = sessions.create(&record.id).expect("create");

        let token_id = sessions.token_for(&session).expect("session is live");
        assert!(
            serving.is_active(&token_id),
            "precondition: the session's token is active"
        );

        cli.rotate(&record.id, 2).expect("rotate");

        assert!(
            sessions.token_for(&session).is_some(),
            "the session itself has not expired"
        );
        assert!(
            !serving.is_active(&token_id),
            "but the token behind it is gone, and `require_session` refuses on that pairing"
        );
    }

    #[test]
    fn the_store_is_replaced_atomically_and_never_left_world_readable() {
        // `std::fs::write` truncates before it writes, so an in-place flush has a window where a
        // concurrent reader parses an empty file and 401s a valid credential for no reproducible
        // reason. A rename has no such window.
        let dir = tempdir();
        let s = store(&dir);
        s.issue("laptop", 1).expect("issue");
        s.issue("desk", 2).expect("issue");

        assert!(
            !dir.join("tokens.tmp").exists(),
            "the staging file must not survive a successful flush"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.join("tokens"))
                .expect("stat")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "the renamed store carries 0600");
        }
    }

    #[test]
    fn the_public_id_reveals_no_part_of_the_stored_digest() {
        // An id derived from a prefix of the digest would announce part of the value `verify`
        // compares against, and ids travel where digests do not.
        let dir = tempdir();
        let s = store(&dir);
        let (_, record) = s.issue("laptop", 1).expect("issue");

        let bare = record.id.trim_start_matches("tok-").to_uppercase();
        assert!(
            !record.sha256_hex.contains(&bare),
            "the id {:?} appears inside the stored digest {:?}",
            record.id,
            record.sha256_hex
        );
        assert!(
            !record.sha256_hex.starts_with(&bare),
            "the id must not be a prefix of the digest"
        );
    }

    #[test]
    fn a_store_round_trips_through_disk() {
        let dir = tempdir();
        let plaintext = {
            let s = store(&dir);
            s.issue("laptop", 1).expect("issue").0
        };
        let reopened = store(&dir);
        assert!(reopened.verify(&plaintext).is_some());
    }

    #[test]
    fn a_deployment_that_never_issued_a_token_gets_one_at_first_serve() {
        // The alternative: start the daemon, open the console, meet a sign-in screen, and
        // discover a second command exists. No comparable product asks for that step.
        let dir = tempdir();
        let s = store(&dir);
        let secret = dir.join("initial-token");

        let record = super::bootstrap_token(&s, &secret, 1)
            .expect("bootstrap")
            .expect("an empty store mints one");
        assert_eq!(record.label, "initial");

        let plaintext = std::fs::read_to_string(&secret).expect("the file exists");
        assert_eq!(
            s.verify(plaintext.trim()).as_deref(),
            Some(record.id.as_str()),
            "the file must hold a credential that actually works — an operator `cat`s this and \
             pastes it, and a mismatch is a sign-in loop with no error to read"
        );
    }

    #[test]
    fn the_first_token_file_is_owner_only_from_the_instant_it_exists() {
        // `fs::write` then `set_permissions` leaves a window where a plaintext viewer credential
        // is world-readable. It is a small window and the file is the whole credential.
        let dir = tempdir();
        let s = store(&dir);
        let secret = dir.join("initial-token");
        super::bootstrap_token(&s, &secret, 1).expect("bootstrap");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&secret)
                .expect("stat")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "the plaintext is owner-only");
        }
    }

    #[test]
    fn a_store_that_already_has_a_token_is_left_completely_alone() {
        // `token issue` stays the AUTOMATED path: a shop that seeds the store with Ansible before
        // first start must never have a second
        // credential minted behind it, and a restart must not disturb a working deployment.
        let dir = tempdir();
        let s = store(&dir);
        let (existing, _) = s.issue("ansible", 1).expect("issue");
        let secret = dir.join("initial-token");

        assert!(
            super::bootstrap_token(&s, &secret, 2)
                .expect("bootstrap")
                .is_none(),
            "a store with a token mints nothing"
        );
        assert!(
            !secret.exists(),
            "and writes no file, because there is no plaintext it could honestly put there"
        );
        assert!(
            s.verify(&existing).is_some(),
            "the seeded token still works"
        );
        assert_eq!(s.records().len(), 1, "and no second record appeared");
    }

    #[test]
    fn bootstrap_sees_a_token_issued_after_this_handle_was_opened() {
        // The same cross-process reality, at the one moment it decides whether to mint.
        //
        // **The ORDER here is the whole test.** `serving` is opened while the store is still
        // empty, so its in-memory copy says "never issued"; the installer then seeds one. A
        // bootstrap that trusted what it read at open would mint a SECOND credential behind a
        // deployment that already has one, and write a file telling the operator to use it.
        //
        // Opened AFTER the issue, `TokenStore::open` would satisfy the assertion on its own and
        // the test would pass with the re-read deleted, which is to say it would assert nothing.
        let dir = tempdir();
        let serving = store(&dir);
        let cli = store(&dir);
        let (seeded, _) = cli.issue("seeded-by-the-installer", 1).expect("issue");

        assert!(
            super::bootstrap_token(&serving, &dir.join("initial-token"), 2)
                .expect("bootstrap")
                .is_none(),
            "the store ON DISK is what decides, not whatever this handle read at open"
        );
        assert!(
            !dir.join("initial-token").exists(),
            "and no file is written, because there is no plaintext it could honestly hold"
        );
        assert!(
            serving.verify(&seeded).is_some(),
            "the installer's credential is the one that works"
        );
    }

    #[test]
    fn the_session_cookie_omits_secure_unless_a_deployment_asks_for_it() {
        // **The failure it prevents is silent.** This listener speaks plaintext and binds
        // loopback by default. Safari discards a `Secure` cookie on http://localhost
        // outright, so an unconditional one means: 200 with a Set-Cookie the browser drops, every
        // read 401s, and the sign-in screen returns with nothing to render. That is the demo kit
        // and the SSH-tunnel topology — the most locked-down way to reach this product.
        let plain = super::session_cookie_header("abc", 60, false);
        assert!(
            !plain.contains("Secure"),
            "the default must be droppable by no browser: {plain}"
        );

        let tls = super::session_cookie_header("abc", 60, true);
        assert!(
            tls.contains("; Secure;"),
            "a TLS-terminated deployment asks for it and gets it: {tls}"
        );
    }

    #[test]
    fn the_session_cookie_is_lax_and_carries_no_host_prefix() {
        let cookie = super::session_cookie_header("abc", 60, true);

        // `Strict` withholds the cookie on cross-site top-level navigation, so a deep link
        // pasted into a runbook or chat always lands on sign-in even for a signed-in operator --
        // against a console whose stated grammar is that URL is state. `Lax` still withholds it
        // on cross-site POST, which is the CSRF vector, and this API has exactly one POST.
        assert!(cookie.contains("SameSite=Lax"), "{cookie}");
        assert!(!cookie.contains("SameSite=Strict"), "{cookie}");

        // Chrome REJECTS `__Host-` on localhost even where it accepts a bare Secure cookie,
        // so the obvious hardening would break the demo kit on a second browser. Its absence is a
        // decision, and this is what keeps it one.
        assert!(
            !cookie.contains("__Host-") && !cookie.contains("__Secure-"),
            "a cookie prefix would break Chrome on localhost: {cookie}"
        );
        assert!(cookie.contains("HttpOnly"), "{cookie}");
    }

    #[test]
    fn signing_out_spells_the_cookie_exactly_as_signing_in_did() {
        // A browser replaces a cookie by matching name, domain and path. A delete that spells any
        // of them differently leaves the original in place and the sign-out silently does nothing,
        // which is why both routes go through one builder rather than two format strings.
        let minted = super::session_cookie_header("abc", 600, true);
        let cleared = super::session_cookie_header("", 0, true);

        let attributes = |c: &str| {
            c.split("; ")
                .skip(1)
                .filter(|a| !a.starts_with("Max-Age"))
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            attributes(&minted),
            attributes(&cleared),
            "the delete must match the mint in every attribute but its lifetime"
        );
        assert!(cleared.contains("Max-Age=0"), "{cleared}");
    }

    #[test]
    fn a_session_is_valid_until_it_is_destroyed() {
        let s = Sessions::default();
        let id = s.create("tok-abc").expect("create");
        assert_eq!(
            s.token_for(&id).as_deref(),
            Some("tok-abc"),
            "a live session names the token that opened it"
        );
        s.destroy(&id);
        assert!(
            s.token_for(&id).is_none(),
            "a destroyed session is not a live one"
        );
        assert!(s.token_for("never-issued").is_none());
    }

    #[test]
    fn using_a_session_slides_its_idle_window_but_not_its_ceiling() {
        // The shape, asserted on the record rather than by waiting a week. The idle window is
        // what an operator working through an incident renews; the ceiling is what they cannot.
        let sessions = Sessions::default();
        let id = sessions.create("tok-abc").expect("create");

        let (created, first_seen) = sessions.stamps_for_test(&id).expect("live");
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(sessions.token_for(&id).is_some(), "a use, which renews");

        let (created_again, seen_again) = sessions.stamps_for_test(&id).expect("still live");
        assert_eq!(
            created, created_again,
            "the absolute ceiling is measured from a fixed instant and using the session must \
             not move it — otherwise a continuously-used session never ends"
        );
        assert!(
            seen_again > first_seen,
            "and the idle window slid, which is what keeps an operator signed in while working"
        );
    }

    #[test]
    fn the_two_lifetimes_are_ordered_and_the_ceiling_is_the_larger() {
        // A ceiling below the idle window would make the sliding renewal unreachable, which is a
        // silent way to ship a flat expiry.
        assert!(
            super::SESSION_ABSOLUTE_TTL > super::SESSION_IDLE_TTL,
            "the absolute lifetime must exceed the idle one or renewal means nothing"
        );
    }

    fn tempdir() -> std::path::PathBuf {
        let base = std::env::temp_dir().join(format!(
            "api-auth-{}-{}",
            std::process::id(),
            analyzer_serving_clickhouse::engine::server::random_token().expect("random")
        ));
        std::fs::create_dir_all(&base).expect("mkdir");
        base
    }
}
