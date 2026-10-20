# The read API

The versioned seam between the analyzer and the console. The console consumes only this
contract and reaches nothing else. `contract/openapi.json` is the machine-readable form,
generated from the running API (section 10); this document says what the contract means.
[Adding an HTTP operation](contributing-an-http-endpoint.md) walks a change through it.

## 1. What it is

- **Endpoints may record declarations; no endpoint alters a measurement.** Two endpoints record
  something. `POST /v1/analyze` asks for a tick now. `POST /v1/annotations` records an
  operator's deploy declaration as a bucket object; the next ingest cycle commits it to the
  input log, and the core states its marker. Neither touches a figure the analysis derived. A
  process with no bucket refuses the declaration. The auth realm (`POST` and `DELETE
  /v1/session`) touches no fleet data.
- **Thin.** A handler maps its path and query to one DAO call and serializes the result. A
  read that needs composition gets a DAO method, not API-side code.
- **The panel list, the DAO method list and the endpoint list are one list.** Section 3's table
  names the DAO method behind each endpoint, and `static_audit` holds every endpoint in that
  table to a route.
- **Addressing mirrors the console's three levels.** Level 0 is the fleet (`/v1/figures` and
  its siblings), level 1 a subject (a holder, a block, a day, an instance), level 2 the evidence
  behind a figure (`/v1/evidence`). A console route and the call behind it read as a pair.

## 2. The envelope

### 2.1 Every response carries its stamp

The top-level object of every `200` has exactly two keys:

```json
{
  "stamp": {
    "fleet_id": "prod-usc1",
    "window": { "start_ms": 1784592000000, "end_ms": 1785801600000,
                "start_day": "2026-07-21", "end_day": "2026-08-03" },
    "tick": { "at_ms": 1785765720000, "classified_to_ms": 1785765600000,
              "classified_day": "2026-08-03", "emitted_to_ms": 1785765600500,
              "covered_to_ms": 1785765600000, "audited_to_ms": 1785765600000,
              "settled_through_day": "2026-08-02" },
    "semantics_version": "js-7",
    "discriminator": "s1000x3600000",
    "raw_horizon": { "from_ms": 1784592000000, "from_day": "2026-07-21", "declared_days": 30 },
    "analyzer_version": "1.0.10"
  },
  "data": { }
}
```

- **`window` is the resolved HTTP window** (section 3.1). `end_ms` is exclusive and `end_day`
  is the last day covered, so a consumer differencing the instants and a consumer reading the
  day labels arrive at the same length. A read with no finite interval (`/v1/delivery`) states
  `window: null`.
- **`tick`** states the edges the last applied tick reached, each null until a tick has stated
  it: `classified_to_ms` (and its UTC day `classified_day`), through which stores are
  classified; `emitted_to_ms`, through which placement is emitted; `covered_to_ms`, through
  which coverage is stated; `audited_to_ms`, before which every block the sampled audit
  re-derived agreed with what its tick stated; `settled_through_day`, the last day whose
  day-grain outputs are stated; and `at_ms`, when the tick was applied. The console renders
  the live edge from them rather than assuming today is classified. `audited_to_ms` equals
  `classified_to_ms` unless a sample disagreed; then it holds where the disagreement was found
  until the derived state is derived afresh.
- **`raw_horizon.from_ms` is measured, never declared**: the first record instant the input
  log holds. `declared_days` beside it is the configured horizon (`ledger_retention_days`), two
  labelled numbers with no inference between them.
- **The stamp is identical across responses at steady state**, so the console renders one
  canonical stamp and only a response's deviations from it.

### 2.2 A naked number is unrepresentable

A figure is a measured token mass, a fraction of stored mass, or a coverage fraction. It is
never a timestamp, a row count, an identifier or an event count. Every figure serializes with
its register and its coverage:

```json
{ "value": 4187266.0, "register": ">=", "coverage": 0.978, "coverage_kind": "token-weighted" }
```

`register` says how the value relates to the truth (`>=` for a lower bound, `=` for an exact
count); `coverage` says how much of the window the value was measured over. An event count is a
bare integer, because it is qualified by the coverage of the mass beside it in the same row.

Two checks hold this: `goldens::every_endpoint_family_carries_its_envelope_and_no_naked_figures`
walks a response from every endpoint family, and `static_audit` holds every figure field on a
wire type to the `Figure` type, which serializes through the one function that writes the
shape.

## 3. The endpoints

| method and path | answers | DAO method |
|---|---|---|
| `GET /v1/stamp` | the canonical stamp; answers before any tick has been applied | — |
| `GET /v1/figures` | recompute mass per bucket per requested dims, segmented by meaning generation | `waste_by_segmented` |
| `GET /v1/figures/bands` | recompute mass banded by time since eviction, with the gap-unobserved mass beside the bands | `bands_by` |
| `GET /v1/holders` | the holders table, with the denominator its shares were taken against | `holders_page` |
| `GET /v1/coverage` | coverage per day, by instance or for the fleet, instance-time weighted | `coverage` |
| `GET /v1/roster` | the fleet's instances and incarnations; the window is a wall clip | `roster_page` |
| `GET /v1/health` | the meter's health: ingest, coverage, the roster, agent versions, module spans | `ingest_status`, `coverage`, `roster`, `agent_versions`, `module_availability` |
| `GET /v1/delivery` | the delivery ledger since install: what the taps produced, what arrived | `delivery` |
| `GET /v1/markers` | the generation markers and annotations over the window | `markers` |
| `GET /v1/markers/{id}` | one marker | `markers` |
| `GET /v1/blocks` | each day's blocks by stored mass, heaviest first, every figure a bound | `block_days` |
| `GET /v1/blocks/{block_id}/holders` | which holders stored a block | `holders_of` |
| `GET /v1/holders/{i}/{d}/{g}/timeline` | one holder's store timeline, event grain | `timeline` |
| `GET /v1/days/{day}` | one UTC day | `day_detail` |
| `GET /v1/evidence` | one holder's or one block's stores over the window | `evidence_page` |
| `GET /v1/tags` | the declared tags grouped over the window, with exposure and refetch figures | `tags` |
| `GET /v1/residual/placement` | co-residency token-time per workload and holder pair | `placement` |
| `GET /v1/residual/replication` | duplicated token-time per bucket per copy band | `copies` |
| `GET /v1/residual/whales` | the heaviest duplicated identities over the window's settled days, each bounded | `whales` |
| `GET /v1/instances/{instance}/duplication` | one node's duplication inside itself | `instance_duplication` |
| `GET /v1/events` | the event stream (section 5) | — |

`POST /v1/analyze`, `POST /v1/annotations` and `POST`/`DELETE /v1/session` are the recording
endpoints of section 1. Each operation's fields are described in `contract/openapi.json`.

### 3.1 Shared query parameters

| param | values | default | notes |
|---|---|---|---|
| `window` | `<N>d`, `<N>h`, or `start_ms,end_ms` | `14d` | one grammar on every windowed read |
| `grain` | `auto`, `minute`, `hour`, `day` | `auto` | one vocabulary on every bucketed read |
| `dims` | comma-separated `class,holder,instance,workload,model` | none | a closed set; an unknown value is a `400` naming the legal set |
| `limit` | `1..=200` | 100 (50 on some reads) | each operation states its default |
| `cursor` | opaque | none | section 8 |

**`window` resolves against wall now.** `<N>d` is N whole UTC days through today; `<N>h` is N
hours ending at the edge of the bucket the span naturally draws (`48h` is forty-eight whole
hours through the current one, `3h` one hundred and eighty whole minutes through the current
one). The trailing bucket is the live, partial one, and the stamp's edges say how far it is
classified and emitted. Two calls inside one bucket resolve to the same window. Explicit
`start_ms,end_ms` bounds are kept exactly. A relative window needs no tick to exist: a fleet
with nothing classified yet gets the window it asked for and a stamp whose edges are null.

**`grain=auto` lets the server choose,** because only the server knows the reach of each tier
it holds. It serves the finest tier whose measured reach covers the range and whose bucket count
fits the tier's point budget (`AUTO_GRAIN_HOUR_POINT_BUDGET`, `docs/conformance.md`), and the
response states the grain it served. **An explicit grain past its tier's reach is refused with
`grain_unavailable`, the reach stated, never downgraded:** a silent fall back would answer a
different question than the one asked.

**Scope** narrows a read to a slice (`workload`, `instance`, `holder`) where `dims` says how to
group it. A holder cut at minute grain is refused by name: the minute cells are kept per rank,
not per holder.

## 4. Registers and seams

**Range queries return segmented series.** `/v1/figures` returns `segments[]`, never one
blended array. Each segment carries its own `discriminator`, `semantics_version` and day count,
and a trend crossing a parameter change is two segments with the marker between them. There is
no `total` spanning segments and no parameter that requests one.

- A **discriminator** seam separates rows this binary can read: both segments carry figures.
- A **semantics version** seam does not. A binary can explain only its own semantics version,
  so a segment of another serializes with `"figures": null` and
  `"reason": "superseded_semantics"`. The console hatches the span; it never renders it as
  zero.

**A cross-instance zero says what it means.** `/v1/residual/replication` carries
`zero_basis` beside `cross_engine_token_ms`: the instances that stored into their own cache
over the window's UTC days, their own and comparable (`content_id`) stores, and the key spaces
of the runs that made them. Its `meaning` is `positive` for a nonzero figure, `measured` for a
zero over two or more instances, every store comparable and one key space, and otherwise the
first condition that failed: `single_holder`, `not_comparable`, `split_key_space`,
`partly_comparable` (spec 5.7). Only `measured` is a finding of no duplication.

**Recompute and duplication are never summed together.** They are different endpoints and
different response types, and each names its own register. The same tokens can appear in both
for different reasons, so a total over both would be false whoever computed it.

## 5. The event stream

`GET /v1/events`, `text/event-stream`, session-authenticated like every read. Frames are
`id: {epoch}:{seq}`, `event: <kind>` and `data: <payload>`; an idle stream carries a
`: keepalive` comment every 20 seconds, so a proxy's read timeout (nginx's default is 60 s)
never tears it down.

```
id: 7:1412
event: tick_applied
data: {"classified_day":"2026-08-03",
       "window":{"start_ms":1784592000000,"end_ms":1785801600000},
       "discriminator":"s1000x3600000","semantics_version":"js-7",
       "at_ms":1785765720000}

id: 7:1413
event: ingest_batch
data: {"segment":"0442","records":186224,
       "span":{"start_ms":1785765300000,"end_ms":1785765480000},
       "at_ms":1785765780000}
```

- **`tick_applied`** carries the classified edge's day, which tells the console its cached
  figures are stale and which window to refetch, and the discriminator, so a parameter change
  mid-session shows as a seam.
- **`ingest_batch`** carries a segment, its record count and its span: the status strip's
  inputs. No accounting figure rides the stream.

**Reconnect with `Last-Event-ID`.** The id is a process epoch minted at start plus a monotonic
counter, and the server holds a bounded ring of recent events and no durable log:

| client sends | server does |
|---|---|
| an id in the current epoch, still in the ring | replays from the next event |
| an id in the current epoch, aged out of the ring | sends `resync`, then streams live |
| an id from a prior epoch (the analyzer restarted) | sends `resync`, then streams live |

`resync` carries the current stamp. Every event is a hint to refetch, and refetching is always
correct, so the server never pretends to have replayed what it does not hold.

## 6. Auth

### 6.1 Bind

The API listens on loopback by default, `127.0.0.1:8088`; `--api-addr` sets the address and
`off` turns it off. Reaching it from off the host is an operator's explicit decision. `run`
starts the analyzer and the API together, and the console the API serves is the product's only
reading surface: the CLI's verbs are box operations (install, token, health, diag).

### 6.2 Tokens

```
infertap-analyzer token issue [--label <name>]
infertap-analyzer token rotate <id>
```

- The plaintext is printed once, to stdout, and never stored. `token rotate` prints the new one
  and invalidates the old in the same operation.
- Only the SHA-256 reaches disk, in a `0600` file in the data directory, replaced by rename and
  never written in place. Comparison is constant-time.
- The store is re-read on every sign-in and every authenticated request, because `token` runs in
  another process than the one serving. Sign-in fails closed on an unreadable store; the
  per-request check fails open on a transient error, at most one reload stale.
- **First run mints one.** When the store holds no records, the analyzer issues a token labelled
  `initial` and writes its plaintext to `<data-dir>/initial-token`, created `0600`. The log names
  the path and the token id, never the plaintext. A store seeded before first start never meets
  this. The file persists; `token rotate` is what invalidates it.
- Viewer is the only role: every endpoint reads the same fleet.

### 6.3 The session

```
POST /v1/session        {"token": "<plaintext>"}        →  Set-Cookie: infertap_session=…
DELETE /v1/session                                      →  clears it
```

- The cookie is `HttpOnly; SameSite=Lax; Path=/`, with `Secure` only under
  `--api-cookie-secure`: the listener speaks plaintext (6.5), and Safari discards a `Secure`
  cookie on `http://localhost`. The analyzer states the posture in force at every start.
  `SameSite=Lax` lets a pasted deep link land signed in and still withholds the cookie on a
  cross-site `POST`.
- A session lives 7 days idle and 30 days absolute. Sessions are in memory, so a restart ends
  them, at the same moment the event stream's epoch changes.
- A session records the token that opened it and ends with it: `token rotate` ends every
  session the old token opened, answered `session_expired`.
- Every other endpoint answers `401` without a session, `/v1/stamp` included. `/metrics` keeps
  its own listener and posture.

### 6.4 CORS

None in production: the analyzer serves the console (section 9), so the two share an origin and
no CORS header is emitted. `--api-dev-origin <origin>` allows exactly one origin, echoed
literally, with credentials and the methods `GET, POST, DELETE, OPTIONS`, for the console's dev
server; the analyzer says so at start whenever it is on.

### 6.5 TLS

The listener speaks plaintext HTTP and is meant to sit behind the operator's proxy, where their
certificate management lives, or to be reached over an SSH tunnel. A publicly trusted
certificate would need the node to reach an ACME issuer every 90 days, which an airgapped
deployment cannot do. Native TLS is for the day a deployment needs the API off-host with no
proxy in front of it.

## 7. Errors

### 7.1 The shape

```json
{ "error": { "cause": "outside_raw_horizon",
             "message": "event grain is retained about 14 days; 2026-06-02 is older than the oldest record this fleet holds",
             "bound": { "raw_from_ms": 1784592000000, "raw_from_day": "2026-07-21",
                        "declared_days": 30 },
             "requested": { "day": "2026-06-02" } } }
```

`cause` is a closed enum a console switches on; `message` is a sentence a person reads;
`bound` and `requested` are the facts that make the refusal actionable.

### 7.2 Status

- **A well-formed request whose answer a data bound prevents → `200`**, with
  `data.refused: {cause, bound}`. A bound is a fact about the meter, rendered as a designed
  state.
- **A subject that never existed → `404`** with its cause (`unknown_holder`, `unknown_block`,
  `unknown_marker`).
- **A malformed request → `400`**: an unparseable window, an unknown dim, a limit out of range.
  A request the contract's shapes cannot read — a parameter the operation does not declare, a
  body that is not the declared JSON, a path segment of the wrong type, a token that was never a
  cursor — is `request_invalid`, in the same envelope. The declared set is read from
  `contract/openapi.json` at runtime, so the contract is the check.
- **No session, or an expired one → `401`.**
- **The engine or the bucket unreachable → `503`** with `engine_unavailable` or
  `bucket_unavailable`. Never a zero.

### 7.3 The causes

`unauthenticated`, `session_expired`, `unknown_holder`, `unknown_block`, `unknown_marker`,
`unknown_route`, `request_invalid`, `window_invalid`, `limit_invalid`, `dim_unknown`,
`grain_unavailable`, `subject_missing`, `outside_raw_horizon`, `superseded_semantics`,
`unlinked_generation`, `cursor_stale`, `annotation_invalid`, `bucket_unavailable`,
`engine_unavailable`, `console_not_configured`.

The set is `error::Cause::all()`, the same slice the error path switches on, and the contract
is generated from it. Every cause has a producer and a golden: a cause no code path raises
would teach a console to handle a state that cannot occur.

## 8. Pagination

A paged read carries `next_cursor` until it is exhausted. `limit` caps at `PAGE_LIMIT_MAX`
(200).

The cursor is an opaque encoding of the sort position, the stamp it was minted under, the
publication revision, and a digest of the question it answers: the surface, the resolved window,
every filter and the drill's subject. **A cursor presented against a newer publication, or with
any parameter changed, is refused with `cursor_stale`**, and the console restarts at page one.
The binding is checked when the cursor is parsed and again after the page is read, so a tick
that publishes while a page reads refuses the page rather than mixing rows of two publications.
A cursor of another format is stale; a token that was never a cursor is `request_invalid`, never
page one.

Offset pagination would skip or repeat rows as ticks restate them, and a bare value cursor has
the same defect. Binding the cursor to the publication closes it: a page is a claim about one
publication, the grammar every figure in the product already uses.

## 9. The console

The console is compiled into the binary. Anything not under `/v1` and not `/metrics` serves from
the embedded bundle, with a fallback to `index.html`, so every console URL deep-links. A build
with no console staged answers `503 console_not_configured`, never a `404`. No route composes a
filesystem path from a request.

`console/PIN.toml` pins the bundle by content hash. `scripts/stage-console.sh` stages it from a
console checkout or a release tarball and refuses bytes that do not hash to the pin;
`crates/api/build.rs` hashes again before embedding, and `INFERTAP_REQUIRE_CONSOLE=1` (set by the
release workflow) makes an unstaged build fail. `infertap-analyzer version` names the embedded
console's version and hash.

## 10. The contract

`contract/openapi.json` is generated by `cargo run -p analyzer-api --bin dump-openapi`, from the
wire types' schemas and the route table, with no daemon and no engine. CI's `contract` job
regenerates it and fails on any difference, so a contract change is a reviewable diff in the
change that causes it. The console vendors a verbatim copy pinned to an analyzer commit
(`contract/PIN.md` in the console repository).
