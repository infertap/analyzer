# Adding an HTTP operation

Start with [the read API](read-api.md). The router, request constructor,
DAO read, wire type and contract each have one job. Use `coverage` for a small unpaged example
and `holders` for a paged example. Neither needs a controller or a new execution framework.

## Follow one operation

`GET /v1/holders` is declared in [routes.rs](../crates/api/src/routes.rs). Authentication wraps
its resource group. [Params](../crates/api/src/extract.rs) checks query names against the
matched OpenAPI operation and deserializes `HoldersQuery`. Its
[constructor](../crates/api/src/requests.rs) validates the window, grain, class and page size
without touching the database. The resulting `HoldersRequest` has private fields and read-only
accessors. Its context captures the clock and resolved window once.

The [handler](../crates/api/src/resources/holders.rs) reads a stamp for that context, names the
operation's question and calls `Input::execute`. The
[pagination owner](../crates/api/src/pagination.rs) binds the already-decoded token, validates
its resource position and runs the DAO closure across the private revision boundary. Only an
available result provides `Completed`, which can mint the response's page metadata. An
`Unavailable` result can become an in-band refusal; invalid input and backend errors remain
`Err(ApiError)`. A handler cannot construct `Unavailable` or `Completed` itself.

The DAO owns the ordered query and continuation position. The handler maps its rows into
[HoldersData](../crates/api/src/wire.rs) and calls `Completed::page`. The shared envelope attaches
the stamp. `wire` is the public JSON representation; DAO records are not implicitly serialized.
`openapi::paths` attaches the request parameters and references the derived response schema.

## Add an operation

1. Define its question and result first. Choose an existing domain parser only if the semantics
   match. In `requests.rs`, define a raw `Deserialize` type with only that operation's fields,
   `deny_unknown_fields`, and a constructor returning immutable validated values. `WindowQuery`
   and `DrillQuery` are reusable where their entire contracts match. Use `NoQuery` even when
   there are no query parameters. Body and path extraction use `Body` and `Subject` so malformed
   transport input gets the same JSON error envelope.
2. Validate before reading a stamp or entering a DAO closure. Resolve relative time with one
   `state.now_ms()` sample. Carry the resulting context; never rebuild the window inside the
   query or response mapping. Explicit, data-dependent grain reach still belongs to the DAO.
   Opaque IDs have no invented character restrictions; typed rank/group selectors are `u32`.
3. For an unpaged operation, use `ApiState::read` and a typed wire result. This keeps blocking
   work, bounded admission and the caller deadline together. Do not add SQL to HTTP code.
4. For a page, construct `pagination::Input` from the validated size and the operation's raw
   spelling, then use `question` with the resolved window, stable operation name and every path
   subject. Raw spelling is retained only for the existing strict cursor identity: omission
   and explicit defaults remain distinct. It is never interpreted again for execution.
   Provide a position decoder and a DAO closure to `execute`. Match `Outcome`, map available
   rows and call `Completed::page(returned, total, next_position)`. Do not decode/mint tokens,
   call the revision guard, catch arbitrary errors as successful refusals, or retry a failed
   page read in the handler. The day resource is the explicit partial-result example: only
   its unpaged sections are reread after a page becomes unavailable.
5. Make the DAO order total over the stored key, including null/epoch handling. Its resume
   predicate, local order and exported-tier order must agree. The event position codecs use
   JSON tuples inside opaque tokens, so identifier delimiters are data. Changing order or
   encoding requires a cursor version change; earlier formats already return `cursor_stale`.
6. Add the handler to its resource family and register it in the authenticated router group.
   Describe it in `openapi.rs` and regenerate `contract/openapi.json` using
   `cargo run -q -p analyzer-api --bin dump-openapi`. Do not add a parameter-name allowlist.
   The static audit follows each real route to its raw request type and compares its fields
   with that operation's committed specification. Defaults, enums and relationships need
   semantic tests as well; field parity alone does not prove them.
7. Add behavioral tests for the new semantics. Reuse the existing API fixture, which waits for
   physical retirement before ordinary reads. For paging, compare complete ordered results
   with page walks at several limits, including tied leading sort values and an exact last
   boundary. Use the existing malformed/stale/writer/unfinished/retirement regressions for the
   shared protocol instead of copying those scenarios into every resource test.

Keep request validation separate from application effects. For example, annotations validate a
`DeployAnnotation` before writing its durable object and derived row; session authentication
still verifies the credential through the session realm. SSE keeps its own replay/epoch protocol
and does not pass through data pagination.

## Checks

Run `scripts/preflight.sh` while working and the affected engine tests. API engine tests use
`scripts/test.sh engine api`; read-layer changes additionally need `cargo test -p analyzer-serving-clickhouse -- --ignored --test-threads=1`.
Run `scripts/prepush.sh` before pushing; it includes the full correctness suites, documentation,
public Rust API and OpenAPI checks. Update intentional Rust surface changes with
`scripts/public-api.sh update`. Never change a golden to conceal a behavior change.

A naming cleanup, a null/empty change or changing what a window means is a client
contract decision, separate from moving implementation code.
