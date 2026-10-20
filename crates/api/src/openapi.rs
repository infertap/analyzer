// SPDX-License-Identifier: AGPL-3.0-only

//! The `OpenAPI` document — schemas DERIVED from the wire types (read-api section 10).
//!
//! **Committed at `contract/openapi.json` and CI-diffed**, so a contract change is a reviewable
//! diff in the PR that causes it — the same *exception is a diff, never a drift* discipline the
//! network fence and the query inventory both run on. `contract/` is where this workspace already
//! keeps the things it must not change by accident; `PIN.md` is its neighbour.
//!
//! # Why the schemas are derived
//!
//! The console generates its client from this file, so a path without response schemas would
//! type every body `any`, and a schema-parity test over such a document would pass trivially.
//!
//! So the schemas are derived from the wire types. **`$ref` targets come from the types too** — `schema_ref::<T>` reads the name off `ToSchema`, so renaming a wire struct
//! moves the reference with it instead of leaving a dangling pointer a reviewer has to notice.

use crate::error::Cause;
use crate::wire;
use serde_json::{json, Value};
use utoipa::PartialSchema;

/// Which causes a parameter can raise, keyed by the parameter's own name.
///
/// Every failure path read-api section 7.2 rules is declared, so a generated client types each
/// failure rather than handling it against prose.
///
/// **Keyed by parameter name rather than listed per endpoint, and that is the point.** A
/// hand-maintained per-operation cause list would be two lists that must correspond, maintained
/// by hand, drifting the moment someone adds a parameter and forgets. Here an operation's declared
/// causes are a FUNCTION of its declared parameters, so the pair cannot separate.
///
/// `the_document_knows_what_every_parameter_can_raise` pins the table against the document, so a
/// new parameter fails the build until its causes are stated.
const PARAM_CAUSES: &[(&str, &[Cause])] = &[
    // Relative forms anchor to the clock, so a window needs no tick to exist.
    ("window", &[Cause::WindowInvalid]),
    ("grain", &[Cause::GrainUnavailable]),
    ("dims", &[Cause::DimUnknown]),
    ("by", &[Cause::DimUnknown]),
    ("limit", &[Cause::LimitInvalid]),
    ("cursor", &[Cause::CursorStale]),
    // `day` is a UTC date on the `/v1/days/{day}` path; malformed is refused before it reaches
    // SQL.
    ("day", &[Cause::WindowInvalid]),
    // Evidence's subject selectors. A holder that never spoke is a 404, not an empty page.
    ("block", &[]),
    ("holder", &[Cause::UnknownHolder]),
    // Path parameters naming a subject that may not exist.
    ("id", &[Cause::UnknownMarker]),
    ("block_id", &[Cause::UnknownBlock]),
    ("instance", &[Cause::UnknownHolder]),
    ("dp_rank", &[Cause::UnknownHolder]),
    ("group_idx", &[Cause::UnknownHolder]),
    // Narrowing selectors over closed sets already expressed as `enum` in the schema.
    ("class", &[]),
    ("workload", &[]),
    // **Nothing.** A malformed or too-old `Last-Event-ID` RESYNCS rather than refusing —
    // `Resume::Resync` says the history is not continuous and the client refetches. Refusing a
    // reconnect would strand a console that had merely been away too long.
    ("Last-Event-ID", &[]),
];

/// Causes whose producer is deliberately NOT a documented `/v1` operation.
///
/// `console_not_configured` is raised by the SPA fallback, which serves a bundle rather than the
/// read API and is out of the document's scope by design. Listing it here is the *exception is a
/// diff, never a drift* discipline: the exemption is one line in review rather than a silent hole.
///
/// **It deletes itself.** `every_cause_that_can_be_raised_has_a_declared_status` asserts each entry
/// is absent from every operation, so the day something documents that route the exemption fails
/// and must be removed — an exemption that cannot outlive its reason.
/// `unknown_route` joins it for a sharper reason: it is raised ONLY on paths that have no
/// operation, so no operation can declare it without lying — an operation that documented it
/// would be claiming a response it can never produce. It reaches consumers through `Cause`'s
/// own enum instead, which is generated from `Cause::all()`, so a generated client still knows
/// the token exists and can switch on it.
#[cfg(test)]
const UNDOCUMENTED_ROUTE_CAUSES: &[Cause] = &[Cause::ConsoleNotConfigured, Cause::UnknownRoute];

/// Surfaces that return a RANKING rather than a walkable list, and therefore issue no cursor.
///
/// `/v1/blocks` is "the worst N blocks by mass": the cut is the question, not a limitation of the
/// response. A cursor would imply the tail is worth walking, and shape 4 exists to name the top of
/// a distribution. Keyset paging over it would also mean a tuple comparison against an AGGREGATE
/// in a `HAVING` — reachable, and the query shape `holders_page` explicitly declines as one nobody
/// can review.
///
/// **The exemption deletes itself**: `a_ranking_surface_does_not_quietly_become_a_paged_one` fails
/// the day one of these grows a cursor.
#[cfg(test)]
const RANKING_SURFACES: &[&str] = &["/v1/blocks"];

/// Every cause an operation can raise: the universal ones, its parameters', and its own.
///
/// Ordered by [`Cause::all`] rather than by discovery, so the emitted document is stable.
fn causes_of(parameters: &[Value], extra: &[Cause]) -> Vec<Cause> {
    // Session-gated, every operation, and every operation's request can fail to parse.
    let mut set = vec![
        Cause::Unauthenticated,
        Cause::SessionExpired,
        Cause::RequestInvalid,
    ];
    set.extend_from_slice(extra);
    for p in parameters {
        let Some(name) = p["name"].as_str() else {
            continue;
        };
        if let Some((_, causes)) = PARAM_CAUSES.iter().find(|(n, _)| *n == name) {
            set.extend_from_slice(causes);
        }
    }
    Cause::all()
        .iter()
        .copied()
        .filter(|c| set.contains(c))
        .collect()
}

/// The response block: `200`, plus one entry per distinct status the causes map to.
///
/// **Bounded refusals contribute no status**, because `Cause::status` maps them to `200` — they
/// ride in the data position by read-api section 7.2 and are already described by the `refused` field on the data
/// schema. The mapping is the same one the error path switches on, so a status appears here
/// exactly when a handler can actually produce it.
fn responses(ok: Value, causes: &[Cause]) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("200".into(), ok);
    for status in [400_u16, 401, 404, 501, 503] {
        let named: Vec<&str> = causes
            .iter()
            .filter(|c| c.status().as_u16() == status)
            .map(|c| c.as_str())
            .collect();
        if named.is_empty() {
            continue;
        }
        map.insert(
            status.to_string(),
            json!({
                "description": named.join(", "),
                // **The causes as DATA, not only prose.** `Error.cause` is the whole closed set;
                // this says which members of it can arrive at THIS status on THIS operation, so a
                // console narrows per status instead of handling twenty-two causes on every catch.
                // It is also what makes `every_cause_that_can_be_raised_has_a_declared_status`
                // structural rather than a description grep.
                "x-causes": named,
                "content": { "application/json": { "schema": {
                    "type": "object",
                    "required": ["error"],
                    "properties": { "error": { "$ref": "#/components/schemas/Error" } },
                }}},
            }),
        );
    }
    Value::Object(map)
}

/// Shapes whose in-band refusal leaves a payload that presents as `[]` rather than as absent.
///
/// **A refusal names its own SCOPE and only that scope goes absent.** `refused` guards the shape's
/// primary collection; `<thing>_refused` guards `<thing>`. `/v1/days` is why the rule needs stating
/// rather than assuming: only its holders list is paged, so a stale holders cursor must not blank
/// the day's class breakdown, which was read perfectly well.
///
/// Returns the violations rather than asserting, so the rule can be tested against shapes written
/// wrong on purpose — reverting a real one does not compile, and a rule only ever run against
/// correct input is one nobody knows works.
#[cfg(test)]
fn refusal_scope_violations(schemas: &serde_json::Map<String, Value>) -> Vec<String> {
    const PRIMARY: &[&str] = &["rows", "records", "figures"];
    let mut out = Vec::new();
    for (name, schema) in schemas {
        let Some(properties) = schema["properties"].as_object() else {
            continue;
        };
        let required = schema["required"].as_array().map_or(&[][..], |a| a);
        let is_required = |f: &str| required.iter().any(|r| r.as_str() == Some(f));
        for field in properties.keys() {
            let Some(scope) = field.strip_suffix("refused") else {
                continue;
            };
            // A REQUIRED refusal means the shape always refuses and has no payload to make absent.
            if is_required(field) {
                continue;
            }
            let scope = scope.trim_end_matches('_');
            if scope.is_empty() {
                // No collection present means no hazard: a per-row refusal guards `Option<Figure>`
                // fields, which already reach a client as nullable.
                for p in PRIMARY.iter().filter(|p| properties.contains_key(**p)) {
                    if is_required(p) {
                        out.push(format!(
                            "{name}.{p} is required on a shape that can refuse it in-band, so a \
                             refusal presents it as `[]` and a consumer that skipped `{field}` \
                             renders that as 'found nothing'. Make it absent instead."
                        ));
                    }
                }
            } else if !properties.contains_key(scope) {
                out.push(format!(
                    "{name}.{field} names scope `{scope}`, which {name} does not have"
                ));
            } else if is_required(scope) {
                out.push(format!(
                    "{name}.{scope} is required on a shape that can refuse it in-band, so a \
                     refusal presents it as `[]` and a consumer that skipped `{field}` renders \
                     that as 'found nothing'. Make it absent instead."
                ));
            }
        }
    }
    out
}

/// The document.
///
/// **Hand-assembled at the top level, derived everywhere it matters.** The envelope, the closed
/// cause set and the path list are values this function reads directly — `Cause::all()` here is
/// the same slice the error path switches on, so the document cannot list a cause the code lacks.
/// Every response BODY is a `$ref` to a schema generated from the struct that serializes it.
///
/// Query names are the runtime extraction authority. The static operation audit traces
/// each route to its raw type and compares its fields with this document.
#[must_use]
pub fn document() -> Value {
    DOCUMENT.get_or_init(build_document).clone()
}

static DOCUMENT: std::sync::OnceLock<Value> = std::sync::OnceLock::new();

/// The query parameters every operation declares, keyed by `(method, path template)` as the
/// router matches them — read from the document itself, once, so the runtime refuses what the
/// contract does not name and the two cannot drift. An operation absent here declares
/// nothing, and a request to it carrying any query parameter is refused.
pub(crate) fn declared_query_parameters(
    method: &str,
    path_template: &str,
) -> &'static std::collections::BTreeSet<String> {
    static DECLARED: std::sync::OnceLock<
        std::collections::BTreeMap<(String, String), std::collections::BTreeSet<String>>,
    > = std::sync::OnceLock::new();
    static NONE: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let declared = DECLARED.get_or_init(|| {
        let document = document();
        let mut declared = std::collections::BTreeMap::new();
        let Some(paths) = document["paths"].as_object() else {
            return declared;
        };
        for (path, operations) in paths {
            let Some(operations) = operations.as_object() else {
                continue;
            };
            for (method, operation) in operations {
                let names = operation["parameters"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|p| p["in"] == "query")
                    .filter_map(|p| p["name"].as_str().map(str::to_string))
                    .collect();
                declared.insert((method.to_ascii_uppercase(), path.clone()), names);
            }
        }
        declared
    });
    declared
        .get(&(method.to_ascii_uppercase(), path_template.to_string()))
        .unwrap_or(&NONE)
}

fn build_document() -> Value {
    let causes: Vec<&str> = crate::error::Cause::all()
        .iter()
        .map(|c| c.as_str())
        .collect();
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "infertap analyzer read API",
            "version": env!("CARGO_PKG_VERSION"),
            "description":
                "The versioned seam between the analyzer and the console. Read-only: no endpoint \
                 mutates fleet data, and the auth realm owns exactly POST and DELETE /v1/session. \
                 Every 200 carries a freshness stamp, and every figure carries its coverage, its \
                 register and the basis that coverage was computed on.",
        },
        "components": { "schemas": schemas(&causes) },
        "paths": all_paths(),
    })
}

/// Every component schema: the hand-written envelope pieces, plus one generated per wire type.
fn schemas(causes: &[&str]) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("Figure".into(), value_of(&wire::Figure::schema()));
    map.insert("Stamp".into(), stamp_schema());
    // **One schema for the cause set, referenced by both bodies.** `Error` and `Refusal` restated
    // it separately — and only one of them restated it as an enum at all.
    map.insert("Cause".into(), json!({ "type": "string", "enum": causes }));
    map.insert(
        "Error".into(),
        json!({
            "type": "object",
            "required": ["cause", "message"],
            "properties": {
                "cause": { "$ref": "#/components/schemas/Cause" },
                "message": { "type": "string" },
                // **`additionalProperties` is what makes these readable.** A bare
                // `"type": "object"` with no properties generates as `Record<string, never>` — a
                // type permitting no keys — so a client could neither read a bound out of an
                // error nor construct one.
                "bound": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "The bound that produced the refusal. A refusal that does not \
                                    state its bound is a vague 404. Shape varies by cause — the \
                                    horizon for `outside_raw_horizon`, the current tick and \
                                    generation for `cursor_stale`.",
                },
                "requested": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "What was asked for, echoed so a console can state the bound \
                                    and the request side by side without re-deriving either.",
                },
            },
        }),
    );
    for (name, schema) in generated() {
        map.insert(name, schema);
    }
    // **The state vocabulary is contract-closed, from the same match the handlers emit**
    // (`resources::observation::state_words`) — the `Cause` treatment applied to the
    // run-state pair, on both the roster row and the holders row it is denormalized onto.
    for name in ["RunRow", "HolderRow"] {
        let row = map.get_mut(name).expect("row schema");
        *row.pointer_mut("/properties/state").expect("state prop") = json!({
            "type": "string",
            "enum": ["active", "inactive"],
            "description": "The server's judgment at read time: paint from this field alone.",
        });
        *row.pointer_mut("/properties/reason").expect("reason prop") = json!({
            "type": "string",
            "enum": ["stopped", "silent", "ungraded"],
            "description": "Which kind of inactive — present exactly when state is inactive. \
                            stopped: announced departure (deploys look like this). silent: past \
                            the threshold with no stop, the alarming one. ungraded: no \
                            declaration observed, a coverage gap rather than a health claim.",
        });
    }
    Value::Object(map)
}

/// One entry per wire type, name and schema both read off the type.
fn generated() -> Vec<(String, Value)> {
    macro_rules! entries {
        ($($t:ty),* $(,)?) => { vec![$( (name_of::<$t>(), value_of(&<$t>::schema())) ),*] };
    }
    entries![
        wire::StampData,
        wire::SessionData,
        wire::SessionState,
        crate::resources::session::TokenBody,
        wire::WindowJson,
        wire::Refusal,
        wire::HolderId,
        wire::CoverageStatement,
        wire::FiguresData,
        wire::Segment,
        wire::Bucket,
        wire::DimValue,
        wire::HoldersData,
        wire::HolderRow,
        wire::Page,
        wire::CoverageData,
        wire::CoverageRow,
        wire::AnalyzeData,
        crate::requests::AnnotationBody,
        wire::AnnotationData,
        wire::DeliveryData,
        wire::DeliveryTotals,
        wire::DeliveryRow,
        wire::RosterData,
        wire::ClipScope,
        wire::RunRow,
        wire::PlacementData,
        wire::PlacementBound,
        wire::PlacementRow,
        wire::TruncationJson,
        wire::WithinEngineRow,
        wire::WhalesData,
        wire::InstanceDuplicationData,
        wire::InstanceDupRowJson,
        wire::WhaleJson,
        wire::ReplicationData,
        wire::ZeroBasisJson,
        wire::CopiesBucketJson,
        wire::CopiesMemberJson,
        wire::BandsData,
        wire::BandRowJson,
        wire::MarkersData,
        wire::MarkerJson,
        wire::DayData,
        wire::ClassMass,
        wire::DayHolder,
        wire::EvidenceData,
        wire::EvidenceRecord,
        wire::ReachJson,
        wire::HealthData,
        wire::ModelCounts,
        wire::ModuleJson,
        wire::IngestJson,
        wire::SpanJson,
        wire::DegradationJson,
        wire::BlocksData,
        wire::BlockDayJson,
        wire::BlockRow,
        wire::TagsData,
        wire::TagCoverageJson,
        wire::TagRowJson,
        wire::RatioJson,
        wire::BlockHoldersData,
        wire::BlockHolderRow,
        wire::TimelineData,
        wire::TimelineRow,
        wire::MarkerData,
        wire::TickAppliedEvent,
        wire::IngestBatchEvent,
        wire::ResyncEvent,
    ]
}

fn name_of<T: utoipa::ToSchema>() -> String {
    T::name().into_owned()
}

fn value_of(schema: &utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>) -> Value {
    serde_json::to_value(schema).unwrap_or_else(|_| json!({ "type": "object" }))
}

/// One query or path parameter, as the document describes it.
///
/// **Built from `routes`' own vocabulary constants**, so the legal values in the spec are the
/// legal values the parser accepts rather than a second list that agrees today.
fn param(name: &str, description: &str, schema: Value) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("name".into(), json!(name));
    o.insert("in".into(), json!("query"));
    o.insert("required".into(), json!(false));
    o.insert("description".into(), json!(description));
    o.insert("schema".into(), schema);
    Value::Object(o)
}

/// The window grammar, stated once: three descriptions saying it in three ways is how one
/// of them ends up subtly wrong.
const WINDOW_GRAMMAR: &str = "`<N>d`, `<N>h`, or `start_ms,end_ms` absolute — one grammar \
     on every windowed read; the panel stops are `14d`, `48h`, `3h`, `90d`. Relative forms \
     anchor to wall now, rounded up to the bucket the span naturally serves (a day span ends \
     at the coming midnight, `48h` at the end of the current hour, `3h` at the end of the \
     current minute), so the trailing bucket is the live, partial one and every response \
     states its own freshness beside it.";

/// A comma-separated CLOSED set.
///
/// **`style: form, explode: false` is what keeps the set closed into a generated client.** The
/// wire bytes are identical to a plain string — `dims=class,holder` — but the type comes out as
/// `("class" | "holder" | ...)[]` rather than `string`, so a misspelling is a compile error
/// instead of a 400. Encoding it as a string flattened a six-value vocabulary the DAO enforces
/// with a closed enum, which is the one place the request half was losing what the response half
/// keeps.
fn csv_enum_param(name: &str, description: &str, values: &[&str]) -> Value {
    let mut p = param(
        name,
        description,
        json!({
            "type": "array",
            "items": { "type": "string", "enum": values },
        }),
    );
    if let Some(o) = p.as_object_mut() {
        o.insert("style".into(), json!("form"));
        o.insert("explode".into(), json!(false));
    }
    p
}

fn enum_schema(values: &[&str], default: Option<&str>) -> Value {
    let mut v = json!({ "type": "string", "enum": values });
    if let Some(d) = default {
        v["default"] = json!(d);
    }
    v
}

/// The window parameter, on every endpoint that takes one.
///
/// **Relative forms anchor to wall now, rounded to the bucket**,
/// and the description says so because a client that assumes a pass-frontier anchor would
/// draw a window one day behind the data.
fn window_param() -> Value {
    param(
        "window",
        WINDOW_GRAMMAR,
        json!({ "type": "string", "default": "14d", "example": "14d" }),
    )
}

/// A `YYYY-MM-DD` UTC date, wherever one appears.
///
/// **The pattern is on BOTH the path and the query form**, so the constraint does not hold only
/// where somebody remembered it.
fn day_schema() -> Value {
    json!({ "type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$", "example": "2026-08-03" })
}

/// The three SCOPE parameters, declared together because they are one concept.
///
/// Scope is not `dims`: `dims` says how to GROUP the answer, these say which slice to answer
/// ABOUT. A scoped read touches one entity's key slice and stays flat as the fleet grows,
/// where the group-by-and-filter-in-the-client shape it replaces cost fleet-proportional work
/// for a single-entity page.
fn scope_params() -> Vec<Value> {
    vec![
        param(
            "workload",
            "Scope to one declared workload. `undeclared` is the legal value for the \
             ABSENT bucket and selects it as a named slice rather than as a gap: workload \
             is a declaration, so its absence reads `undeclared`.",
            json!({ "type": "string" }),
        ),
        param(
            "instance",
            "Scope to one instance — every rank and cache group on that node. The instance is a SLOT \
             the operator named, not a discovered identity.",
            json!({ "type": "string" }),
        ),
        param(
            "holder",
            "Scope to one holder, `instance/dp_rank/group_idx` — the finest scope there is. Where both this and `instance` are given this wins, being the narrower claim.",
            json!({ "type": "string" }),
        ),
    ]
}

fn grain_param() -> Value {
    param(
        "grain",
        "Time bucket. Omitted (or `auto`), the SERVER chooses from the range's length and \
         the spans its tiers actually hold, and the response states the served grain in its \
         `grain` field — render what you got, never re-derive the tier from a client-side \
         constant. An explicit tier is honored verbatim, or REFUSED with `grain_unavailable` \
         rather than silently downgraded: `minute` serves the recompute figures and the \
         banded thrash read at the fleet, class, workload and model cuts (one dimension \
         set on every minute table) and, from cells kept per rank, the instance cut and \
         any instance or holder scope (a holder is a rank there: cache groups and tiers \
         roll up, so the holder and tier cuts stay at `hour`), each within its minute \
         table's measured reach (seven days by default); `hour` only within the hourly tier's retention (365 days by \
         default).",
        enum_schema(crate::requests::GRAINS, Some("auto")),
    )
}

/// The cursor, on every paged surface.
fn cursor_param() -> Value {
    param(
        "cursor",
        "Opaque continuation from a previous page's `page.next_cursor`. **Bound to the tick that \
         minted it**: presented after a later tick was applied, or after the classification \
         parameters changed, it is refused with `cursor_stale` rather than silently answering \
         about different rows.",
        json!({ "type": "string" }),
    )
}

fn limit_param(default: u32, max: u32) -> Value {
    // The ceiling is PASSED IN — each operation hands this the same constant its
    // handler refuses on, so the document cannot bless a value the code rejects: a shared
    // helper baking one ceiling for every operation would bless values two handlers refuse.
    param(
        "limit",
        &format!(
            "Rows to return, 1..={max}. The ceiling is the operation's own bound, \
             stated from the same constant its handler enforces. For the paged row \
             surfaces it derives from response size (~339 B a row), deliberately below \
             a typical fleet's holder count so paging is the normal path."
        ),
        json!({
            "type": "integer",
            "minimum": 1,
            "maximum": max,
            "default": default,
        }),
    )
}

/// A GET that answers the two-key envelope wrapping `T`.
///
/// **The envelope is written here rather than derived**, because `Envelope<T>` is generic over
/// every data type and instantiating a schema per response family would put forty-five near
/// identical wrappers in the document. The two keys are stable and the interesting half is the
/// `$ref`, which IS derived.
fn read<T: utoipa::ToSchema>(operation_id: &str, summary: &str, parameters: &[Value]) -> Value {
    // Every read goes through `ApiState::read`, which maps ANY engine failure to
    // `engine_unavailable` — so 503 is reachable from all of them, and never a zero.
    operation::<T>(
        operation_id,
        summary,
        parameters,
        &[Cause::EngineUnavailable],
    )
}

/// [`read`], with the causes this operation raises that its parameters do not imply.
fn operation<T: utoipa::ToSchema>(
    operation_id: &str,
    summary: &str,
    parameters: &[Value],
    extra: &[Cause],
) -> Value {
    let ok = json!({
        "description": "stamp + data",
        "content": { "application/json": { "schema": {
            "type": "object",
            "required": ["stamp", "data"],
            "properties": {
                "stamp": { "$ref": "#/components/schemas/Stamp" },
                "data": { "$ref": schema_ref::<T>() },
            },
        }}},
    });
    json!({
        "get": {
            // **Verb-noun, and it is what a generator names its types from.** Without it a
            // generator munges the path — `getV1ResidualPlacement` rather than
            // `GetPlacement{Request,Response}` — and the console has no naming to control.
            "operationId": operation_id,
            "summary": summary,
            "parameters": parameters,
            "responses": responses(ok, &causes_of(parameters, extra)),
        }
    })
}

/// [`operation`]'s POST sibling: same envelope, no parameters, no request body.
fn post_operation<T: utoipa::ToSchema>(operation_id: &str, summary: &str) -> Value {
    let ok = json!({
        "description": "stamp + data",
        "content": { "application/json": { "schema": {
            "type": "object",
            "required": ["stamp", "data"],
            "properties": {
                "stamp": { "$ref": "#/components/schemas/Stamp" },
                "data": { "$ref": schema_ref::<T>() },
            },
        }}},
    });
    json!({
        "post": {
            "operationId": operation_id,
            "summary": summary,
            "responses": responses(ok, &causes_of(&[], &[Cause::EngineUnavailable])),
        }
    })
}

/// The body-carrying POST — the generalization [`post_operation`]'s note promised when a
/// second POST needed one (the annotations endpoint). Body `$ref` comes from
/// the TYPE, same as every response schema, so a renamed body struct moves the pointer.
fn post_operation_with_body<T: utoipa::ToSchema, B: utoipa::ToSchema>(
    operation_id: &str,
    summary: &str,
    extra_causes: &[Cause],
) -> Value {
    let ok = json!({
        "description": "stamp + data",
        "content": { "application/json": { "schema": {
            "type": "object",
            "required": ["stamp", "data"],
            "properties": {
                "stamp": { "$ref": "#/components/schemas/Stamp" },
                "data": { "$ref": schema_ref::<T>() },
            },
        }}},
    });
    let mut causes = vec![Cause::EngineUnavailable];
    causes.extend_from_slice(extra_causes);
    json!({
        "post": {
            "operationId": operation_id,
            "summary": summary,
            "requestBody": {
                "required": true,
                "content": { "application/json": { "schema": { "$ref": schema_ref::<B>() } } },
            },
            "responses": responses(ok, &causes_of(&[], &causes)),
        }
    })
}

/// A `$ref` whose target comes from the TYPE, so a rename moves the pointer with it.
fn schema_ref<T: utoipa::ToSchema>() -> String {
    format!("#/components/schemas/{}", name_of::<T>())
}

fn session_response(description: &str) -> Value {
    json!({ "description": description,
        "headers": { "Set-Cookie": { "schema": { "type": "string" }, "description": "infertap_session; HttpOnly; Path=/; SameSite=Lax; Secure when configured" } },
        "content": { "application/json": { "schema": { "$ref": schema_ref::<wire::SessionData>() } } }
    })
}

/// The recorded-declaration class's one body-carrying member, out of [`paths`] for its
/// length alone.
fn annotations_operation() -> Value {
    post_operation_with_body::<wire::AnnotationData, crate::requests::AnnotationBody>(
        "recordAnnotation",
        "Declare a deploy: a label and an instant, drawn on the metric charts once the \
         next ingest cycle records it. The declaration is a bucket object replay recovers; \
         a process with no bucket attached refuses it",
        &[Cause::AnnotationInvalid, Cause::BucketUnavailable],
    )
}

// One entry per operation, each a literal beside its own parameter list — long by nature and
// long on purpose: the document reads top to bottom as the API's own table of contents, and
// splitting it into helpers per route would scatter that reading across the file for no reader's
// benefit. The parameter vocabularies it draws on are constants, so the length here is
// enumeration rather than logic.
fn paths() -> Value {
    // The legal set comes from the parser's own constant rather than being retyped here, so a
    // new `Dim` reaches the document by adding it in one place.
    let dims = csv_enum_param(
        "dims",
        "Dimensions to group by, from the closed set the rollup keys on. `holder` is ONE \
         dimension over three columns and arrives as one tab-joined value; `instance` rolls \
         a node's ranks up, which is a different question and a real entity — it does not \
         reopen the triple, since `dp_rank` alone is meaningless across instances.",
        crate::requests::DIMS,
    );
    let classes = crate::requests::classes();
    json!({
        "/v1/session": {
            "post": {
                "operationId": "createSession",
                "summary": "Exchange a viewer token for a session cookie. The auth realm; touches \
                            no fleet data.",
                "requestBody": { "required": true, "content": { "application/json": {
                    "schema": { "$ref": schema_ref::<crate::resources::session::TokenBody>() }
                }}},
                "responses": responses(
                    session_response("Session created; HttpOnly session cookie set"),
                    // Token-store/session creation can fail, so this path can 503 too —
                    // and a console that cannot tell "wrong token" from "meter not up" retries
                    // the one case that will never succeed.
                    &causes_of(&[], &[Cause::EngineUnavailable]),
                ),
            },
            "delete": {
                "operationId": "destroySession",
                "summary": "End the session.",
                "responses": responses(session_response("Session destroyed; cookie expired"), &[Cause::RequestInvalid]),
            },
        },
        "/v1/stamp": read::<wire::StampData>(
            "getStamp",
            "The canonical stamp; answers before any tick has been applied",
            &[]),
        "/v1/figures": read::<wire::FiguresData>(
            "listFigures",
            "Shape 1, segmented by meaning generation — never one blended series",
            &[vec![window_param(), grain_param(), dims], scope_params()].concat()),
        "/v1/holders": read::<wire::HoldersData>(
            "listHolders",
            "The holders table, with the denominator its shares were taken against",
            &[
                window_param(),
                grain_param(),
                // A closed set of six, from `analysis::Class::ALL` rather than retyped — so a
                // console gets a union and a misspelling is a compile error.
                param("class", "Narrow to one of the six classification buckets.",
                      enum_schema(&classes, None)),
                param("workload", "Narrow to one declared workload. `undeclared` is a legal \
                       value and selects the absent bucket — segments whose producer declared \
                       no class (the spec's workload clause) — as a named slice, never as a gap.",
                      json!({ "type": "string" })),
                limit_param(100, crate::requests::PAGE_LIMIT_MAX),
                cursor_param(),
            ]),
        "/v1/coverage": read::<wire::CoverageData>(
            "listCoverage",
            "Shape 3, instance-time weighted",
            &[
                window_param(),
                param("by", "Grouping. The weighting is identical either way; only the grain \
                       changes.", enum_schema(crate::requests::COVERAGE_BY, Some("fleet"))),
            ]),
        "/v1/analyze": post_operation::<wire::AnalyzeData>(
            "analyzeNow",
            "Kick an incremental analysis now. In-band acknowledgement: a read-only \
             process answers accepted: false rather than raising",
        ),
        "/v1/annotations": annotations_operation(),
        "/v1/delivery": read::<wire::DeliveryData>(
            "deliveryLedger",
            "The delivery ledger, since install: of what the taps produced, what arrived. \
             stamp.window is null; these totals do not describe a finite query interval.",
            &[]),
        "/v1/health": read::<wire::HealthData>(
            "getHealth",
            "The METER's health, plus the ingest block the status strip hydrates from",
            &[window_param()]),
        "/v1/roster": read::<wire::RosterData>(
            "listRoster",
            "Shape 2; the window is a wall CLIP, stated on the response",
            &[window_param(), limit_param(100, crate::requests::PAGE_LIMIT_MAX), cursor_param()]),
        "/v1/residual/placement": read::<wire::PlacementData>(
            "getPlacement",
            "Shape 10, where the duplication sits: co-residency token-time per workload and \
             holder pair. This measure SUMS — day rows sum to the window figure — which shape 6's \
             order statistic does not. `rows` \
             (cross-engine) and `within_engine` (same-engine rank pairs) are disjoint partitions \
             of `window_total`. `limit` bounds the PAIRS returned, its own ceiling rather than \
             the row surfaces'; every excluded pair is counted and priced in the response",
            &[
                window_param(),
                limit_param(
                    crate::requests::PLACEMENT_PAIRS_DEFAULT,
                    crate::requests::PLACEMENT_PAIRS_MAX,
                ),
            ]),
        "/v1/markers": read::<wire::MarkersData>(
            "listMarkers",
            "Shape 7; `breaks` is decided server-side",
            &[window_param()]),
    })
}

/// The drill and comparison half of the path list.
///
/// Split from [`paths`] purely for length — clippy's 100-line function budget, which is a real
/// signal here: a single function assembling fifteen endpoints is one nobody reads to the end.
// One entry per drill operation, each a literal beside its own parameter list. Long by
// enumeration rather than by logic, and split from `paths` already; splitting again per route
// would scatter the document's own table of contents for no reader's benefit.
fn drill_paths() -> Value {
    json!({
        "/v1/figures/bands": read::<wire::BandsData>(
            "getBands",
            "The thrash lens: recompute mass banded by time-since-eviction (<10s, 10s-1m, \
             1-10m, 10-60m) per bucket per requested dims. The gap-unobserved mass — \
             no observed prior close, first-seen and gone-past-lookback indistinguishable \
             by construction — rides beside the bands as its own figure, never a fifth \
             band. Minute grain serves within the banded cells' 48-hour reach (fleet, \
             workload and model cuts; a holder cut at that grain is refused by name). The \
             window and grain are the standard grammar and vocabulary, one resolver for \
             every read, so this chart and the copies chart cannot disagree about which \
             three hours exist",
            &[vec![window_param(), grain_param(), param("dims",
                "Comma-separated cut: class, holder, instance, workload, model. \
                 `instance` rolls a node's ranks up; `holder` is the full triple",
                json!({ "type": "string" })),
                limit_param(crate::requests::WHALES_DEFAULT, crate::requests::WHALES_MAX)],
            scope_params()].concat()),
        "/v1/residual/replication": read::<wire::ReplicationData>(
            "getReplication",
            "The copies histogram: duplicated token-time per bucket per fixed copy band \
             (k2/k34/k58/k9), the workload members table, and the anchor's fleet scalars \
             (remedy split, mean-copies numerator over all cached mass). Window and grain \
             are the standard grammar and vocabulary: `auto` ladders by span against each \
             tier's measured reach (the minute cells, the hourly rollup, the daily rollup: \
             one emission at three widths), an explicit minute or hour past its tier's reach \
             is refused by name, and the response states the served grain and bucket width. Figures are complete \
             through emitted_to_ms and unstated beyond it. `zero_basis` states what a cross-engine \
             zero means (spec 5.7): measured only over two or more instances, portable \
             identities and one key space",
            &[
                window_param(),
                grain_param(),
                param("workload", "The panel's one cut; absent = fleet",
                      json!({ "type": "string" })),
            ]),
        "/v1/instances/{instance}/duplication": read::<wire::InstanceDuplicationData>(
            "getInstanceDuplication",
            "One node's INTERNAL duplication: mass it held more than once inside itself, \
             split into `within_ranks_token_ms` (across its data-parallel ranks — the half \
             with a lever, since vLLM's own DP balancer scores by load with no prefix term, \
             so a gateway setting `data_parallel_rank` by prefix affinity is the remedy) and \
             `cache_groups_token_ms` (across one rank's KV cache groups — a hybrid model's \
             architecture, which no policy removes). Duplication SPANNING engines is absent \
             by construction, not by a filter: it belongs to no single node and would double \
             on any sum across instances, so the fleet copies histogram keeps that half. \
             Served at `grain`: minute buckets from the minute cut inside the minute knob's \
             reach, hour buckets otherwise, on the per-instance detail retention window",
            &[window_param(), grain_param(), json!({
                "name": "instance", "in": "path", "required": true,
                "description": "Operator-assigned instance id, percent-encoded.",
                "schema": { "type": "string" },
            })]),
        "/v1/residual/whales": read::<wire::WhalesData>(
            "getWhales",
            "The heaviest duplicated identities over the window's stated days, ranked by \
             duplicated token-time. Each settled day keeps its heaviest identities up to the \
             cut, and its exact totals over every identity; a row is exact for the days it was \
             in the cut and states the most it falls short by",
            &[window_param(), limit_param(crate::requests::WHALES_DEFAULT, crate::requests::WHALES_MAX)]),
        "/v1/days/{day}": read_with_path::<wire::DayData>(
            "getDayDetail",
            "One UTC day",
            &[
                // **The path parameter, declared.** A templated path whose parameter is not
                // declared is not merely thin documentation — it is invalid OpenAPI, and a
                // generator has nothing to put in the URL.
                json!({
                    "name": "day",
                    "in": "path",
                    "required": true,
                    "description": "`YYYY-MM-DD`, UTC. Parsed before it reaches SQL, so a \
                                    malformed date is a 400 rather than a quoting question.",
                    "schema": day_schema(),
                }),
                limit_param(50, crate::requests::PAGE_LIMIT_MAX),
                cursor_param(),
            ]),
        // `operation` rather than `read` for the extra cause: the subject selectors are three
        // optional parameters of which exactly one is required, which OpenAPI cannot express — so
        // the refusal for none-of-them is declared here as the operation's own.
        "/v1/evidence": operation::<wire::EvidenceData>(
            "listEvidence",
            "Level 2; one holder's or one block's stores over the window, bounded by the \
             source's reach and saying so",
            &[
                param("window", &format!("{WINDOW_GRAMMAR} Defaults to 14d."),
                      json!({ "type": "string", "example": "14d" })),
                // **Exactly ONE of these two is required**, which OpenAPI 3.1 cannot express
                // across parameters — there is no `oneOf` for a parameter set. So each says it,
                // and the handler refuses with a named cause rather than guessing.
                param("block", "Subject: one block. Exactly one of `block` or `holder` is \
                       required.", json!({ "type": "string" })),
                param("holder", "Subject: one holder. Exactly one of `block` or `holder` is \
                       required.",
                      json!({
                          "type": "string",
                          "pattern": "^[^/]+/\\d+/\\d+$",
                          "example": "i-001/0/0",
                      })),
                limit_param(100, crate::requests::PAGE_LIMIT_MAX),
                cursor_param(),
            ],
            &[Cause::EngineUnavailable, Cause::SubjectMissing]),
    })
}

/// The stream, alone — it is the only endpoint that is not a request/response pair, and grouping
/// it with anything else would bury the one shape a reader needs to look at differently.
fn stream_paths() -> Value {
    // The parameter list as a value, because `responses` derives the error block FROM it, so the
    // stream's error response is the same body and the same type as every other operation's.
    let parameters = vec![json!({
        "name": "Last-Event-ID",
        "in": "header",
        "required": false,
        "description": "The last id this client saw. Absent streams live from now.",
        "schema": { "type": "string", "example": "7:1412" },
    })];
    let ok = json!({
        "description": "an open event stream",
        "content": { "text/event-stream": { "schema": {
            "type": "string",
            "description":
                "SSE frames. The `data:` payload of each frame is one of the \
                 event schemas, selected by the frame's `event:` kind.",
        }}},
    });
    // No engine touch in the handler, so `engine_unavailable` is correctly absent and the
    // derivation yields exactly 200 + 401 — declaring a 503 here would be the contract fiction
    // read-api section 7.3 rules against, one level up at the status.
    let responses = responses(ok, &causes_of(&parameters, &[]));
    json!({
        "/v1/events": {
            "get": {
                "operationId": "streamEvents",
                "summary": "The one event stream: tick_applied, ingest_batch, and an honest \
                            resync",
                "description":
                    "Server-sent events. Each frame is `id: {epoch}:{seq}`, `event: <kind>`, \
                     `data: <payload>`, with a `: keepalive` comment every 20 s so an idle \
                     stream survives a proxy read timeout.\n\n\
                     **Reconnect with `Last-Event-ID`.** Three decided cases: an id in the \
                     current epoch and still in the ring REPLAYS from the next event; an id aged \
                     out of the ring, or from a prior epoch (the analyzer restarted), yields a \
                     `resync` frame and then live. The server never pretends to have replayed \
                     what it does not hold — every event is a hint to refetch, and refetching is \
                     always correct because the API is read-only.\n\n\
                     **The frame grammar is prose because OpenAPI cannot describe one.** What is \
                     schematised below is the `data:` payload of each kind, which is the part \
                     that has fields to rename — and therefore the part a generated client and \
                     its canary can actually check.",
                "parameters": parameters,
                "responses": responses,
                // **The payload schemas, keyed by the `event:` kind that selects them.** A
                // generator emits types for all three; a console switches on the kind and gets
                // the matching shape, instead of hand-writing it from read-api section 5's prose.
                "x-event-payloads": {
                    "tick_applied": { "$ref": schema_ref::<wire::TickAppliedEvent>() },
                    "ingest_batch": { "$ref": schema_ref::<wire::IngestBatchEvent>() },
                    "resync": { "$ref": schema_ref::<wire::ResyncEvent>() },
                },
            }
        },
    })
}

/// Shapes 4, 5 and 7's detail — the drills a figure descends into.
///
/// A third group rather than a longer second one: clippy's 100-line budget is a real signal, and
/// one function assembling nineteen endpoints is one nobody reads to the end.
fn detail_paths() -> Value {
    json!({
        "/v1/markers/{id}": read_with_path::<wire::MarkerData>(
            "getMarker",
            "One discontinuity marker, reached by clicking its dagger",
            &[
                json!({
                    "name": "id", "in": "path", "required": true,
                    "description": "`{kind}-{at_ms}`, re-derivable from a marker row.",
                    "schema": { "type": "string" },
                }),
                window_param(),
            ]),
        "/v1/blocks": read::<wire::BlocksData>(
            "listBlocks",
            "Shape 4; each day's blocks by stored mass, heaviest first, every figure a bound",
            &[window_param(), limit_param(50, crate::requests::PAGE_LIMIT_MAX)]),
        "/v1/tags": read::<wire::TagsData>(
            "listTags",
            "The declared tags grouped over the window: exposure, refetch per identity-minute \
             (primary) and per stored token (secondary), each with a 90 % interval over \
             identities, and coverage",
            &[window_param()]),
        "/v1/blocks/{block_id}/holders": read_with_path::<wire::BlockHoldersData>(
            "listBlockHolders",
            "Shape 4's other direction; which holders STORED this block, never resident-on",
            &[
                json!({
                    "name": "block_id", "in": "path", "required": true,
                    "description": "The block's identity as the tap recorded it.",
                    "schema": { "type": "string" },
                }),
                window_param(),
                limit_param(100, crate::requests::PAGE_LIMIT_MAX),
                cursor_param(),
            ]),
        "/v1/holders/{instance}/{dp_rank}/{group_idx}/timeline":
            read_with_path::<wire::TimelineData>(
                "getHolderTimeline",
                "Shape 5; one holder's store timeline, event grain and raw-window bounded",
                &[
                    json!({
                        "name": "instance", "in": "path", "required": true,
                        "description": "Operator-assigned instance id.",
                        "schema": { "type": "string" },
                    }),
                    json!({
                        "name": "dp_rank", "in": "path", "required": true,
                        "schema": { "type": "integer", "minimum": 0 },
                    }),
                    json!({
                        "name": "group_idx", "in": "path", "required": true,
                        "schema": { "type": "integer", "minimum": 0 },
                    }),
                    window_param(),
                    limit_param(100, crate::requests::PAGE_LIMIT_MAX),
                    cursor_param(),
                ]),
    })
}

/// The three halves, merged.
fn all_paths() -> Value {
    let mut all = paths();
    if let Some(target) = all.as_object_mut() {
        for group in [drill_paths(), detail_paths(), stream_paths()] {
            if let Some(group) = group.as_object() {
                for (k, v) in group {
                    target.insert(k.clone(), v.clone());
                }
            }
        }
    }
    all
}

/// [`read`] for a templated path — identical but for the parameter list already carrying an
/// `in: path` entry.
fn read_with_path<T: utoipa::ToSchema>(
    operation_id: &str,
    summary: &str,
    parameters: &[Value],
) -> Value {
    read::<T>(operation_id, summary, parameters)
}

/// The `Stamp` schema, hand-written like the envelope and for the same reason: it is assembled by
/// `envelope::Stamp::to_json` rather than by a `Serialize` derive.
///
/// Hand-written is not hand-CHECKED. A schema can demand a key it never describes, or describe
/// one the serializer never emits, and the goldens cannot see either: they assert against
/// serialized bytes with their own expectations, so a schema that describes those bytes wrongly
/// passes them and fails a validator. `the_stamp_schema_describes_every_key_the_stamp_emits`
/// compares this against a real serialized stamp, so the pair cannot drift in either direction.
/// The stamp's edges are what a console renders the live edge from (read-api section 2.1), the
/// most load-bearing fields in the document.
fn stamp_schema() -> Value {
    json!({
        "type": "object",
        "required": ["fleet_id", "window", "tick", "semantics_version", "discriminator",
                     "raw_horizon", "analyzer_version"],
        "properties": {
            "fleet_id": { "type": "string" },
            "window": {
                "description": "The window these figures describe, or **null when none was \
                                requested** — which is what `/v1/stamp` answers, since it takes \
                                no window parameter and answers before any tick has run. Never a \
                                nominal epoch day: the same t0 rule the `tick` block below states \
                                applies here.",
                "oneOf": [
                    { "$ref": "#/components/schemas/WindowJson" },
                    { "type": "null" },
                ],
            },
            "tick": tick_schema(),
            "semantics_version": { "type": "string" },
            "discriminator": { "type": "string" },
            "raw_horizon": {
                "type": "object",
                "description": "The bound on EVENT-grain reads. Drills below this are refused \
                                with `outside_raw_horizon`; `evidence_available` carries the \
                                same bound per row.",
                "required": ["from_ms", "from_day", "declared_days"],
                "properties": {
                    "from_ms": {
                        "type": ["number", "null"],
                        "description": "Oldest retained raw instant, epoch ms. Null when no raw \
                                        is retained at all.",
                    },
                    "from_day": {
                        "type": ["string", "null"],
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                        "description": "The same instant as a UTC day label.",
                    },
                    "declared_days": {
                        "type": "integer",
                        "description": "The CONFIGURED retention. Declared, not observed — it \
                                        exceeds what is held while a deployment is younger than \
                                        it, which is why `from_ms` is carried beside it rather \
                                        than derived from it.",
                    },
                },
            },
            "analyzer_version": {
                "type": "string",
                "description": "The build that produced this response.",
            },
        },
    })
}

/// The stamp's `tick` block: the edges the last applied tick stated.
fn tick_schema() -> Value {
    json!({
        "type": "object",
        "description": "The tick this response's figures come from: the edges the last \
                        applied tick stated. **Every field is null before the first \
                        tick** — that is the t0 state, not a missing value.",
        "required": ["at_ms", "classified_to_ms", "classified_day", "emitted_to_ms",
                     "covered_to_ms", "audited_to_ms", "settled_through_day"],
        "properties": {
            "at_ms": {
                "type": ["number", "null"],
                "description": "When the tick was applied, epoch ms.",
            },
            "classified_to_ms": {
                "type": ["number", "null"],
                "description": "Repeat compute's classified edge, epoch ms: every \
                                store before it is classified, and the class and band \
                                figures hold every store before it.",
            },
            "classified_day": {
                "type": ["string", "null"],
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                "description": "The UTC day of the classified edge: the newest day \
                                holding classified stores. A console states that days \
                                past it are not yet classified, rather than assuming \
                                today is.",
            },
            "emitted_to_ms": {
                "type": ["number", "null"],
                "description": "Placement's emitted edge, epoch ms: the placement \
                                figures are stated before it.",
            },
            "covered_to_ms": {
                "type": ["number", "null"],
                "description": "Coverage's stated edge, epoch ms: coverage is settled \
                                before it; past it, a source's tail is not yet stated.",
            },
            "audited_to_ms": {
                "type": ["number", "null"],
                "description": "The sampled audit's edge, epoch ms. Each tick re-derives \
                                a few of the blocks it classified from the log and \
                                compares them with what it stated; before this edge every \
                                sample agreed. It trails classified_to_ms only once a \
                                sample disagreed, and then holds until the derived state \
                                is derived afresh.",
            },
            "settled_through_day": {
                "type": ["string", "null"],
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                "description": "The last UTC day every edge has passed by the settle \
                                delay: through it, the day-grain outputs (the public \
                                modules, the generation markers) are stated.",
            },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{document, name_of, Cause};
    use crate::wire;
    use serde_json::Value;

    #[test]
    fn every_path_references_a_real_schema() {
        // A path carrying a summary and two status codes and NO response schema types every body
        // `any` in a generated client, and a schema-parity test over it validates nothing.
        let doc = document();
        let schemas = doc["components"]["schemas"]
            .as_object()
            .expect("components");
        let paths = doc["paths"].as_object().expect("paths");

        let mut checked = 0;
        for (path, item) in paths {
            let Some(get) = item.get("get") else { continue };
            // **A stream is exempted by its SHAPE, not by its name.** `text/event-stream` has no
            // envelope to carry a `data` ref — its payloads hang off `x-event-payloads` and are
            // checked by `the_stream_documents_every_event_payload`. Exempting by path would let
            // a future endpoint opt out of the check by being added to a list.
            if get["responses"]["200"]["content"]
                .get("text/event-stream")
                .is_some()
            {
                continue;
            }
            let target = get["responses"]["200"]["content"]["application/json"]["schema"]
                ["properties"]["data"]["$ref"]
                .as_str()
                .unwrap_or_else(|| panic!("{path} has no 200 data schema"));
            let name = target
                .rsplit('/')
                .next()
                .expect("a ref has a trailing segment");
            assert!(
                schemas.contains_key(name),
                "{path} references {target}, which is not a defined schema"
            );
            checked += 1;
        }
        assert!(checked >= 13, "only {checked} GET paths carried a schema");
    }

    /// A generator reads `properties` and a validator reads `required`, so a schema whose
    /// `required` names a key its `properties` does not describe generates a type and then
    /// rejects every instance of it. `every_path_references_a_real_schema` asserts that paths
    /// point at schemas that EXIST; this asserts that every schema describes what it DEMANDS,
    /// including ones not yet written.
    #[test]
    fn every_schema_describes_what_it_requires() {
        let doc = document();
        let schemas = doc["components"]["schemas"]
            .as_object()
            .expect("components");
        let mut checked = 0;
        for (name, schema) in schemas {
            // Walk nested objects too: `Stamp.pass` and `Stamp.raw_horizon` are inline objects
            // with their own required lists, and the defect was one level down from a component.
            let mut stack = vec![(name.clone(), schema)];
            while let Some((path, node)) = stack.pop() {
                let required = node["required"].as_array().map_or(&[][..], |a| a);
                if !required.is_empty() {
                    let properties = node["properties"].as_object();
                    for key in required {
                        let key = key.as_str().expect("a required entry is a string");
                        assert!(
                            properties.is_some_and(|p| p.contains_key(key)),
                            "{path}.required names `{key}`, which {path}.properties does not \
                             describe. A generator reads properties and a validator reads \
                             required, so this generates a type that cannot satisfy itself."
                        );
                        checked += 1;
                    }
                }
                if let Some(properties) = node["properties"].as_object() {
                    for (key, child) in properties {
                        stack.push((format!("{path}.{key}"), child));
                    }
                }
            }
        }
        assert!(
            checked > 40,
            "only {checked} required keys — the walk missed schemas"
        );
    }

    /// The other direction, which a `required` check structurally cannot see: a key the
    /// serializer emits that the schema names in NEITHER `required` nor `properties` is invisible
    /// to a validator and to codegen alike. Undescribed extra keys are how a hand-written schema
    /// rots quietly.
    ///
    /// **This is the check that lets `stamp_schema` stay hand-written.** `Stamp` has no `Serialize`
    /// derive to read a schema off, so the schema is assembled by hand; comparing it against a
    /// really-serialized stamp means the pair is machine-checked even though one half is hand-made.
    #[test]
    fn the_stamp_schema_describes_every_key_the_stamp_emits() {
        let stamp = crate::envelope::Stamp {
            fleet_id: "f".into(),
            window: Some(analyzer_analysis::Window {
                start_ms: 1_785_024_000_000.0,
                end_ms: 1_785_110_400_000.0,
            }),
            revision: Some(0),
            tick_at_ms: Some(1_785_110_000_000.0),
            classified_to_ms: Some(1_785_100_000_000.0),
            emitted_to_ms: Some(1_785_090_000_000.0),
            covered_to_ms: Some(1_785_080_000_000.0),
            audited_to_ms: None,
            settled_through_day: Some(20_667),
            semantics_version: "js-7",
            discriminator: "s1000x3600000".into(),
            raw_from_ms: Some(1_783_900_800_000.0),
            declared_raw_days: 14,
        };
        let emitted = stamp.to_json();
        let emitted = emitted.as_object().expect("a stamp is an object");
        let schema = super::stamp_schema();
        let described = schema["properties"].as_object().expect("properties");

        for key in emitted.keys() {
            assert!(
                described.contains_key(key),
                "`Stamp::to_json` emits `{key}` and the schema does not describe it. A consumer \
                 generating from this document gets a type missing a field it will receive."
            );
        }
        for key in described.keys() {
            assert!(
                emitted.contains_key(key),
                "the schema describes `{key}` and `Stamp::to_json` never emits it — a generated \
                 type with a field that cannot arrive."
            );
        }
        // The nested shapes are where the two undescribed keys lived, so pin them by name rather
        // than trusting that the top level matching means the insides do.
        for (parent, children) in [
            (
                "tick",
                [
                    "at_ms",
                    "classified_to_ms",
                    "classified_day",
                    "emitted_to_ms",
                    "covered_to_ms",
                    "settled_through_day",
                ]
                .as_slice(),
            ),
            (
                "raw_horizon",
                ["from_ms", "from_day", "declared_days"].as_slice(),
            ),
        ] {
            let inner = emitted[parent].as_object().expect("a nested object");
            for child in children {
                assert!(
                    inner.contains_key(*child),
                    "{parent}.{child} is not emitted"
                );
                assert!(
                    described[parent]["properties"]
                        .as_object()
                        .is_some_and(|p| p.contains_key(*child)),
                    "{parent}.{child} is emitted and undescribed"
                );
            }
        }
    }

    /// Read-api section 7.2 rules four failure paths. A document declaring `200` and `401` and
    /// nothing else would make a generated client type `400`, `404` and `503` as `never` and
    /// handle them against prose; a cause with a producer and no declared status has nowhere to
    /// arrive. Declaring the statuses, from the same mapping the error path switches on, is what
    /// gives every cause somewhere to arrive.
    #[test]
    fn every_cause_that_can_be_raised_has_a_declared_status() {
        let doc = document();
        let mut declared: Vec<&str> = Vec::new();
        for item in doc["paths"].as_object().expect("paths").values() {
            for op in item.as_object().expect("path item").values() {
                for (code, response) in op["responses"].as_object().expect("responses") {
                    for cause in response["x-causes"].as_array().map_or(&[][..], |a| a) {
                        let cause = cause.as_str().expect("a cause token");
                        // The status a response is filed under must be the one the mapping gives,
                        // or the document contradicts the code it describes.
                        let mapped = Cause::all()
                            .iter()
                            .find(|c| c.as_str() == cause)
                            .expect("a documented cause is in the closed set")
                            .status();
                        assert_eq!(
                            code,
                            &mapped.as_u16().to_string(),
                            "`{cause}` is declared at {code} and `Cause::status` says {mapped}"
                        );
                        declared.push(cause);
                    }
                }
            }
        }
        for cause in super::UNDOCUMENTED_ROUTE_CAUSES {
            assert!(
                !declared.contains(&cause.as_str()),
                "`{}` is exempted as belonging to no documented operation, and an operation now \
                 declares it. Delete the exemption rather than keeping it past its reason.",
                cause.as_str()
            );
        }
        for cause in Cause::all() {
            // A bounded refusal rides inside a 200 by read-api section 7.2, described by the data schema's
            // `refused` field, so it is correctly absent from every error response.
            if cause.status() == axum::http::StatusCode::OK
                || super::UNDOCUMENTED_ROUTE_CAUSES.contains(cause)
            {
                continue;
            }
            assert!(
                declared.contains(&cause.as_str()),
                "`{}` maps to {} and no operation declares it. A consumer cannot handle a status \
                 the contract does not mention, so the cause has a producer and nowhere to arrive.",
                cause.as_str(),
                cause.status()
            );
        }
    }

    /// A parameter contributes its causes to every operation carrying it, so a parameter missing
    /// from [`super::PARAM_CAUSES`] silently contributes none — the operation would under-declare
    /// and nothing would say so.
    #[test]
    fn the_document_knows_what_every_parameter_can_raise() {
        let doc = document();
        for (path, item) in doc["paths"].as_object().expect("paths") {
            for op in item.as_object().expect("path item").values() {
                for p in op["parameters"].as_array().map_or(&[][..], |a| a) {
                    let name = p["name"].as_str().expect("a parameter has a name");
                    assert!(
                        super::PARAM_CAUSES.iter().any(|(n, _)| *n == name),
                        "`{name}` on {path} has no entry in PARAM_CAUSES, so it contributes no \
                         causes and its operation under-declares its failures. State them — an \
                         empty slice is a legitimate answer and a recorded one."
                    );
                }
            }
        }
    }

    /// Does this data schema carry a `Page`, at any depth? Recursive, because a page block can
    /// sit inside a nested object rather than beside the rows.
    fn pages(schemas: &serde_json::Map<String, Value>, name: &str, seen: &mut Vec<String>) -> bool {
        if name == "Page" {
            return true;
        }
        if seen.iter().any(|s| s == name) {
            return false;
        }
        seen.push(name.to_string());
        let Some(properties) = schemas[name]["properties"].as_object() else {
            return false;
        };
        properties.values().any(|v| {
            let target = v["$ref"].as_str().or_else(|| v["items"]["$ref"].as_str());
            target.is_some_and(|r| pages(schemas, r.rsplit('/').next().unwrap_or(r), seen))
        })
    }

    /// **A surface that emits a `Page` must accept a `cursor`.**
    ///
    /// A surface that fetches every row, takes the first `limit` and reports a `total_rows`
    /// above what it returned with no cursor to continue with is an unbounded scan that refuses
    /// its own tail; one with no page block at all makes a top-50 indistinguishable from a
    /// complete list. Every other check here asks whether the document DESCRIBES the code; this
    /// asks whether the shape it describes is one a client can walk.
    #[test]
    fn a_surface_that_pages_can_be_walked() {
        let doc = document();
        let schemas = doc["components"]["schemas"]
            .as_object()
            .expect("components");

        let mut checked = 0;
        for (path, item) in doc["paths"].as_object().expect("paths") {
            let Some(get) = item.get("get") else { continue };
            let Some(data) = get["responses"]["200"]["content"]["application/json"]["schema"]
                ["properties"]["data"]["$ref"]
                .as_str()
            else {
                continue;
            };
            let data = data.rsplit('/').next().unwrap_or(data);
            if !pages(schemas, data, &mut Vec::new()) {
                continue;
            }
            checked += 1;
            if super::RANKING_SURFACES.contains(&path.as_str()) {
                continue;
            }
            let names: Vec<&str> = get["parameters"]
                .as_array()
                .map_or(&[][..], |a| a)
                .iter()
                .filter_map(|p| p["name"].as_str())
                .collect();
            assert!(
                names.contains(&"cursor"),
                "{path} returns a `Page` and accepts no `cursor`, so a client can be told rows \
                 remain and has no way to ask for them. Either page it or declare it a ranking."
            );
        }
        assert!(
            checked >= 6,
            "only {checked} paged surfaces found — the walk missed some"
        );
    }

    /// **A shape that can refuse in-band must make its payload ABSENT, not empty.**
    ///
    /// `Segment` states the argument: "`None` where this process cannot state figures, never an
    /// empty array, which would read as 'looked and found nothing'". A shape that keeps that
    /// distinction in a doc comment over a plain array holds the rule in prose: correct
    /// everywhere it is written down, unenforced where it matters.
    ///
    /// The type is what makes it stick. `T[] | null` forces a generated client to narrow, so a
    /// consumer that forgets `refused` gets a compile error; `T[]` lets the same oversight render
    /// an empty table as a real answer.
    #[test]
    fn a_shape_that_refuses_states_its_payload_as_absent() {
        let doc = document();
        let found = super::refusal_scope_violations(
            doc["components"]["schemas"]
                .as_object()
                .expect("components"),
        );
        assert!(found.is_empty(), "{}", found.join("\n"));
    }

    /// **The check itself, against shapes written wrong on purpose.**
    ///
    /// Reverting a real wire struct to a nullable payload does not compile — the handlers already
    /// say `Some(..)` — so mutating one proves nothing about this rule. The case that matters is a
    /// NEW shape written wrong, which would compile perfectly, and only a synthetic document
    /// reaches it. Same lesson as the zero-limit page: the interesting input is the one no request
    /// can produce.
    #[test]
    fn the_refusal_rule_catches_a_shape_written_the_old_way() {
        let bad = serde_json::json!({
            "OldStyle": {
                "required": ["rows"],
                "properties": { "rows": { "type": "array" }, "refused": { "type": "object" } },
            },
        });
        let found = super::refusal_scope_violations(bad.as_object().expect("map"));
        assert_eq!(
            found.len(),
            1,
            "a required array beside a refusal must be caught: {found:?}"
        );
        assert!(found[0].contains("OldStyle.rows"), "{}", found[0]);

        // A refusal naming a scope the shape does not carry.
        let orphan = serde_json::json!({
            "Orphan": { "required": [], "properties": { "holders_refused": { "type": "object" } } },
        });
        let found = super::refusal_scope_violations(orphan.as_object().expect("map"));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("names scope `holders`"), "{}", found[0]);

        // And the two shapes that are legitimately exempt stay silent.
        let ok = serde_json::json!({
            "Scoped": {
                "required": ["by_class"],
                "properties": {
                    "by_class": { "type": "array" },
                    "holders": { "type": "array" },
                    "holders_refused": { "type": "object" },
                },
            },
            "AlwaysRefuses": {
                "required": ["refused"],
                "properties": { "refused": { "type": "object" } },
            },
        });
        assert!(
            super::refusal_scope_violations(ok.as_object().expect("map")).is_empty(),
            "an unpaged sibling and an always-refusing shape are both legitimate"
        );
    }

    /// The ranking exemptions delete themselves: a ranking that grows a cursor is no longer a
    /// ranking, and the entry must go rather than sit there contradicting the document.
    #[test]
    fn a_ranking_surface_does_not_quietly_become_a_paged_one() {
        let doc = document();
        for path in super::RANKING_SURFACES {
            let names: Vec<&str> = doc["paths"][path]["get"]["parameters"]
                .as_array()
                .map_or(&[][..], |a| a)
                .iter()
                .filter_map(|p| p["name"].as_str())
                .collect();
            assert!(
                !names.contains(&"cursor"),
                "{path} is exempted as a ranking and now accepts a cursor. Remove the exemption."
            );
        }
    }

    #[test]
    fn figure_schema_matches_what_covered_json_emits() {
        // **The one hand-written schema in a generated document**, and therefore the one place
        // the spec could describe a shape that never ships. Pinned against the actual serializer
        // rather than against a struct definition, because `Figure`'s bytes come from
        // `wire::covered_json` and not from a derive.
        let doc = document();
        let props = doc["components"]["schemas"]["Figure"]["properties"]
            .as_object()
            .expect("Figure has properties");

        let emitted = crate::wire::covered_json(&analyzer_serving_clickhouse::Covered {
            value: 1.0,
            register: ">=",
            coverage: 0.5,
            coverage_kind: "token-weighted",
        });
        let emitted = emitted.as_object().expect("an object");

        let mut described: Vec<&str> = props.keys().map(String::as_str).collect();
        let mut actual: Vec<&str> = emitted.keys().map(String::as_str).collect();
        described.sort_unstable();
        actual.sort_unstable();
        assert_eq!(
            described, actual,
            "the Figure schema and covered_json's output have diverged"
        );
        let mut expected = wire::FIGURE_FIELDS.to_vec();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn every_operation_has_a_unique_operation_id() {
        // **What a generator names its types from.** Without it the name is munged from the path
        // — `getV1ResidualPlacement` — and the console has no naming to control. Verb-noun, so a
        // generator emits the `ListHolders{Request,Response}` pair the convention expects.
        let doc = document();
        let mut ids = Vec::new();
        for (path, item) in doc["paths"].as_object().expect("paths") {
            for (method, op) in item.as_object().expect("operations") {
                let id = op["operationId"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{method} {path} has no operationId"));
                ids.push(id.to_string());
            }
        }
        let total = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), total, "two operations share an operationId");
        assert!(total >= 15, "only {total} operations carried one");
    }

    #[test]
    fn a_templated_path_declares_its_parameter() {
        // Not thin documentation — INVALID OpenAPI, and a generator has nothing to put in the
        // URL.
        let doc = document();
        for (path, item) in doc["paths"].as_object().expect("paths") {
            for placeholder in path.split('{').skip(1).filter_map(|p| p.split('}').next()) {
                for (method, op) in item.as_object().expect("operations") {
                    let declared = op["parameters"].as_array().is_some_and(|ps| {
                        ps.iter().any(|p| {
                            p["name"] == placeholder && p["in"] == "path" && p["required"] == true
                        })
                    });
                    assert!(
                        declared,
                        "{method} {path} templates {{{placeholder}}} without declaring it"
                    );
                }
            }
        }
    }

    #[test]
    fn every_route_the_server_serves_is_in_the_document() {
        // **The direction the other checks cannot see.** Every check so far runs FROM the
        // document: paths reference schemas, parameters are attached, refs resolve. None of them
        // can see a route that is served and never written down: routed, handled and tested,
        // and absent from the contract until a consumer finds it rather than the build.
        //
        // Read from the router's source rather than from a list, so it cannot drift from what
        // axum actually serves.
        let routes_rs = include_str!("routes.rs");
        let served: std::collections::BTreeSet<String> = routes_rs
            .split(".route(")
            .skip(1)
            .filter_map(|r| {
                if r.contains("axum::routing::any(") {
                    assert!(r.contains("any(system::unknown_v1_route)"), "only the JSON unknown-route handler is a fallback; register application operations with explicit methods");
                    return None;
                }
                let start = r.find('"')?;
                r[start + 1..].split('"').next().map(str::to_string)
            })
            .collect();
        assert!(served.len() >= 15, "only {} routes parsed", served.len());

        let doc = document();
        let paths = doc["paths"].as_object().expect("paths");
        // Compared by SHAPE: the document writes `{day}` where the router writes `{day}`, but a
        // future rename of a path parameter should not read as a missing endpoint.
        let shape = |p: &str| -> Vec<String> {
            p.split('/')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    if s.starts_with('{') {
                        "*".to_string()
                    } else {
                        s.to_string()
                    }
                })
                .collect()
        };
        let documented: Vec<Vec<String>> = paths.keys().map(|p| shape(p)).collect();

        let undocumented: Vec<&String> = served
            .iter()
            .filter(|r| !documented.contains(&shape(r)))
            .collect();
        assert!(
            undocumented.is_empty(),
            "these routes are served and are in no path of the document, so a client generated \
             from it cannot reach them:\n  {undocumented:?}"
        );
    }

    #[test]
    fn the_stream_documents_every_event_payload() {
        // Read-api section 5 specifies the stream in prose. A document carrying none of it would
        // leave the console to hand-write three shapes from that prose — making the stream the
        // one part of the seam a regenerated client cannot check, and the one part a generation
        // canary is blind to. Rename a field on `tick_applied` and the slide-forward breaks
        // silently.
        let doc = document();
        let stream = &doc["paths"]["/v1/events"]["get"];
        assert_eq!(stream["operationId"], "streamEvents");
        assert!(
            stream["responses"]["200"]["content"]["text/event-stream"].is_object(),
            "a stream declares its media type"
        );

        let payloads = stream["x-event-payloads"]
            .as_object()
            .expect("the event payloads are documented");
        let schemas = doc["components"]["schemas"]
            .as_object()
            .expect("components");
        // Every kind `sse.rs` can publish, and nothing else. A kind here with no schema is prose
        // wearing a contract's clothes; a schema here for a kind nothing emits is fiction.
        let mut kinds: Vec<&str> = payloads.keys().map(String::as_str).collect();
        kinds.sort_unstable();
        assert_eq!(kinds, ["ingest_batch", "resync", "tick_applied"]);
        for (kind, r) in payloads {
            let name = r["$ref"]
                .as_str()
                .unwrap_or_else(|| panic!("{kind} has no $ref"))
                .rsplit('/')
                .next()
                .expect("a ref has a trailing segment");
            assert!(
                schemas.contains_key(name),
                "{kind} references {name}, which is not a defined schema"
            );
        }
    }

    #[test]
    fn a_closed_vocabulary_reaches_the_document_as_an_enum() {
        // A closed set flattened to `string` is a set that stops being closed at the client
        // boundary — the console gets a 400 at runtime where the compiler should have refused.
        let doc = document();
        let params = |path: &str| -> Vec<Value> {
            doc["paths"][path]["get"]["parameters"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        };
        let find = |ps: &[Value], name: &str| {
            ps.iter()
                .find(|p| p["name"] == name)
                .cloned()
                .unwrap_or(Value::Null)
        };

        let dims = find(&params("/v1/figures"), "dims");
        assert_eq!(dims["schema"]["type"], "array");
        assert_eq!(dims["style"], "form");
        assert_eq!(
            dims["explode"], false,
            "explode=false is what keeps the wire bytes `dims=a,b` while the TYPE stays a union"
        );
        assert_eq!(
            dims["schema"]["items"]["enum"],
            serde_json::json!(crate::requests::DIMS)
        );

        let class = find(&params("/v1/holders"), "class");
        assert_eq!(
            class["schema"]["enum"],
            serde_json::json!(crate::requests::classes()),
            "the six buckets come from analysis::Class::ALL, not a retyped list"
        );
    }

    #[test]
    fn the_legal_values_in_the_document_are_the_parsers_own() {
        // Two lists that must agree are two lists that eventually do not. These come from
        // `routes`' constants, beside the parsers that enforce them.
        let doc = document();
        let grain = &doc["paths"]["/v1/figures"]["get"]["parameters"][1];
        assert_eq!(grain["name"], "grain");
        assert_eq!(
            grain["schema"]["enum"],
            serde_json::json!(crate::requests::GRAINS)
        );
    }

    #[test]
    fn a_schema_ref_follows_a_rename_because_it_comes_from_the_type() {
        // Not a behaviour test so much as a statement of the mechanism: the `$ref` target is
        // `ToSchema::name()`, so it cannot drift from the struct the way a string literal would.
        assert_eq!(name_of::<wire::FiguresData>(), "FiguresData");
        assert!(
            document()["components"]["schemas"]["FiguresData"]["properties"]["segments"]
                .is_object()
        );
    }
}
