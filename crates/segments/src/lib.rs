// SPDX-License-Identifier: AGPL-3.0-only

//! **The segment decoder: sealed bytes in, the record model out.**
//!
//! This is what a consumer links to read the segments a producer ships, without running our
//! appliance and without a database. It holds the record model's decode — both permanent
//! encodings of it (spec 2.1: Parquet is the shipped form, JSON Lines the write-ahead form,
//! dispatched on content), the shaping that turns a wire record into a typed row, the tail rule,
//! the content-hash batch key, and the declarations a segment header carries — and nothing that
//! talks to a store.
//!
//! **Its dependency set is the assertion.** It depends on no store, so linking the decoder drags no
//! database in: a dependency a partner never calls stays out of a binary they never wanted it in. The bright line pins `analyzer-analysis`
//! to nothing; it says nothing about the crates that implement a source, and this is the other half
//! of that claim.
//!
//! A partner's link closure is this crate plus `analyzer-analysis`: the reader and the folds.
//!
//! # Where to start
//!
//! - [`decode_segment`] is the entry: sealed bytes in, a [`DecodedSegment`] out, dispatched on
//!   content between the Parquet shipped form and the JSON Lines write-ahead form.
//! - [`columnar`] is the projected, row-group-pruned decode the analyzer's source uses;
//!   [`topic`] recognises the named, versioned topic conventions.
//!
//! A partner's link closure is this crate plus `analyzer-analysis`: the reader and the folds.

pub mod columnar;
pub mod error;

/// The record kinds that are lifecycle rows — the agent's starts, stops and heartbeats,
/// the segments' opens, recoveries and drops, the identities refused — the degradation
/// ledger's input. The ingest banks them and the source's lifecycle read
/// filters by them; one list, so the two cannot drift.
pub const LIFECYCLE_KINDS: &[&str] = &[
    "agent_start",
    "heartbeat",
    "agent_stop",
    "segment_open",
    "segment_recovered",
    "segments_dropped",
    "identity_refused",
];
/// The llm-d topic recognizer — contract vocabulary (spec 2.4, dashboard), so it
/// lives beside the kind roster: both ingest and the derivation read the wire through it.
pub mod topic;

pub use error::Error;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

/// Prefix of a sealed segment's filename: `seg-<incarnation>-<seq>`. Mirrors
/// `sealed_prefix` in `conformance/delivery/layout.json`, pinned against the vendored copy
/// by test. It scopes the header requirement rather than defining what is ingestible: a
/// file making the sealed-segment claim must honour it; an archive without the prefix is
/// a legitimate input that carries no delivery framing.
pub const SEALED_PREFIX: &str = "seg-";

/// Wire fields with typed columns, per record family; everything else rides `extra`.
const CACHE_FIELDS: &[&str] = &[
    "kind",
    "instance_id",
    // Origin scope. A block hash is an identity only within (instance, dp_rank, group_idx):
    // data-parallel workers hold physically independent caches — each rank publishes on its
    // own port, never a shared endpoint — and KV cache groups hash independently. Dropping
    // these collapses distinct caches onto one instance and reports their duplication as zero.
    "dp_rank",
    "group_idx",
    "block_id",
    // The fleet-stable identity, where the tap could derive one. Preferred over `block_id`
    // wherever present: the engine's id is comparable only inside the process that computed it.
    "content_id",
    "parent_id",
    // Distinguishes "no parent" (a root) from "parent undeterminable". Collapsing them makes
    // a root, which R1 counts as contributing (join-semantics).
    "parent_unknown",
    "n_tokens",
    "tier",
    "backend_id",
    // Whether the fact describes the publisher's OWN copy or one it can merely reach. Load
    // bearing: a REMOTE block is not this holder's copy, and counting it as one invents
    // duplication (join-semantics).
    "locality",
    "scope",
    "at_ms",
    // holder_reset's engine-clock ordering instant, beside its producer-clock at_ms — two
    // domains on one record, never compared (spec 2.3). Untyped here and it lands in
    // `extra` as a string, where the loader's fail-closed f64 read cannot see it, and the
    // record goes missing from the pipeline.
    "boundary_ms",
    // The transport sequence of the MESSAGE this event arrived in (spec 2.3). Typed because
    // Spec 5.4's narrowing is a comparison over integers: it turns a degraded span from "somewhere
    // in this heartbeat interval" into the bracket around the discontinuity.
    "seq",
    // Present only under a `labelled` declaration (spec 2.3), and NULL is not false: a
    // producer that cannot distinguish a reuse announcement from a fresh insertion emits
    // nothing here and declares `unlabelled` on its lifecycle records instead.
    "reused",
];
const LIFECYCLE_FIELDS: &[&str] = &[
    "kind",
    "at_ms",
    "reason",
    // The two declared liveness bounds (spec 5.8), from `agent_start`. Typed because they
    // gate the staleness verdict: they are the ENTIRE basis on which a reader may call a
    // producer stale, so leaving them behind a Map cast would put that verdict's threshold
    // behind a string.
    "msgs_seen",
    "dropped",
    "oversized",
    "noraw_scanned",
    "events_ingested",
    "rss_bytes",
    "endpoints",
    // Which key material this instance holds, as a value comparable across nodes. Typed because
    // it gates a headline figure: pseudonyms under two different keys cannot match at any
    // overlap, so a cross-node zero from a split fleet is an artifact rather than a measurement,
    // and this is the only positive proof that two nodes share a pseudonym space at all.
    // Exactly one: a rotation is a declared discontinuity (spec 3.3), and producer testimony
    // relating two key spaces is inadmissible (spec 3.4), so no companion field exists.
    "canary",
    // What a `store` MEANS on this producer's stream (spec 2.3). Typed because it gates
    // whether a figure may be presented at all rather than decorating one that may:
    // under `unlabelled` an unqualified count of stores is not an insertion count, and a
    // reader that could not reach this value would have to assume the flattering case.
    "reuse_reporting",
];
/// The delivery family: records that describe the FILE rather than the cache.
///
/// `incarnation` and `segment_seq` get columns because continuity is scoped to
/// (instance, incarnation) — a fresh incarnation at seq 0 is a RESTART, a missing seq within one
/// incarnation is LOSS, and without both typed the two are indistinguishable.
///
/// `first_seq`/`last_seq` join them because they are what localizes a DECLARED loss, and
/// reconciling a declaration against a structural gap is a comparison over integers. The tap also
/// carries the same range as filenames (`first`/`last`), which stay in `extra`: they are the
/// faithful record of what was unlinked and reconcile against a shipper's logs, but recovering a
/// number from them means parsing `seg-<incarnation>-<seq>` — and the delivery contract's own rule
/// is that correctness must not bind to a filename.
///
/// The rest (`attributed`, `dropped_bytes`, `count`, `first`, `last`) rides `extra`: these records
/// are rare — one per file, per recovery, per drop event — so a column each would cost more in
/// schema than it returns.
const DELIVERY_FIELDS: &[&str] = &[
    "kind",
    "at_ms",
    "incarnation",
    "segment_seq",
    "first_seq",
    "last_seq",
];
/// The capacity family: `identity_refused`, the identity mechanism's `segments_dropped` — a
/// declared, holder-scoped resolution loss (the tap's resident cap refused new identity
/// promises, so the named holder's evicts over the window carry no `content_id`).
///
/// Typed because the values localize a declared loss, the same argument that typed
/// `first_seq`/`last_seq`: `refused` is the count by which resolution is short, and
/// `window_start_ms`/`window_end_ms` bound which ENGINE-time span it degrades — engine clock,
/// unlike the record's own producer-clock `at_ms`, and never comparable against it. The
/// holder scope rides the cache family's existing columns.
const CAPACITY_FIELDS: &[&str] = &[
    "kind",
    "at_ms",
    "instance_id",
    "dp_rank",
    "group_idx",
    "refused",
    "window_start_ms",
    "window_end_ms",
];
/// A declared record's fields (spec 2.8): a declarer's statement, on its producer clock.
const DECLARED_FIELDS: &[&str] = &[
    "kind",
    "at_ms",
    "declaration_id",
    "identity",
    "tag",
    "from_ms",
    "until_ms",
];
const UNKNOWN_KIND_FIELDS: &[&str] = &["kind", "at_ms", "instance_id"];
/// Identifier-valued fields: numeric on the raw wire (block hashes), hex strings after
/// pseudonymization — stored as strings either way, so numerics are stringified here.
const ID_FIELDS: &[&str] = &["instance_id", "block_id", "parent_id"];
fn is_cache_kind(kind: &str) -> bool {
    matches!(kind, "store" | "evict" | "clear" | "holder_reset")
}
fn is_lifecycle_kind(kind: &str) -> bool {
    matches!(kind, "agent_start" | "heartbeat" | "agent_stop")
}
/// Records about the delivery layer itself, per `conformance/delivery/layout.json`.
///
/// These are as contractual as the cache kinds — the layout fixture states that a segment's
/// identity lives in its first record rather than its name — and a reader that did not model
/// them would count them as drift, flatten their fields into `extra`, and in the case of
/// `segments_dropped` lose a DECLARED data loss.
///
/// `segment_open` appears once per segment file, so a reader that did not model it would hold
/// `unknown_kinds` at roughly the file count. A fail-honest counter pinned above zero for a
/// benign reason cannot report the reason it exists for.
fn is_delivery_kind(kind: &str) -> bool {
    matches!(
        kind,
        "segment_open" | "segment_recovered" | "segments_dropped"
    )
}
/// The capacity signal — instance-scoped like a cache record (it declares a loss for one
/// observed holder), emitted on the producer's clock like a lifecycle one.
fn is_capacity_kind(kind: &str) -> bool {
    kind == "identity_refused"
}
/// The declared kinds (spec 2.8): statements a declarer makes about identities, which the
/// ingest banks and the reader groups by without interpreting.
pub const DECLARED_KINDS: &[&str] = &["tag_interval"];

fn is_declared_kind(kind: &str) -> bool {
    DECLARED_KINDS.contains(&kind)
}
/// Does this reader model `kind`, or would it land in the unknown branch?
///
/// Public so the conformance suite can assert coverage in the one direction that matters: every
/// kind the pinned contract carries is a kind this reader models. That assertion is why it
/// exists — three contractual delivery kinds were counted as drift for as long as they did
/// precisely because nothing compared the reader's list against the producer's, and a list kept
/// in sync by intention rather than by test is a list that drifts.
#[must_use]
pub fn models_kind(kind: &str) -> bool {
    is_cache_kind(kind)
        || is_lifecycle_kind(kind)
        || is_delivery_kind(kind)
        || is_capacity_kind(kind)
        || is_declared_kind(kind)
}

/// Whether a cache record's `locality` declares the publisher's OWN cache: absent or `LOCAL`.
/// An unrecognized value does not — "declared nothing" and "declared something this build
/// cannot read" differ, and only the first is safe to read as local. The same rule as
/// `analyzer_analysis::events::is_own_copy`; a pipeline test holds the two equal.
#[must_use]
pub fn is_own_copy(locality: Option<&str>) -> bool {
    matches!(locality, None | Some("LOCAL"))
}

/// The content-hash batch key (hex SHA-256 of the file's bytes) — public so recovery
/// tooling and tests can name a file's batch without re-deriving the rule.
#[must_use]
pub fn content_batch_id(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}
/// Heartbeat `endpoints` sub-stats into the parallel arrays, validated whole against the
/// record kind's definition first: a projection of `endpoints` decodes every nested column
/// (`columnar::decode`), so each element is a complete `Endpoint` and its required fields and
/// bounds all apply.
fn shape_endpoints(row: &mut Map<String, Value>, kind: &str, value: &Value) -> Result<(), Error> {
    inference_events::validate_field(kind, "endpoints", value)
        .map_err(|error| Error::Malformed(error.to_string()))?;
    let list = value
        .as_array()
        .ok_or(Error::Malformed("endpoints is not an array".to_string()))?;
    // `topic` is the relayed channel name the model dimension reads through the
    // recognizer — dropping it here is what kept a fully-wired dimension null.
    // `publisher_restarts` is the per-endpoint scope of the envelope total (spec 2.4).
    let mut cols: [(&str, Vec<Value>); 7] = [
        ("source", Vec::new()),
        ("endpoint", Vec::new()),
        ("msgs_seen", Vec::new()),
        ("dropped", Vec::new()),
        ("last_msg_at_ms", Vec::new()),
        ("topic", Vec::new()),
        ("publisher_restarts", Vec::new()),
    ];
    for entry in list {
        let o = entry.as_object().ok_or(Error::Malformed(
            "endpoint entry is not an object".to_string(),
        ))?;
        for (key, col) in &mut cols {
            col.push(o.get(*key).cloned().unwrap_or(Value::Null));
        }
    }
    for (key, col) in cols {
        row.insert(format!("endpoints.{key}"), Value::Array(col));
    }
    Ok(())
}
/// Shape one wire record into a storage row: typed columns out, leftovers into `extra`,
/// unknown kinds whole and counted. Fails closed on contract violations — corruption is
/// not data.
fn shape(
    record: &Map<String, Value>,
    fleet_id: &str,
    batch_id: &str,
    unknown_kinds: &mut u64,
) -> Result<Value, Error> {
    let kind = record
        .get("kind")
        .and_then(Value::as_str)
        .ok_or(Error::Malformed("record has no string 'kind'".to_string()))?
        .to_string();

    let known: &[&str] = if is_cache_kind(&kind) {
        CACHE_FIELDS
    } else if is_lifecycle_kind(&kind) {
        LIFECYCLE_FIELDS
    } else if is_delivery_kind(&kind) {
        DELIVERY_FIELDS
    } else if is_capacity_kind(&kind) {
        CAPACITY_FIELDS
    } else if is_declared_kind(&kind) {
        DECLARED_FIELDS
    } else {
        *unknown_kinds += 1;
        UNKNOWN_KIND_FIELDS
    };

    let mut row = Map::new();
    let mut extra: BTreeMap<String, String> = BTreeMap::new();
    for (key, value) in record {
        if key == "endpoints" && known.contains(&"endpoints") {
            shape_endpoints(&mut row, &kind, value)?;
        } else if known.contains(&key.as_str()) {
            let shaped = if ID_FIELDS.contains(&key.as_str()) && value.is_number() {
                Value::String(value.to_string())
            } else {
                value.clone()
            };
            // Validate only decoded fields: projected Parquet reads intentionally omit columns.
            inference_events::validate_field(&kind, key, &shaped)
                .map_err(|error| Error::Malformed(error.to_string()))?;
            row.insert(key.clone(), shaped);
        } else {
            let rendered = match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            extra.insert(key.clone(), rendered);
        }
    }

    if !row.contains_key("at_ms") {
        return Err(Error::Malformed(format!("{kind} record has no at_ms")));
    }
    if is_cache_kind(&kind) {
        if !row.contains_key("instance_id") {
            return Err(Error::Malformed(format!(
                "{kind} record has no instance_id"
            )));
        }
        if (kind == "store" || kind == "evict") && !row.contains_key("block_id") {
            return Err(Error::Malformed(format!("{kind} record has no block_id")));
        }
    } else {
        // agent-scoped rows take the '' sentinel (storage-schema 3.1); the wire contract
        // stays absent-not-null — the sentinel exists only in storage
        row.entry("instance_id".to_string())
            .or_insert_with(|| Value::String(String::new()));
    }

    row.insert("fleet_id".into(), fleet_id.into());
    row.insert("batch_id".into(), batch_id.into());
    if !extra.is_empty() {
        row.insert(
            "extra".into(),
            Value::Object(extra.into_iter().map(|(k, v)| (k, v.into())).collect()),
        );
    }
    Ok(Value::Object(row))
}
/// Parse a file's lines with the truncated-tail rule applied (spec 5.5): a final line that
/// does not parse is a crash artifact and is discarded; any other failure is corruption and
/// fails closed. The loss a torn tail represents is priced by sequence accounting (spec 4.2)
/// and by the producer's own `dropped_bytes` declaration, not here.
pub(crate) fn parse_lines(text: &str, file: &Path) -> Result<Vec<Map<String, Value>>, Error> {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut parsed = Vec::with_capacity(lines.len());
    for (i, line) in lines.iter().enumerate() {
        match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(o)) => parsed.push(o),
            Ok(_) | Err(_) if i + 1 == lines.len() => {}
            Ok(_) => {
                return Err(Error::Corrupt(format!(
                    "{}: line {} is valid JSON but not an object, and is not the final \
                     line — corrupt input, refusing",
                    file.display(),
                    i + 1
                )));
            }
            Err(e) => {
                return Err(Error::Corrupt(format!(
                    "{}: line {} does not parse ({e}) and is not the final line — \
                     corrupt input, refusing",
                    file.display(),
                    i + 1
                )));
            }
        }
    }
    Ok(parsed)
}
/// Decode a Parquet segment's rows back into wire records, in row order (spec 2.1).
///
/// One row is one record. A non-null cell becomes the field of its column's name; a null cell
/// is ABSENT — no key — which is what carries the absent-versus-empty distinction (spec 2.2)
/// across the encoding. Each `extra` map entry becomes a top-level field with its string value
/// verbatim: the producer stores strings verbatim and non-strings as their JSON text, and
/// `shape` routes unknown fields into its own extra map as strings, so passing the string
/// through unchanged is exactly the JSONL path end to end. Re-parsing JSON out of an `extra`
/// value would be a second decoder for the same field, and two decoders drift.
///
/// Fails closed on a file that does not open, a row that does not decode, a value outside the
/// contract schema's type vocabulary, and a row with no `kind`. There is no tail rule here:
/// spec 5.5 governs the write-ahead form alone.
fn parquet_records(file: &Path, bytes: &[u8]) -> Result<Vec<Map<String, Value>>, Error> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    // `bytes::Bytes` is the reader's only public in-memory `ChunkReader` at this pin, which is
    // the whole reason the `bytes` dependency exists (see Cargo.toml). The copy is one
    // allocation per segment, the same cost the text path pays in `from_utf8_lossy`.
    let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(bytes)).map_err(|e| {
        Error::Corrupt(format!(
            "{}: starts with the Parquet magic but does not open as Parquet ({e}) — \
             corrupt input, refusing",
            file.display()
        ))
    })?;
    let rows = reader
        .get_row_iter(None)
        .map_err(|e| Error::Corrupt(format!("{}: rows do not read ({e})", file.display())))?;
    let mut records = Vec::new();
    for (i, row) in rows.enumerate() {
        let row = row.map_err(|e| {
            Error::Corrupt(format!("{}: row {i} does not decode ({e})", file.display()))
        })?;
        let mut record = Map::new();
        for (name, field) in row.get_column_iter() {
            if name == "extra" {
                for (key, value) in map_entries(field)
                    .map_err(|e| Error::Corrupt(format!("{}: row {i}: {e}", file.display())))?
                {
                    record.insert(key, Value::String(value));
                }
            } else if let Some(value) = json_of_field(field).map_err(|e| {
                Error::Corrupt(format!("{}: row {i}, column {name}: {e}", file.display()))
            })? {
                record.insert(name.clone(), value);
            }
        }
        if record.get("kind").and_then(Value::as_str).is_none() {
            return Err(Error::Malformed(format!(
                "{}: row {i} has no string 'kind' — every record carries one (spec 2.2)",
                file.display()
            )));
        }
        records.push(record);
    }
    Ok(records)
}
/// The `extra` map's entries, both halves strings — the only shape the contract writes.
fn map_entries(field: &parquet::record::Field) -> Result<Vec<(String, String)>, Error> {
    use parquet::record::Field;
    match field {
        Field::Null => Ok(Vec::new()),
        Field::MapInternal(map) => map
            .entries()
            .iter()
            .map(|(key, value)| match (key, value) {
                (Field::Str(k), Field::Str(v)) => Ok((k.clone(), v.clone())),
                _ => Err(Error::Corrupt(
                    "extra carries a non-string entry — the map is string to string \
                          (spec 2.1), refusing"
                        .to_string(),
                )),
            })
            .collect(),
        _ => Err(Error::Corrupt(
            "extra is not a map — corrupt input, refusing".to_string(),
        )),
    }
}
/// One Parquet cell as the JSON value the wire form carries, `None` for a null cell (absent).
///
/// The accepted variants are exactly the contract schema's type vocabulary — utf8, int64,
/// float64, bool, list<utf8>, and the `endpoints` list<struct>, whose element structs omit
/// their null fields the way an optional field is omitted inline. Anything else is a column
/// this contract does not carry, and fails closed rather than guessing a mapping.
fn json_of_field(field: &parquet::record::Field) -> Result<Option<Value>, Error> {
    use parquet::record::Field;
    Ok(Some(match field {
        Field::Null => return Ok(None),
        Field::Bool(b) => Value::Bool(*b),
        Field::Long(i) => Value::Number((*i).into()),
        Field::Double(d) => {
            serde_json::Number::from_f64(*d)
                .map(Value::Number)
                .ok_or(Error::Corrupt(
                    "float64 cell is not a finite number".to_string(),
                ))?
        }
        Field::Str(s) => Value::String(s.clone()),
        Field::ListInternal(list) => Value::Array(
            list.elements()
                .iter()
                .map(|element| {
                    json_of_field(element)?
                        .ok_or_else(|| Error::Corrupt("list carries a null element".to_string()))
                })
                .collect::<Result<_, _>>()?,
        ),
        Field::Group(row) => {
            let mut object = Map::new();
            for (name, field) in row.get_column_iter() {
                if let Some(value) = json_of_field(field)? {
                    object.insert(name.clone(), value);
                }
            }
            Value::Object(object)
        }
        other => {
            return Err(Error::Malformed(format!(
                "value {other:?} is outside the contract schema's type vocabulary"
            )))
        }
    }))
}
/// Everything a file needs doing to it that does NOT touch the engine.
///
/// Split out because it is the parallelizable half: reading, hashing, parsing and shaping are
/// pure CPU over one file's bytes, while purge/insert/commit must stay ordered on one
/// connection.
///
/// Parsing scales across cores; the per-file fixed costs around it do not, so parallelizing
/// the decode alone moves the whole path less than the core count suggests.
struct Prepared {
    batch_id: String,
    shaped: String,
    records: u64,
    unknown_kinds: u64,
}
/// Everything a segment header declares once for the records behind it (spec 2.2, 2.7).
///
/// **Provenance keys are whatever the header carries that this contract does not name.** They are
/// operator vocabulary, so a reader carries them without validating them against anything.
pub(crate) struct Declared {
    pub incarnation: String,
    /// The epoch scoping this segment's identity space (contract 3.3). Stamped onto every record
    /// behind the header that declares it, like the workload class.
    pub key_epoch: Option<u64>,
    pub workload_class: Option<String>,
    /// The declared liveness bounds (spec 5.8). They ride the header so a run is judgeable from
    /// ANY segment of it, and they are stamped like the other run-constant declarations —
    /// unreachable bounds would leave every run `NoBasis` however loudly the producer declared.
    pub heartbeat_secs: Option<u64>,
    pub max_segment_secs: Option<u64>,
    pub provenance: Vec<(String, String)>,
}
/// The fields `segment_open` carries in its own right. Everything else on it is provenance.
const HEADER_FIELDS: &[&str] = &[
    "key_epoch",
    "heartbeat_secs",
    "max_segment_secs",
    "kind",
    "at_ms",
    "contract_version",
    "incarnation",
    "segment_seq",
    "workload_class",
    "attributed",
    "dropped_bytes",
];
pub(crate) fn batch_declarations(
    records: &[Map<String, Value>],
    file: &Path,
) -> Result<Declared, Error> {
    let incarnation = batch_incarnation(records, file)?;
    let header = records
        .iter()
        .find(|r| r.get("kind").and_then(Value::as_str) == Some("segment_open"));
    let (workload_class, provenance) = header.map_or_else(
        || (None, Vec::new()),
        |h| {
            let class = h
                .get("workload_class")
                .and_then(Value::as_str)
                .map(str::to_string);
            let prov = h
                .iter()
                .filter(|(k, _)| !HEADER_FIELDS.contains(&k.as_str()))
                .map(|(k, v)| {
                    let rendered = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), rendered)
                })
                .collect();
            (class, prov)
        },
    );
    Ok(Declared {
        incarnation,
        key_epoch: header
            .and_then(|h| h.get("key_epoch"))
            .and_then(Value::as_u64),
        workload_class,
        heartbeat_secs: header
            .and_then(|h| h.get("heartbeat_secs"))
            .and_then(Value::as_u64),
        max_segment_secs: header
            .and_then(|h| h.get("max_segment_secs"))
            .and_then(Value::as_u64),
        provenance,
    })
}
/// Which tap run wrote this batch, read from the batch's own `segment_open` header.
///
/// Derived here rather than carried on every record, because the tap already emits it once per
/// file at the place where it is true — repeating it per record would be compression, not
/// information, and it is not free: ~40 bytes on a ~150-byte record is a 25% inflation of the
/// segment stream, and `--max-disk-bytes` bounds the tap's local retention, so it would trade
/// directly against the completeness that cap exists to protect.
///
/// **Verified, not inferred.** "Take the first header and apply it to everything" is an inference
/// from a FILE BOUNDARY, and `layout.json` is explicit that correctness must not bind to a
/// property of the transport. One segment file is one batch today by construction, but a shipper
/// that concatenated two segments would put two headers in one batch, and first-one-wins would
/// silently misattribute half the rows — to a run whose key, configuration and continuity
/// sequence are all different. So the count is checked, and ambiguity refuses.
///
/// A file with no header is only an error if it CLAIMS to be a sealed segment. This reader accepts
/// any `.jsonl`, and two legitimate inputs carry no delivery framing: an archive or hand-assembled
/// corpus, which never had a header; and an `orphan-<ms>.jsonl`, the marker-first form producers
/// before contract 1.11 shipped when a crash took the header, admitted under spec 4.1's reader
/// tolerance (a 1.11 producer discards such a file and declares the discard in its own first
/// segment instead). Both take the empty incarnation, which is a real state rather than a
/// default, and one that reads as "not comparable" wherever key space is resolved.
fn batch_incarnation(records: &[Map<String, Value>], file: &Path) -> Result<String, Error> {
    let headers: Vec<&Map<String, Value>> = records
        .iter()
        .filter(|r| r.get("kind").and_then(Value::as_str) == Some("segment_open"))
        .collect();
    let name = file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    match headers.as_slice() {
        [one] => Ok(one
            .get("incarnation")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()),
        [] if name.starts_with(SEALED_PREFIX) => Err(Error::Malformed(format!(
            "{name}: a sealed segment carries its identity in its FIRST record \
             (conformance/delivery/layout.json) and this one has no segment_open. Corruption is \
             not data; a producer that finds a headerless file discards it and declares the \
             discard (spec 4.3)."
        ))),
        [] => Ok(String::new()),
        many => Err(Error::Malformed(format!(
            "{name}: {} segment_open records in one batch. A batch is one segment file, so \
             something merged them — and since batch identity is a hash of file contents, the \
             merged batch is a different batch from either half. Attributing every row to the \
             first header would misfile the rest under a run with a different key, configuration \
             and sequence.",
            many.len()
        ))),
    }
}
/// The engine-free half over bytes already in hand — the bucket reader's entry, where
/// the bytes arrived on the connection rather than from a file.
fn prepare_content(file: &Path, fleet_id: &str, bytes: &[u8]) -> Result<Prepared, Error> {
    let batch_id = content_batch_id(bytes);
    // The encoding dispatch, pinned to THIS spot: the hash above is
    // over raw bytes and already encoding-agnostic, and the text path's `from_utf8_lossy`
    // substitutes U+FFFD on binary input rather than failing — anywhere later, a Parquet
    // segment mangles into text and fails as JSON, loudly, for the wrong reason. The
    // discriminant is content, never the filename (spec 2.1, layout.json): a Parquet segment
    // begins with the magic bytes, a JSONL segment with a JSON object. One shaping path from
    // here down, two decoders — the truncated-tail rule (spec 5.5) governs the write-ahead
    // form alone; a torn Parquet file does not open, and corruption is not data.
    let records = if bytes.starts_with(b"PAR1") {
        parquet_records(file, bytes)?
    } else {
        let text = String::from_utf8_lossy(bytes);
        parse_lines(&text, file)?
    };
    let declared = batch_declarations(&records, file)?;
    let mut unknown_kinds = 0u64;
    let mut shaped = String::new();
    for record in &records {
        let row = stamped_row(record, fleet_id, &batch_id, &declared, &mut unknown_kinds)?;
        shaped.push_str(&row.to_string());
        shaped.push('\n');
    }
    Ok(Prepared {
        batch_id,
        shaped,
        records: records.len() as u64,
        unknown_kinds,
    })
}
/// Shape one record and apply the segment's declarations to it — the one write of the
/// run-constant facts, shared by the whole-file path and the columnar path so the two cannot
/// disagree about what a header stamps.
///
/// Onto EVERY row, not only the header's own. A segment declares its run-constant facts
/// once (spec 2.2, 2.7) and they belong to every record behind that declaration: the tap
/// run that wrote it, the key space and configuration that run carried, and what the
/// producer's traffic is for.
pub(crate) fn stamped_row(
    record: &Map<String, Value>,
    fleet_id: &str,
    batch_id: &str,
    declared: &Declared,
    unknown_kinds: &mut u64,
) -> Result<Value, Error> {
    let mut row = shape(record, fleet_id, batch_id, unknown_kinds)?;
    if let Some(obj) = row.as_object_mut() {
        // Onto every row EXCEPT the one kind whose own incarnation is contractually
        // ANOTHER run's: `segments_dropped` names the run whose DATA IS GONE, never the
        // process that did the dropping (spec 2.5) — one reclaim pass can drop a previous
        // run's segments, and stamping the dropper here points a declared loss at the wrong
        // history. The raw and Parquet paths preserve the record's own value, and so does
        // this one.
        let keeps_own = obj.get("kind").and_then(Value::as_str) == Some("segments_dropped")
            && obj.contains_key("incarnation");
        if !keeps_own {
            obj.insert("incarnation".into(), declared.incarnation.clone().into());
        }
        if let Some(class) = &declared.workload_class {
            obj.insert("workload_class".into(), class.clone().into());
        }
        if let Some(e) = declared.key_epoch {
            obj.insert("key_epoch".into(), e.into());
        }
        if let Some(hb) = declared.heartbeat_secs {
            obj.insert("heartbeat_secs".into(), hb.into());
        }
        if let Some(seg) = declared.max_segment_secs {
            obj.insert("max_segment_secs".into(), seg.into());
        }
        // Provenance is operator vocabulary this contract does not name, so it rides in
        // `extra` beside every other unrecognized field rather than claiming a column.
        if !declared.provenance.is_empty() {
            let extra = obj
                .entry("extra".to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Some(map) = extra.as_object_mut() {
                for (k, v) in &declared.provenance {
                    map.entry(k.clone()).or_insert_with(|| v.clone().into());
                }
            }
        }
    }
    Ok(row)
}
/// One sealed segment, decoded — the engine-free half, exposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSegment {
    /// The content hash. This is the delivery identity records link on, and it is the same value
    /// whichever path decodes the bytes, because it is a function of the bytes alone.
    pub batch_id: String,
    /// The shaped rows as NDJSON, one object per line, in the wire contract's own field names.
    pub shaped: String,
    /// Records the segment decoded, of every kind.
    pub records: u64,
    /// Records of a kind this reader does not know, counted and passed over.
    pub unknown_kinds: u64,
}
/// Decode a segment's bytes without an engine.
///
/// **The one decoder, shared by both directions.** The ingest path calls the same code to write
/// records; a segment-reading `EventSource` calls it to build the IR. Two decoders would drift on
/// exactly the fields that decide whether a taint fires, which is the failure the shared
/// lifecycle projection constant was written to prevent.
///
/// # Errors
/// Corrupt input — a line that does not parse and is not the final one — or a record missing a
/// field the shaping requires.
pub fn decode_segment(source: &str, fleet_id: &str, bytes: &[u8]) -> Result<DecodedSegment, Error> {
    let prep = if bytes.starts_with(b"PAR1") {
        contain_reader_panic(source, || {
            prepare_content(Path::new(source), fleet_id, bytes)
        })?
    } else {
        prepare_content(Path::new(source), fleet_id, bytes)?
    };
    Ok(DecodedSegment {
        batch_id: prep.batch_id,
        shaped: prep.shaped,
        records: prep.records,
        unknown_kinds: prep.unknown_kinds,
    })
}

/// Run a read of untrusted Parquet bytes, turning a panic inside the `parquet` reader into
/// [`Error::Corrupt`].
///
/// The reader panics on some damaged inputs instead of returning an error: a single inverted
/// byte in a valid segment reaches assertions in its bit and record readers
/// (`tests/hostile_bytes.rs` finds them). A segment comes from a customer's bucket, so one
/// damaged object must be refused like any other corrupt segment, not take the process down.
/// Only the Parquet path is wrapped: a panic in this crate's own code stays a panic.
pub(crate) fn contain_reader_panic<T>(
    source: &str,
    read: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(read)) {
        Ok(result) => result,
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "no message".to_string());
            Err(Error::Corrupt(format!(
                "{source}: the Parquet reader failed on damaged bytes: {message}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{content_batch_id, parse_lines, shape};
    use serde_json::{json, Value};
    use std::path::Path;

    fn shape_one(v: &Value) -> Value {
        let mut unknown = 0;
        shape(v.as_object().expect("object"), "f", "b", &mut unknown).expect("shape")
    }

    #[test]
    fn typed_columns_out_leftovers_into_extra() {
        let row = shape_one(&json!({
            "kind": "store", "instance_id": "i1", "block_id": 7, "parent_id": 1,
            "n_tokens": 4, "tier": "GPU", "at_ms": 1.5,
            "gpu": "fixture", "vllm_version": "0.11.0"
        }));
        assert_eq!(row["block_id"], json!("7")); // numeric wire ids stored as strings
        assert_eq!(row["parent_id"], json!("1"));
        assert_eq!(row["extra"]["gpu"], json!("fixture"));
        assert_eq!(row["extra"]["vllm_version"], json!("0.11.0"));
        assert_eq!(row["fleet_id"], json!("f"));
    }

    #[test]
    fn unknown_kinds_land_whole_and_counted() {
        let mut unknown = 0;
        let row = shape(
            json!({"kind": "future_kind", "at_ms": 2.0, "payload": {"x": 1}, "note": "n"})
                .as_object()
                .expect("object"),
            "f",
            "b",
            &mut unknown,
        )
        .expect("shape");
        assert_eq!(unknown, 1);
        assert_eq!(row["kind"], json!("future_kind"));
        assert_eq!(row["instance_id"], json!("")); // agent-scoped sentinel
        assert_eq!(row["extra"]["payload"], json!("{\"x\":1}"));
        assert_eq!(row["extra"]["note"], json!("n"));
    }

    #[test]
    fn the_tail_rule_discards_only_the_final_line_and_refuses_mid_file() {
        let rows = parse_lines(
            "{\"kind\":\"store\"}\n{\"kind\":\"sto",
            Path::new("t.jsonl"),
        )
        .expect("tail case");
        assert_eq!(rows.len(), 1, "the torn final line is discarded, silently");
        let err = parse_lines("garbage\n{\"kind\":\"store\"}", Path::new("t.jsonl"))
            .expect_err("mid-file corruption must refuse");
        assert!(
            matches!(&err, crate::Error::Corrupt(m) if m.contains("not the final line")),
            "mid-file corruption refuses as corruption, and says which line: {err}"
        );
    }

    #[test]
    fn the_batch_key_is_the_content_hash() {
        assert_eq!(
            content_batch_id(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
