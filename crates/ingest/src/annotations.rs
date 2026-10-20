// SPDX-License-Identifier: AGPL-3.0-only

//! The declared-annotation contract: the durable object form and the prefix walk.
//!
//! **A declared deploy is an INPUT, and inputs live in the bucket.** It is the first fact no
//! segment implies — an operator statement, not an observation — so without a bucket form,
//! replay-is-recovery silently loses it on a rebuild. The object under [`ANNOTATIONS_PREFIX`]
//! is the source of truth; the marker row is its derived cache, exactly as every rollup row is
//! the derived cache of segments. Two front doors write the same format: a customer's deploy
//! pipeline putting JSON with its own credentials (more customer data through the same
//! untrusted-ingest door as segments), and the serve-mode endpoint on behalf of a console
//! session, and the CLI's `annotate`. All converge here, where [`ingest_annotations`] records
//! objects in the input log on the ingest cadence — which is also what replays them on a
//! rebuild — and the core publishes each as its marker, BEFORE any pass completes. That
//! ordering is correct: a seam is a fact about the timeline whether or not the series beneath
//! it has been recomputed yet.

use crate::Error;
use analyzer_objectstore::ObjectStore;

/// The object shape's version. Bump on any field addition, removal or retyping — customers
/// write this format from their own pipelines, so it is a contract like the emitted facts'.
pub(crate) const ANNOTATION_CONTRACT: &str = "an-1";

/// Where annotation objects live, collection-relative — beside the dated segment layout,
/// never inside it, so the segment walk and this walk cannot shadow each other.
pub const ANNOTATIONS_PREFIX: &str = "_analyzer/annotations/";

/// The size cap, refused before fetch where the listing already knows the size. An
/// annotation is a label and two instants; kilobytes is generous, and an unbounded fetch
/// from a customer-writable prefix is the exact shape the ingest bounds exist to refuse.
pub const ANNOTATION_MAX_BYTES: u64 = 4096;

/// Who recorded a deploy annotation. Closed, because `markers.source` is how a reader tells
/// an operator's declaration from the analyzer's own derivations, and an open string here
/// would let a writer claim to be either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploySource {
    /// `infertap-analyzer annotate`, run by somebody on the box.
    Cli,
    /// The serve-mode annotations endpoint, authenticated by the console session.
    Api,
    /// The customer's own deploy pipeline, writing the object directly into the bucket.
    Pipeline,
}

impl DeploySource {
    /// The token `markers.source` carries: `cli`, `api` or `pipeline`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Api => "api",
            Self::Pipeline => "pipeline",
        }
    }

    fn parse(token: &str) -> Result<Self, Error> {
        match token {
            "cli" => Ok(Self::Cli),
            "api" => Ok(Self::Api),
            "pipeline" => Ok(Self::Pipeline),
            other => Err(Error::Malformed(format!(
                "source {other:?} is not one of cli | api | pipeline"
            ))),
        }
    }
}

/// One declared deploy — the semantics; [`encode`]/[`parse`] are its one wire spelling.
#[derive(Debug, Clone, PartialEq)]
pub struct DeployAnnotation {
    /// The instant the deploy is ABOUT — where the marker sits on the trend. UTC epoch ms,
    /// the trend's own domain; the declaration is authoritative about its own instant.
    pub at_ms: f64,
    /// The words on the marker.
    pub label: String,
    /// The declarer's stated scope, or none. Never a blast radius — scope is the reader's
    /// lens, and the platform infers nothing from this field.
    pub scope: Option<String>,
    /// Which front door recorded the declaration, from the closed set — how a reader tells
    /// an operator's statement from the analyzer's own derivations.
    pub source: DeploySource,
    /// When the statement was MADE, distinct from `at_ms` (the
    /// `blessed_at_ms` argument): the lateness of a declaration is provenance the glass may
    /// state. `None` = the writer stated no declaration time.
    pub declared_at_ms: Option<f64>,
}

impl DeployAnnotation {
    /// Refuse the states no reader could render honestly.
    ///
    /// # Errors
    /// A non-finite instant, an empty label, an over-long field, or a non-finite
    /// declaration time.
    pub fn validate(&self) -> Result<(), Error> {
        if !self.at_ms.is_finite() {
            return Err(Error::Refused(format!(
                "a deploy instant must be finite, got {}",
                self.at_ms
            )));
        }
        if self.label.trim().is_empty() {
            return Err(Error::Refused(
                "a deploy annotation needs a label: it is the words on the marker".to_string(),
            ));
        }
        // The caps keep a customer-writable format from becoming a payload channel; they are
        // generous for their purpose (words on a marker chip, a lens token).
        if self.label.len() > 256 {
            return Err(Error::Refused(format!(
                "label is {} bytes; the cap is 256",
                self.label.len()
            )));
        }
        if self.scope.as_ref().is_some_and(|s| s.len() > 128) {
            return Err(Error::Refused("scope is over the 128-byte cap".to_string()));
        }
        if self.declared_at_ms.is_some_and(|d| !d.is_finite()) {
            return Err(Error::Refused(
                "declared_at_ms must be finite where present".to_string(),
            ));
        }
        Ok(())
    }
}

/// The object's bytes — one JSON object, newline-terminated like every emitted body.
#[must_use]
pub fn encode(a: &DeployAnnotation) -> String {
    let mut body = serde_json::json!({
        "annotation_contract": ANNOTATION_CONTRACT,
        "kind": "deploy",
        "at_ms": a.at_ms,
        "label": a.label.trim(),
        "scope": a.scope,
        "source": a.source.as_str(),
        "declared_at_ms": a.declared_at_ms,
    })
    .to_string();
    body.push('\n');
    body
}

/// The object's bytes back to the semantics, refusing what [`DeployAnnotation::validate`]
/// refuses. Unknown fields are ignored (a NEWER writer may add some); an unknown CONTRACT
/// version is refused, because ignoring fields is forward-compatible and guessing at a
/// reshaped object is not.
///
/// # Errors
/// Not JSON, wrong contract version, wrong kind, or an invalid annotation.
pub fn parse(bytes: &[u8]) -> Result<DeployAnnotation, Error> {
    let v: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| Error::Malformed(format!("not a JSON object: {e}")))?;
    let contract = v["annotation_contract"].as_str().unwrap_or("");
    if contract != ANNOTATION_CONTRACT {
        return Err(Error::Malformed(format!(
            "annotation_contract {contract:?} is not {ANNOTATION_CONTRACT:?}; this reader \
             refuses rather than guesses at a reshaped object"
        )));
    }
    let kind = v["kind"].as_str().unwrap_or("");
    if kind != "deploy" {
        return Err(Error::Malformed(format!(
            "kind {kind:?} is not a declared-annotation kind"
        )));
    }
    let ann = DeployAnnotation {
        at_ms: v["at_ms"]
            .as_f64()
            .ok_or(Error::Malformed("at_ms is not a number".to_string()))?,
        label: v["label"]
            .as_str()
            .ok_or(Error::Malformed("label is not a string".to_string()))?
            .to_string(),
        scope: v["scope"].as_str().map(str::to_string),
        source: DeploySource::parse(v["source"].as_str().unwrap_or("pipeline"))?,
        declared_at_ms: v["declared_at_ms"].as_f64(),
    };
    ann.validate()?;
    Ok(ann)
}

/// The object's key: content-addressed over `(at_ms, label)` — the same pair the markers
/// table replaces on — so re-declaring one deploy overwrites its own object instead of
/// accumulating siblings, and the row it derives replaces on the same identity.
#[must_use]
pub fn object_key(at_ms: f64, label: &str) -> String {
    #[allow(clippy::cast_possible_truncation)] // epoch ms is far inside i64
    let ms = at_ms as i64;
    let hash = analyzer_segments::content_batch_id(label.trim().as_bytes());
    format!("{ANNOTATIONS_PREFIX}ann-{ms}-{}.json", &hash[..16])
}

impl DeployAnnotation {
    /// The declaration as the input log records it.
    #[must_use]
    pub fn record(&self) -> analyzer_lake::ledger::DeclaredAnnotation {
        analyzer_lake::ledger::DeclaredAnnotation {
            at_ms: self.at_ms,
            label: self.label.trim().to_string(),
            scope: self.scope.clone(),
            source: self.source.as_str().to_string(),
            declared_at_ms: self.declared_at_ms,
        }
    }
}

/// Declare a deploy: write its object under [`ANNOTATIONS_PREFIX`] in `store`, the durable
/// form every front door shares. The next annotation walk records it in the input log, and
/// the core publishes its marker from there. Returns the object's key.
///
/// # Errors
/// An invalid annotation, or the store refused the write.
pub fn declare(store: &dyn ObjectStore, a: &DeployAnnotation) -> Result<String, Error> {
    a.validate()?;
    let key = object_key(a.at_ms, &a.label);
    store.put(&key, encode(a).as_bytes())?;
    Ok(key)
}

/// What one walk of the prefix did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AnnotationIngest {
    /// Objects turned into marker rows this walk.
    pub ingested: u64,
    /// Objects refused — over the size cap, or unparseable. Each is ledgered with
    /// `unknown_kinds = 1` so the refusal is a fact, and a FIXED re-upload (new content,
    /// new identity) ingests cleanly.
    pub refused: u64,
}

/// Walk [`ANNOTATIONS_PREFIX`] and commit every new object to the input log, as one ingest
/// cycle: its declaration, or its refusal.
///
/// **Dedup is the input log's head, pre-fetch** — the identity is `key@etag`, known from the
/// LISTING, and the head keeps the identities it holds, so an already-recorded object costs
/// zero GETs on every later cycle. That is deliberately NOT the segments'
/// content-hash-after-fetch discipline: a per-object fetch on every
/// cycle is probe cost growing with directory size forever. The etag IS the store's own
/// content fingerprint, so a re-declared object (same key, new content) gets a new identity
/// and re-ingests. Where a store reports no etag, the size stands in — weaker, and honest
/// about being so: a same-size rewrite on such a store waits for its key to change.
///
/// Runs on the ingest cadence AND on replay: the core publishes the marker from the log, so
/// the marker row is a derived cache rather than a second source of truth.
///
/// # Errors
/// Transport failure on the listing, the input log, or engine failure. A single bad OBJECT
/// is not an error: it is refused, recorded, and counted, so one malformed upload cannot
/// wedge the cycle forever.
pub fn ingest_annotations(
    lake: &analyzer_lake::Lake,
    store: &dyn ObjectStore,
) -> Result<AnnotationIngest, Error> {
    let listed = store.list_after(ANNOTATIONS_PREFIX, "")?;
    if listed.is_empty() {
        return Ok(AnnotationIngest::default());
    }
    let log = lake.ledger(analyzer_lake::ledger::SHARD)?;
    let seen = log.head()?.annotations;

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let position = log.next_position()?;
    let mut out = AnnotationIngest::default();
    let mut records = Vec::new();
    for meta in listed {
        let etag = meta.etag.clone().unwrap_or_else(|| meta.size.to_string());
        let batch_id = format!("ann:{}@{etag}", meta.key);
        if seen.contains(&batch_id) {
            continue;
        }
        let declared = if meta.size > ANNOTATION_MAX_BYTES {
            Err(format!(
                "{} bytes is over the {ANNOTATION_MAX_BYTES}-byte annotation cap",
                meta.size
            ))
        } else {
            match store.get(&meta.key).map(|bytes| parse(&bytes)) {
                Ok(Ok(ann)) => Ok(ann),
                Ok(Err(parse_err)) => Err(parse_err.to_string()),
                // A fetch that fails is a TRANSPORT fact, not an object fact: leave it
                // unrecorded so the next cycle retries, and let the listing's own error
                // handling decide whether the cycle is healthy.
                Err(e) => {
                    log::info!("annotation {} unfetchable this cycle: {e}", meta.key);
                    continue;
                }
            }
        };
        let (count, unknown, annotations) = match &declared {
            Ok(ann) => (1, 0, vec![ann.record()]),
            Err(why) => {
                log::warn!(
                    "annotation {} refused: {why} (recorded; a fixed re-upload ingests \
                     cleanly)",
                    meta.key
                );
                (0, 1, Vec::new())
            }
        };
        records.push(analyzer_lake::ledger::BatchRecord {
            batch_id,
            carrier: "annotation".to_string(),
            source: meta.key.clone(),
            object_key: meta.key.clone(),
            etag,
            input_position: position,
            ingested_at_ms: stamp.saturating_mul(1000),
            record_count: count,
            unknown_kinds: unknown,
            first_at_ms: None,
            last_at_ms: None,
            days: Vec::new(),
            segments: Vec::new(),
            drops: Vec::new(),
            lifecycle: Vec::new(),
            stored: Vec::new(),
            annotations,
            tags: Vec::new(),
        });
        if declared.is_ok() {
            out.ingested += 1;
        } else {
            out.refused += 1;
        }
    }
    log.commit_cycle(position, records)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ann() -> DeployAnnotation {
        DeployAnnotation {
            at_ms: 1_787_616_000_000.0,
            label: "affinity v2 rollout".into(),
            scope: Some("rag-batch".into()),
            source: DeploySource::Pipeline,
            declared_at_ms: Some(1_787_616_100_000.0),
        }
    }

    #[test]
    fn the_object_round_trips_and_both_instants_survive() {
        let a = ann();
        let parsed = parse(encode(&a).as_bytes()).expect("round trip");
        assert_eq!(parsed, a);
    }

    #[test]
    fn an_unknown_contract_version_is_refused_never_guessed() {
        let body = encode(&ann()).replace("an-1", "an-9");
        let err = parse(body.as_bytes()).expect_err("must refuse");
        assert!(
            matches!(&err, crate::Error::Malformed(m) if m.contains("an-9")),
            "{err}"
        );
    }

    #[test]
    fn unknown_fields_are_ignored_within_a_known_contract() {
        // A newer an-1 writer may add fields; an old reader keeps working. The CONTRACT
        // version is what gates reshaping, not field count.
        let mut v: serde_json::Value = serde_json::from_str(&encode(&ann())).unwrap();
        v["future_field"] = serde_json::json!("whatever");
        assert_eq!(parse(v.to_string().as_bytes()).expect("parses"), ann());
    }

    #[test]
    fn the_refusals_are_the_validators() {
        for (mutate, needle) in [
            (
                Box::new(|a: &mut DeployAnnotation| a.at_ms = f64::NAN)
                    as Box<dyn Fn(&mut DeployAnnotation)>,
                "finite",
            ),
            (
                Box::new(|a: &mut DeployAnnotation| a.label = "  ".into()),
                "label",
            ),
            (
                Box::new(|a: &mut DeployAnnotation| a.label = "x".repeat(257)),
                "cap is 256",
            ),
            (
                Box::new(|a: &mut DeployAnnotation| a.declared_at_ms = Some(f64::INFINITY)),
                "declared_at_ms",
            ),
        ] {
            let mut a = ann();
            mutate(&mut a);
            let err = a.validate().expect_err("must refuse");
            assert!(
                matches!(&err, crate::Error::Refused(m) if m.contains(needle)),
                "{err:?} should mention {needle:?}"
            );
        }
    }

    #[test]
    fn the_key_is_stable_over_the_replace_identity_and_nothing_else() {
        let a = ann();
        let k1 = object_key(a.at_ms, &a.label);
        let k2 = object_key(a.at_ms, " affinity v2 rollout "); // trim-insensitive
        assert_eq!(k1, k2, "the key is the replace identity, whitespace aside");
        assert!(k1.starts_with(ANNOTATIONS_PREFIX));
        assert_ne!(
            k1,
            object_key(a.at_ms + 1.0, &a.label),
            "a different instant is a different deploy"
        );
        assert_ne!(
            k1,
            object_key(a.at_ms, "other label"),
            "a different label is a different deploy"
        );
    }
}
