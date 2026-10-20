// SPDX-License-Identifier: AGPL-3.0-only

//! Topic recognition: named, versioned conventions matched at exact arity.
//! The tap relays the frame verbatim and unparsed; recognizing
//! a convention is this reader's business, and **refusal is the only failure mode** — never
//! heuristic extraction. A recognizer added later re-derives labels over archived records by
//! ordinary re-analysis, because the relay is verbatim.

/// What a recognized topic names. Both segments are the deployment's declarations riding the
/// wire (every surviving source is a declaration); the pod segment is the engine's own
/// name for itself under the convention, relayed beside `instance_id` rather than replacing
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recognized<'a> {
    /// The engine's own name for itself under the convention — the middle `@`-part.
    /// Relayed beside `instance_id`, never in its place.
    pub pod: &'a str,
    /// The model the topic declares — the last `@`-part, verbatim, `org/name` included.
    pub model: &'a str,
}

/// Recognizer #1 — llm-d's `kv@<pod>@<model>`: exactly three `@`-parts, the first literally
/// `kv`, every segment non-empty. Anything else yields nothing.
#[must_use]
pub fn recognize(topic: &str) -> Option<Recognized<'_>> {
    let mut parts = topic.split('@');
    let (kv, pod, model) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || kv != "kv" || pod.is_empty() || model.is_empty() {
        return None;
    }
    Some(Recognized { pod, model })
}

#[cfg(test)]
mod tests {
    use super::recognize;

    #[test]
    fn the_llmd_convention_is_recognized_at_exact_arity() {
        let r = recognize("kv@10.0.1.7:8000@meta-llama/Llama-3.1-8B-Instruct").expect("match");
        assert_eq!(r.pod, "10.0.1.7:8000");
        assert_eq!(r.model, "meta-llama/Llama-3.1-8B-Instruct");
    }

    #[test]
    fn refusal_is_the_only_failure_mode() {
        // Wrong literal, wrong arity (short and long), empty segments: nothing, never a guess.
        for t in [
            "kvx@pod@model",
            "kv@pod",
            "kv@pod@model@extra",
            "kv@@model",
            "kv@pod@",
            "",
            "an-arbitrary-topic",
        ] {
            assert!(recognize(t).is_none(), "must refuse {t:?}");
        }
    }

    #[test]
    fn a_model_containing_at_is_refused_not_split() {
        // Exact arity means a fourth part refuses the whole topic rather than guessing
        // which `@` was the separator.
        assert!(recognize("kv@pod@org@model").is_none());
    }
}
