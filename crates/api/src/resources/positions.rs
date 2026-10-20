// SPDX-License-Identifier: AGPL-3.0-only

//! Positions operations and their resource-specific mapping.

use crate::error::ApiError;
use crate::error::Cause;

pub(crate) fn holder_position(h: &analyzer_serving_clickhouse::Holder) -> String {
    serde_json::to_string(&(&h.instance, h.dp_rank, h.group_idx)).expect("holder position")
}
pub(crate) fn holder_cursor(
    position: &str,
) -> Result<analyzer_serving_clickhouse::HolderCursor, ApiError> {
    let (instance, dp_rank, group_idx) = serde_json::from_str::<(String, u32, u32)>(position)
        .map_err(|_| cursor_payload("its position is unreadable"))?;
    if instance.is_empty() {
        return Err(cursor_payload("its position is unreadable"));
    }
    Ok(analyzer_serving_clickhouse::HolderCursor {
        tokens: 0.0,
        holder: analyzer_serving_clickhouse::Holder {
            instance,
            dp_rank,
            group_idx,
        },
    })
}
pub(crate) fn roster_position(instance: &str, incarnation: &str) -> String {
    serde_json::to_string(&(instance, incarnation)).expect("roster position")
}
pub(crate) fn roster_cursor(position: &str) -> Result<(String, String), ApiError> {
    serde_json::from_str(position).map_err(|_| cursor_payload("its position is unreadable"))
}

pub(crate) fn holder_event_position(c: &analyzer_serving_clickhouse::HolderEventCursor) -> String {
    serde_json::to_string(&(
        c.at_ms,
        &c.holder.instance,
        c.holder.dp_rank,
        c.holder.group_idx,
        c.key_epoch,
    ))
    .expect("finite position")
}
pub(crate) fn holder_event_cursor(
    position: &str,
) -> Result<analyzer_serving_clickhouse::HolderEventCursor, ApiError> {
    let (at_ms, instance, dp_rank, group_idx, key_epoch) =
        serde_json::from_str::<(f64, String, u32, u32, Option<u32>)>(position)
            .map_err(|_| cursor_payload("its position is unreadable"))?;
    if instance.is_empty() {
        return Err(cursor_payload("its position is unreadable"));
    }
    Ok(analyzer_serving_clickhouse::HolderEventCursor {
        at_ms,
        holder: analyzer_serving_clickhouse::Holder {
            instance,
            dp_rank,
            group_idx,
        },
        key_epoch,
    })
}
pub(crate) fn timeline_position(c: &analyzer_serving_clickhouse::TimelineCursor) -> String {
    serde_json::to_string(&(c.at_ms, &c.block_id, c.key_epoch)).expect("finite position")
}
pub(crate) fn timeline_cursor(
    position: &str,
) -> Result<analyzer_serving_clickhouse::TimelineCursor, ApiError> {
    let (at_ms, block_id, key_epoch) = serde_json::from_str::<(f64, String, Option<u32>)>(position)
        .map_err(|_| cursor_payload("its position is unreadable"))?;
    Ok(analyzer_serving_clickhouse::TimelineCursor {
        at_ms,
        block_id,
        key_epoch,
    })
}
pub(crate) fn evidence_position(c: &analyzer_serving_clickhouse::EvidenceCursor) -> String {
    serde_json::to_string(&(
        c.at_ms,
        &c.block_id,
        &c.holder.instance,
        c.holder.dp_rank,
        c.holder.group_idx,
        c.key_epoch,
    ))
    .expect("finite position")
}
pub(crate) fn evidence_cursor(
    position: &str,
) -> Result<analyzer_serving_clickhouse::EvidenceCursor, ApiError> {
    let (at_ms, block_id, instance, dp_rank, group_idx, key_epoch) =
        serde_json::from_str::<(f64, String, String, u32, u32, Option<u32>)>(position)
            .map_err(|_| cursor_payload("its position is unreadable"))?;
    if instance.is_empty() {
        return Err(cursor_payload("its position is unreadable"));
    }
    Ok(analyzer_serving_clickhouse::EvidenceCursor {
        at_ms,
        block_id,
        holder: analyzer_serving_clickhouse::Holder {
            instance,
            dp_rank,
            group_idx,
        },
        key_epoch,
    })
}

pub(crate) fn cursor_payload(why: &'static str) -> ApiError {
    ApiError::new(
        Cause::RequestInvalid,
        format!("the cursor is not one this API minted: {why}"),
    )
}

#[cfg(test)]
mod position_tests {
    use super::*;
    #[test]
    fn event_positions_require_finite_instants() {
        for at in ["NaN", "inf", "-inf", "not-time"] {
            assert!(timeline_cursor(&format!("[{at},\"block\",null]")).is_err());
            assert!(holder_event_cursor(&format!("[{at},\"instance\",0,0,null]")).is_err());
            assert!(evidence_cursor(&format!("[{at},\"block\",\"instance\",0,0,null]")).is_err());
        }
    }
    #[test]
    fn opaque_subjects_and_epochs_round_trip_without_separator_assumptions() {
        let holder = analyzer_serving_clickhouse::Holder {
            instance: "instance/with\u{1f}separators".into(),
            dp_rank: 2,
            group_idx: 3,
        };
        assert_eq!(
            holder_cursor(&holder_position(&holder)).unwrap().holder,
            holder
        );
        let event = analyzer_serving_clickhouse::EvidenceCursor {
            at_ms: 1.5,
            block_id: "block/with\u{1f}separators".into(),
            holder,
            key_epoch: Some(u32::MAX),
        };
        assert_eq!(evidence_cursor(&evidence_position(&event)).unwrap(), event);
        assert_eq!(
            roster_cursor(&roster_position("a\u{1f}b", "c/d")).unwrap(),
            ("a\u{1f}b".into(), "c/d".into())
        );
    }
}
