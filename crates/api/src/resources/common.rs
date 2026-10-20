// SPDX-License-Identifier: AGPL-3.0-only

//! Common operations and their resource-specific mapping.

use crate::envelope::day_string;
use crate::wire;

pub(crate) fn holder_id(h: &analyzer_serving_clickhouse::Holder) -> wire::HolderId {
    wire::HolderId {
        instance: h.instance.clone(),
        dp_rank: h.dp_rank,
        group_idx: h.group_idx,
    }
}

pub(crate) fn reach_json(
    reach: analyzer_serving_clickhouse::RawReach,
    declared_days: u32,
) -> wire::ReachJson {
    match reach {
        analyzer_serving_clickhouse::RawReach::Covers => wire::ReachJson {
            covers: true,
            raw_from_ms: None,
            raw_from_day: None,
            declared_days: None,
            reason: None,
        },
        analyzer_serving_clickhouse::RawReach::Outside { raw_from_ms } => wire::ReachJson {
            covers: false,
            raw_from_ms,
            raw_from_day: raw_from_ms.map(|ms| day_string(crate::envelope::day_index(ms))),
            declared_days: Some(declared_days),
            // The two readings a bare absence cannot distinguish.
            reason: Some(if raw_from_ms.is_some() {
                "older than the oldest record this fleet holds"
            } else {
                "this fleet holds no records at all"
            }),
        },
    }
}
