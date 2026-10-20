// SPDX-License-Identifier: AGPL-3.0-only

//! The default analysis window: the fleet's full span of cache facts, measured over the input
//! log (the lineage's record of what arrived), never over the cache facts themselves.

use crate::Error;
use analyzer_analysis::Window;

/// The fleet's whole span of cache facts, from the input log's head (the event rows live
/// in the bucket; the head carries each day's first and last record instant), so no day is
/// read.
///
/// # Errors
/// The input log could not be read; `Ok(None)` when it holds no committed batch with cache
/// facts.
pub fn full_span(lake: &analyzer_lake::Lake) -> Result<Option<Window>, Error> {
    let head = crate::inputs::log(lake)?.head()?;
    let min = head
        .days
        .values()
        .filter_map(|d| d.first_at_ms)
        .reduce(f64::min);
    let max = head
        .days
        .values()
        .filter_map(|d| d.last_at_ms)
        .reduce(f64::max);
    let (Some(min), Some(max)) = (min, max) else {
        return Ok(None);
    };
    // 1 ms pad each side: the window predicate is half-open [start, end), so an
    // unpadded max would exclude the last fact from its own default window.
    Ok(Some(Window {
        start_ms: min - 1.0,
        end_ms: max + 1.0,
    }))
}
