// SPDX-License-Identifier: AGPL-3.0-only

//! Time boundaries are validated without rounding away fractional event semantics.

use analyzer_analysis::{Params, Window};
use analyzer_pipeline::source::InputSelection;

#[test]
fn public_intervals_refuse_nonfinite_reversed_and_overflowing_bounds() {
    for (start, end) in [
        (f64::NAN, 0.0),
        (0.0, f64::NAN),
        (f64::NEG_INFINITY, 0.0),
        (0.0, f64::INFINITY),
        (2.0, 1.0),
        (-1e20, 0.0),
        (0.0, 9_223_372_036_854_775_808.0),
    ] {
        assert!(Window::new(start, end).is_err());
        assert!(InputSelection::Window(Window {
            start_ms: start,
            end_ms: end
        })
        .validate()
        .is_err());
    }
    assert!(Window::whole_days(1, 0).is_err());
    assert!(Window::whole_days(i64::MIN, 0).is_err());
    assert!(Window::whole_days(0, i64::MAX).is_err());
}

#[test]
#[allow(clippy::float_cmp)] // exact binary fractions
fn fractional_edges_and_empty_ranges_have_half_open_semantics() {
    let window = Window::new(-0.25, 0.5).unwrap();
    assert_eq!(window.length_ms(), 0.75);
    let selection = InputSelection::Window(window);
    assert!(!selection.contains(-0.250_001));
    assert!(selection.contains(-0.25));
    assert!(selection.contains(0.499_999));
    assert!(!selection.contains(0.5));
    let empty = InputSelection::Window(Window::new(0.25, 0.25).unwrap());
    assert!(!empty.contains(0.25));
    assert_eq!(
        Window::whole_days(-1, -1).unwrap(),
        Window::new(-86_400_000.0, 0.0).unwrap()
    );
}

#[test]
fn whole_batches_have_no_timestamp_pruning_hint_or_fake_window() {
    assert_eq!(InputSelection::WholeBatches.bounds(), None);
    assert!(InputSelection::WholeBatches.contains(-1e20));
    assert!(InputSelection::WholeBatches.contains(1e20));
    assert!(!InputSelection::WholeBatches.contains(f64::NAN));
    assert!(!InputSelection::WholeBatches.contains(f64::INFINITY));
}

#[test]
fn semantic_durations_are_checked_before_execution() {
    assert!(Params::default().validate().is_ok());
    for duration in [-0.01, f64::NAN, f64::INFINITY, 1e20] {
        assert!(Params::default()
            .with_delta_skew_ms(duration)
            .validate()
            .is_err());
        assert!(Params::default()
            .with_examined_history_ms(duration)
            .validate()
            .is_err());
    }
    // The bands end at the hour, so the history does too.
    let hour = analyzer_analysis::EXAMINED_HISTORY_MS;
    assert!(Params::default()
        .with_examined_history_ms(hour)
        .validate()
        .is_ok());
    assert!(Params::default()
        .with_examined_history_ms(hour + 1.0)
        .validate()
        .is_err());
}
