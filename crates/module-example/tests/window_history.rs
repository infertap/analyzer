// SPDX-License-Identifier: AGPL-3.0-only

//! Unsupported input contracts must fail through the public registration boundary,
//! before execution or persistence, even when the module starts disabled.

use analyzer_analysis::module::{
    Contribution, DerivationModule, Initialization, Mergeable, Row, Table,
};
use analyzer_analysis::Window;
use analyzer_pipeline::{
    adapter::{Adapter, Cadence, Context, Dependencies, Registration},
    modules::ModuleSet,
    Error,
};

#[derive(Default)]
struct Cells;
impl Mergeable for Cells {
    fn merge(&mut self, _: Self) {}
}

struct Definition<const HISTORY: bool>;
impl<const HISTORY: bool> DerivationModule for Definition<HISTORY> {
    type Key = ();
    type State = ();
    type Event = ();
    type Cells = Cells;
    type Ctx<'a> = ();
    const NAME: &'static str = "history_probe";
    const INITIALIZATION: Initialization = if HISTORY {
        Initialization::ExaminedHistory
    } else {
        Initialization::OutputWindow
    };
    fn fold(&self, (): &(), _: &[()], (): (), _: Window, (): &()) -> Contribution<Cells, ()> {
        panic!("registration must not execute the module")
    }
}

struct Probe<const HISTORY: bool, const WINDOW: bool>;
impl<const HISTORY: bool, const WINDOW: bool> Adapter for Probe<HISTORY, WINDOW> {
    type Definition = Definition<HISTORY>;
    type Output = ();
    const CADENCE: Cadence = if WINDOW {
        Cadence::Window
    } else {
        Cadence::Day
    };
    fn reconstruct(_: &Context<'_>, _: &Dependencies<'_>) -> Result<(), Error> {
        panic!("registration must not reconstruct output")
    }
    const TABLES: &'static [Table] = &[];
    fn rows(_: &Context<'_>, (): &()) -> Vec<Row> {
        panic!("registration must not render output")
    }
}

#[test]
fn window_examined_history_is_rejected_even_when_disabled() {
    let registrations = [Registration::of::<Probe<true, true>>()];
    for disabled in [&[][..], &["history_probe"][..]] {
        let error = ModuleSet::from_registrations(&registrations, disabled)
            .expect_err("window adapters cannot receive examined-history context");
        assert!(
            matches!(&error, analyzer_analysis::Error::Modules(message)
                if message.contains("history_probe")
                    && message.contains("ExaminedHistory")
                    && message.contains("window")),
            "registration must diagnose the unsupported contract: {error}"
        );
    }
}

#[test]
fn supported_day_history_and_window_output_contracts_still_register() {
    for registration in [
        Registration::of::<Probe<true, false>>(),
        Registration::of::<Probe<false, true>>(),
    ] {
        let registrations = [registration];
        let enabled = ModuleSet::from_registrations(&registrations, &[])
            .expect("supported input contract must register");
        assert!(enabled.has("history_probe"));
        let disabled = ModuleSet::from_registrations(&registrations, &["history_probe"])
            .expect("supported input contract may start disabled");
        assert!(!disabled.has("history_probe"));
    }
}
