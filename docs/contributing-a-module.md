# Contributing a derivation module

The pure fold belongs in `analysis`; input selection, compatible recovery and persistence
belong in `pipeline`. [The shared executor](../crates/pipeline/src/executor.rs) is the
supported operation surface for standalone callers and the daemon. Static module
registration installs a typed adapter in the execution registry. Separate Rust crates use
[`pipeline::adapter`](../crates/pipeline/src/adapter.rs) and supplied `ModuleSet` registrations.
The [coverage-gap example](../crates/module-example/src/lib.rs) and
[public-API acceptance tests](../crates/module-example/tests/registration.rs) demonstrate the
boundary. The core registers (classes, placement, coverage, the roster) are the keyed
driver's operators, not modules.

Open an executor with `Executor::open(spine, &params, serving)` after binding the serving store
to the spine's namespace, and give it the module set with `with_modules`. `advance(source)` runs
one keyed tick: it reads what the input log committed, advances the operators, publishes the
cells that moved, and runs every enabled public module over each day the tick settled.
`availability()` reports each registered module and the span its output is stated over. The
executor owns no timer or worker lease; the daemon chooses the cadence, and each tick owns its
exclusion and completion boundary.

## Current extension boundary

Static day/window registration is public. Start with the separate `analyzer-module-example`
crate: its coverage-gap fold derives each source's unobserved duration from heartbeat coverage
and persists per-source exposure plus a daily total. It does not interpret missing observation
as source downtime. The example uses only public dependencies and has no execution-journal code.

```rust
use analyzer_pipeline::{adapter::Registration, modules::{self, ModuleSet}};
use analyzer_module_example::GapAdapter;

let mut registrations = modules::registrations();
registrations.push(Registration::of::<GapAdapter>());
let modules = ModuleSet::from_registrations(&registrations, &[])?;
// Executor::open(spine, &params, serving)?.with_modules(modules)
# Ok::<(), analyzer_analysis::Error>(())
```

The application gives this module set to the executor (`Executor::with_modules`). Construction
validates names (`[a-z][a-z0-9_]*`), duplicates, missing/cyclic dependencies and matching
cadence, even for disabled definitions, and refuses a core register's name. Enabled dependents
cannot have disabled prerequisites. A module may depend on the day's coverage (`coverage`), the
one core register offered as a dependency.

Implement `adapter::Adapter` with a pure `Definition`, typed `Output`, `CADENCE`, its
`TABLES`, and two methods: `reconstruct(context, dependencies)` and `rows(context, output)`.
The settled-day hook runs the module over each settled day: it reconstructs, then the
framework makes the day's rows exactly the output's, writing them under the tick's publication
and retiring every older version in the day. A key absent from the new output therefore
leaves the served tables. If any step fails, the tick stays unapplied, and the next tick
states the day again whole, under a fresh revision.

`Context` carries the committed source, fleet, parameters, input context and authorized
output window. It does not carry the engine: a module never writes SQL.
`Dependencies::require::<A>()` accepts only declared prerequisites completed for this day;
`Dependencies::coverage()` returns the day's coverage built from the summed coverage cells
(`CoverageOutput::coverage()`), so it agrees with the published `coverage_daily`.
Reconstruction must not mutate serving output. Missing required history is an error.

Each table is an `analysis::module::Table`: key and measure columns with their kinds, and the
column that places a row in its day. `adapter::install::<A>(&engine)` creates the tables
where absent, with the framework's head (`fleet_id`, `semantics_version`, `discriminator`)
before the key and its tail (`version`, `gone`) after the measures; readers take the latest
version per key that is not `gone`. The application calls `install` once, before
registration. Registration provides no dynamic loading or distributed scheduling. Changing
an external module's semantics or tables requires a fresh derivation (`reset-derived`).
Window adapters support only `Initialization::OutputWindow`: `ExaminedHistory` is rejected
at registration, including for disabled definitions, because window dispatch supplies no
separate history context. Day adapters may declare `ExaminedHistory`.
`ObservedPrefix` is refused for day/window registrations because this boundary has no portable
prefix seed contract.

The external test proves nonempty output, a failed write and a lost apply each recovered whole
by the next tick (a stale key retired), the stated span, and that a module enabled after days
settled states only the days that settle after.

## What a module is

The analyzer derives facts that are **latent in a per-identity sequence**: truth about the
relationship between one entity's events over time that no point-in-time metric can state.
A module is one such fact behind one boundary, `analyzer_analysis::module::DerivationModule`:

```rust
pub trait DerivationModule: Send + Sync + 'static {
    type Key: Ord + Clone + Send;                       // the entity whose ordered events decide the fact
    type State: Default + Clone + PartialEq + Send;     // what one key carries to the next window
    type Event;                                         // what the fold reads
    type Cells: Mergeable + Send;                       // what one window contributes, keyed
    type Ctx<'a>;                                       // the window's read-only context

    const NAME: &'static str;
    const INITIALIZATION: Initialization;               // OutputWindow | ExaminedHistory | ObservedPrefix
    const NEEDS: &'static [&'static str] = &[];         // modules folded before this one

    fn fold(&self, key: &Self::Key, events: &[Self::Event], state: Self::State,
            window: Window, ctx: &Self::Ctx<'_>) -> Contribution<Self::Cells, Self::State>;
}
```

The pure module owns arithmetic and contribution types. The pure `run_window` helper handles
grouped events per key, contribution composition and carried states; the keyed driver's lanes
run the core modules through it. `Persisted` provides typed day-cell writes, restoration and
retirement, while typed adapters reconstruct and persist output. The registry executes
prerequisites and passes typed results through `Dependencies`; the executor owns mutation
exclusion and the journal's intent.

Two properties shape a fold, and each module's doc states them:

- **The key grain**: what "one entity" is. A block identity (the fence's unit for cache facts),
  a source (a producer, or a producer's run), a holder (one cache), or the fleet. This is a fold
  grouping key. The execution namespace owns a whole fleet; moving smaller partitions requires
  dependency and reduction proofs.
- **The algebra**: mergeable when one key's contributions compose under the type's documented sum
  or replacement law; prefix-stateful when one key's ordered prefix must be folded in one place
  because the state is that prefix (open intervals, frontiers, seeds).

They are orthogonal: a prefix-stateful fact can still have mergeable cells (class/band does),
and a mergeable fact can be stateless (coverage, the roster).

## The four modules in the tree (copy the closest one)

| module | file | key grain | algebra | state | event | needs |
|---|---|---|---|---|---|---|
| `Duplication` | `crates/analysis/src/dup_cells/mod.rs` | Identity | PrefixStateful | `IdentityState` (open seeds per interval key) | cache `Event` | — |
| `ClassBand` | `crates/analysis/src/class_band.rs` | Identity (with its key epoch) | PrefixStateful | `Vec<Frontier>` | the group's events | `coverage` |
| `ObservedCoverage` | `crates/analysis/src/observed_coverage.rs` | Source | Mergeable | `()` | `HeartbeatSpan` | — |
| `Roster` | `crates/analysis/src/roster.rs` | Source (the run) | Mergeable | `()` | `Sighting` | — |

What each one shows:

- **Duplication** — the plain prefix-stateful fold: one identity's events grouped by interval
  key, folded from the carried seeds, the next seeds handed back as the state. Placement's
  lanes drive it through `run_window` (`crates/pipeline/src/keyed/placement.rs`).
- **Class/band** — a module whose cells carry its **ledger rows** beside its mergeable cells
  (`ClassBandCells`: the classified facts, the stores and residencies the residual keeps,
  the tokens, the buckets). One write path per module: the rows ride the same contribution
  the cells do. It also shows `NEEDS`: its facts carry the coverage figure, so the pure fold
  consumes it in `ClassCtx`. Repeat compute's lanes drive it (`keyed/repeat.rs`).
- **Coverage** — a source-keyed, stateless, mergeable fold whose whole-window figure is
  assembled from the merged cells (`Coverage::of_window`); an evidence-only source folds as
  an empty group and comes out uncertain for the whole window.
- **Roster** — a mergeable fact with **no day axis** (it implements `Mergeable` and `Tracked`, with no `DayPartitioned` implementation) and **one event from two input arms**: the pipeline turns arrivals
  rows or lifecycle records into `Sighting`s, so the fold sees one shape and the store
  question of which arm to take stays out of the module.

### What is not a module

The degradation ledger is framework: an ephemeral qualifier of other
facts' figures, re-derived per window, consulted in-process, never a keyed fact of its own —
and its run incidents need a store query, which no fold may issue. Not everything the
analyzer computes is a module; the trait stays honest by leaving those out.

## Writing one

1. **State the two declarations first.** "My key is X; my fact is a mergeable sum / a
   prefix-stateful fold." If you cannot say it in a sentence, the fact is not module-shaped
   yet — see the admission criterion below.
2. **Write the contribution type and its `Mergeable` implementation.** State exactly
   whether repeated keys sum or replace and whether order matters. Implement `Tracked`
   only for materialized state with changed-key tracking, and `DayPartitioned` only where
   day removal is meaningful. `Persisted` requires those two storage capabilities, not
   contribution composition. Coverage replaces a repeated source/day contribution; roster
   replaces a sighting only under its documented freshness order. Neither law permits
   adding a replay twice or implies distributed commutativity.
3. **Write the fold.** One key, one window: its events in the order given, its carried
   state, the window's context; hand back what this window contributes, the state the next
   window resumes from, and the days it asks repaired. No store, no clock, no I/O.
4. **Declare `NEEDS`** only if your context reads another module's *window-level* outcome.
   A need is an ordering, never a join: you get a whole window's outcome, not one key's.
5. **Register its adapter** with `Registration::of::<A>()`, append it to
   `modules::registrations()`, and pass the validated `ModuleSet` to the executor. An adapter
   executes once per settled day, and again whenever a correction restates the day or a
   failed tick is replayed. Dependencies require the same cadence.
6. **Pin it with no engine.** A unit test in your module's file: events in, cells out, and
   the merge law (two lanes' contributions merge to the whole). Every module in the tree
   carries one; copy its shape. `cargo test -p analyzer-analysis` runs in well under a
   second.
7. **Implement its adapter** from the smallest actual derivation. Use
   `Dependencies::require` for declared prerequisite results. Reconstruct complete output
   and render it as rows of the declared tables; the framework writes and retires them.
   Disabled adapters do not execute.
   **Persistence:** for keyed day-cell state needing incremental restoration, implement `Persisted`
   (`analyzer_analysis::module`) over `Tracked` and `DayPartitioned`: the tables as typed column specs
   (`Table`, `Column`, `Kind`, the day column), `render` for the changed cells and
   `render_day` for a whole day as `Row`s of typed `Value`s, and `restore` taking a served
   row as the text the engine renders each kind in — a date its day index, a datetime its
   unix seconds, a null `\N`. The serving port (`publish::Publish`: `apply` for changed
   cells, `replace` for a restated day) and `persist::read_back` do the rest for every module
   alike, and the static audit
   holds each spec to the schema. Pin it with no engine: `module::round_trip_day` returns
   the state from its own rows' text (`emitted/cells.rs` and `dup_cells/mod.rs` each carry one). A
   stateless, mergeable module that re-derives whole per window (coverage, the roster) needs
   none of this.

## The admission criterion

A fact earns a module only if it is latent in a per-identity sequence — one of three shapes:

1. correlating one identity's events across time (residency = store→evict gap; duplication =
   one identity in N places at once; reuse = a later event referencing an earlier one);
2. recovering signal from a partial or censored stream (coverage, the banded model);
3. reconstructing state the source never kept (open intervals, the frontier).

If the source could emit it as a gauge or a counter, the analyzer adds nothing — that is
Prometheus's job. This is what keeps the module set from sprawling into a general telemetry
pipeline; a module that fails it is refused at review, not at runtime.

## Execution time and recovery

Use `Window::new` or `Window::whole_days`; ranges are finite and half-open. Fractional
milliseconds remain significant. Whole carrier reads use `InputSelection::WholeBatches`,
never a fabricated timestamp range.

A day module runs over a settled day: one every edge (classified, emitted, covered) has passed
by the settle delay. A module's output is stated from the first day that settled while it was
enabled through the last settled day (`availability()`'s `stated_from_day` and
`stated_through_day`); a day settled before it was enabled is never stated by it, and a disabled
module's span ends. A fresh derivation (`reset-derived`) is the only backfill.

Declare `INITIALIZATION`. `OutputWindow` requires only the
output day's input; `ExaminedHistory` needs the examined history before the day, which the input
log must still hold, or the day fails with `UnavailableHistory`, never an empty success.

Copy the separate-crate example and its acceptance test to prove registration, persistence, a failed write, replacement
executor recovery and availability for a new module. Keep a separate pure arithmetic pin
and compare equivalent output intervals across whole, incremental, late and replay paths.
Run `scripts/preflight.sh`, `scripts/test.sh engine module-example` and the affected engine suites; measure a representative
streaming workload when adapter changes could alter event materialization.
