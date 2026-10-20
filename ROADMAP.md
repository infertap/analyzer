# Roadmap

Where the analyzer is going, at the altitude a contributor can plan against. Phases, not
dates; a phase is done when its work is on `main` behind the pins. None of this is a
commitment to a schedule, and a phase can be reordered by a recorded ruling
(`GOVERNANCE.md`).

## Done

- **The grain ladder.** Every figure is derived from the record stream at the grain it is
  read at — minute, hour, day — with the per-identity fold as the one place meaning is
  decided (the fence). The pass works per stripe under a memory budget; the lake spine
  holds a day's arrivals in key order; the lifecycle bank makes the anchoring read cheap.
- **The library shape.** The crates read as a library: one concern per file, curated
  public surfaces pinned as baselines, typed errors, `ARCHITECTURE.md` and `GLOSSARY.md`.
- **Contributor readiness.** A hermetic `cargo test`, one command for the engine, tiered
  tests with actionable failures, a parallel CI, the suite checked a month ahead.
- **Licensing.** `AGPL-3.0-only` with a CLA; the community files.

## Next

- **The Grafana surface.** A Prometheus export of the fleet's figures and a dashboard, so
  the first day with the analyzer is a panel in a tool the operator already runs, not a
  new console to learn.
- **A boring install.** The OCI bundle with one binary and the engine, the quickstart as
  the bundle's own README, the chart as the Kubernetes path — installable on a clean
  machine without a conversation.
- **The public repository.** A fresh, sanitised public cut of this repository: the public
  documents, a clean initial commit, the internal design log left behind.

## Later

- **Derivation modules.** The per-identity facts become pluggable modules behind an
  in-tree trait — the fence as the plugin contract — so a new fact family is a module,
  not a change to the fold.
- **Cells.** One analyzer serving many fleets: the cell as the unit of deployment, with
  the same figures and the same pins.
- **The lake as the product's core.** A/B replays over stored runs, comparison across
  fleets and versions, and operation from the lake rather than from a single box.

## Not planned

- Per-crate publication to a registry. The crates are one workspace and ship as one
  binary; `publish = false` stays.
- A second storage engine. The pinned ClickHouse is the engine; the analytical path is
  measured against it and stays there.
