# infertap-analyzer

The analyzer for the InferTap record stream: the aggregator-side half of the system —
ingestion, storage, the executable interpretation contract, the join inventory, and the console
that serves it.

**Installing it?** [`docs/quickstart.md`](docs/quickstart.md) — artifact to dashboard, on
Kubernetes or a VM. That document also ships as the release bundle's own README.

**Working on it?** [`CONTRIBUTING.md`](CONTRIBUTING.md) is the way in: build, the engine, the
test tiers, the gates. [`CLAUDE.md`](CLAUDE.md) carries the two gates: `scripts/preflight.sh` while
you work, `scripts/prepush.sh` before every push, both judged by exit code.

## What is where

| | |
|---|---|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | the map: the crates, the library seam, the fence, the grain ladder, where to start reading |
| [`GLOSSARY.md`](GLOSSARY.md) | the vocabulary: the fence, the grain ladder, tick, edges, lane, frontier, ring, residual |
| [`docs/read-api.md`](docs/read-api.md) | the versioned seam between the analyzer and the console |
| [`docs/classification-semantics.md`](docs/classification-semantics.md) | what every class means, and the rules that decide it |
| [`docs/conformance.md`](docs/conformance.md) | the query inventory and every analytic constant's derivation |
| [`charts/`](charts/) · [`packaging/`](packaging/) | how it reaches a customer |

Nine library crates and one binary: `analysis`, `segments`, `objectstore`, `lake`, `ingest`,
`pipeline`, `serving-clickhouse`, `api`, `daemon` and `cli`; beside them `module-example`, a
worked derivation module, and `harness`, the test fixture the suites share (`ARCHITECTURE.md`
maps them).
The console lives in its own repository and is compiled
into this binary at build time, consumed by pinned version and content hash
([`console/PIN.toml`](console/PIN.toml)).

## The contract

The public contract this component consumes — record schema, interpretation rules, conformance
corpora — is the [inference-events](https://github.com/infertap/inference-events) spec repo,
vendored as a submodule and normative here. Open contract, open implementation.

## Licence

GNU Affero General Public License, version 3 only (`AGPL-3.0-only`) — see [`LICENSE`](LICENSE).
Contributions are accepted under the [`CLA`](CLA.md). Third-party components are
enumerated in [`THIRD-PARTY-NOTICES`](THIRD-PARTY-NOTICES), which travels with every release.
