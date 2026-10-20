# Soak runbook

The standing 2–4 week deployment that turns declared numbers into measured ones. The
accumulated coverage and self-health record is the confidence artifact. The analyzer runs on
ONE host; the traffic needs no GPUs when it is synthetic, and the two tasks that need a real
engine are called out in §6.

## 1. What this run is

- One analyzer daemon (`infertap-analyzer run`) ingesting continuously for 2–4 weeks.
- Taps shipping sealed segments to the bucket: a real fleet, or synthetic engine traffic
  through the real tap (the InferTap harness's load-test profile), at a controlled fleet
  scale that exercises all six classification classes, duplication and lifecycle
  accounting. A 500-GPU fleet at about 5 events/s per instance is **about 2.5K records/s**.
- Scheduled chaos (§5) and a measurement ledger (§4). The run is not a smoke test; if
  nothing is recorded, it did not happen.

## 2. Host and prerequisites

- Linux host, ≥8 cores, ≥32 GiB RAM, ≥500 GiB disk (the segment cache, the working state and
  serving at 2.5K records/s are the driver; watch the §4 capacity projection rather than
  trusting this estimate).
- The pinned ClickHouse binary (`.tools/clickhouse` or `CLICKHOUSE_BIN`).
- An S3-API bucket as the transport: S3, or a compatible store that keeps conditional writes
  (the release battery runs SeaweedFS).
- `infertap-analyzer preflight` MUST pass before the clock starts: directory permissions,
  engine child health, clock sanity, disk headroom, bucket reachability, and the bucket's
  refusal of conflicting conditional writes (`bucket_fences_writers`).
- `infertap-analyzer selftest` MUST pass on the installed binary: this deployment
  implements the contract, proven, before any number from it is trusted.

## 3. Bring-up

```sh
# the taps ship to s3://soak-records/<prefix><yyyy-mm-dd>/<hh>/...; a local endpoint rides env
AWS_ENDPOINT=http://127.0.0.1:8333 AWS_ALLOW_HTTP=true \
INFERTAP_S3_ACCESS_KEY_ID=... INFERTAP_S3_SECRET_ACCESS_KEY=... \
infertap-analyzer run \
  --bucket-url s3://soak-records/ \
  --fleet-id soak \
  --data-dir /srv/soak/db \
  --out /srv/soak/out \
  --clickhouse <pinned>
```

Run the daemon under `packaging/systemd/infertap-analyzer.service` — the shipped unit, not
a hand-written one, because two of its directives are load-bearing and the soak is where
that has to be proven. `Restart=always` with `StartLimitIntervalSec=0` is what the
in-process supervision cap assumes: the daemon gives up after bounded engine restarts and
exits on purpose, so a rate limit would let a kill drill end in a `failed` unit instead of a
recovery. `KillMode=mixed` is the second: the default signals the engine
child directly on `systemctl stop`, racing the daemon's ordered shutdown, so a drill would
be exercising crash recovery while claiming to exercise a clean stop.

Capture stdout/stderr to the journal — the run's log IS an artifact. Start the wall clock
only after preflight and selftest are green and the first hour of ingest shows a flat
backlog.

**The unit itself is an artifact under test.** Its hardening directives are unverified against
the pinned engine until a soak boots it. Before the clock starts: confirm the engine comes up
under `SystemCallFilter=@system-service` and `CapabilityBoundingSet=`, confirm `systemctl stop`
during a tick takes the ordered path inside `TimeoutStopSec`, and record the observed
worst-case stop duration against that value. A directive that has to be removed is a result,
recorded like any other.

## 4. The measurement ledger

One row per constant, appended to `soak-log.md` on the host as measured. The constants come
from `docs/conformance.md` §2; this run is that list's collection pass.

| what | how it is measured here |
|---|---|
| Ingest ceiling | raise the synthetic fleet size in steps during week 1 until ingest lags, note the knee; the declared envelope stays ≤30% of it |
| Recovery absolutes (≤15 min live, 72 h backlog healed ≤12 h) | the week-2 and week-3 kill drills (§5), timed from the journal and `health.json` |
| Catch-up real-time multiple (~8–10× target) | same drills: simulated span over wall clock |
| `LIFECYCLE_CLIP_MARGIN_MS` (7 d) | the heartbeat retention span actually observed |
| `δ_skew` presets (incl. the 1–5 ms PTP candidate) | observed inter-instance offsets; floor noted at batch-stamping granularity |
| `HULL_COMPACTION_BOUND` (32) | ledger entry counts during the crash-loop drill |
| Steady-state analyzer memory and CPU | daily `health.json` + OS sampling; publish p50/p99 over the run |

## 5. Chaos calendar

One drill per scheduled slot, never two at once (attribution dies when causes overlap), each
recorded in `soak-log.md` as: what, when, what the dashboards showed, time to green, and any
figure that moved that should not have.

| week | drill |
|---|---|
| 1 | baseline only: no chaos, measure the ceiling and steady state |
| 2 | `kill -9` the analyzer mid-ingest; then a 24 h simulated outage backlog fed after a planned stop |
| 2 | disk pressure: shrink free space under the data directory until the pressure ladder engages |
| 3 | bucket partition: make the bucket unreachable for hours; verify staleness and coverage tell the truth during and heal after |
| 3 | clock skew: step the host clock (or the synthetic traffic's stamps) and verify no verdict flips — the corpus clock family, live |
| 4 | torn deploy: replace the binary mid-run, a standby taking the lease; every upgrade is a restore drill |
| 4 | crash-loop hour: restart the daemon every few minutes; ledger sizes and hull compaction observed |

The failover drill (`chaos.rs`) runs in the release battery; the soak's week-4 torn deploy is
its full-scale rehearsal with real wall clock.

## 6. Standing tasks that ride the soak (need a real engine, not the rig)

- **Capture a real `AllBlocksCleared`**: the `cleared_all` fixture is constructed, not
  captured; one real capture converts the assumption into an observation and re-cuts
  the fixture. Needs a real vLLM build coaxed into a cache reset.
- **A real-capture sanity day**: one live vLLM + tap beside the rig, long enough to
  compare the synthetic behavior mix against reality and re-cut the payload-size figures
  labelled synthetic.

## 7. Daily check (five minutes)

`/metrics` or `health.json`: backlog flat, coverage where expected, disk projection not
alarming. Append one line to the log either way — "nothing" recorded daily is what makes the
final artifact credible.

## 8. Exit criteria and what changes afterward

The run ends when 2–4 weeks have accumulated with every §4 row filled, every §5 drill
recorded with recovery times, and green throughout. Then, in one sitting: update the
`docs/conformance.md` §2 constants from measurements, replace the declared recovery,
catch-up and envelope figures with measured ones, decide the δ_skew PTP preset, and re-derive
`HULL_COMPACTION_BOUND`. The soak log itself is retained as the confidence artifact.
