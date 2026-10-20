# InferTap analyzer — quickstart

From an artifact to the dashboard. Ten minutes on Kubernetes, fifteen on a VM.

## What you are installing

The analyzer reads the sealed segments your InferTap taps ship to object storage, derives what
your KV cache actually did, and serves a console that shows it. It runs unattended: one process,
one fleet, its own embedded storage engine. Nothing it collects leaves your network, and it never
contacts us.

You need three things before you start:

- **An S3-API object store** holding segments from at least one tap. We do not ship one, and
  running without one is not a supported configuration.
- **Credentials for that store, with read AND write on your prefix.** Write is not optional, and
  it is not for your telemetry: install-time preflight writes one probe object and reads it back,
  because a transport that cannot round-trip cannot carry records. Nothing it writes is ever read
  by us.
- **A Kubernetes cluster or a Linux VM.** Either is a first-class path; pick the one your team
  already operates. **2 vCPU and 8 GB of RAM is the smallest we have run it on**, with 60 GB of
  disk; that is a tested floor rather than a recommendation, and how much you actually need
  depends on your fleet's record volume. The analyzer tells you whether it is keeping up — see
  "when something is not right" below — so you can start small and watch.
- **[cosign](https://github.com/sigstore/cosign) on the machine that unpacks the release**, to
  check our signature before you run anything. The VM path below starts with that check.

## Kubernetes

**1. Mirror the release into your own registry.** The image and the chart are published to the
GitHub container registry. The chart has no default registry on purpose: nothing about running
this product should depend on reaching ours.

```sh
oras copy ghcr.io/infertap/analyzer:1.0.10 <your-registry>/analyzer:1.0.10
oras copy ghcr.io/infertap/charts/infertap-analyzer:1.0.10 <your-registry>/charts/infertap-analyzer:1.0.10
```

**2. Decide how the analyzer proves who it is.** Two supported answers, and the first is stronger
where you can have it.

*Your cluster's own identity — no secret anywhere.* Annotate the service account for IRSA (or
whatever your cloud calls it) and configure no credential at all. The analyzer asks the AWS
provider chain, which reaches an instance profile, IRSA, `AssumeRole` or SSO. Skip to step 3 and
leave `bucket.existingSecret` unset.

```sh
helm install ... --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=<role-arn>
```

*A static key pair.* Put it in a Secret; it never belongs in a values file.

```sh
kubectl create secret generic infertap-bucket \
  --from-literal=INFERTAP_S3_ACCESS_KEY_ID=... \
  --from-literal=INFERTAP_S3_SECRET_ACCESS_KEY=...
```

**3. Install.**

```sh
helm install infertap-analyzer oci://<your-registry>/charts/infertap-analyzer --version 1.0.10 \
  --set image.registry=<your-registry> \
  --set fleet.id=prod \
  --set bucket.url=https://s3.example.internal/records/ \
  --set bucket.prefix=infertap/prod-us-east/ \
  --set bucket.existingSecret=infertap-bucket   # omit entirely to use the cluster's identity
```

It refuses rather than guesses if any of those are missing. Everything else has a default; put
your real settings in a values file you keep in git.

**4. Reach the console.**

```sh
kubectl port-forward svc/infertap-analyzer 8088:8088
```

No Ingress, no certificate, no open port. If you would rather publish it, set `ingress.enabled`
and a host — the chart turns on the session cookie's `Secure` flag for you when you do.

**5. Read the token and sign in.**

```sh
kubectl exec infertap-analyzer-0 -- cat /var/lib/infertap-analyzer/initial-token
```

Open <http://localhost:8088> and paste it. Nothing is printed to the pod log, so this file is
where the credential lives.

## A Linux VM

**1. Verify what you downloaded.** By identity, with cosign's own trust root — offline, and
with no key to obtain or pin.

This needs `cosign`, which a clean machine does not have. It is one binary, and it is worth checking its own digest before you trust it to
check ours:

```sh
curl -sSfLO https://github.com/sigstore/cosign/releases/download/v3.1.3/cosign-linux-amd64
echo "4629c757b7618056f8ddd7e2625ae9fdd94c0372a65049520bc7d9df9efc7f71  cosign-linux-amd64" \
  | sha256sum -c -
sudo install -m 0755 cosign-linux-amd64 /usr/local/bin/cosign
```

That is the same version and the same digest our release pipeline pins, so you are running the
tool we ran. On a machine with no outbound access, fetch it once somewhere that has some and copy
it across with the bundle — nothing below reaches the network.

```sh
cosign verify-blob --bundle infertap-analyzer-v1.0.10-x86_64-linux.tar.gz.bundle \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity https://github.com/infertap/analyzer/.github/workflows/release.yml@refs/tags/v1.0.10 \
  infertap-analyzer-v1.0.10-x86_64-linux.tar.gz
```

It prints `Verified OK`. Anything else means stop.

Releases are signed keyless with Sigstore, exactly as the tap's are: the release workflow
exchanges its GitHub OIDC token for a short-lived certificate, signs, and records the
signature in the Rekor transparency log. What you check is the identity in the certificate —
this repository's release workflow, at the tag you are installing, issued by GitHub. The
`.bundle` beside each artifact carries the certificate and the log entry, so the check works
offline with the trust root cosign ships; the pipeline runs this same command on every
release before it publishes.

The container image carries the same ceremony, on its digest. Verify it before you run it:

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity https://github.com/infertap/analyzer/.github/workflows/release.yml@refs/tags/v1.0.10 \
  ghcr.io/infertap/analyzer:1.0.10
```

**2. Install.**

*If this host has an AWS role attached* — an EC2 instance profile is the usual case — configure no
credential at all. The analyzer asks the provider chain, and nothing is written to disk:

```sh
tar xzf infertap-analyzer-v1.0.10-x86_64-linux.tar.gz
cd infertap-analyzer-v1.0.10-x86_64-linux
sudo ./install.sh --fleet-id prod --bucket-url https://s3.example.internal/records/ \
  --bucket-prefix infertap/prod-us-east/kv/
```

**The prefix is its own flag, never a path on the URL.** The URL names the endpoint and the
bucket; anything after the bucket is silently ignored by the storage layer, not refused — a
clean-room install pointed its prefix through the URL and preflight probed the bucket root,
outside the credential's scope. `--bucket-prefix` (and `bucket.prefix` on Kubernetes) is where
the key prefix goes, and it is the destination your taps ship to, **ending in the stream's own
component, `kv/`** — the tap's `INFERTAP_DEST` ends in `kv/`, and so does this prefix (the
delivery contract's `stream` component; one prefix per stream). Under it the shipper writes the
dated layout, `<prefix>YYYY-MM-DD/HH/<node>/seg-*.parquet`, which the analyzer walks by date
and hour. Everything the analyzer writes into the bucket lives under `<prefix>_analyzer/`
(the preflight probe under `_analyzer/preflight/`, deploy annotations under
`_analyzer/annotations/`, the derived classified rows under `_analyzer/derived/`), a root
no hostname or date can spell, so the documented
least-privilege credential — read and write on your prefix, nothing wider — passes it.

**A file and the environment, beneath the flags.** Every flag `run` and `preflight` take can
also come from a TOML file (`--config /etc/infertap-analyzer/analyzer.toml`, or
`INFERTAP_CONFIG` naming it) whose keys are the flags without the dashes and with underscores,
or from the environment as `INFERTAP_` plus the flag upper-cased — the tap's layering, mirrored.
A flag beats the environment, the environment beats the file, key by key:

```toml
fleet_id = "prod"
bucket_url = "https://s3.example.internal/records/"
bucket_prefix = "infertap/prod-us-east/kv/"
disable_module = ["duplication"]
log_format = "json"
```

Credentials are not flags and never go in the file: `INFERTAP_S3_ACCESS_KEY_ID` and its
secret keep their own reader, as do the daemon's own knobs, and an `INFERTAP_*` name that is
not a flag is left alone rather than refused. `--log-level` (default `info`) and
`--log-format` (`text`, or `json` for one object per line) take the same layers; under systemd
the text form drops its timestamp for the journal's own and carries the `<N>` priority prefix
the journal filters on.

*If it does not*, supply a static pair. It goes in the environment, never in the argument list —
`ps` shows arguments to every user on the box, and `-E` is what carries it through `sudo`:

```sh
INFERTAP_S3_ACCESS_KEY_ID=... INFERTAP_S3_SECRET_ACCESS_KEY=... \
  sudo -E ./install.sh --fleet-id prod --bucket-url https://s3.example.internal/records/ \
    --bucket-prefix infertap/prod-us-east/kv/
```

The installer verifies the bundled engine against its checksum, creates a system user, writes any
credentials you supplied to `/etc/infertap-analyzer/bucket.env` at mode 0600, writes the unit, and
runs preflight. **Preflight is the real test of either answer**: it writes a probe object to your
bucket and reads it back, so a host whose role cannot reach the bucket fails here with a named
reason rather than at three in the morning. If it refuses, the service is installed but not
started — fix what it named, then re-run.

Prefer not to put a key on a command line at all? Run `install.sh` without one, then
`sudoedit /etc/infertap-analyzer/bucket.env` and re-run the same command. It will not overwrite
credentials that already exist.

**3. Start it.**

```sh
sudo systemctl enable --now infertap-analyzer
```

**4. Reach the console and sign in.**

```sh
cat /var/lib/infertap-analyzer/initial-token     # on the VM
ssh -L 8088:127.0.0.1:8088 your-analyzer-host    # from your workstation
```

Open <http://localhost:8088> and paste the token. The analyzer listens on loopback only, so the
tunnel is the whole access story — nothing is exposed to your network unless you choose to
expose it.

## Signing in, after the first time

The token is a standing credential; the *session* is what expires. Let your browser save the
token on first use and every later sign-in is a click.

Rotate it whenever you need to. Rotation also ends every session it opened:

```sh
infertap-analyzer token rotate --data-dir /var/lib/infertap-analyzer --id tok-...
```

## What you will see

The first ingest happens within a minute and the first pass within the hour. Until a pass has
run, the console says it has nothing yet rather than showing zeros — a figure with no
measurement behind it is not something this product will draw.

Once it has passed, start on the document view: it states what your cache is doing, with the
coverage each figure was measured at. Every number is clickable down to the evidence it came
from.

## When something is not right

**The console loads but signing in never sticks.** Almost always `--api-cookie-secure on` while
reaching the console over plain HTTP. Safari discards a `Secure` cookie on a non-HTTPS origin
without saying so. Use it only when TLS terminates in front of the analyzer.

**Ingest keeps failing.** Check the bucket URL and the credentials in `bucket.env` or your
Secret. The log names the S3 error verbatim.

**On Kubernetes, the pod says `Init:Error`.** That is preflight refusing, which is the same gate
the VM installer runs and means the deployment cannot work as configured. It says why:

```sh
kubectl logs infertap-analyzer-0 -c preflight
```

Almost always the bucket URL or the Secret. Nothing starts until it passes, on purpose.

**On Kubernetes, the pod stays `Pending`.** That is the scheduler, not us — the node cannot fit the
request. `kubectl describe pod infertap-analyzer-0` names it on the last line. Lower
`resources.requests`, or use a bigger node. Note that a `helm upgrade` alone will not fix it: a
StatefulSet declines to roll a pod that never became Ready, so delete the pod after correcting the
values.

**`within_envelope` is warning.** Ingest is using more than 30% of this analyzer's cycle,
sustained. It is still keeping up — what is shrinking is the margin for a burst. One analyzer
serves one fleet; split the fleet across analyzers, one per cluster.

**Everything looks healthy but the numbers seem low.** Check coverage on the figure. The
analyzer reports what it could observe, and says how much that was, rather than extrapolating to
what it could not.

**You just deployed a fix and want to see it in the numbers.** Classified figures settle at
the ingest cadence: every cycle that commits segments runs a tick that classifies them (one
minute by default), and the stamp's `tick` edges say how far it reached. `POST /v1/analyze` on
the read API runs a cycle within a second. Both are cheap: a tick reads only what arrived.

## Where to look next

- `infertap-analyzer version` — what is in the binary, including the console it serves.
- `/metrics` on port 9363 — Prometheus format, unauthenticated, loopback by default. Beside
  the analyzer's own health, it carries the fleet's stamped figures under `infertap_fleet_*`,
  refreshed at each pass, so your existing Alertmanager can watch the numbers the console
  states: `infertap_fleet_recompute_fraction > 0.05` pages on the headline,
  `infertap_fleet_segments_missing_unexplained > 0` on undeclared telemetry loss, and
  `time() - infertap_fleet_frontier_timestamp_seconds` on analysis freshness — the figures
  are pass-settled, so freshness is judged against the frontier, never the scrape clock.
- `infertap-analyzer diag --from /var/lib/infertap-analyzer/out` — a support bundle carrying
  health artifacts and versions, and zero records.
