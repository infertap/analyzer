#!/usr/bin/env bash
# Install the InferTap analyzer on this host.
#
# **This is the systemd unit's own header, made executable.** That header already carried the
# correct sequence — user creation, binary and engine placement, `bucket.env` at 0600, and
# `preflight` as a gate before `systemctl enable`. It was a comment somebody retyped, which is
# where install mistakes come from.
#
# Run it from inside an unpacked release bundle:
#
# A literal version here would be wrong one release after it was written, and this is a block a
# customer copy-pastes.
#
#   tar xzf infertap-analyzer-*-x86_64-linux.tar.gz
#   cd infertap-analyzer-*-x86_64-linux
#   sudo ./install.sh --fleet-id prod --bucket-url https://s3.corp/records/
#
# It installs and validates. It does NOT start the service: the last step is yours, after
# preflight has told you the deployment can work.
set -euo pipefail

BIN_DIR=/usr/local/bin
LIB_DIR=/usr/local/lib/infertap-analyzer
ETC_DIR=/etc/infertap-analyzer
STATE_DIR=/var/lib/infertap-analyzer
UNIT=/etc/systemd/system/infertap-analyzer.service
USER_NAME=infertap-analyzer

fleet_id=""
bucket_url=""
bucket_prefix=""

usage() {
    cat >&2 <<'USAGE'
usage: install.sh --fleet-id <ID> --bucket-url <URL> [--bucket-prefix <P>]

  --fleet-id      what every figure this analyzer emits is keyed by. One analyzer, one fleet.
  --bucket-url    your S3-API object store, holding the tap's sealed segments. We never ship
                  one; "no object store" is not a supported configuration.
  --bucket-prefix key prefix within that bucket: the destination your taps ship to, ending in
                  the stream's component kv/ (the tap's INFERTAP_DEST ends in kv/; so does this).

Every flag of `run` can also come from a TOML file (--config, or INFERTAP_CONFIG naming it;
keys are the flags without dashes, with underscores) or the environment (INFERTAP_<FLAG>);
a flag beats the environment beats the file.

Credentials are NOT arguments. Put them in /etc/infertap-analyzer/bucket.env, which this
script creates with mode 0600 — an argument list is visible to every user on the box via `ps`,
a process environment is not.

They can also ride THIS SCRIPT'S environment, which finishes the install in one step rather
than two, because preflight cannot pass without them:

  INFERTAP_S3_ACCESS_KEY_ID=... INFERTAP_S3_SECRET_ACCESS_KEY=... sudo -E ./install.sh ...
USAGE
    exit 2
}

while [ $# -gt 0 ]; do
    case "$1" in
        --fleet-id) fleet_id=${2:?}; shift 2 ;;
        --bucket-url) bucket_url=${2:?}; shift 2 ;;
        --bucket-prefix) bucket_prefix=${2:?}; shift 2 ;;
        -h|--help) usage ;;
        *) echo "install.sh: unknown argument $1" >&2; usage ;;
    esac
done
[ -n "$fleet_id" ] && [ -n "$bucket_url" ] || usage
[ "$(id -u)" -eq 0 ] || { echo "install.sh: must run as root" >&2; exit 1; }

here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

# **The engine is verified against the hash we shipped beside it, not trusted for being here.**
# A bundle whose tarball was replaced in transit is exactly what a checksum beside it is for, and
# the customer is about to run this binary as a service.
echo "==> verifying the pinned engine"
sha512sum -c CLICKHOUSE_TGZ_SHA512

echo "==> installing"
install -d -m 0755 "$LIB_DIR" "$ETC_DIR"
install -m 0755 infertap-analyzer "$BIN_DIR/infertap-analyzer"

pin=$(cat CLICKHOUSE_VERSION)
tar xzf "clickhouse-common-static-${pin}-amd64.tgz"
install -m 0755 "clickhouse-common-static-${pin}/usr/bin/clickhouse" "$LIB_DIR/clickhouse"
rm -rf "clickhouse-common-static-${pin}"

install -m 0644 LICENSE THIRD-PARTY-NOTICES "$LIB_DIR/"
install -m 0644 systemd/infertap-analyzer.service "$UNIT"

# A static system user rather than DynamicUser: the state directory outlives any single start
# and the customer's own tooling reads what lands in it, which a rotating uid makes unownable.
if ! getent passwd "$USER_NAME" >/dev/null; then
    groupadd --system "$USER_NAME"
    useradd --system --gid "$USER_NAME" --home-dir "$STATE_DIR" \
            --shell /usr/sbin/nologin "$USER_NAME"
fi
install -d -o "$USER_NAME" -g "$USER_NAME" -m 0750 "$STATE_DIR" "$STATE_DIR/out"

# Created owner-only if absent, never overwritten: a re-install must not silently replace
# credentials that are already working.
#
# **Credentials may ride THIS SCRIPT'S environment**, which is what makes a first install one
# step instead of two:
#
#   INFERTAP_S3_ACCESS_KEY_ID=… INFERTAP_S3_SECRET_ACCESS_KEY=… sudo -E ./install.sh …
#
# Environment, never arguments — the same law the analyzer's own `bucket_creds` follows, for the
# same reason: `ps` shows an argument list to every user on the box.
# **"Never overwrite" means never overwrite REAL credentials — not never fill in a blank this
# script wrote itself.** The first version tested `[ ! -f ]`, which looked right and was not: a run
# with no credentials creates the file EMPTY, and from then on every later run — including one
# supplying credentials in the environment — skipped this block and refused at the check below.
# The one-step path became unreachable the moment somebody tried the zero-step path first, which
# is the order a person actually does things in. Found on the second clean-box install.
existing_id=""
if [ -f "$ETC_DIR/bucket.env" ]; then
    existing_id=$(sed -n 's/^INFERTAP_S3_ACCESS_KEY_ID=//p' "$ETC_DIR/bucket.env" | head -1)
fi

if [ -z "$existing_id" ]; then
    # umask in a subshell, so the file is never briefly world-readable between create and chmod.
    ( umask 077
      cat > "$ETC_DIR/bucket.env" <<ENV
# Bucket credentials. Read by the analyzer's environment, never passed as arguments.
INFERTAP_S3_ACCESS_KEY_ID=${INFERTAP_S3_ACCESS_KEY_ID:-}
INFERTAP_S3_SECRET_ACCESS_KEY=${INFERTAP_S3_SECRET_ACCESS_KEY:-}
ENV
    )
    chown "$USER_NAME:$USER_NAME" "$ETC_DIR/bucket.env"
    chmod 0600 "$ETC_DIR/bucket.env"
    if [ -n "${INFERTAP_S3_ACCESS_KEY_ID:-}" ]; then
        echo "    wrote $ETC_DIR/bucket.env from this script's environment"
    else
        echo "    created $ETC_DIR/bucket.env — empty, see below"
    fi
else
    echo "    $ETC_DIR/bucket.env already carries credentials — left untouched"
fi

# The unit ships with CHANGEME placeholders because a unit file with a plausible default is a
# unit file somebody starts without reading. Fill them from what was passed here.
sed -i \
    -e "s|--bucket-url https://CHANGEME/records-bucket/|--bucket-url ${bucket_url}|" \
    -e "s|--fleet-id CHANGEME|--fleet-id ${fleet_id}|" \
    "$UNIT"
if [ -n "$bucket_prefix" ]; then
    sed -i -e "s|--bucket-prefix CHANGEME/|--bucket-prefix ${bucket_prefix}|" "$UNIT"
else
    sed -i -e "/--bucket-prefix CHANGEME\//d" "$UNIT"
fi
systemctl daemon-reload

# **Preflight is a GATE, not a suggestion.** It validates exactly what the unit assumes —
# directory permissions, the engine child's health, clock sanity, disk headroom — and every one
# of those failures is cheapest to hit now rather than after a pilot has been running for a day.
# **The credentials must be handed to preflight EXPLICITLY, and this is where the first real
# install failed.** `bucket.env` is read by systemd's `EnvironmentFile` at SERVICE START —
# nothing reads it here — and `sudo` resets the environment anyway. So preflight ran with no
# credentials at all and its bucket probe returned an S3 403, which reads like a bucket-policy
# problem when the actual cause is a file this script had just created empty three lines earlier.
set -a
# shellcheck disable=SC1091
. "$ETC_DIR/bucket.env"
set +a

# **An absent credential is a CHOICE.** The analyzer asks for the AWS provider chain when no
# static pair is configured — instance profile via IMDS, IRSA, AssumeRole, SSO. That is the
# posture a security review asks for, and on an EC2 instance with a role attached it is the
# NORMAL one; the Helm path behaves the same way.
#
# So an absent credential is announced rather than refused, and **preflight decides**, because
# preflight is the thing that actually tests it — it writes a probe object and
# reads it back. If the instance has a usable role, that passes. If it does not, preflight refuses
# below with its own diagnosis instead of this script guessing at one.
if [ -z "${INFERTAP_S3_ACCESS_KEY_ID:-}" ] || [ -z "${INFERTAP_S3_SECRET_ACCESS_KEY:-}" ]; then
    cat <<MSG

==> no static credentials configured — using this host's AWS identity.

$ETC_DIR/bucket.env carries no credential, so the analyzer will resolve one from the
environment it runs in: an EC2 instance profile, IRSA, AssumeRole or SSO. Nothing is
written to disk, which is the stronger posture where the host has a role.

Preflight below writes a probe object and reads it back, so it is the real test of
whether that identity can reach your bucket. If this host has NO role, preflight will
say so, and you can supply a static pair instead:

  sudoedit $ETC_DIR/bucket.env      # then re-run this exact command

or in one step:

  INFERTAP_S3_ACCESS_KEY_ID=… INFERTAP_S3_SECRET_ACCESS_KEY=… sudo -E ./install.sh …

MSG
fi

echo "==> preflight"
# `--preserve-env` rather than `env VAR=…`, which would put the secret on an argument list.
if ! sudo -u "$USER_NAME" \
        --preserve-env=INFERTAP_S3_ACCESS_KEY_ID,INFERTAP_S3_SECRET_ACCESS_KEY \
        "$BIN_DIR/infertap-analyzer" preflight \
        --data-dir "$STATE_DIR" --out "$STATE_DIR/out" \
        --bucket-url "$bucket_url" \
        ${bucket_prefix:+--bucket-prefix "$bucket_prefix"} \
        --clickhouse "$LIB_DIR/clickhouse"; then
    echo >&2
    echo "install.sh: preflight REFUSED. The service is installed but not enabled — fix what" >&2
    echo "it named, then re-run preflight before starting." >&2
    exit 1
fi

cat <<DONE

==> installed, and preflight passed.

Start it, then read the token it issues on first run:

  systemctl enable --now infertap-analyzer
  cat $STATE_DIR/initial-token

Reach the console from your workstation without opening a port:

  ssh -L 8088:127.0.0.1:8088 <your-analyzer-host>

then browse http://localhost:8088 and paste the token.

DONE
