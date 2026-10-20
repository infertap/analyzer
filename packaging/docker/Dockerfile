# The analyzer as a container image.
#
# **Layer order is the rule, not a preference** (distribution §7). The engine changes only when
# its pin moves; our binary changes every release. So the engine is copied FIRST and ours LAST,
# and a release moves only the small layer. Inverting these two COPY lines silently multiplies
# every upgrade and nothing would fail to tell you.
#
# **Measured 2026-08-15, because the figures I had were wrong in both directions:**
#   ClickHouse, unpacked      754 MB   (the 212 MB in distribution §1 is the COMPRESSED tgz)
#   infertap-analyzer          15.4 MB (console included; NOT the ~2 MB §7 quotes — that is the
#                                       TAP's binary, and I had misapplied it to this one)
#   whole image, on disk      ~1.17 GB
#
# Distribution §7's "500 nodes × 2 MB per rollout" arithmetic is the TAP's too: that ships as a
# sidecar or DaemonSet across a GPU fleet. **The analyzer is one replica per fleet**, so its
# rollout traffic is one binary and the layering rule earns its place here on build-cache time
# and registry storage rather than on pull volume.
#
# Not stripped, deliberately. `strip = true` would take a few megabytes off the last layer and
# take the symbol table with it — on an unattended appliance whose crash evidence arrives through
# a `diag` bundle rather than a debugger, readable backtraces are worth more than the megabytes.
# The lever is here and its cost is stated, should rollout size ever start to matter.
#
# **Both stages are bookworm, deliberately.** A binary built against a newer glibc than it runs on
# fails at load with an unresolved symbol, which is a confusing way to learn that two base images
# drifted. Matching them removes the question.
#
# TODO before the first published image: pin both bases by digest, the way `.github/workflows`
# SHA-pins its actions. Tags move; a release that cannot be rebuilt from its own Dockerfile is not
# reproducible. Left as a tag here because a digest invented rather than resolved is worse than an
# honest tag.
#
# Build (the console must be staged first — the build REFUSES without it):
#   scripts/stage-console.sh ~/workspace/console
#   docker build -f packaging/docker/Dockerfile -t infertap-analyzer:1.0.0 .

# ---------------------------------------------------------------------------------------------
# The engine, fetched and hash-verified exactly as CI does it. Its own stage so the download is
# cached against CLICKHOUSE_VERSION rather than against our source tree.
# ---------------------------------------------------------------------------------------------
FROM debian:bookworm-slim AS engine

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /engine
COPY CLICKHOUSE_VERSION CLICKHOUSE_TGZ_SHA512 ./

# The same three lines as `.github/workflows/ci.yml`'s fetch step, and they must stay the same:
# a second way to obtain the engine is a second thing that can obtain a different engine.
RUN set -eux; \
    PIN="$(cat CLICKHOUSE_VERSION)"; \
    curl -sSfL -o "clickhouse-common-static-${PIN}-amd64.tgz" \
      "https://packages.clickhouse.com/tgz/lts/clickhouse-common-static-${PIN}-amd64.tgz"; \
    sha512sum -c CLICKHOUSE_TGZ_SHA512; \
    tar xzf "clickhouse-common-static-${PIN}-amd64.tgz"; \
    mv "clickhouse-common-static-${PIN}/usr/bin/clickhouse" /engine/clickhouse; \
    /engine/clickhouse --version

# ---------------------------------------------------------------------------------------------
# The analyzer. `rust-toolchain.toml` pins 1.97.0, so the tag here only has to be new enough to
# honour it — the toolchain file is what actually decides.
# ---------------------------------------------------------------------------------------------
FROM rust:1.97-slim-bookworm AS build

WORKDIR /src
COPY . .

# `librocksdb-sys` builds RocksDB (C++20) and binds its C API through libclang; cmake builds
# `aws-lc-sys`, make the jemalloc allocator. Bookworm's GCC 12 compiles C++20, and none of these
# reach the runtime stage.
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake make g++ clang libclang-dev \
 && rm -rf /var/lib/apt/lists/*

# **The release image cannot be built without a console.** `build.rs` generates an empty asset
# table when nothing is staged, which is correct for a clean checkout and wrong for an artifact —
# it would produce a binary that answers `console_not_configured` on every page, and look like a
# successful build doing it.
ENV INFERTAP_REQUIRE_CONSOLE=1
RUN cargo build --release -p infertap-analyzer \
 && /src/target/release/infertap-analyzer version

# ---------------------------------------------------------------------------------------------
# Runtime.
# ---------------------------------------------------------------------------------------------
FROM debian:bookworm-slim

# `ca-certificates` for the engine's S3 client reaching the customer's bucket over TLS; `tzdata`
# because every day boundary in this product is UTC and a container without a zone database
# resolves them by guessing.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 infertap \
 && useradd --system --uid 10001 --gid infertap --home-dir /var/lib/infertap-analyzer \
            --shell /usr/sbin/nologin infertap \
 && mkdir -p /var/lib/infertap-analyzer \
 && chown infertap:infertap /var/lib/infertap-analyzer \
 && chmod 0750 /var/lib/infertap-analyzer

# THE STABLE LAYER. ~212 MB, and it moves only when CLICKHOUSE_VERSION does.
COPY --from=engine /engine/clickhouse /usr/local/lib/infertap-analyzer/clickhouse

# What the analyzer is, legally, travelling with the thing it describes.
COPY LICENSE THIRD-PARTY-NOTICES /usr/local/share/infertap-analyzer/

COPY packaging/docker/entrypoint.sh /usr/local/bin/entrypoint.sh

# THE LAST LAYER, and the only one a release moves. ~2 MB of binary and 632 KB of console.
COPY --from=build /src/target/release/infertap-analyzer /usr/local/bin/infertap-analyzer

# The state directory is a mount point in every real deployment; declaring it means a `docker run`
# with no volume still gets a writable one rather than failing at the first insert.
VOLUME ["/var/lib/infertap-analyzer"]

# 8088 the console and read API, 9363 `/metrics`. Both bind loopback in the binary by default,
# which inside a network namespace means unreachable — the entrypoint is what corrects that.
EXPOSE 8088 9363

USER infertap:infertap
WORKDIR /var/lib/infertap-analyzer

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["run"]
