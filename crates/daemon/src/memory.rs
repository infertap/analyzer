// SPDX-License-Identifier: AGPL-3.0-only

//! How much memory this process may actually use.
//!
//! **The engine is told a ceiling and believes it.** `Daemon::start` sets the server's
//! `max_server_memory_bytes` from what this module reports, and `ClickHouse` then fills its caches
//! up to that figure. So a number that is too large is not a tuning miss — it is an instruction to
//! exceed a limit somebody else enforces with a kill.
//!
//! `/proc/meminfo` is not namespaced, so inside a container it reports the node's memory,
//! and a systemd unit with `MemoryMax=` has the same shape without a container. The ceiling
//! is therefore the **minimum of every limit found**: the host's `MemTotal`, and every cgroup
//! limit on the path `/proc/self/cgroup` names, v2 (`memory.max` and `memory.high`) and v1
//! (`memory.limit_in_bytes`). Each source reports a real limit, the smallest binds, and a
//! source that is absent or unreadable contributes nothing. cgroup v1's unlimited sentinel
//! needs no special case: it is a number larger than any real `MemTotal`, and the minimum
//! discards it for that reason alone.
//!
//! The engine's tracker measures the shared cgroup's total, so the ceiling it is handed is
//! a backstop on both processes' sum, not the engine's share.

use std::path::Path;

/// The default cgroup mount. Inside a container with its own cgroup namespace — Docker and
/// containerd's default, and how Kubernetes runs — the container's own limit is at this root.
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// The share of the box's ceiling the fold's working state may hold: its block cache, index
/// and filter blocks and memtables, one bound (`analyzer_lake::working`). The working state
/// holds its cache between ticks; the probes by hash read the filters and the hot blocks, not
/// the state, so a small share serves a state many times its size. Re-derive from a rig's
/// block cache hit rate and write stalls.
pub(crate) const WORKING_SHARE: f64 = 0.05;

/// One gibibyte, for the ceiling a human reads on the startup line.
pub(crate) const GIB: f64 = 1_073_741_824.0;

/// Which ceiling bound: a cgroup cap, or the host's `MemTotal`. Named on the startup line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A cgroup limit bound, which means this process is in a container or a unit with a cap.
    Cgroup,
    /// No cgroup limit was lower than the host's own memory.
    HostTotal,
}

/// The memory ceiling this process is subject to, and which source set it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ceiling {
    /// The ceiling in bytes.
    pub bytes: u64,
    /// Which limit bound.
    pub source: Source,
}

impl Ceiling {
    /// The ceiling the engine is told: [`MAX_SERVER_MEMORY_RATIO`] of the whole, a backstop
    /// on both processes' sum since the engine's tracker counts the whole cgroup.
    ///
    /// [`MAX_SERVER_MEMORY_RATIO`]: analyzer_serving_clickhouse::engine::server::MAX_SERVER_MEMORY_RATIO
    #[must_use]
    pub fn engine_share(self) -> u64 {
        self.share(analyzer_serving_clickhouse::engine::server::MAX_SERVER_MEMORY_RATIO)
    }

    /// The fold's working state's memory: the working share of the whole.
    #[must_use]
    pub fn working_state(self) -> usize {
        usize::try_from(self.share(WORKING_SHARE)).unwrap_or(usize::MAX)
    }

    /// The ceiling in gibibytes, for a sentence.
    #[must_use]
    pub fn gib(self) -> f64 {
        // Precision loss past 2^52 bytes is four petabytes away and would not change the sentence.
        #[allow(clippy::cast_precision_loss)]
        {
            self.bytes as f64 / GIB
        }
    }

    fn share(self, ratio: f64) -> u64 {
        // The ratio is in (0, 1], so the product is a non-negative fraction of a u64.
        #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
        #[allow(clippy::cast_sign_loss)]
        {
            (self.bytes as f64 * ratio) as u64
        }
    }
}

/// The memory ceiling this process is subject to, and which source set it.
///
/// `None` only when nothing could be read at all: off Linux, or a `/proc` this process
/// cannot see. The caller leaves the engine's own default in place rather than guessing.
#[must_use]
pub fn ceiling() -> Option<Ceiling> {
    let own = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    resolve(Path::new("/proc/meminfo"), Path::new(CGROUP_ROOT), &own)
}

fn resolve(meminfo: &Path, cgroup_root: &Path, own_cgroup: &str) -> Option<Ceiling> {
    let host = std::fs::read_to_string(meminfo)
        .ok()
        .and_then(|text| parse_meminfo_total(&text));
    let cgroup = cgroup_limit(cgroup_root, own_cgroup);
    let (bytes, source) = match (host, cgroup) {
        // The ordinary container case.
        (Some(host), Some(limit)) if limit < host => (limit, Source::Cgroup),
        (Some(host), _) => (host, Source::HostTotal),
        // No `/proc/meminfo` but a cgroup limit is still a real ceiling and still binding.
        (None, Some(limit)) => (limit, Source::Cgroup),
        (None, None) => return None,
    };
    Some(Ceiling { bytes, source })
}

/// The lowest cgroup limit that binds this process: the root's, and every ancestor's on
/// the path `/proc/self/cgroup` names, across v2 and v1.
///
/// Inside a container the process's own limit sits at the mount root, because the
/// container has its own cgroup namespace. Under a systemd unit cap there is no namespace:
/// the process reads `0::/system.slice/infertap-analyzer.service`, its limit lives at that
/// path under the mount, and the root says `max`. Limits are hierarchical and the smallest
/// ancestor binds, so every prefix of the path contributes a candidate. Both layouts are
/// tried rather than detected, because a file that does not exist reads as `None` for
/// free, and hybrid hosts mount both.
///
/// `memory.high` is a candidate beside `memory.max`: the kernel enforces the high mark by
/// stalling every allocating thread of the group rather than by a kill, so an engine
/// ceiling derived from the wall alone lets the group sit above the mark with nothing
/// reclaimable, and every allocation waits.
fn cgroup_limit(root: &Path, own_cgroup: &str) -> Option<u64> {
    let mut candidates: Vec<u64> = Vec::new();
    for dir in ancestor_dirs(root, v2_path(own_cgroup)) {
        for file in ["memory.max", "memory.high"] {
            if let Some(limit) = std::fs::read_to_string(dir.join(file))
                .ok()
                .and_then(|text| parse_cgroup_v2(&text))
            {
                candidates.push(limit);
            }
        }
    }
    for dir in ancestor_dirs(&root.join("memory"), v1_memory_path(own_cgroup)) {
        if let Some(limit) = std::fs::read_to_string(dir.join("memory.limit_in_bytes"))
            .ok()
            .and_then(|text| parse_cgroup_v1(&text))
        {
            candidates.push(limit);
        }
    }
    candidates.into_iter().min()
}

/// The v2 line of `/proc/self/cgroup`: `0::<path>`.
fn v2_path(own_cgroup: &str) -> Option<&str> {
    own_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(str::trim)
}

/// The v1 memory-controller line: `<n>:<controllers>:<path>`, where the comma-separated
/// controller list names `memory`.
fn v1_memory_path(own_cgroup: &str) -> Option<&str> {
    own_cgroup.lines().find_map(|line| {
        let mut parts = line.splitn(3, ':');
        let _hierarchy = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?.trim();
        controllers
            .split(',')
            .any(|c| c == "memory")
            .then_some(path)
    })
}

/// The mount root plus each ancestor of `path` down to the process's own group — every
/// level whose limit binds. An absent or empty path yields the root alone, which is the
/// container case.
fn ancestor_dirs(root: &Path, path: Option<&str>) -> Vec<std::path::PathBuf> {
    let mut dirs = vec![root.to_path_buf()];
    let mut current = root.to_path_buf();
    for segment in path
        .unwrap_or("")
        .split('/')
        .filter(|s| !s.is_empty() && *s != "..")
    {
        current = current.join(segment);
        dirs.push(current.clone());
    }
    dirs
}

/// `MemTotal:` in kB, as bytes.
fn parse_meminfo_total(text: &str) -> Option<u64> {
    text.lines()
        .find(|l| l.starts_with("MemTotal:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

/// cgroup v2 `memory.max`: a byte count, or the literal `max` for unlimited.
fn parse_cgroup_v2(text: &str) -> Option<u64> {
    let text = text.trim();
    if text == "max" {
        return None;
    }
    positive(text.parse().ok()?)
}

/// cgroup v1 `memory.limit_in_bytes`: a byte count, with a huge sentinel for unlimited.
///
/// **No sentinel check, deliberately.** The sentinel is a number larger than any real `MemTotal`,
/// so `resolve`'s minimum already discards it. A threshold here would be a second rule that can
/// disagree with the first.
fn parse_cgroup_v1(text: &str) -> Option<u64> {
    positive(text.trim().parse().ok()?)
}

/// A ceiling of zero is not a ceiling, it is an unreadable file that happened to parse. Treating
/// it as real would hand the engine `max_server_memory_bytes = 0`.
fn positive(value: u64) -> Option<u64> {
    (value > 0).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::{
        ceiling, parse_cgroup_v1, parse_cgroup_v2, parse_meminfo_total, resolve, Ceiling, Source,
    };
    use std::path::{Path, PathBuf};

    const MEMINFO: &str = "MemTotal:       536870912 kB\nMemFree:         1000 kB\n";

    /// A scratch cgroup tree, removed when the test ends however it ends.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "analyzer-memory-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos())
            ));
            std::fs::create_dir_all(dir.join("memory")).expect("scratch");
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn write(&self, rel: &str, text: &str) {
            let path = self.0.join(rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("scratch tree");
            }
            std::fs::write(path, text).expect("scratch file");
        }

        fn resolve(&self, own_cgroup: &str) -> Option<Ceiling> {
            resolve(&self.0.join("meminfo"), &self.0, own_cgroup)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn rig(label: &str, meminfo: &str, v2: Option<&str>, v1: Option<&str>) -> (u64, Source) {
        let dir = Scratch::new(label);
        dir.write("meminfo", meminfo);
        if let Some(v2) = v2 {
            dir.write("memory.max", v2);
        }
        if let Some(v1) = v1 {
            dir.write("memory/memory.limit_in_bytes", v1);
        }
        let Ceiling { bytes, source } = dir.resolve("").expect("a ceiling");
        (bytes, source)
    }

    #[test]
    fn a_cgroup_limit_below_host_memory_is_what_binds() {
        // 512 GiB of host, 16 GiB of container: the engine must be told the container's.
        let (bytes, source) = rig("container", MEMINFO, Some("17179869184\n"), None);
        assert_eq!(bytes, 17_179_869_184);
        assert_eq!(source, Source::Cgroup);
    }

    #[test]
    fn an_unlimited_v2_cgroup_falls_through_to_the_host() {
        // `max` is what a process outside any memory-limited cgroup reads, which is most VMs.
        let (bytes, source) = rig("unlimited", MEMINFO, Some("max\n"), None);
        assert_eq!(bytes, 536_870_912 * 1024);
        assert_eq!(source, Source::HostTotal);
    }

    #[test]
    fn v1s_unlimited_sentinel_needs_no_special_case_because_the_minimum_eats_it() {
        // 9223372036854771712 is the conventional "no limit" value. It is a number, it is larger
        // than any real MemTotal, and that is the whole reason it can be left alone.
        let (bytes, source) = rig("v1-sentinel", MEMINFO, None, Some("9223372036854771712\n"));
        assert_eq!(bytes, 536_870_912 * 1024);
        assert_eq!(source, Source::HostTotal);
    }

    #[test]
    fn the_lowest_ceiling_wins_when_both_layouts_are_mounted() {
        // Hybrid hosts mount v1 and v2 together. Reading one and trusting it would take whichever
        // the code happened to check first.
        let (bytes, _) = rig("hybrid", MEMINFO, Some("8589934592"), Some("4294967296"));
        assert_eq!(
            bytes, 4_294_967_296,
            "the smaller limit is the one enforced"
        );
    }

    #[test]
    fn a_zero_limit_is_treated_as_no_limit_rather_than_as_a_ceiling_of_zero() {
        // An unreadable-but-parseable file must not hand the engine `max_server_memory = 0`.
        let (bytes, source) = rig("zero", MEMINFO, Some("0\n"), None);
        assert_eq!(bytes, 536_870_912 * 1024);
        assert_eq!(source, Source::HostTotal);
    }

    #[test]
    fn a_systemd_unit_cap_is_found_at_the_process_own_cgroup_path() {
        // Under `MemoryMax=` there is no cgroup namespace: the root says `max`, and the
        // limit lives at the path `/proc/self/cgroup` names.
        let dir = Scratch::new("unit-cap");
        dir.write("memory.max", "max\n");
        dir.write(
            "system.slice/infertap-analyzer.service/memory.max",
            "6442450944\n",
        );
        dir.write("meminfo", MEMINFO);
        let ceiling = dir
            .resolve("0::/system.slice/infertap-analyzer.service\n")
            .expect("a ceiling");
        assert_eq!(ceiling.bytes, 6_442_450_944);
        assert_eq!(ceiling.source, Source::Cgroup);
    }

    #[test]
    fn the_throttle_point_binds_below_the_wall() {
        // `MemoryHigh` is enforced by stalling the group's allocations, so the smaller of
        // the high mark and the wall is what the engine is told.
        let dir = Scratch::new("throttle-point");
        dir.write("memory.max", "max\n");
        dir.write(
            "system.slice/infertap-analyzer.service/memory.max",
            "8589934592\n",
        );
        dir.write(
            "system.slice/infertap-analyzer.service/memory.high",
            "6442450944\n",
        );
        dir.write("meminfo", MEMINFO);
        let ceiling = dir
            .resolve("0::/system.slice/infertap-analyzer.service\n")
            .expect("a ceiling");
        assert_eq!(ceiling.bytes, 6_442_450_944);
        assert_eq!(ceiling.source, Source::Cgroup);
    }

    #[test]
    fn the_smallest_ancestor_binds_because_limits_are_hierarchical() {
        // A slice capped below the service's own declaration is what actually kills the
        // process, so it is what the engine must be told.
        let dir = Scratch::new("ancestor");
        dir.write("system.slice/memory.max", "4294967296\n");
        dir.write(
            "system.slice/infertap-analyzer.service/memory.max",
            "8589934592\n",
        );
        dir.write("meminfo", MEMINFO);
        let ceiling = dir
            .resolve("0::/system.slice/infertap-analyzer.service\n")
            .expect("a ceiling");
        assert_eq!(
            ceiling.bytes, 4_294_967_296,
            "the slice's smaller cap is enforced"
        );
    }

    #[test]
    fn a_v1_memory_controller_path_is_walked_too() {
        let dir = Scratch::new("v1-path");
        dir.write("memory/machine/memory.limit_in_bytes", "3221225472\n");
        dir.write("meminfo", MEMINFO);
        let ceiling = dir
            .resolve("7:cpuacct,memory:/machine\n1:cpu:/elsewhere\n")
            .expect("a ceiling");
        assert_eq!(ceiling.bytes, 3_221_225_472);
        assert_eq!(ceiling.source, Source::Cgroup);
    }

    #[test]
    fn a_cgroup_limit_still_binds_when_proc_meminfo_cannot_be_read() {
        let dir = Scratch::new("no-meminfo");
        dir.write("memory.max", "2147483648");
        let ceiling = resolve(&dir.path().join("absent"), dir.path(), "").expect("a ceiling");
        assert_eq!(ceiling.bytes, 2_147_483_648);
        assert_eq!(ceiling.source, Source::Cgroup);
    }

    #[test]
    fn nothing_readable_reports_nothing_rather_than_guessing() {
        // The caller leaves the engine's own default in place. Guessing a ceiling for a host we
        // cannot measure is how a wrong number gets into a config that outlives the guess.
        let dir = Scratch::new("empty");
        assert!(resolve(
            &dir.path().join("absent"),
            &dir.path().join("absent-too"),
            ""
        )
        .is_none());
    }

    #[test]
    fn the_parsers_reject_what_they_cannot_read() {
        assert_eq!(
            parse_meminfo_total("MemTotal:       1024 kB"),
            Some(1_048_576)
        );
        assert_eq!(parse_meminfo_total("MemFree: 12 kB"), None);
        assert_eq!(parse_meminfo_total("MemTotal: notanumber kB"), None);
        assert_eq!(parse_cgroup_v2("max"), None);
        assert_eq!(parse_cgroup_v2("  12345\n"), Some(12345));
        assert_eq!(parse_cgroup_v2(""), None);
        assert_eq!(parse_cgroup_v1("12345"), Some(12345));
        assert_eq!(
            parse_cgroup_v1("max"),
            None,
            "v1 never writes `max`, but a stray one must not parse"
        );
    }

    #[test]
    fn the_real_host_reports_something_sane() {
        // Not an assertion about this machine's size — an assertion that the shipped entry point
        // reads SOMETHING on a platform we run on, so the plumbing is exercised rather than only
        // the parsers.
        if cfg!(target_os = "linux") {
            let ceiling = ceiling().expect("a Linux host reports its memory");
            assert!(
                ceiling.bytes > 64 * 1024 * 1024,
                "implausibly small ceiling: {}",
                ceiling.bytes
            );
        }
    }
}
