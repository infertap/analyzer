// SPDX-License-Identifier: AGPL-3.0-only

//! The command line: `--flag value` pairs, and the two layers beneath them.
//!
//! Every command parses through [`Flags`]: an allowed set, two tokens per flag, unknown
//! flags refused with the flag named, a repeatable flag keeping every value. The `run` and
//! `preflight` commands first layer a TOML file and `INFERTAP_*` environment names beneath
//! their arguments ([`layer`]), in the flag vocabulary and merged into one synthetic argv,
//! so the flag parser stays the single definition of every option.
//!
//! Precedence is flag over environment over file, per key: a key the command line sets is
//! dropped from both lower layers, and a key the environment sets is dropped from the file.
//! The file's keys are the flags without the dashes and with underscores; an environment
//! name is `INFERTAP_` plus the key upper-cased. `--config` names the file, or
//! `INFERTAP_CONFIG` when the flag is absent; a command with no file and no `INFERTAP_*`
//! flag names sees its arguments untouched. An `INFERTAP_*` name that is not a flag is
//! ignored, never refused: the bucket credentials (`INFERTAP_S3_*`) and the daemon's own
//! switches live in the same namespace and are read by their own readers.

use crate::error::Error;
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;

/// The parsed flags of one command: each flag's values, in the order given.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Flags(BTreeMap<&'static str, Vec<String>>);

impl Flags {
    /// Parse `args` as `--flag value` pairs drawn from `allowed`.
    ///
    /// # Errors
    /// A flag not in `allowed`, or a flag with no value after it.
    pub(crate) fn parse(args: &[String], allowed: &[&'static str]) -> Result<Self, Error> {
        let mut out: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
        let mut i = 0;
        while i < args.len() {
            let Some(flag) = allowed.iter().copied().find(|f| *f == args[i]) else {
                return Err(Error::usage(format!(
                    "unknown flag {:?}; this command takes {allowed:?}",
                    args[i]
                )));
            };
            let value = args
                .get(i + 1)
                .cloned()
                .ok_or_else(|| Error::usage(format!("{flag} requires a value")))?;
            out.entry(flag).or_default().push(value);
            i += 2;
        }
        Ok(Self(out))
    }

    /// The flag's value, the last given when it was repeated.
    pub(crate) fn value(&self, flag: &str) -> Option<&str> {
        self.0.get(flag).and_then(|v| v.last()).map(String::as_str)
    }

    /// Every value the flag was given.
    pub(crate) fn values(&self, flag: &str) -> &[String] {
        self.0.get(flag).map_or(&[], Vec::as_slice)
    }

    /// The flag's value, or a usage error naming what the command requires.
    ///
    /// # Errors
    /// The flag was not given.
    pub(crate) fn required(&self, flag: &str, requires: &str) -> Result<&str, Error> {
        self.value(flag)
            .ok_or_else(|| Error::usage(format!("{requires} requires {flag}")))
    }

    /// The flag's value parsed as `T`, or `None` when absent.
    ///
    /// # Errors
    /// The value does not parse; the message names the flag, the value and `what` it takes.
    pub(crate) fn parsed<T: FromStr>(&self, flag: &str, what: &str) -> Result<Option<T>, Error> {
        self.value(flag)
            .map(|v| {
                v.parse::<T>()
                    .map_err(|_| Error::usage(format!("{flag} takes {what}, got {v:?}")))
            })
            .transpose()
    }

    /// The flag's value parsed as `T` and checked against `floor`, or `None` when absent.
    ///
    /// # Errors
    /// The value does not parse, or is below the floor; the message carries `why`.
    pub(crate) fn at_least<T>(
        &self,
        flag: &str,
        what: &str,
        floor: &T,
        why: &str,
    ) -> Result<Option<T>, Error>
    where
        T: FromStr + PartialOrd + std::fmt::Display,
    {
        match self.parsed::<T>(flag, what)? {
            Some(n) if n < *floor => Err(Error::usage(format!(
                "{flag} {n} is below the floor {floor}: {why}"
            ))),
            other => Ok(other),
        }
    }

    /// An `on`/`off` switch, or `None` when absent.
    ///
    /// # Errors
    /// A value other than `on` or `off`.
    pub(crate) fn switch(&self, flag: &str) -> Result<Option<bool>, Error> {
        match self.value(flag) {
            None => Ok(None),
            Some("on") => Ok(Some(true)),
            Some("off") => Ok(Some(false)),
            Some(other) => Err(Error::usage(format!(
                "{flag} takes on or off, not {other:?}"
            ))),
        }
    }
}

/// A flag a file or the environment may set for the command, if that command takes it.
/// `--config` itself is argv-or-environment only.
fn key_of<'a>(allowed: &[&'a str], name: &str) -> Option<&'a str> {
    allowed
        .iter()
        .copied()
        .find(|k| k.strip_prefix("--") == Some(name) && *k != "--config")
}

/// Flags that may be given more than once: a file lists them, and each item becomes one
/// `--flag value` pair, as argv takes them.
const REPEATABLE: &[&str] = &["--disable-module"];

type Layer = BTreeMap<&'static str, Vec<String>>;

/// The command's arguments with the file's and the environment's layered beneath them.
///
/// # Errors
/// A file that does not read or parse, an unknown key in it, or a value of the wrong shape.
pub(crate) fn layer(
    args: &[String],
    allowed: &[&'static str],
    env: impl Iterator<Item = (String, String)>,
    read_file: impl Fn(&Path) -> std::io::Result<String>,
) -> Result<Vec<String>, Error> {
    let env: Vec<(String, String)> = env.filter(|(k, _)| k.starts_with("INFERTAP_")).collect();
    let argv_keys: Vec<&str> = args
        .iter()
        .filter_map(|a| a.strip_prefix("--"))
        .filter_map(|a| key_of(allowed, a))
        .collect();
    let config_path = args
        .iter()
        .position(|a| a == "--config")
        .and_then(|i| args.get(i + 1).cloned())
        .or_else(|| {
            env.iter()
                .find(|(k, _)| k == "INFERTAP_CONFIG")
                .map(|(_, v)| v.clone())
        });
    let mut file_layer = match config_path {
        Some(path) => {
            let text = read_file(Path::new(&path)).map_err(|e| Error::Config {
                path: path.clone(),
                message: format!("reading it failed: {e}"),
            })?;
            from_toml(allowed, &path, &text)?
        }
        None => Layer::new(),
    };
    let mut env_layer = from_env(allowed, &env);
    for k in &argv_keys {
        env_layer.remove(k);
        file_layer.remove(k);
    }
    for k in env_layer.keys() {
        file_layer.remove(k);
    }
    let mut out: Vec<String> = Vec::new();
    for tokens in file_layer.into_values() {
        out.extend(tokens);
    }
    for tokens in env_layer.into_values() {
        out.extend(tokens);
    }
    out.extend(args.iter().cloned());
    Ok(out)
}

fn tokens(key: &'static str, value: &toml::Value, path: &str) -> Result<Vec<String>, Error> {
    let refuse = |message: String| Error::Config {
        path: path.to_string(),
        message,
    };
    match value {
        toml::Value::String(s) => Ok(vec![key.to_string(), s.clone()]),
        toml::Value::Integer(n) => Ok(vec![key.to_string(), n.to_string()]),
        toml::Value::Float(f) => Ok(vec![key.to_string(), f.to_string()]),
        toml::Value::Boolean(b) => Ok(vec![key.to_string(), b.to_string()]),
        toml::Value::Array(items) if REPEATABLE.contains(&key) => {
            let mut out = Vec::with_capacity(items.len() * 2);
            for item in items {
                match item {
                    toml::Value::String(s) => {
                        out.push(key.to_string());
                        out.push(s.clone());
                    }
                    other => {
                        return Err(refuse(format!(
                            "{key} must be a list of strings, found {other:?}"
                        )))
                    }
                }
            }
            Ok(out)
        }
        other => Err(refuse(format!("{key} has an unsupported value {other:?}"))),
    }
}

fn from_toml(allowed: &[&'static str], path: &str, text: &str) -> Result<Layer, Error> {
    let table = text.parse::<toml::Table>().map_err(|e| Error::Config {
        path: path.to_string(),
        message: e.to_string(),
    })?;
    let mut layer = Layer::new();
    for (name, value) in table {
        let flag_name = name.replace('_', "-");
        let Some(key) = key_of(allowed, &flag_name) else {
            return Err(Error::Config {
                path: path.to_string(),
                message: format!(
                    "unknown key {name:?} (the keys are this command's flags in --help, with \
                     underscores; `config` is argv-only)"
                ),
            });
        };
        layer.insert(key, tokens(key, &value, path)?);
    }
    Ok(layer)
}

/// The environment's layer: `INFERTAP_<FLAG>` for the flags this command takes; every
/// other `INFERTAP_*` name belongs to another reader and passes through untouched.
fn from_env(allowed: &[&'static str], env: &[(String, String)]) -> Layer {
    let mut layer = Layer::new();
    for (name, value) in env {
        let Some(rest) = name.strip_prefix("INFERTAP_") else {
            continue;
        };
        if rest == "CONFIG" {
            continue;
        }
        let flag_name = rest.to_ascii_lowercase().replace('_', "-");
        let Some(key) = key_of(allowed, &flag_name) else {
            continue;
        };
        if REPEATABLE.contains(&key) {
            let mut tokens = Vec::new();
            for item in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                tokens.push(key.to_string());
                tokens.push(item.to_string());
            }
            layer.insert(key, tokens);
        } else {
            layer.insert(key, vec![key.to_string(), value.clone()]);
        }
    }
    layer
}

#[cfg(test)]
mod tests {
    use super::{layer, Flags};
    use std::path::Path;

    const ALLOWED: &[&str] = &[
        "--config",
        "--fleet-id",
        "--bucket-url",
        "--bucket-prefix",
        "--disable-module",
        "--log-level",
    ];

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn a_flag_beats_the_environment_beats_the_file_and_unknown_environment_names_pass() {
        let file = "fleet_id = \"from-file\"\nbucket_url = \"https://file/\"\n\
                    bucket_prefix = \"p/kv/\"\ndisable_module = [\"roster\", \"duplication\"]\n\
                    log_level = \"debug\"\n";
        let env = vec![
            ("INFERTAP_CONFIG".to_string(), "x.toml".to_string()),
            (
                "INFERTAP_BUCKET_URL".to_string(),
                "https://env/".to_string(),
            ),
            (
                "INFERTAP_S3_ACCESS_KEY_ID".to_string(),
                "not a flag".to_string(),
            ),
            ("INFERTAP_HOST_CLAIM".to_string(), "off".to_string()),
        ];
        let out = layer(
            &argv(&["--fleet-id", "from-argv"]),
            ALLOWED,
            env.into_iter(),
            |p: &Path| {
                assert_eq!(p, Path::new("x.toml"));
                Ok(file.to_string())
            },
        )
        .expect("layers");
        let s = out.join(" ");
        assert!(
            s.starts_with(
                "--bucket-prefix p/kv/ --disable-module roster --disable-module duplication \
                 --log-level debug"
            ),
            "{s}"
        );
        assert!(
            s.contains("--bucket-url https://env/"),
            "the environment beats the file: {s}"
        );
        assert!(!s.contains("https://file/"), "{s}");
        assert!(
            s.ends_with("--fleet-id from-argv"),
            "the command line comes last and wins: {s}"
        );
        assert!(!s.contains("from-file"), "{s}");
        assert!(
            !s.contains("not a flag") && !s.contains("HOST_CLAIM"),
            "{s}"
        );
        // and the layered argv parses into the flags the command reads, the file's level included
        let flags = Flags::parse(&out, ALLOWED).expect("parses");
        assert_eq!(flags.value("--log-level"), Some("debug"));
        assert_eq!(flags.values("--disable-module"), ["roster", "duplication"]);
        assert_eq!(flags.value("--fleet-id"), Some("from-argv"));
    }

    #[test]
    fn an_unknown_file_key_is_refused_by_name_and_no_layers_means_no_change() {
        let err = layer(
            &argv(&["--config", "c.toml"]),
            ALLOWED,
            std::iter::empty(),
            |_: &Path| Ok("http_port = 1\n".to_string()),
        )
        .expect_err("preflight takes no port");
        assert!(
            err.to_string().contains("unknown key \"http_port\""),
            "{err}"
        );
        let same = layer(
            &argv(&["--fleet-id", "f"]),
            ALLOWED,
            std::iter::empty(),
            |_: &Path| unreachable!("no file named"),
        )
        .expect("no layers");
        assert_eq!(same, argv(&["--fleet-id", "f"]));
    }

    #[test]
    fn flags_take_pairs_refuse_strangers_and_keep_every_repeat() {
        let flags = Flags::parse(
            &argv(&[
                "--fleet-id",
                "a",
                "--fleet-id",
                "b",
                "--disable-module",
                "roster",
            ]),
            ALLOWED,
        )
        .expect("parses");
        assert_eq!(flags.value("--fleet-id"), Some("b"), "the last repeat wins");
        assert_eq!(flags.values("--fleet-id"), ["a", "b"]);
        assert_eq!(flags.value("--bucket-url"), None);
        assert!(Flags::parse(&argv(&["--fleet-id"]), ALLOWED).is_err());
        assert!(Flags::parse(&argv(&["--port", "1"]), ALLOWED).is_err());
        assert!(flags.parsed::<u32>("--fleet-id", "a number").is_err());
        assert!(flags
            .at_least("--fleet-id", "a number", &1_u32, "why")
            .is_err());
        assert_eq!(flags.switch("--fleet-id").ok(), None);
    }
}
