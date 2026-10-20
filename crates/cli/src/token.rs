// SPDX-License-Identifier: AGPL-3.0-only

//! `token issue|rotate|list`: the viewer credential the read API verifies.

use crate::error::Error;
use crate::flags::Flags;
use crate::settings;
use crate::Command;
use std::path::Path;
use std::process::ExitCode;
use std::str::FromStr;

/// The `token` verbs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenVerb {
    Issue,
    Rotate,
    List,
}

impl FromStr for TokenVerb {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "issue" => Ok(Self::Issue),
            "rotate" => Ok(Self::Rotate),
            "list" => Ok(Self::List),
            other => Err(Error::usage(format!(
                "token: unknown verb {other:?}; {}",
                Command::Token.synopsis()
            ))),
        }
    }
}

/// `token issue|rotate|list` — the viewer credential the read API verifies.
///
/// **No engine, no server, no data directory lock.** The store is a file; minting a credential
/// must not require the daemon to be stopped, because the operator issuing one is usually
/// standing up the console against a daemon that is already running.
///
/// The plaintext is printed once, to stdout, and never stored, the `users.xml` pattern the
/// engine credential already uses. There is no `token show`, and there cannot be: nothing on
/// disk can reconstruct it.
pub(crate) fn token(args: &[String]) -> Result<ExitCode, Error> {
    let verb: TokenVerb = args
        .first()
        .ok_or_else(|| {
            Error::usage(format!(
                "token requires a verb: {}",
                Command::Token.synopsis()
            ))
        })?
        .parse()?;
    let flags = Flags::parse(&args[1..], &["--data-dir", "--label", "--id"])?;
    let data_dir = Path::new(flags.required("--data-dir", "token")?);
    let store_path = analyzer_api::token_store_for(data_dir);
    let store = analyzer_api::auth::TokenStore::open(&store_path)?;
    let now_ms = settings::now_ms();

    let minted = match verb {
        TokenVerb::Issue => store.issue(flags.value("--label").unwrap_or("viewer"), now_ms)?,
        TokenVerb::Rotate => {
            let id = flags.required("--id", "token rotate")?;
            let minted = store.rotate(id, now_ms)?;
            eprintln!("rotated: {id} is now refused; its replacement is below");
            minted
        }
        TokenVerb::List => {
            // Digests are never printed either: an operator auditing what exists needs the id,
            // the label and whether it still works, and nothing here is a credential.
            for r in store.records() {
                println!(
                    "{}\t{}\t{}\t{}",
                    r.id,
                    if r.active() { "active" } else { "rotated" },
                    r.issued_at_ms,
                    r.label
                );
            }
            return Ok(ExitCode::SUCCESS);
        }
    };
    // The plaintext goes to stdout and everything else to stderr, so a
    // `token issue... > secret` captures the credential and nothing else, and a copied
    // terminal line cannot pick up the id by accident.
    let (plaintext, record) = minted;
    eprintln!("token id:  {}", record.id);
    eprintln!("label:     {}", record.label);
    eprintln!("stored as: SHA-256 only, at {}", store_path.display());
    eprintln!("PRINTED ONCE. Nothing on disk can reconstruct it; lose it and rotate.");
    println!("{plaintext}");
    Ok(ExitCode::SUCCESS)
}
