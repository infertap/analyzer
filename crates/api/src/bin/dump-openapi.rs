// SPDX-License-Identifier: AGPL-3.0-only

//! Regenerate `contract/openapi.json`.
//!
//! Needs no daemon and no engine, which is what lets CI diff the committed document on every
//! build rather than only where a database exists.
fn main() {
    let doc = analyzer_api::openapi::document();
    match serde_json::to_string_pretty(&doc) {
        Ok(text) => println!("{text}"),
        Err(e) => {
            eprintln!("dump-openapi: {e}");
            std::process::exit(1);
        }
    }
}
