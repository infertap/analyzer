// SPDX-License-Identifier: AGPL-3.0-only

//! One row of a read's tab-separated output, consumed field by field.
//!
//! A read names itself and each field as it takes it, so a row that does not parse says which
//! read, which field and which value refused, and a row with too few or too many fields says so
//! before any value is trusted.

use std::fmt::Display;
use std::str::FromStr;

use crate::Error;

/// The engine's null in the tab-separated form.
pub(crate) const NULL: &str = "\\N";

/// One row, read left to right.
pub(crate) struct Row<'a> {
    read: &'static str,
    line: &'a str,
    fields: std::str::Split<'a, char>,
}

impl<'a> Row<'a> {
    /// The row `line` of `read`.
    pub(crate) fn new(read: &'static str, line: &'a str) -> Self {
        Self {
            read,
            line,
            fields: line.split('\t'),
        }
    }

    /// Every non-empty line of `out`, each a row of `read`.
    pub(crate) fn each(read: &'static str, out: &'a str) -> impl Iterator<Item = Row<'a>> {
        out.lines()
            .filter(|l| !l.is_empty())
            .map(move |line| Self::new(read, line))
    }

    /// The one row of an aggregate `read`: its first line, empty when the engine returned none.
    pub(crate) fn single(read: &'static str, out: &'a str) -> Self {
        Self::new(read, out.lines().next().unwrap_or(""))
    }

    /// The next field as text.
    pub(crate) fn text(&mut self, name: &str) -> Result<&'a str, Error> {
        self.fields
            .next()
            .ok_or_else(|| Error::Row(format!("{}: no {name} field in {:?}", self.read, self.line)))
    }

    /// The next field as text, `None` on the engine's null.
    pub(crate) fn text_or_null(&mut self, name: &str) -> Result<Option<&'a str>, Error> {
        Ok(Some(self.text(name)?).filter(|v| *v != NULL))
    }

    /// The next field as text, `None` when empty or the engine's null: the convention of a
    /// column that sits in a key path and so renders absence as `''`.
    pub(crate) fn text_or_empty(&mut self, name: &str) -> Result<Option<&'a str>, Error> {
        Ok(self.text_or_null(name)?.filter(|v| !v.is_empty()))
    }

    /// The next field parsed as `T`.
    pub(crate) fn parse<T: FromStr>(&mut self, name: &str) -> Result<T, Error>
    where
        T::Err: Display,
    {
        let read = self.read;
        let v = self.text(name)?;
        v.parse()
            .map_err(|e| Error::Row(format!("{read}: {name} {v:?}: {e}")))
    }

    /// The next field parsed as `T`, `None` on the engine's null.
    pub(crate) fn nullable<T: FromStr>(&mut self, name: &str) -> Result<Option<T>, Error>
    where
        T::Err: Display,
    {
        let read = self.read;
        match self.text_or_null(name)? {
            None => Ok(None),
            Some(v) => v
                .parse()
                .map(Some)
                .map_err(|e| Error::Row(format!("{read}: {name} {v:?}: {e}"))),
        }
    }

    /// The next `n` fields as text: the dimension columns of a grouped read.
    pub(crate) fn texts(&mut self, n: usize, name: &str) -> Result<Vec<&'a str>, Error> {
        (0..n).map(|_| self.text(name)).collect()
    }

    /// No field is left: the read's column list and its reader agree.
    pub(crate) fn end(mut self) -> Result<(), Error> {
        match self.fields.next() {
            None => Ok(()),
            Some(_) => Err(Error::Row(format!(
                "{}: more fields than columns in {:?}",
                self.read, self.line
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Row;

    fn text(e: &crate::Error) -> String {
        e.to_string()
    }

    #[test]
    fn a_row_is_read_left_to_right_and_ends_when_its_columns_do() {
        let mut row = Row::new("r", "a\t7\t\\N\t2.5");
        assert_eq!(row.text("name").unwrap(), "a");
        assert_eq!(row.parse::<u64>("count").unwrap(), 7);
        assert_eq!(row.nullable::<f64>("gap").unwrap(), None);
        assert_eq!(row.nullable::<f64>("ms").unwrap(), Some(2.5));
        row.end().unwrap();
    }

    #[test]
    fn every_refusal_names_the_read_the_field_and_the_value() {
        let mut row = Row::new("whales", "x\tnot-a-number");
        row.text("identity").unwrap();
        let e = text(&row.parse::<f64>("dup_token_ms").unwrap_err());
        assert!(
            e.contains("whales") && e.contains("dup_token_ms") && e.contains("not-a-number"),
            "{e}"
        );

        let mut short = Row::new("whales", "x");
        short.text("identity").unwrap();
        let e = text(&short.text("dup_token_ms").unwrap_err());
        assert!(e.contains("no dup_token_ms field"), "{e}");

        let long = Row::new("whales", "x\ty");
        let e = text(&long.end().unwrap_err());
        assert!(e.contains("more fields than columns"), "{e}");
    }

    #[test]
    fn each_skips_blank_lines_and_single_takes_the_first() {
        assert_eq!(Row::each("r", "a\n\nb\n").count(), 2);
        assert_eq!(Row::single("r", "1\t2\n").parse::<u64>("n").unwrap(), 1);
        assert!(Row::single("r", "").parse::<u64>("n").is_err());
    }

    #[test]
    fn the_key_path_convention_reads_empty_as_absent() {
        let mut row = Row::new("r", "\t\\N\tv");
        assert_eq!(row.text_or_empty("a").unwrap(), None);
        assert_eq!(row.text_or_empty("b").unwrap(), None);
        assert_eq!(row.text_or_empty("c").unwrap(), Some("v"));
    }
}
