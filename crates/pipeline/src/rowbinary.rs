// SPDX-License-Identifier: AGPL-3.0-only

//! The binary wire between the engine and the fold:
//! `RowBinary`, the engine's own row encoding — a `LEB128` length before each string, a
//! null byte before each nullable value, little-endian numbers, nothing else — read straight
//! into typed rows and written straight from typed rows. No text is lexed or formatted on
//! the hot path in either direction. `LowCardinality(T)` travels as `T`, which is how the
//! engine serializes it in this format.
//!
//! The reader is a cursor over a byte slice that says, per field, either the value or that
//! the slice ended mid-row (`Incomplete`), so a stream that arrives in chunks can hold the
//! tail of one chunk for the head of the next without ever copying a whole row twice.

/// A row ended before its last field: keep the bytes and read on with the next chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Incomplete;

/// A cursor over one chunk of `RowBinary` bytes.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(crate) fn at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Incomplete> {
        let end = self.pos.checked_add(n).ok_or(Incomplete)?;
        if end > self.bytes.len() {
            return Err(Incomplete);
        }
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, Incomplete> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, Incomplete> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, Incomplete> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub(crate) fn i64(&mut self) -> Result<i64, Incomplete> {
        #[allow(clippy::cast_possible_wrap)] // the wire's own two's complement
        Ok(self.u64()? as i64)
    }

    pub(crate) fn f64(&mut self) -> Result<f64, Incomplete> {
        Ok(f64::from_bits(self.u64()?))
    }

    /// An unsigned `LEB128` length or count.
    pub(crate) fn varint(&mut self) -> Result<u64, Incomplete> {
        let mut out: u64 = 0;
        for shift in (0..64).step_by(7) {
            let b = self.u8()?;
            out |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
        }
        Err(Incomplete)
    }

    pub(crate) fn string(&mut self) -> Result<&'a str, Incomplete> {
        let len = usize::try_from(self.varint()?).map_err(|_| Incomplete)?;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes).map_err(|_| Incomplete)
    }

    /// `Nullable(T)`: a null byte, then the value when the byte is zero.
    pub(crate) fn nullable<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, Incomplete>,
    ) -> Result<Option<T>, Incomplete> {
        if self.u8()? != 0 {
            return Ok(None);
        }
        read(self).map(Some)
    }
}

/// A growing `RowBinary` body: the writer side of the wire.
#[derive(Default)]
pub(crate) struct Writer {
    pub(crate) bytes: Vec<u8>,
}

impl Writer {
    pub(crate) fn u8(&mut self, v: u8) {
        self.bytes.push(v);
    }

    pub(crate) fn u32(&mut self, v: u32) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, v: u64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn i64(&mut self, v: i64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn f64(&mut self, v: f64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn varint(&mut self, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.bytes.push(b);
                return;
            }
            self.bytes.push(b | 0x80);
        }
    }

    pub(crate) fn string(&mut self, s: &str) {
        self.varint(s.len() as u64);
        self.bytes.extend_from_slice(s.as_bytes());
    }

    pub(crate) fn nullable<T>(&mut self, v: Option<T>, write: impl FnOnce(&mut Self, T)) {
        match v {
            None => self.u8(1),
            Some(v) => {
                self.u8(0);
                write(self, v);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Incomplete, Reader, Writer};

    #[test]
    fn every_scalar_and_the_nullables_round_trip_and_a_cut_row_says_incomplete() {
        let mut w = Writer::default();
        w.u8(7);
        w.u32(u32::MAX);
        w.i64(-42);
        w.f64(1_788_562_200_952.152);
        w.varint(300);
        w.string("clear");
        w.nullable::<u32>(None, Writer::u32);
        w.nullable(Some(9u32), Writer::u32);
        w.nullable(Some("GPU"), Writer::string);
        let bytes = w.bytes;
        let mut r = Reader::new(&bytes);
        assert_eq!(r.u8(), Ok(7));
        assert_eq!(r.u32(), Ok(u32::MAX));
        assert_eq!(r.i64(), Ok(-42));
        assert_eq!(r.f64(), Ok(1_788_562_200_952.152));
        assert_eq!(r.varint(), Ok(300));
        assert_eq!(r.string(), Ok("clear"));
        assert_eq!(r.nullable(Reader::u32), Ok(None));
        assert_eq!(r.nullable(Reader::u32), Ok(Some(9)));
        assert_eq!(r.nullable(Reader::string), Ok(Some("GPU")));
        assert!(r.at_end());
        // A row cut anywhere is Incomplete, never a wrong value.
        for cut in 0..bytes.len() {
            let mut r = Reader::new(&bytes[..cut]);
            let all = (|| {
                r.u8()?;
                r.u32()?;
                r.i64()?;
                r.f64()?;
                r.varint()?;
                r.string()?;
                r.nullable(Reader::u32)?;
                r.nullable(Reader::u32)?;
                r.nullable(Reader::string)?;
                Ok::<(), Incomplete>(())
            })();
            assert_eq!(all, Err(Incomplete), "cut at {cut}");
        }
    }
}
