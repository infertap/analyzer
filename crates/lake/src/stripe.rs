// SPDX-License-Identifier: AGPL-3.0-only

//! The key hash, and the stripes: the hash space cut into sixteen ranges by the hash's top
//! four bits. The working state's keys lead with the hash, so a stripe is one key range, and a
//! lane or a compaction can take the space a stripe at a time.

/// The stripes a key hash falls into: its top four bits.
pub const STRIPES: usize = 16;

/// The hash every keyed relation stripes by: FNV-1a over the key's bytes, 64 bits.
///
/// Named and owned, because the persisted layout rests on it: a standard-library hasher's
/// output is not promised stable across releases, and a layout keyed on one would silently
/// re-stripe a lake at the next toolchain. FNV-1a is stated in one line, has no key, and is
/// distribution-free enough that no fleet's naming of its blocks collapses a read's
/// parallelism.
#[must_use]
pub fn key_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The stripe of an identity hash: its top four bits, so a stripe is one range of hashes.
#[must_use]
pub fn of(hash: u64) -> usize {
    usize::try_from(hash >> 60).unwrap_or(0)
}

/// The first hash of `stripe`'s range ([`of`]'s inverse).
#[must_use]
pub fn first_hash(stripe: usize) -> u64 {
    (stripe as u64) << 60
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_key_hash_is_fnv1a_64() {
        // The published test vectors, so the algorithm is the one the doc names.
        assert_eq!(super::key_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(super::key_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(super::key_hash(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn a_stripe_is_one_range_of_hashes() {
        for stripe in 0..super::STRIPES {
            let first = super::first_hash(stripe);
            assert_eq!(super::of(first), stripe);
            assert_eq!(
                super::of(first.wrapping_sub(1)),
                (stripe + super::STRIPES - 1) % super::STRIPES
            );
        }
        assert_eq!(super::of(u64::MAX), super::STRIPES - 1);
    }
}
