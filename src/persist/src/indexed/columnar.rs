// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A columnar representation of ((Key, Val), Time, i64) data suitable for in-memory
//! reads and persistent storage.

use std::mem::size_of;
use std::sync::Arc;
use std::{cmp, fmt};

use ::arrow::array::StructArray;
use ::arrow::datatypes::ArrowNativeType;
use bytes::Bytes;
use mz_ore::bytes::MaybeLgBytes;
use mz_ore::lgbytes::MetricsRegion;
use mz_proto::{ProtoType, RustType, TryFromProtoError};

use crate::gen::persist::ProtoColumnarRecords;
use crate::metrics::ColumnarMetrics;

pub mod arrow;
pub mod parquet;

/// The maximum allowed amount of total key data (similarly val data) in a
/// single ColumnarBatch.
///
/// Note that somewhat counter-intuitively, this also includes offsets (counting
/// as 4 bytes each) in the definition of "key/val data".
///
/// TODO: The limit on the amount of {key,val} data is because we use i32
/// offsets in parquet; this won't change. However, we include the offsets in
/// the size because the parquet library we use currently maps each Array 1:1
/// with a parquet "page" (so for a "binary" column this is both the offsets and
/// the data). The parquet format internally stores the size of a page in an
/// i32, so if this gets too big, our library overflows it and writes bad data.
/// There's no reason it needs to map an Array 1:1 to a page (it could instead
/// be 1:1 with a "column chunk", which contains 1 or more pages). For now, we
/// work around it.
// TODO(benesch): find a way to express this without `as`.
#[allow(clippy::as_conversions)]
pub const KEY_VAL_DATA_MAX_LEN: usize = i32::MAX as usize;

const BYTES_PER_KEY_VAL_OFFSET: usize = 4;

/// A set of ((Key, Val), Time, Diff) records stored in a columnar
/// representation.
///
/// Note that the data are unsorted, and unconsolidated (so there may be
/// multiple instances of the same ((Key, Val), Time), and some Diffs might be
/// zero, or add up to zero).
///
/// Both Time and Diff are presented externally to persist users as a type
/// parameter that implements [mz_persist_types::Codec64]. Our columnar format
/// intentionally stores them both as i64 columns (as opposed to something like
/// a fixed width binary column) because this allows us additional compression
/// options.
///
/// Also note that we intentionally use an i64 over a u64 for Time. Over the
/// range `[0, i64::MAX]`, the bytes are the same and we've talked at various
/// times about changing Time in mz to an i64. Both millis since unix epoch and
/// nanos since unix epoch easily fit into this range (the latter until some
/// time after year 2200). Using a i64 might be a pessimization for a
/// non-realtime mz source with u64 timestamps in the range `(i64::MAX,
/// u64::MAX]`, but realtime sources are overwhelmingly the common case.
///
/// The i'th key's data is stored in
/// `key_data[key_offsets[i]..key_offsets[i+1]]`. Similarly for val.
///
/// Invariants:
/// - len < usize::MAX (so len+1 can fit in a usize)
/// - key_offsets.len() * BYTES_PER_KEY_VAL_OFFSET + key_data.len() <= KEY_VAL_DATA_MAX_LEN
/// - key_offsets.len() == len + 1
/// - key_offsets are non-decreasing
/// - Each key_offset is <= key_data.len()
/// - key_offsets.first().unwrap() == 0
/// - key_offsets.last().unwrap() == key_data.len()
/// - val_offsets.len() * BYTES_PER_KEY_VAL_OFFSET + val_data.len() <= KEY_VAL_DATA_MAX_LEN
/// - val_offsets.len() == len + 1
/// - val_offsets are non-decreasing
/// - Each val_offset is <= val_data.len()
/// - val_offsets.first().unwrap() == 0
/// - val_offsets.last().unwrap() == val_data.len()
/// - timestamps.len() == len
/// - diffs.len() == len
#[derive(Clone, PartialEq)]
pub struct ColumnarRecords {
    len: usize,
    key_data: MaybeLgBytes,
    key_offsets: Arc<MetricsRegion<i32>>,
    val_data: MaybeLgBytes,
    val_offsets: Arc<MetricsRegion<i32>>,
    timestamps: Arc<MetricsRegion<i64>>,
    diffs: Arc<MetricsRegion<i64>>,
}

impl fmt::Debug for ColumnarRecords {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.borrow(), fmt)
    }
}

impl ColumnarRecords {
    /// The number of (potentially duplicated) ((Key, Val), Time, i64) records
    /// stored in Self.
    pub fn len(&self) -> usize {
        self.len
    }

    /// The number of logical bytes in the represented data, excluding offsets
    /// and lengths.
    pub fn goodbytes(&self) -> usize {
        self.key_data.as_ref().len()
            + self.val_data.as_ref().len()
            + 8 * (*self.timestamps).as_ref().len()
            + 8 * (*self.diffs).as_ref().len()
    }

    /// Read the record at `idx`, if there is one.
    ///
    /// Returns None if `idx >= self.len()`.
    pub fn get<'a>(&'a self, idx: usize) -> Option<((&'a [u8], &'a [u8]), [u8; 8], [u8; 8])> {
        self.borrow().get(idx)
    }

    /// Borrow Self as a [ColumnarRecordsRef].
    fn borrow<'a>(&'a self) -> ColumnarRecordsRef<'a> {
        // The ColumnarRecords constructor already validates, so don't bother
        // doing it again.
        //
        // TODO: Forcing everything through a `fn new` would make this more
        // obvious.
        ColumnarRecordsRef {
            len: self.len,
            key_data: self.key_data.as_ref(),
            key_offsets: (*self.key_offsets).as_ref(),
            val_data: self.val_data.as_ref(),
            val_offsets: (*self.val_offsets).as_ref(),
            timestamps: (*self.timestamps).as_ref(),
            diffs: (*self.diffs).as_ref(),
        }
    }

    /// Iterate through the records in Self.
    pub fn iter<'a>(&'a self) -> ColumnarRecordsIter<'a> {
        self.borrow().iter()
    }
}

/// A reference to a [ColumnarRecords].
#[derive(Clone)]
struct ColumnarRecordsRef<'a> {
    len: usize,
    key_data: &'a [u8],
    key_offsets: &'a [i32],
    val_data: &'a [u8],
    val_offsets: &'a [i32],
    timestamps: &'a [i64],
    diffs: &'a [i64],
}

impl<'a> fmt::Debug for ColumnarRecordsRef<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> ColumnarRecordsRef<'a> {
    fn validate(&self) -> Result<(), String> {
        let key_data_size = self.key_offsets.len() * BYTES_PER_KEY_VAL_OFFSET + self.key_data.len();
        if key_data_size > KEY_VAL_DATA_MAX_LEN {
            return Err(format!(
                "expected encoded key offsets and data size to be less than or equal to {} got {}",
                KEY_VAL_DATA_MAX_LEN, key_data_size
            ));
        }
        if self.key_offsets.len() != self.len + 1 {
            return Err(format!(
                "expected {} key_offsets got {}",
                self.len + 1,
                self.key_offsets.len()
            ));
        }
        if let Some(first_key_offset) = self.key_offsets.first() {
            if *first_key_offset != 0 {
                return Err(format!(
                    "expected first key offset to be 0 got {}",
                    first_key_offset
                ));
            }
        }
        if let Some(last_key_offset) = self.key_offsets.last() {
            let last_key_offset = last_key_offset.to_usize().ok_or_else(|| {
                format!("last_key_offset is not valid usize, {}", last_key_offset)
            })?;
            if last_key_offset != self.key_data.len() {
                return Err(format!(
                    "expected {} bytes of key data got {}",
                    last_key_offset,
                    self.key_data.len()
                ));
            }
        }
        let val_data_size = self.val_offsets.len() * BYTES_PER_KEY_VAL_OFFSET + self.val_data.len();
        if val_data_size > KEY_VAL_DATA_MAX_LEN {
            return Err(format!(
                "expected encoded val offsets and data size to be less than or equal to {} got {}",
                KEY_VAL_DATA_MAX_LEN, val_data_size
            ));
        }
        if self.val_offsets.len() != self.len + 1 {
            return Err(format!(
                "expected {} val_offsets got {}",
                self.len + 1,
                self.val_offsets.len()
            ));
        }
        if let Some(first_val_offset) = self.val_offsets.first() {
            if *first_val_offset != 0 {
                return Err(format!(
                    "expected first val offset to be 0 got {}",
                    first_val_offset
                ));
            }
        }
        if let Some(last_val_offset) = self.val_offsets.last() {
            let last_val_offset = last_val_offset.to_usize().ok_or_else(|| {
                format!("last_val_offset is not valid usize, {}", last_val_offset)
            })?;
            if last_val_offset != self.val_data.len() {
                return Err(format!(
                    "expected {} bytes of val data got {}",
                    last_val_offset,
                    self.val_data.len()
                ));
            }
        }
        if self.diffs.len() != self.len {
            return Err(format!(
                "expected {} diffs got {}",
                self.len,
                self.diffs.len()
            ));
        }
        if self.timestamps.len() != self.len {
            return Err(format!(
                "expected {} timestamps got {}",
                self.len,
                self.timestamps.len()
            ));
        }

        // Unlike most of our Validate methods, this one is called in a
        // production code path: when decoding a columnar batch. Only check the
        // more expensive assertions in debug.
        #[cfg(debug_assertions)]
        {
            let (mut prev_key, mut prev_val) = (0, 0);
            for i in 0..=self.len {
                let (key, val) = (self.key_offsets[i], self.val_offsets[i]);
                if key < prev_key {
                    return Err(format!(
                        "expected non-decreasing key offsets got {} followed by {}",
                        prev_key, key
                    ));
                }
                if val < prev_val {
                    return Err(format!(
                        "expected non-decreasing val offsets got {} followed by {}",
                        prev_val, val
                    ));
                }
                prev_key = key;
                prev_val = val;
            }
        }

        Ok(())
    }

    /// Read the record at `idx`, if there is one.
    ///
    /// Returns None if `idx >= self.len()`.
    fn get(&self, idx: usize) -> Option<((&'a [u8], &'a [u8]), [u8; 8], [u8; 8])> {
        if idx >= self.len {
            return None;
        }

        let key_start = self.key_offsets[idx]
            .to_usize()
            .expect("key_start to be valid usize");
        let key_end = self.key_offsets[idx + 1]
            .to_usize()
            .expect("key_end to be valid usize");

        let val_start = self.val_offsets[idx]
            .to_usize()
            .expect("val_start to be valid usize");
        let val_end = self.val_offsets[idx + 1]
            .to_usize()
            .expect("val_end to be valid usize");

        // There used to be `debug_assert_eq!(self.validate(), Ok(()))`, but it
        // resulted in accidentally O(n^2) behavior in debug mode. Instead, we
        // push that responsibility to the ColumnarRecordsRef constructor.
        let key_range = key_start..key_end;
        let val_range = val_start..val_end;
        let key = &self.key_data[key_range];
        let val = &self.val_data[val_range];
        let ts = i64::to_le_bytes(self.timestamps[idx]);
        let diff = i64::to_le_bytes(self.diffs[idx]);
        Some(((key, val), ts, diff))
    }

    /// Iterate through the records in Self.
    fn iter(&self) -> ColumnarRecordsIter<'a> {
        ColumnarRecordsIter {
            idx: 0,
            records: self.clone(),
        }
    }
}

/// An [Iterator] over the records in a [ColumnarRecords].
#[derive(Clone, Debug)]
pub struct ColumnarRecordsIter<'a> {
    idx: usize,
    records: ColumnarRecordsRef<'a>,
}

impl<'a> Iterator for ColumnarRecordsIter<'a> {
    type Item = ((&'a [u8], &'a [u8]), [u8; 8], [u8; 8]);

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.records.len, Some(self.records.len))
    }

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.records.get(self.idx);
        self.idx += 1;
        ret
    }
}

impl<'a> ExactSizeIterator for ColumnarRecordsIter<'a> {}

/// An abstraction to incrementally add ((Key, Value), Time, i64) records
/// in a columnar representation, and eventually get back a [ColumnarRecords].
pub struct ColumnarRecordsBuilder {
    len: usize,
    key_data: Vec<u8>,
    key_offsets: Vec<i32>,
    val_data: Vec<u8>,
    val_offsets: Vec<i32>,
    timestamps: Vec<i64>,
    diffs: Vec<i64>,
}

impl fmt::Debug for ColumnarRecordsBuilder {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.borrow(), fmt)
    }
}

impl Default for ColumnarRecordsBuilder {
    fn default() -> Self {
        let mut ret = ColumnarRecordsBuilder {
            len: 0,
            key_data: Vec::new(),
            key_offsets: Vec::new(),
            val_data: Vec::new(),
            val_offsets: Vec::new(),
            timestamps: Vec::new(),
            diffs: Vec::new(),
        };
        // Push initial 0 offsets to maintain our invariants, even as we build.
        ret.key_offsets.push(0);
        ret.val_offsets.push(0);
        debug_assert_eq!(ret.borrow().validate(), Ok(()));
        ret
    }
}

impl ColumnarRecordsBuilder {
    /// The number of (potentially duplicated) ((Key, Val), Time, i64) records
    /// stored in Self.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Borrow Self as a [ColumnarRecordsRef].
    fn borrow<'a>(&'a self) -> ColumnarRecordsRef<'a> {
        let ret = ColumnarRecordsRef {
            len: self.len,
            key_data: self.key_data.as_slice(),
            key_offsets: self.key_offsets.as_slice(),
            val_data: self.val_data.as_slice(),
            val_offsets: self.val_offsets.as_slice(),
            timestamps: self.timestamps.as_slice(),
            diffs: self.diffs.as_slice(),
        };
        debug_assert_eq!(ret.validate(), Ok(()));
        ret
    }

    /// Reserve space for `additional` more records, based on `key_size_guess` and
    /// `val_size_guess`.
    ///
    /// The guesses for key and val sizes are best effort, and if they end up being
    /// too small, the underlying buffers will be resized.
    pub fn reserve(&mut self, additional: usize, key_size_guess: usize, val_size_guess: usize) {
        self.key_offsets.reserve(additional);
        self.key_data
            .reserve(cmp::min(additional * key_size_guess, KEY_VAL_DATA_MAX_LEN));
        self.val_offsets.reserve(additional);
        self.val_data
            .reserve(cmp::min(additional * val_size_guess, KEY_VAL_DATA_MAX_LEN));
        self.timestamps.reserve(additional);
        self.diffs.reserve(additional);
        debug_assert_eq!(self.borrow().validate(), Ok(()));
    }

    /// Reserve space for `additional` more records, with exact sizes for the key and value data.
    pub fn reserve_exact(&mut self, additional: usize, key_bytes: usize, val_bytes: usize) {
        self.key_offsets.reserve(additional);
        self.key_data
            .reserve(cmp::min(key_bytes, KEY_VAL_DATA_MAX_LEN));
        self.val_offsets.reserve(additional);
        self.val_data
            .reserve(cmp::min(val_bytes, KEY_VAL_DATA_MAX_LEN));
        self.timestamps.reserve(additional);
        self.diffs.reserve(additional);
        debug_assert_eq!(self.borrow().validate(), Ok(()));
    }

    /// Returns if the given key_offsets+key_data or val_offsets+val_data fits
    /// in the limits imposed by ColumnarRecords.
    ///
    /// Note that limit is always [KEY_VAL_DATA_MAX_LEN] in production. It's
    /// only override-able here for testing.
    pub fn can_fit(&self, key: &[u8], val: &[u8], limit: usize) -> bool {
        let key_data_size = (self.key_offsets.len() + 1) * BYTES_PER_KEY_VAL_OFFSET
            + self.key_data.len()
            + key.len();
        let val_data_size = (self.val_offsets.len() + 1) * BYTES_PER_KEY_VAL_OFFSET
            + self.val_data.len()
            + val.len();
        key_data_size <= limit && val_data_size <= limit
    }

    /// Add a record to Self.
    ///
    /// Returns whether the record was successfully added. A record will not a
    /// added if it exceeds the size limitations of ColumnarBatch. This method
    /// is atomic, if it fails, no partial data will have been added.
    #[must_use]
    pub fn push(&mut self, record: ((&[u8], &[u8]), [u8; 8], [u8; 8])) -> bool {
        let ((key, val), ts, diff) = record;

        // Check size invariants ahead of time so we stay atomic when we can't
        // add the record.
        if !self.can_fit(key, val, KEY_VAL_DATA_MAX_LEN) {
            return false;
        }

        // NB: We should never hit the following expects because we check them
        // above.
        self.key_data.extend_from_slice(key);
        self.key_offsets
            .push(i32::try_from(self.key_data.len()).expect("key_data is smaller than 2GB"));
        self.val_data.extend_from_slice(val);
        self.val_offsets
            .push(i32::try_from(self.val_data.len()).expect("val_data is smaller than 2GB"));
        self.timestamps.push(i64::from_le_bytes(ts));
        self.diffs.push(i64::from_le_bytes(diff));
        self.len += 1;

        true
    }

    /// Finalize constructing a [ColumnarRecords].
    pub fn finish(self, metrics: &ColumnarMetrics) -> ColumnarRecords {
        // We're almost certainly going to immediately encode this and drop it,
        // so don't bother actually copying the data into lgalloc, use the
        // `heap_region` method instead. Revisit if that changes.
        let ret = ColumnarRecords {
            len: self.len,
            key_data: MaybeLgBytes::Bytes(Bytes::from(self.key_data)),
            key_offsets: Arc::new(metrics.lgbytes_arrow.heap_region(self.key_offsets)),
            val_data: MaybeLgBytes::Bytes(Bytes::from(self.val_data)),
            val_offsets: Arc::new(metrics.lgbytes_arrow.heap_region(self.val_offsets)),
            timestamps: Arc::new(metrics.lgbytes_arrow.heap_region(self.timestamps)),
            diffs: Arc::new(metrics.lgbytes_arrow.heap_region(self.diffs)),
        };
        debug_assert_eq!(ret.borrow().validate(), Ok(()));
        ret
    }

    /// Size of an update record as stored in the columnar representation
    pub fn columnar_record_size(key_bytes_len: usize, value_bytes_len: usize) -> usize {
        (key_bytes_len + BYTES_PER_KEY_VAL_OFFSET)
            + (value_bytes_len + BYTES_PER_KEY_VAL_OFFSET)
            + (2 * size_of::<u64>()) // T and D
    }
}

impl ColumnarRecords {
    /// See [RustType::into_proto].
    pub fn into_proto(&self) -> ProtoColumnarRecords {
        ProtoColumnarRecords {
            len: self.len.into_proto(),
            key_offsets: (*self.key_offsets).as_ref().to_vec(),
            key_data: Bytes::copy_from_slice(self.key_data.as_ref()),
            val_offsets: (*self.val_offsets).as_ref().to_vec(),
            val_data: Bytes::copy_from_slice(self.val_data.as_ref()),
            timestamps: (*self.timestamps).as_ref().to_vec(),
            diffs: (*self.diffs).as_ref().to_vec(),
        }
    }

    /// See [RustType::from_proto].
    pub fn from_proto(
        lgbytes: &ColumnarMetrics,
        proto: ProtoColumnarRecords,
    ) -> Result<Self, TryFromProtoError> {
        let ret = ColumnarRecords {
            len: proto.len.into_rust()?,
            key_offsets: Arc::new(lgbytes.lgbytes_arrow.heap_region(proto.key_offsets)),
            key_data: MaybeLgBytes::Bytes(proto.key_data),
            val_offsets: Arc::new(lgbytes.lgbytes_arrow.heap_region(proto.val_offsets)),
            val_data: MaybeLgBytes::Bytes(proto.val_data),
            timestamps: Arc::new(lgbytes.lgbytes_arrow.heap_region(proto.timestamps)),
            diffs: Arc::new(lgbytes.lgbytes_arrow.heap_region(proto.diffs)),
        };
        let () = ret
            .borrow()
            .validate()
            .map_err(TryFromProtoError::InvalidPersistState)?;
        Ok(ret)
    }
}

/// An "extension" to [`ColumnarRecords`] that duplicates the "key" (`K`) and "val" (`V`) columns
/// as structured Arrow data.
///
/// [`ColumnarRecords`] stores the key and value columns as binary blobs encoded with the [`Codec`]
/// trait. We're migrating to instead store the key and value columns as structured Parquet data,
/// which we interface with via Arrow.
///
/// [`Codec`]: mz_persist_types::Codec
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnarRecordsStructuredExt {
    /// The structured `k` column.
    ///
    /// [`arrow`] does not allow empty [`StructArray`]s so we model an empty `key` column as None.
    pub key: Option<StructArray>,
    /// The structured `v` column.
    ///
    /// [`arrow`] does not allow empty [`StructArray`]s so we model an empty `val` column as None.
    pub val: Option<StructArray>,
}

#[cfg(test)]
mod tests {
    use mz_persist_types::Codec64;

    use super::*;

    /// Smoke test some edge cases around empty sets of records and empty keys/vals
    ///
    /// Most of this functionality is also well-exercised in other unit tests as well.
    #[mz_ore::test]
    fn columnar_records() {
        let metrics = ColumnarMetrics::disconnected();
        let builder = ColumnarRecordsBuilder::default();

        // Empty builder.
        let records = builder.finish(&metrics);
        let reads: Vec<_> = records.iter().collect();
        assert_eq!(reads, vec![]);

        // Empty key and val.
        let updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)> = vec![
            (("".into(), "".into()), 0, 0),
            (("".into(), "".into()), 1, 1),
        ];
        let mut builder = ColumnarRecordsBuilder::default();
        for ((key, val), time, diff) in updates.iter() {
            assert!(builder.push(((key, val), u64::encode(time), i64::encode(diff))));
        }

        let records = builder.finish(&metrics);
        let reads: Vec<_> = records
            .iter()
            .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), u64::decode(t), i64::decode(d)))
            .collect();
        assert_eq!(reads, updates);
    }
}
