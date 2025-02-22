// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Functionality used both on logical and physical plans

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type, Int8Type,
    TimeUnit,
};
use datafusion::error::{DataFusionError, Result};

#[inline]
fn spark_compatible_murmur3_hash<T: AsRef<[u8]>>(data: T, seed: u32) -> u32 {
    #[inline]
    fn mix_k1(mut k1: i32) -> i32 {
        k1 *= 0xcc9e2d51u32 as i32;
        k1 = k1.rotate_left(15);
        k1 *= 0x1b873593u32 as i32;
        k1
    }

    #[inline]
    fn mix_h1(mut h1: i32, k1: i32) -> i32 {
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1 * 5 + 0xe6546b64u32 as i32;
        h1
    }

    #[inline]
    fn fmix(mut h1: i32, len: i32) -> i32 {
        h1 ^= len;
        h1 ^= (h1 as u32 >> 16) as i32;
        h1 *= 0x85ebca6bu32 as i32;
        h1 ^= (h1 as u32 >> 13) as i32;
        h1 *= 0xc2b2ae35u32 as i32;
        h1 ^= (h1 as u32 >> 16) as i32;
        h1
    }

    #[inline]
    unsafe fn hash_bytes_by_int(data: &[u8], seed: u32) -> i32 {
        // safety: data length must be aligned to 4 bytes
        let mut h1 = seed as i32;
        for i in (0..data.len()).step_by(4) {
            let mut half_word = *(data.as_ptr().add(i) as *const i32);
            if cfg!(target_endian = "big") {
                half_word = half_word.reverse_bits();
            }
            h1 = mix_h1(h1, mix_k1(half_word));
        }
        h1
    }
    let data = data.as_ref();
    let len = data.len();
    let len_aligned = len - len % 4;

    // safety:
    // avoid boundary checking in performance critical codes.
    // all operations are garenteed to be safe
    unsafe {
        let mut h1 = hash_bytes_by_int(
            std::slice::from_raw_parts(data.get_unchecked(0), len_aligned),
            seed,
        );

        for i in len_aligned..len {
            let half_word = *data.get_unchecked(i) as i8 as i32;
            h1 = mix_h1(h1, mix_k1(half_word));
        }
        fmix(h1, len as i32) as u32
    }
}

#[test]
fn test_murmur3() {
    let _hashes = ["", "a", "ab", "abc", "abcd", "abcde"]
        .into_iter()
        .map(|s| spark_compatible_murmur3_hash(s.as_bytes(), 42) as i32)
        .collect::<Vec<_>>();
    let _expected = vec![142593372, 1485273170, -97053317, 1322437556, -396302900, 814637928];
    assert_eq!(_hashes, _expected)
}

macro_rules! hash_array {
    ($array_type:ident, $column: ident, $hashes: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                *hash = spark_compatible_murmur3_hash(&array.value(i), *hash);
            }
        } else {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(&array.value(i), *hash);
                }
            }
        }
    };
}

macro_rules! hash_list {
    ($array_type:ident, $column: ident, $hash: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            for i in 0..array.len() {
                *$hash = spark_compatible_murmur3_hash(&array.value(i), *$hash);
            }
        } else {
            for i in 0..array.len() {
                if !array.is_null(i) {
                    *$hash = spark_compatible_murmur3_hash(&array.value(i), *$hash);
                }
            }
        }
    };
}

macro_rules! hash_array_primitive {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                *hash = spark_compatible_murmur3_hash((*value as $ty).to_le_bytes(), *hash);
            }
        } else {
            for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash((*value as $ty).to_le_bytes(), *hash);
                }
            }
        }
    };
}

macro_rules! hash_list_primitive {
    ($array_type:ident, $column: ident, $ty: ident, $hash: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();
        if array.null_count() == 0 {
            for value in values.iter() {
                *$hash = spark_compatible_murmur3_hash((*value as $ty).to_le_bytes(), *$hash);
            }
        } else {
            for (i, value) in values.iter().enumerate() {
                if !array.is_null(i) {
                    *$hash = spark_compatible_murmur3_hash((*value as $ty).to_le_bytes(), *$hash);
                }
            }
        }
    };
}

macro_rules! hash_array_decimal {
    ($array_type:ident, $column: ident, $hashes: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        if array.null_count() == 0 {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                *hash = spark_compatible_murmur3_hash(array.value(i).to_le_bytes(), *hash);
            }
        } else {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
    };
}

macro_rules! hash_list_decimal {
    ($array_type:ident, $column: ident, $hash: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        if array.null_count() == 0 {
            for i in 0..array.len() {
                *$hash = spark_compatible_murmur3_hash(array.value(i).to_le_bytes(), *$hash);
            }
        } else {
            for i in 0..array.len() {
                if !array.is_null(i) {
                    *$hash = spark_compatible_murmur3_hash(array.value(i).to_le_bytes(), *$hash);
                }
            }
        }
    };
}

/// Hash the values in a dictionary array
fn create_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes_buffer: &mut [u32],
) -> Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let dict_values = Arc::clone(dict_array.values());
    let mut dict_hashes = vec![0; dict_values.len()];
    create_hashes(&[dict_values], &mut dict_hashes)?;

    for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
        if let Some(key) = key {
            let idx = key.to_usize().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Can not convert key value {:?} to usize in dictionary of type {:?}",
                    key,
                    dict_array.data_type()
                ))
            })?;
            *hash = dict_hashes[idx]
        } // no update for Null, consistent with other hashes
    }
    Ok(())
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
pub fn create_hashes<'a>(
    arrays: &[ArrayRef],
    hashes_buffer: &'a mut Vec<u32>,
) -> Result<&'a mut Vec<u32>> {
    for col in arrays {
        match col.data_type() {
            DataType::Null => {}
            DataType::Boolean => {
                let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                        *hash = spark_compatible_murmur3_hash(
                            (if array.value(i) { 1u32 } else { 0u32 }).to_le_bytes(),
                            *hash,
                        );
                    }
                } else {
                    for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = spark_compatible_murmur3_hash(
                                (if array.value(i) { 1u32 } else { 0u32 }).to_le_bytes(),
                                *hash,
                            );
                        }
                    }
                }
            }
            DataType::Int8 => {
                hash_array_primitive!(Int8Array, col, i32, hashes_buffer);
            }
            DataType::Int16 => {
                hash_array_primitive!(Int16Array, col, i32, hashes_buffer);
            }
            DataType::Int32 => {
                hash_array_primitive!(Int32Array, col, i32, hashes_buffer);
            }
            DataType::Int64 => {
                hash_array_primitive!(Int64Array, col, i64, hashes_buffer);
            }
            DataType::Float32 => {
                hash_array_primitive!(Float32Array, col, f32, hashes_buffer);
            }
            DataType::Float64 => {
                hash_array_primitive!(Float64Array, col, f64, hashes_buffer);
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                hash_array_primitive!(TimestampSecondArray, col, i64, hashes_buffer);
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                hash_array_primitive!(TimestampMillisecondArray, col, i64, hashes_buffer);
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                hash_array_primitive!(TimestampMicrosecondArray, col, i64, hashes_buffer);
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                hash_array_primitive!(TimestampNanosecondArray, col, i64, hashes_buffer);
            }
            DataType::Date32 => {
                hash_array_primitive!(Date32Array, col, i32, hashes_buffer);
            }
            DataType::Date64 => {
                hash_array_primitive!(Date64Array, col, i64, hashes_buffer);
            }
            DataType::Binary => {
                hash_array!(BinaryArray, col, hashes_buffer);
            }
            DataType::LargeBinary => {
                hash_array!(LargeBinaryArray, col, hashes_buffer);
            }
            DataType::Utf8 => {
                hash_array!(StringArray, col, hashes_buffer);
            }
            DataType::LargeUtf8 => {
                hash_array!(LargeStringArray, col, hashes_buffer);
            }
            DataType::Decimal128(_, _) => {
                hash_array_decimal!(Decimal128Array, col, hashes_buffer);
            }
            DataType::Dictionary(index_type, _) => match **index_type {
                DataType::Int8 => {
                    create_hashes_dictionary::<Int8Type>(col, hashes_buffer)?;
                }
                DataType::Int16 => {
                    create_hashes_dictionary::<Int16Type>(col, hashes_buffer)?;
                }
                DataType::Int32 => {
                    create_hashes_dictionary::<Int32Type>(col, hashes_buffer)?;
                }
                DataType::Int64 => {
                    create_hashes_dictionary::<Int64Type>(col, hashes_buffer)?;
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported dictionary type in hasher hashing: {}",
                        col.data_type(),
                    )))
                }
            },
            DataType::List(field) => {
                let list_array = col.as_any().downcast_ref::<ListArray>().unwrap();
                for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                    let sub_array = &list_array.value(i);
                    match field.data_type() {
                        DataType::Boolean => {
                            let array = sub_array.as_any().downcast_ref::<BooleanArray>().unwrap();
                            if array.null_count() == 0 {
                                for index in 0..array.len() {
                                    *hash = spark_compatible_murmur3_hash(
                                        (if array.value(index) { 1u32 } else { 0u32 })
                                            .to_le_bytes(),
                                        *hash,
                                    );
                                }
                            } else {
                                for index in 0..array.len() {
                                    if !array.is_null(index) {
                                        *hash = spark_compatible_murmur3_hash(
                                            (if array.value(index) { 1u32 } else { 0u32 })
                                                .to_le_bytes(),
                                            *hash,
                                        );
                                    }
                                }
                            }
                        }
                        DataType::Int8 => {
                            hash_list_primitive!(Int8Array, sub_array, i32, hash);
                        }
                        DataType::Int16 => {
                            hash_list_primitive!(Int16Array, sub_array, i32, hash);
                        }
                        DataType::Int32 => {
                            hash_list_primitive!(Int32Array, sub_array, i32, hash);
                        }
                        DataType::Int64 => {
                            hash_list_primitive!(Int64Array, sub_array, i64, hash);
                        }
                        DataType::Float32 => {
                            hash_list_primitive!(Float32Array, sub_array, f32, hash);
                        }
                        DataType::Float64 => {
                            hash_list_primitive!(Float64Array, sub_array, f64, hash);
                        }
                        DataType::Timestamp(TimeUnit::Second, _) => {
                            hash_list_primitive!(TimestampSecondArray, sub_array, i64, hash);
                        }
                        DataType::Timestamp(TimeUnit::Millisecond, _) => {
                            hash_list_primitive!(TimestampMillisecondArray, sub_array, i64, hash);
                        }
                        DataType::Timestamp(TimeUnit::Microsecond, _) => {
                            hash_list_primitive!(TimestampMicrosecondArray, sub_array, i64, hash);
                        }
                        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                            hash_list_primitive!(TimestampNanosecondArray, sub_array, i64, hash);
                        }
                        DataType::Date32 => {
                            hash_list_primitive!(Date32Array, sub_array, i32, hash);
                        }
                        DataType::Date64 => {
                            hash_list_primitive!(Date64Array, sub_array, i64, hash);
                        }
                        DataType::Binary => {
                            hash_list!(BinaryArray, sub_array, hash);
                        }
                        DataType::LargeBinary => {
                            hash_list!(LargeBinaryArray, sub_array, hash);
                        }
                        DataType::Utf8 => {
                            hash_list!(StringArray, sub_array, hash);
                        }
                        DataType::LargeUtf8 => {
                            hash_list!(LargeStringArray, sub_array, hash);
                        }
                        DataType::Decimal128(_, _) => {
                            hash_list_decimal!(Decimal128Array, sub_array, hash);
                        }
                        _ => {
                            return Err(DataFusionError::Internal(format!(
                                "Unsupported list data type in hasher: {}",
                                field.data_type()
                            )));
                        }
                    }
                }
            }
            DataType::Map(_, _) => {
                let map_array = col.as_any().downcast_ref::<MapArray>().unwrap();
                let key_array = map_array.keys();
                let value_array = map_array.values();
                let offsets_buffer = map_array.value_offsets();
                let mut cur_offset = 0;
                for (&next_offset, hash) in
                    offsets_buffer.iter().skip(1).zip(hashes_buffer.iter_mut())
                {
                    for idx in cur_offset..next_offset {
                        update_map_hashes(key_array, idx, hash)?;
                        update_map_hashes(value_array, idx, hash)?;
                    }
                    cur_offset = next_offset;
                }
            }
            DataType::Struct(_) => {
                let struct_array = col.as_any().downcast_ref::<StructArray>().unwrap();
                create_hashes(struct_array.columns(), hashes_buffer)?;
            }
            _ => {
                // This is internal because we should have caught this before.
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type in hasher: {}",
                    col.data_type()
                )));
            }
        }
    }
    Ok(hashes_buffer)
}

macro_rules! hash_map_primitive {
    ($array_type:ident, $column: ident, $ty: ident, $hash: ident, $idx: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        *$hash = spark_compatible_murmur3_hash(
            (array.value($idx as usize) as $ty).to_le_bytes(),
            *$hash,
        );
    };
}

macro_rules! hash_map_binary {
    ($array_type:ident, $column: ident, $hash: ident, $idx: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        *$hash = spark_compatible_murmur3_hash(&array.value($idx as usize), *$hash);
    };
}

macro_rules! hash_map_decimal {
    ($array_type:ident, $column: ident, $hash: ident, $idx: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        *$hash = spark_compatible_murmur3_hash(array.value($idx as usize).to_le_bytes(), *$hash);
    };
}

fn update_map_hashes(array: &ArrayRef, idx: i32, hash: &mut u32) -> Result<()> {
    if array.is_valid(idx as usize) {
        match array.data_type() {
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                *hash = spark_compatible_murmur3_hash(
                    (if array.value(idx as usize) {
                        1u32
                    } else {
                        0u32
                    })
                    .to_le_bytes(),
                    *hash,
                );
            }
            DataType::Int8 => {
                hash_map_primitive!(Int8Array, array, i32, hash, idx);
            }
            DataType::Int16 => {
                hash_map_primitive!(Int16Array, array, i32, hash, idx);
            }
            DataType::Int32 => {
                hash_map_primitive!(Int32Array, array, i32, hash, idx);
            }
            DataType::Int64 => {
                hash_map_primitive!(Int64Array, array, i64, hash, idx);
            }
            DataType::Float32 => {
                hash_map_primitive!(Float32Array, array, f32, hash, idx);
            }
            DataType::Float64 => {
                hash_map_primitive!(Float64Array, array, f64, hash, idx);
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                hash_map_primitive!(TimestampSecondArray, array, i64, hash, idx);
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                hash_map_primitive!(TimestampMillisecondArray, array, i64, hash, idx);
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                hash_map_primitive!(TimestampMicrosecondArray, array, i64, hash, idx);
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                hash_map_primitive!(TimestampNanosecondArray, array, i64, hash, idx);
            }
            DataType::Date32 => {
                hash_map_primitive!(Date32Array, array, i32, hash, idx);
            }
            DataType::Date64 => {
                hash_map_primitive!(Date64Array, array, i64, hash, idx);
            }
            DataType::Binary => {
                hash_map_binary!(BinaryArray, array, hash, idx);
            }
            DataType::LargeBinary => {
                hash_map_binary!(LargeBinaryArray, array, hash, idx);
            }
            DataType::Utf8 => {
                hash_map_binary!(StringArray, array, hash, idx);
            }
            DataType::LargeUtf8 => {
                hash_map_binary!(LargeStringArray, array, hash, idx);
            }
            DataType::Decimal128(_, _) => {
                hash_map_decimal!(Decimal128Array, array, hash, idx);
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported map key/value data type in hasher: {}",
                    array.data_type()
                )));
            }
        }
    }
    Ok(())
}

pub fn pmod(hash: u32, n: usize) -> usize {
    let hash = hash as i32;
    let n = n as i32;
    let r = hash % n;
    let result = if r < 0 { (r + n) % n } else { r };
    result as usize
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spark_hash::{create_hashes, pmod, spark_compatible_murmur3_hash};
    use arrow::array::{
        make_array, Array, ArrayData, ArrayRef, Int32Array, Int64Array, Int8Array, MapArray,
        StringArray, StructArray, UInt32Array,
    };
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field, ToByteSlice};

    #[test]
    fn test_list() {
        let mut hashes_buffer = vec![42; 4];
        for hash in hashes_buffer.iter_mut() {
            *hash = spark_compatible_murmur3_hash(5_i32.to_le_bytes(), *hash);
        }
    }

    #[test]
    fn test_i8() {
        let i = Arc::new(Int8Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i8::MAX),
            Some(i8::MIN),
        ])) as ArrayRef;
        let mut hashes = vec![42; 5];
        create_hashes(&[i], &mut hashes).unwrap();

        // generated with Spark Murmur3_x86_32
        let expected = vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x43b4d8ed, 0x422a1365];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_i32() {
        let i = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let mut hashes = vec![42; 1];
        create_hashes(&[i], &mut hashes).unwrap();

        let j = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;
        create_hashes(&[j], &mut hashes).unwrap();

        let m = Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef;
        create_hashes(&[m], &mut hashes).unwrap();

        let n = Arc::new(Int32Array::from(vec![Some(4)])) as ArrayRef;
        create_hashes(&[n], &mut hashes).unwrap();
    }

    #[test]
    fn test_i64() {
        let i = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i64::MAX),
            Some(i64::MIN),
        ])) as ArrayRef;
        let mut hashes = vec![42; 5];
        create_hashes(&[i], &mut hashes).unwrap();

        // generated with Spark Murmur3_x86_32
        let expected = vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_str() {
        let i = Arc::new(StringArray::from(vec!["hello", "bar", "", "😁", "天地"]));
        let mut hashes = vec![42; 5];
        create_hashes(&[i], &mut hashes).unwrap();

        // generated with Murmur3Hash(Seq(Literal("")), 42).eval() since Spark is tested against this as well
        let expected = vec![3286402344, 2486176763, 142593372, 885025535, 2395000894];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_pmod() {
        let i: Vec<u32> = vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb];
        let result = i.into_iter().map(|i| pmod(i, 200)).collect::<Vec<usize>>();

        // expected partition from Spark with n=200
        let expected = vec![69, 5, 193, 171, 115];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_map_array() {
        // Construct key and values
        let key_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build()
            .unwrap();
        let value_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from(
                &[0u32, 10, 20, 0, 40, 0, 60, 70].to_byte_slice(),
            ))
            .null_bit_buffer(Some(Buffer::from(&[0b11010110])))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        let keys_field = Arc::new(Field::new("keys", DataType::Int32, false));
        let values_field = Arc::new(Field::new("values", DataType::UInt32, true));
        let entry_struct = StructArray::from(vec![
            (keys_field.clone(), make_array(key_data)),
            (values_field.clone(), make_array(value_data.clone())),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                true,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        assert_eq!(&value_data, &map_array.values().to_data());
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(3, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[2]);
        assert_eq!(2, map_array.value_length(2));

        let key_array = Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef;
        let value_array =
            Arc::new(UInt32Array::from(vec![None, Some(10u32), Some(20)])) as ArrayRef;
        let struct_array = StructArray::from(vec![
            (keys_field.clone(), key_array),
            (values_field.clone(), value_array),
        ]);
        assert_eq!(
            struct_array,
            StructArray::from(map_array.value(0).into_data())
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
        for i in 0..3 {
            assert!(map_array.is_valid(i));
            assert!(!map_array.is_null(i));
        }

        // Now test with a non-zero offset
        let map_data = ArrayData::builder(map_array.data_type().clone())
            .len(2)
            .offset(1)
            .add_buffer(map_array.to_data().buffers()[0].clone())
            .add_child_data(map_array.to_data().child_data()[0].clone())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        assert_eq!(&value_data, &map_array.values().to_data());
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(2, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[1]);
        assert_eq!(2, map_array.value_length(1));

        let key_array = Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef;
        let value_array = Arc::new(UInt32Array::from(vec![None, Some(40), None])) as ArrayRef;
        let struct_array =
            StructArray::from(vec![(keys_field, key_array), (values_field, value_array)]);
        assert_eq!(
            &struct_array,
            map_array
                .value(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
    }
}
