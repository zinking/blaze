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

use arrow::array::*;
use datafusion::common::Result;
use datafusion::physical_plan::ColumnarValue;
use datafusion_ext_commons::spark_hash::create_hashes;
use std::sync::Arc;

/// implements org.apache.spark.sql.catalyst.expressions.UnscaledValue
pub fn spark_murmur3_hash(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let len = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        })
        .max()
        .unwrap_or(0);

    let arrays = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len),
        })
        .collect::<Vec<_>>();

    // use identical seed as spark hash partition
    let spark_murmur3_default_seed = 42u32;
    let mut hash_buffer = vec![spark_murmur3_default_seed; len];
    create_hashes(&arrays, &mut hash_buffer)?;

    Ok(ColumnarValue::Array(Arc::new(
        Int32Array::from_iter_values(hash_buffer.into_iter().map(|hash| hash as i32)),
    )))
}

#[cfg(test)]
mod test {
    use crate::spark_murmur3_hash::spark_murmur3_hash;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use datafusion::logical_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_murmur3_hash_int64() {
        let result = spark_murmur3_hash(&vec![ColumnarValue::Array(Arc::new(Int64Array::from(
            vec![Some(1), Some(0), Some(-1), Some(i64::MAX), Some(i64::MIN)],
        )))])
        .unwrap()
        .into_array(5);

        let expected = Int32Array::from(vec![
            Some(-1712319331),
            Some(-1670924195),
            Some(-939490007),
            Some(-1604625029),
            Some(-853646085),
        ]);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
    }

    #[test]
    fn test_murmur3_hash_string() {
        let result = spark_murmur3_hash(&vec![ColumnarValue::Array(Arc::new(
            StringArray::from_iter_values(["hello", "bar", "", "😁", "天地"]),
        ))])
        .unwrap()
        .into_array(5);

        let expected = Int32Array::from(vec![
            Some(-1008564952),
            Some(-1808790533),
            Some(142593372),
            Some(885025535),
            Some(-1899966402),
        ]);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
    }
}
