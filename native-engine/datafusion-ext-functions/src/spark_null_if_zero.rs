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
use arrow::compute::*;
use arrow::datatypes::*;
use datafusion::common::Result;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// used to avoid DivideByZero error in divide/modulo
pub fn spark_null_if_zero(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    Ok(match &args[0] {
        ColumnarValue::Scalar(scalar) => {
            let data_type = scalar.get_datatype();
            let zero = ScalarValue::new_zero(&data_type)?;
            if scalar.eq(&zero) {
                ColumnarValue::Scalar(ScalarValue::try_from(data_type)?)
            } else {
                ColumnarValue::Scalar(scalar.clone())
            }
        }
        ColumnarValue::Array(array) => {
            macro_rules! handle {
                ($dt:ident) => {{
                    type T = paste::paste! {arrow::datatypes::[<$dt Type>]};
                    let array = as_primitive_array::<T>(array);
                    let eq_zeros = eq_scalar(array, T::default_value())?;
                    Arc::new(nullif(array, &eq_zeros)?) as ArrayRef
                }};
            }
            macro_rules! handle_decimal {
                ($dt:ident, $precision:expr, $scale:expr) => {{
                    type T = paste::paste! {arrow::datatypes::[<$dt Type>]};
                    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                    let _0 = <T as ArrowPrimitiveType>::Native::from_le_bytes([0; T::BYTE_LENGTH]);
                    let filtered = array.iter().map(|v| v.filter(|v| *v != _0));
                    Arc::new(
                        PrimitiveArray::<T>::from_iter(filtered)
                            .with_precision_and_scale($precision, $scale)?,
                    )
                }};
            }
            ColumnarValue::Array(match array.data_type() {
                DataType::Int8 => handle!(Int8),
                DataType::Int16 => handle!(Int16),
                DataType::Int32 => handle!(Int32),
                DataType::Int64 => handle!(Int64),
                DataType::UInt8 => handle!(UInt8),
                DataType::UInt16 => handle!(UInt16),
                DataType::UInt32 => handle!(UInt32),
                DataType::UInt64 => handle!(UInt64),
                DataType::Float32 => handle!(Float32),
                DataType::Float64 => handle!(Float64),
                DataType::Decimal128(precision, scale) => {
                    handle_decimal!(Decimal128, *precision, *scale)
                }
                DataType::Decimal256(precision, scale) => {
                    handle_decimal!(Decimal256, *precision, *scale)
                }
                dt => {
                    return Err(DataFusionError::Execution(format!(
                        "Unsupported data type: {:?}",
                        dt
                    )));
                }
            })
        }
    })
}

#[cfg(test)]
mod test {
    use crate::spark_null_if_zero::spark_null_if_zero;
    use arrow::array::{ArrayRef, Decimal128Array, Float32Array, Int32Array};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_null_if_zero_int() {
        let result = spark_null_if_zero(&vec![ColumnarValue::Array(Arc::new(Int32Array::from(
            vec![Some(1), None, Some(-1), Some(0)],
        )))])
        .unwrap()
        .into_array(4);

        let expected = Int32Array::from(vec![Some(1), None, Some(-1), None]);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
    }

    #[test]
    fn test_null_if_zero_decimal() {
        let result = spark_null_if_zero(&vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(1230427389124691),
            20,
            2,
        ))])
        .unwrap()
        .into_array(1);

        let expected = Decimal128Array::from(vec![Some(1230427389124691)])
            .with_precision_and_scale(20, 2)
            .unwrap();
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
    }

    #[test]
    fn test_null_if_zero_float() {
        let result = spark_null_if_zero(&vec![ColumnarValue::Scalar(ScalarValue::Float32(Some(
            0.0,
        )))])
        .unwrap()
        .into_array(1);

        let expected = Float32Array::from(vec![None]);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
    }
}
