use crate::conversion::*;
use mongodb::bson::RawBsonRef;
use num::traits::NumCast;
use polars::prelude::*;
use polars_arrow::array::{
    BinaryViewArray, BooleanArray, MutableBinaryViewArray, MutableBooleanArray,
    MutablePrimitiveArray, PrimitiveArray, Utf8ViewArray,
};

use polars_arrow::types::NativeType;
pub(crate) fn init_buffers(
    schema: &polars::prelude::Schema,
    capacity: usize,
) -> PolarsResult<PlIndexMap<PlSmallStr, Buffer<'_>>> {
    schema
        .iter()
        .map(|(name, dtype)| {
            let builder = match &dtype {
                DataType::Boolean => Buffer::Boolean(MutableBooleanArray::with_capacity(capacity)),
                DataType::Int32 => Buffer::Int32(MutablePrimitiveArray::with_capacity(capacity)),
                DataType::Int64 => Buffer::Int64(MutablePrimitiveArray::with_capacity(capacity)),
                DataType::UInt32 => Buffer::UInt32(MutablePrimitiveArray::with_capacity(capacity)),
                DataType::UInt64 => Buffer::UInt64(MutablePrimitiveArray::with_capacity(capacity)),
                DataType::Float32 => {
                    Buffer::Float32(MutablePrimitiveArray::with_capacity(capacity))
                }
                DataType::Float64 => {
                    Buffer::Float64(MutablePrimitiveArray::with_capacity(capacity))
                }
                DataType::String => Buffer::String(MutableBinaryViewArray::with_capacity(capacity)),
                DataType::Datetime(_, _) => {
                    Buffer::Datetime(MutablePrimitiveArray::with_capacity(capacity))
                }
                DataType::Date => Buffer::Date(MutablePrimitiveArray::with_capacity(capacity)),
                DataType::Binary => Buffer::Binary(MutableBinaryViewArray::with_capacity(capacity)),
                _ => Buffer::All(Vec::with_capacity(capacity)),
            };
            Ok((name.clone(), builder))
        })
        .collect()
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum Buffer<'a> {
    Boolean(MutableBooleanArray),
    Int32(MutablePrimitiveArray<i32>),
    Int64(MutablePrimitiveArray<i64>),
    UInt32(MutablePrimitiveArray<u32>),
    UInt64(MutablePrimitiveArray<u64>),
    Float32(MutablePrimitiveArray<f32>),
    Float64(MutablePrimitiveArray<f64>),
    String(MutableBinaryViewArray<str>),
    Datetime(MutablePrimitiveArray<i64>),
    Date(MutablePrimitiveArray<i32>),
    Binary(MutableBinaryViewArray<[u8]>),
    All(Vec<AnyValue<'a>>),
}

impl<'a> Buffer<'a> {
    pub(crate) fn into_series(self, name: PlSmallStr) -> PolarsResult<Series> {
        let s = match self {
            Buffer::Boolean(vals) => Series::from_array(name, BooleanArray::from(vals)),
            Buffer::Int32(vals) => Series::from_array(name, PrimitiveArray::from(vals)),
            Buffer::Int64(vals) => Series::from_array(name, PrimitiveArray::from(vals)),
            Buffer::UInt32(vals) => Series::from_array(name, PrimitiveArray::from(vals)),
            Buffer::UInt64(vals) => Series::from_array(name, PrimitiveArray::from(vals)),
            Buffer::Float32(vals) => Series::from_array(name, PrimitiveArray::from(vals)),
            Buffer::Float64(vals) => Series::from_array(name, PrimitiveArray::from(vals)),
            Buffer::Datetime(vals) => Series::from_array(name, PrimitiveArray::from(vals))
                .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?,
            Buffer::Date(vals) => {
                Series::from_array(name, PrimitiveArray::from(vals)).cast(&DataType::Date)?
            }
            Buffer::String(vals) => {
                let arr: Utf8ViewArray = vals.into();
                Series::from_array(name, arr)
            }
            Buffer::Binary(vals) => {
                let arr: BinaryViewArray = vals.into();
                Series::from_array(name, arr)
            }
            Buffer::All(vals) => Series::new(name.into(), vals),
        };
        Ok(s)
    }

    pub(crate) fn add_null(&mut self) {
        match self {
            Buffer::Boolean(v) => v.push(None),
            Buffer::Int32(v) => v.push(None),
            Buffer::Int64(v) => v.push(None),
            Buffer::UInt64(v) => v.push(None),
            Buffer::UInt32(v) => v.push(None),
            Buffer::Float32(v) => v.push(None),
            Buffer::Float64(v) => v.push(None),
            Buffer::String(v) => v.push(Some("")),
            Buffer::Datetime(v) => v.push(None),
            Buffer::Date(v) => v.push(None),
            Buffer::Binary(v) => v.push(Some(b"0x0")),
            Buffer::All(v) => v.push(AnyValue::Null),
        };
    }
    pub(crate) fn add(&mut self, value: RawBsonRef) -> PolarsResult<()> {
        use Buffer::*;
        match self {
            Boolean(buf) => {
                match value {
                    RawBsonRef::Boolean(v) => buf.push(Some(v)),
                    _ => buf.push(None),
                }
                Ok(())
            }
            Int32(buf) => {
                let n = deserialize_number::<i32>(value);
                buf.push(n);
                Ok(())
            }
            Int64(buf) => {
                let n = deserialize_number::<i64>(value);
                buf.push(n);
                Ok(())
            }
            UInt64(buf) => {
                let n = deserialize_number::<u64>(value);
                buf.push(n);
                Ok(())
            }
            UInt32(buf) => {
                let n = deserialize_number::<u32>(value);
                buf.push(n);
                Ok(())
            }
            Float32(buf) => {
                let n = deserialize_float::<f32>(value);
                buf.push(n);
                Ok(())
            }
            Float64(buf) => {
                let n = deserialize_float::<f64>(value);
                buf.push(n);
                Ok(())
            }

            String(buf) => {
                match value {
                    RawBsonRef::String(v) => buf.push(Some(v)),
                    RawBsonRef::Int32(v) => {
                        let mut buffer = itoa::Buffer::new();
                        let s = buffer.format(v);
                        buf.push_value(s);
                    }
                    RawBsonRef::Int64(v) => {
                        let mut buffer = itoa::Buffer::new();
                        let s = buffer.format(v);
                        buf.push_value(s);
                    }
                    RawBsonRef::Boolean(v) => buf.push(Some(if v { "true" } else { "false" })),
                    RawBsonRef::Document(doc) => buf.push(Some(format!("{:?}", doc))),
                    RawBsonRef::Array(arr) => buf.push(Some(format!("{:#?}", arr))),
                    RawBsonRef::Decimal128(v) => buf.push(Some(v.to_string())),
                    RawBsonRef::Double(v) => buf.push(Some(v.to_string())),
                    RawBsonRef::RegularExpression(r) => {
                        buf.push(Some(format!("/{}/{}", r.pattern, r.options)))
                    }
                    RawBsonRef::ObjectId(oid) => buf.push(Some(oid.to_hex())),
                    RawBsonRef::JavaScriptCode(v) => buf.push(Some(v)),
                    RawBsonRef::Symbol(s) => buf.push(Some(s)),
                    _ => buf.push::<std::string::String>(None),
                }
                Ok(())
            }
            Datetime(buf) => {
                let v = deserialize_date::<i64>(value);
                buf.push(v);
                Ok(())
            }
            Date(buf) => {
                let v = deserialize_date::<i32>(value);
                buf.push(v);
                Ok(())
            }
            Binary(buf) => {
                match value {
                    RawBsonRef::Binary(b) => buf.push(Some(&b.bytes)),
                    RawBsonRef::ObjectId(oid) => buf.push(Some(&oid.bytes())),
                    _ => buf.push::<&[u8]>(None),
                }
                Ok(())
            }
            All(buf) => {
                let av: Wrap<AnyValue> = value.into();
                buf.push(av.0);
                Ok(())
            }
        }
    }
}
fn deserialize_float<T: NativeType + NumCast>(value: RawBsonRef) -> Option<T> {
    match value {
        RawBsonRef::Double(num) => num::traits::cast::<f64, T>(num),
        RawBsonRef::Int32(num) => num::traits::cast::<i32, T>(num),
        RawBsonRef::Int64(num) => num::traits::cast::<i64, T>(num),
        RawBsonRef::Boolean(b) => num::traits::cast::<i32, T>(b as i32),
        _ => None,
    }
}

fn deserialize_number<T: NativeType + NumCast>(value: RawBsonRef) -> Option<T> {
    match value {
        RawBsonRef::Double(num) => num::traits::cast::<f64, T>(num),
        RawBsonRef::Int32(num) => num::traits::cast::<i32, T>(num),
        RawBsonRef::Int64(num) => num::traits::cast::<i64, T>(num),
        RawBsonRef::Boolean(b) => num::traits::cast::<i32, T>(b as i32),
        _ => None,
    }
}

fn deserialize_date<T: NativeType + NumCast>(value: RawBsonRef) -> Option<T> {
    match value {
        RawBsonRef::Double(num) => num::traits::cast::<f64, T>(num),
        RawBsonRef::Int32(num) => num::traits::cast::<i32, T>(num),
        RawBsonRef::Int64(num) => num::traits::cast::<i64, T>(num),
        RawBsonRef::Boolean(b) => num::traits::cast::<i32, T>(b as i32),
        RawBsonRef::DateTime(dt) => num::traits::cast::<i64, T>(dt.timestamp_millis()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_double() {
        let d = RawBsonRef::Double(314159.265359);
        assert_eq!(deserialize_number(d), Some(314159.265359));
    }
    #[test]
    fn deserialize_i32() {
        let d = RawBsonRef::Int32(314159);
        assert_eq!(deserialize_number(d), Some(314159i32));
    }
    #[test]
    fn deserialize_i64() {
        let d = RawBsonRef::Int64(314159);
        assert_eq!(deserialize_number(d), Some(314159i64));
    }
    #[test]
    fn deserialize_bool_true() {
        let d = RawBsonRef::Boolean(true);
        assert_eq!(deserialize_number(d), Some(1));
    }
    #[test]
    fn deserialize_bool_false() {
        let d = RawBsonRef::Boolean(false);
        assert_eq!(deserialize_number(d), Some(0));
    }
    #[test]
    fn test_deserialize_date() {
        use mongodb::bson::DateTime as MongoDateTime;
        let millis = 1704067200000i64;
        let dt = RawBsonRef::DateTime(MongoDateTime::from_millis(millis));

        assert_eq!(deserialize_date::<i64>(dt), Some(millis));

        let d_num = RawBsonRef::Int64(millis);
        assert_eq!(deserialize_date::<i64>(d_num), Some(millis));

        let small_millis = 12345678i64;
        let dt_small = RawBsonRef::DateTime(MongoDateTime::from_millis(small_millis));
        assert_eq!(deserialize_date::<i32>(dt_small), Some(12345678i32));

        let invalid = RawBsonRef::String("2024-01-01");
        assert_eq!(deserialize_date::<i64>(invalid), None);
    }
    //     #[test]
    //     fn test_bool_buffer() -> Result<(), Box<dyn std::error::Error>> {
    //         let mut buf = Buffer::Boolean(BooleanChunkedBuilder::new(PlSmallStr::from_str("name"), 3));
    //         buf.add(RawBsonRef::Boolean(false))?;
    //         buf.add(RawBsonRef::Boolean(false))?;
    //         buf.add(RawBsonRef::Boolean(true))?;

    //         assert_eq!(
    //             buf.into_series()?,
    //             Series::from_vec(PlSmallStr::from_str("name"), vec![0, 0, 1])
    //                 .cast(&DataType::Boolean)?
    //         );
    //         Ok(())
    //     }
    //     #[test]
    //     fn test_i32_buffer() -> Result<(), Box<dyn std::error::Error>> {
    //         let mut buf = Buffer::Int32(PrimitiveChunkedBuilder::new(
    //             PlSmallStr::from_str("name"),
    //             3,
    //         ));
    //         buf.add(RawBsonRef::Int32(i32::MIN))?;
    //         buf.add(RawBsonRef::Int32((i32::MIN + i32::MAX) / 2))?;
    //         buf.add(RawBsonRef::Int32(i32::MAX))?;

    //         assert_eq!(
    //             buf.into_series()?,
    //             Series::from_vec(
    //                 PlSmallStr::from_str("name"),
    //                 vec![i32::MIN, ((i32::MIN + i32::MAX) / 2), i32::MAX]
    //             )
    //         );
    //         Ok(())
    //     }
    //     #[test]
    //     fn test_i64_buffer() -> Result<(), Box<dyn std::error::Error>> {
    //         let mut buf = Buffer::Int64(PrimitiveChunkedBuilder::new(
    //             PlSmallStr::from_str("name"),
    //             3,
    //         ));
    //         buf.add(RawBsonRef::Int64(i64::MIN))?;
    //         buf.add(RawBsonRef::Int64((i64::MIN + i64::MAX) / 2))?;
    //         buf.add(RawBsonRef::Int64(i64::MAX))?;

    //         assert_eq!(
    //             buf.into_series()?,
    //             Series::from_vec(
    //                 PlSmallStr::from_str("name"),
    //                 vec![i64::MIN, ((i64::MIN + i64::MAX) / 2), i64::MAX]
    //             )
    //         );
    //         Ok(())
    //     }
    //     #[test]
    //     fn test_buffer_null_handling() -> PolarsResult<()> {
    //         let mut buf = Buffer::Int32(PrimitiveChunkedBuilder::new(
    //             PlSmallStr::from_str("name"),
    //             3,
    //         ));

    //         buf.add(RawBsonRef::Int32(10))?;
    //         buf.add_null();
    //         buf.add(RawBsonRef::Null)?;

    //         let series = buf.into_series()?;
    //         assert_eq!(series.null_count(), 2);
    //         assert_eq!(series.len(), 3);
    //         assert_eq!(
    //             series,
    //             Series::new(PlSmallStr::from_str("name"), vec![Some(10i32), None, None])
    //         );
    //         Ok(())
    //     }
    //     #[test]
    //     fn test_buffer_type_mismatch_safety() -> PolarsResult<()> {
    //         let mut buf = Buffer::Int32(PrimitiveChunkedBuilder::new(PlSmallStr::from_str("age"), 1));
    //         buf.add(RawBsonRef::String("thirty"))?;
    //         let series = buf.into_series()?;
    //         assert!(series.is_null().get(0).unwrap()); // Should be null, not a crash
    //         Ok(())
    //     }
    //     #[test]
    //     fn test_binary_buffer() -> PolarsResult<()> {
    //         let mut buf = Buffer::Binary(BinaryChunkedBuilder::new(PlSmallStr::from_str("data"), 2));
    //         let raw_bytes = b"ferris";
    //         buf.add(RawBsonRef::Binary(mongodb::bson::RawBinaryRef {
    //             subtype: mongodb::bson::spec::BinarySubtype::Generic,
    //             bytes: raw_bytes.as_slice(),
    //         }))?;

    //         let series = buf.into_series()?;
    //         assert_eq!(series.dtype(), &DataType::Binary);
    //         assert_eq!(series.len(), 1);
    //         let val = series.get(0).unwrap();
    //         if let AnyValue::Binary(b) = val {
    //             assert_eq!(b, raw_bytes);
    //         } else {
    //             panic!("Expected binary value");
    //         }
    //         Ok(())
    //     }

    //     #[test]
    //     fn test_object_id_to_binary_buffer() -> PolarsResult<()> {
    //         let mut buf = Buffer::Binary(BinaryChunkedBuilder::new(PlSmallStr::from_str("oid"), 1));
    //         let oid = mongodb::bson::oid::ObjectId::new();
    //         buf.add(RawBsonRef::ObjectId(oid))?;
    //         let series = buf.into_series()?;
    //         assert_eq!(series.dtype(), &DataType::Binary);
    //         let val = series.get(0).unwrap();
    //         if let AnyValue::Binary(b) = val {
    //             assert_eq!(b, &oid.bytes());
    //         } else {
    //             panic!("Expected binary value for ObjectId");
    //         }
    //         Ok(())
    //     }
}
