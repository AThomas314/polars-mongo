use crate::conversion::*;
use mongodb::bson::Bson;
use num::traits::NumCast;
use polars::prelude::*;
use polars_arrow::types::NativeType;

pub(crate) fn init_buffers(
    schema: &polars::prelude::Schema,
    capacity: usize,
) -> PolarsResult<PlIndexMap<PlSmallStr, Buffer<'_>>> {
    schema
        .iter()
        .map(|(name, dtype)| {
            let builder = match &dtype {
                DataType::Boolean => {
                    Buffer::Boolean(BooleanChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::Int32 => {
                    Buffer::Int32(PrimitiveChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::Int64 => {
                    Buffer::Int64(PrimitiveChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::UInt32 => {
                    Buffer::UInt32(PrimitiveChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::UInt64 => {
                    Buffer::UInt64(PrimitiveChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::Float32 => {
                    Buffer::Float32(PrimitiveChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::Float64 => {
                    Buffer::Float64(PrimitiveChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::String => {
                    Buffer::String(StringChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::Datetime(_, _) => {
                    Buffer::Datetime(PrimitiveChunkedBuilder::new(name.clone(), capacity))
                }
                DataType::Date => {
                    Buffer::Date(PrimitiveChunkedBuilder::new(name.clone(), capacity))
                }
                _ => Buffer::All((Vec::with_capacity(capacity), name.as_str())),
            };
            Ok((name.clone(), builder))
        })
        .collect()
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum Buffer<'a> {
    Boolean(BooleanChunkedBuilder),
    Int32(PrimitiveChunkedBuilder<Int32Type>),
    Int64(PrimitiveChunkedBuilder<Int64Type>),
    UInt32(PrimitiveChunkedBuilder<UInt32Type>),
    UInt64(PrimitiveChunkedBuilder<UInt64Type>),
    Float32(PrimitiveChunkedBuilder<Float32Type>),
    Float64(PrimitiveChunkedBuilder<Float64Type>),
    String(StringChunkedBuilder),
    Datetime(PrimitiveChunkedBuilder<Int64Type>),
    Date(PrimitiveChunkedBuilder<Int32Type>),
    All((Vec<AnyValue<'a>>, &'a str)),
}

impl<'a> Buffer<'a> {
    pub(crate) fn into_series(self) -> PolarsResult<Series> {
        let s = match self {
            Buffer::Boolean(v) => v.finish().into_series(),
            Buffer::Int32(v) => v.finish().into_series(),
            Buffer::Int64(v) => v.finish().into_series(),
            Buffer::UInt32(v) => v.finish().into_series(),
            Buffer::UInt64(v) => v.finish().into_series(),
            Buffer::Float32(v) => v.finish().into_series(),
            Buffer::Float64(v) => v.finish().into_series(),
            Buffer::Datetime(v) => v
                .finish()
                .into_series()
                .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                .unwrap(),
            Buffer::Date(v) => v.finish().into_series().cast(&DataType::Date).unwrap(),
            Buffer::String(v) => v.finish().into_series(),
            Buffer::All((vals, name)) => Series::new(name.into(), vals),
        };
        Ok(s)
    }

    pub(crate) fn add_null(&mut self) {
        match self {
            Buffer::Boolean(v) => v.append_null(),
            Buffer::Int32(v) => v.append_null(),
            Buffer::Int64(v) => v.append_null(),
            Buffer::UInt32(v) => v.append_null(),
            Buffer::UInt64(v) => v.append_null(),
            Buffer::Float32(v) => v.append_null(),
            Buffer::Float64(v) => v.append_null(),
            Buffer::String(v) => v.append_null(),
            Buffer::Datetime(v) => v.append_null(),
            Buffer::Date(v) => v.append_null(),
            Buffer::All((v, _)) => v.push(AnyValue::Null),
        };
    }
    pub(crate) fn add(&mut self, value: &Bson) -> PolarsResult<()> {
        use Buffer::*;
        match self {
            Boolean(buf) => {
                match value {
                    Bson::Boolean(v) => buf.append_value(*v),
                    _ => buf.append_null(),
                }
                Ok(())
            }
            Int32(buf) => {
                let n = deserialize_number::<i32>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            Int64(buf) => {
                let n = deserialize_number::<i64>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            UInt64(buf) => {
                let n = deserialize_number::<u64>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            UInt32(buf) => {
                let n = deserialize_number::<u32>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            Float32(buf) => {
                let n = deserialize_float::<f32>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            Float64(buf) => {
                let n = deserialize_float::<f64>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }

            String(buf) => {
                match value {
                    Bson::Int32(v) => buf.append_value(v.to_string()),
                    Bson::Int64(v) => buf.append_value(v.to_string()),
                    Bson::Decimal128(v) => buf.append_value(v.to_string()),
                    Bson::Boolean(v) => buf.append_value(v.to_string()),
                    Bson::Double(v) => buf.append_value(v.to_string()),
                    Bson::RegularExpression(r) => buf.append_value(r.to_string()),
                    Bson::ObjectId(oid) => buf.append_value(oid.to_hex()),
                    Bson::JavaScriptCode(v) => buf.append_value(v),
                    Bson::String(v) => buf.append_value(v),
                    Bson::Document(doc) => buf.append_value(doc.to_string()),
                    Bson::Array(arr) => buf.append_value(format!("{:#?}", arr)),
                    Bson::Symbol(s) => buf.append_value(s),
                    _ => buf.append_null(),
                }
                Ok(())
            }
            Datetime(buf) => {
                let v = deserialize_date::<i64>(value);
                buf.append_option(v);
                Ok(())
            }
            Date(buf) => {
                let v = deserialize_date::<i32>(value);
                buf.append_option(v);
                Ok(())
            }
            All((buf, _)) => {
                let av: Wrap<AnyValue> = value.into();
                buf.push(av.0);
                Ok(())
            }
        }
    }
}
fn deserialize_float<T: NativeType + NumCast>(value: &Bson) -> Option<T> {
    match value {
        Bson::Double(num) => num::traits::cast::<f64, T>(*num),
        Bson::Int32(num) => num::traits::cast::<i32, T>(*num),
        Bson::Int64(num) => num::traits::cast::<i64, T>(*num),
        Bson::Boolean(b) => num::traits::cast::<i32, T>(*b as i32),
        _ => None,
    }
}

fn deserialize_number<T: NativeType + NumCast>(value: &Bson) -> Option<T> {
    match value {
        Bson::Double(num) => num::traits::cast::<f64, T>(*num),
        Bson::Int32(num) => num::traits::cast::<i32, T>(*num),
        Bson::Int64(num) => num::traits::cast::<i64, T>(*num),
        Bson::Boolean(b) => num::traits::cast::<i32, T>(*b as i32),
        _ => None,
    }
}

fn deserialize_date<T: NativeType + NumCast>(value: &Bson) -> Option<T> {
    match value {
        Bson::Double(num) => num::traits::cast::<f64, T>(*num),
        Bson::Int32(num) => num::traits::cast::<i32, T>(*num),
        Bson::Int64(num) => num::traits::cast::<i64, T>(*num),
        Bson::Boolean(b) => num::traits::cast::<i32, T>(*b as i32),
        Bson::DateTime(dt) => num::traits::cast::<i64, T>(dt.timestamp_millis()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::i32;

    use super::*;

    #[test]
    fn deserialize_double() {
        let d = Bson::Double(314159.265359);
        assert_eq!(deserialize_number(&d), Some(314159.265359));
    }
    #[test]
    fn deserialize_i32() {
        let d = Bson::Int32(314159);
        assert_eq!(deserialize_number(&d), Some(314159i32));
    }
    #[test]
    fn deserialize_i64() {
        let d = Bson::Int64(314159);
        assert_eq!(deserialize_number(&d), Some(314159i64));
    }
    #[test]
    fn deserialize_bool_true() {
        let d = Bson::Boolean(true);
        assert_eq!(deserialize_number(&d), Some(1));
    }
    #[test]
    fn deserialize_bool_false() {
        let d = Bson::Boolean(false);
        assert_eq!(deserialize_number(&d), Some(0));
    }
    #[test]
    fn test_deserialize_date() {
        use mongodb::bson::DateTime as MongoDateTime;
        let millis = 1704067200000i64;
        let dt = Bson::DateTime(MongoDateTime::from_millis(millis));

        assert_eq!(deserialize_date::<i64>(&dt), Some(millis));

        let d_num = Bson::Int64(millis);
        assert_eq!(deserialize_date::<i64>(&d_num), Some(millis));

        let small_millis = 12345678i64;
        let dt_small = Bson::DateTime(MongoDateTime::from_millis(small_millis));
        assert_eq!(deserialize_date::<i32>(&dt_small), Some(12345678i32));

        let invalid = Bson::String("2024-01-01".to_string());
        assert_eq!(deserialize_date::<i64>(&invalid), None);
    }
    #[test]
    fn test_bool_buffer() -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = Buffer::Boolean(BooleanChunkedBuilder::new(PlSmallStr::from_str("name"), 3));
        buf.add(&Bson::Boolean(false))?;
        buf.add(&Bson::Boolean(false))?;
        buf.add(&Bson::Boolean(true))?;

        assert_eq!(
            buf.into_series()?,
            Series::from_vec(PlSmallStr::from_str("name"), vec![0, 0, 1])
                .cast(&DataType::Boolean)?
        );
        Ok(())
    }
    #[test]
    fn test_i32_buffer() -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = Buffer::Int32(PrimitiveChunkedBuilder::new(
            PlSmallStr::from_str("name"),
            3,
        ));
        buf.add(&Bson::Int32(i32::MIN))?;
        buf.add(&Bson::Int32((i32::MIN + i32::MAX) / 2))?;
        buf.add(&Bson::Int32(i32::MAX))?;

        assert_eq!(
            buf.into_series()?,
            Series::from_vec(
                PlSmallStr::from_str("name"),
                vec![i32::MIN, ((i32::MIN + i32::MAX) / 2), i32::MAX]
            )
        );
        Ok(())
    }
    #[test]
    fn test_i64_buffer() -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = Buffer::Int64(PrimitiveChunkedBuilder::new(
            PlSmallStr::from_str("name"),
            3,
        ));
        buf.add(&Bson::Int64(i64::MIN))?;
        buf.add(&Bson::Int64((i64::MIN + i64::MAX) / 2))?;
        buf.add(&Bson::Int64(i64::MAX))?;

        assert_eq!(
            buf.into_series()?,
            Series::from_vec(
                PlSmallStr::from_str("name"),
                vec![i64::MIN, ((i64::MIN + i64::MAX) / 2), i64::MAX]
            )
        );
        Ok(())
    }
    #[test]
    fn test_buffer_null_handling() -> PolarsResult<()> {
        let mut buf = Buffer::Int32(PrimitiveChunkedBuilder::new(
            PlSmallStr::from_str("name"),
            3,
        ));

        buf.add(&Bson::Int32(10))?;
        buf.add_null();
        buf.add(&Bson::Null)?;

        let series = buf.into_series()?;
        assert_eq!(series.null_count(), 2);
        assert_eq!(series.len(), 3);
        assert_eq!(
            series,
            Series::new(PlSmallStr::from_str("name"), vec![Some(10i32), None, None])
        );
        Ok(())
    }
    #[test]
    fn test_buffer_type_mismatch_safety() -> PolarsResult<()> {
        let mut buf = Buffer::Int32(PrimitiveChunkedBuilder::new(PlSmallStr::from_str("age"), 1));

        // Schema expects Int32, but we give it a String
        buf.add(&Bson::String("thirty".to_string()))?;

        let series = buf.into_series()?;
        assert!(series.is_null().get(0).unwrap()); // Should be null, not a crash
        Ok(())
    }
}
