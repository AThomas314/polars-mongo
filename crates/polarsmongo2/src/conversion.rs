use polars::prelude::*;

use mongodb::bson::{RawBson, RawBsonRef, RawDocument};

#[derive(Debug)]
#[repr(transparent)]
pub struct Wrap<T>(pub T);

impl<T> Clone for Wrap<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Wrap(self.0.clone())
    }
}
impl<T> From<T> for Wrap<T> {
    fn from(t: T) -> Self {
        Wrap(t)
    }
}

impl From<&RawDocument> for Wrap<DataType> {
    fn from(doc: &RawDocument) -> Self {
        let fields = doc.iter().map(|x| x.unwrap()).map(|(key, value)| {
            let dtype: Wrap<DataType> = value.into();
            Field::new(key.into(), dtype.0)
        });
        DataType::Struct(fields.collect()).into()
    }
}

impl From<RawBsonRef<'_>> for Wrap<DataType> {
    fn from(bson: RawBsonRef) -> Self {
        let dt = match bson {
            RawBsonRef::Double(_) => DataType::Float64,
            RawBsonRef::String(_) => DataType::String,

            RawBsonRef::Array(arr) => {
                use polars::frame::row::coerce_dtype;

                let dtypes: Vec<_> = arr
                    .into_iter()
                    .map(|doc| {
                        let dt: Self = doc.unwrap().into();
                        dt.0
                    })
                    .collect();
                let dtype = if dtypes.is_empty() {
                    DataType::Null
                } else {
                    coerce_dtype(&dtypes)
                };
                DataType::List(Box::new(dtype))
            }
            RawBsonRef::Boolean(_) => DataType::Boolean,
            RawBsonRef::Null => DataType::Null,
            RawBsonRef::Int32(_) => DataType::Int32,
            RawBsonRef::Int64(_) => DataType::Int64,
            RawBsonRef::Timestamp(_) => DataType::String,
            RawBsonRef::Document(doc) => return doc.into(),
            RawBsonRef::DateTime(_) => DataType::Datetime(TimeUnit::Milliseconds, None),
            RawBsonRef::ObjectId(_) => DataType::Binary,
            RawBsonRef::Symbol(_) => DataType::String,
            RawBsonRef::Undefined => DataType::Unknown(UnknownKind::Any),
            RawBsonRef::Binary(_) => DataType::Binary,
            _ => DataType::String,
        };
        Wrap(dt)
    }
}

impl<'a> From<RawBsonRef<'_>> for Wrap<AnyValue<'a>> {
    fn from(bson: RawBsonRef) -> Self {
        let dt = match bson {
            RawBsonRef::Double(v) => AnyValue::Float64(v),
            RawBsonRef::String(v) => AnyValue::StringOwned(v.into()),
            RawBsonRef::Array(arr) => {
                let vals: Vec<Wrap<AnyValue>> =
                    arr.into_iter().map(|v| v.unwrap().into()).collect();
                // Wrap is transparent, so this is safe
                let vals = unsafe { std::mem::transmute::<_, Vec<AnyValue>>(vals) };
                let s = Series::new("".into(), vals);
                AnyValue::List(s)
            }
            RawBsonRef::Boolean(b) => AnyValue::Boolean(b),
            RawBsonRef::Null | RawBsonRef::Undefined => AnyValue::Null,
            RawBsonRef::Int32(v) => AnyValue::Int32(v),
            RawBsonRef::Int64(v) => AnyValue::Int64(v),
            RawBsonRef::Timestamp(v) => AnyValue::StringOwned(format!("{:#?}", v).into()),
            RawBsonRef::DateTime(dt) => {
                AnyValue::Datetime(dt.timestamp_millis(), TimeUnit::Milliseconds, None)
            }
            RawBsonRef::Binary(b) => AnyValue::BinaryOwned(b.bytes.to_vec()),
            RawBsonRef::ObjectId(oid) => AnyValue::BinaryOwned(oid.bytes().to_vec()),
            RawBsonRef::Symbol(s) => AnyValue::StringOwned(s.into()),
            v => AnyValue::StringOwned(format!("{:#?}", v).into()),
        };
        Wrap(dt)
    }
}

impl<'a, 'b> From<&'b RawBson> for Wrap<AnyValue<'a>> {
    fn from(bson: &'b RawBson) -> Self {
        let dt = match bson {
            RawBson::Double(v) => AnyValue::Float64(*v),
            RawBson::String(v) => AnyValue::StringOwned(v.into()),
            RawBson::Array(arr) => {
                let vals: Vec<Wrap<AnyValue>> =
                    arr.into_iter().map(|v| v.unwrap().into()).collect();
                // Wrap is transparent, so this is safe
                let vals = unsafe { std::mem::transmute::<_, Vec<AnyValue>>(vals) };
                let s = Series::new("".into(), vals);
                AnyValue::List(s)
            }
            RawBson::Boolean(b) => AnyValue::Boolean(*b),
            RawBson::Null | RawBson::Undefined => AnyValue::Null,
            RawBson::Int32(v) => AnyValue::Int32(*v),
            RawBson::Int64(v) => AnyValue::Int64(*v),
            RawBson::Timestamp(v) => AnyValue::StringOwned(format!("{:#?}", v).into()),
            RawBson::Binary(b) => AnyValue::BinaryOwned(b.bytes.clone()),
            RawBson::DateTime(dt) => {
                AnyValue::Datetime(dt.timestamp_millis(), TimeUnit::Milliseconds, None)
            }
            RawBson::Document(doc) => {
                let vals: (Vec<AnyValue>, Vec<Field>) = doc
                    .into_iter()
                    .map(|x| x.unwrap())
                    .map(|(key, value)| {
                        let dt: Wrap<DataType> = value.into();
                        let fld = Field::new(key.into(), dt.0);
                        let av: Wrap<AnyValue<'a>> = value.into();
                        (av.0, fld)
                    })
                    .unzip();

                AnyValue::StructOwned(Box::new(vals))
            }
            RawBson::ObjectId(oid) => AnyValue::BinaryOwned(oid.bytes().to_vec()),
            RawBson::Symbol(s) => AnyValue::StringOwned(s.into()),
            v => AnyValue::StringOwned(format!("{:#?}", v).into()),
        };
        Wrap(dt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{
        DateTime as MongoDateTime, Document, RawArrayBuf, RawDocumentBuf, doc, oid::ObjectId,
    };

    #[test]
    fn test_dtype_primitives() {
        let cases = vec![
            (RawBson::Int32(1), DataType::Int32),
            (RawBson::Int64(1), DataType::Int64),
            (RawBson::Double(1.0), DataType::Float64),
            (RawBson::Boolean(true), DataType::Boolean),
            (RawBson::String("test".into()), DataType::String),
            (RawBson::Null, DataType::Null),
            (
                RawBson::DateTime(MongoDateTime::from_millis(1000)),
                DataType::Datetime(TimeUnit::Milliseconds, None),
            ),
            (
                RawBson::Binary(mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: b"123".to_vec(),
                }),
                DataType::Binary,
            ),
            (RawBson::ObjectId(ObjectId::new()), DataType::Binary),
        ];

        for (bson_val, expected_dt) in cases {
            let dt: Wrap<DataType> = (bson_val.as_raw_bson_ref()).into();
            assert_eq!(dt.0, expected_dt, "Failed on {:?}", bson_val);
        }
    }

    #[test]
    fn test_dtype_document() -> Result<(), Box<dyn std::error::Error>> {
        let d = doc! {
            "name": "Alice",
            "age": 30i32
        };
        let doc = RawBson::Document(RawDocumentBuf::from_document(&d)?);
        let dt: Wrap<DataType> = (doc.as_raw_bson_ref()).into();

        let expected = DataType::Struct(vec![
            Field::new("name".into(), DataType::String),
            Field::new("age".into(), DataType::Int32),
        ]);

        assert_eq!(dt.0, expected);
        Ok(())
    }

    #[test]
    fn test_dtype_array_coercion() {
        // Empty array -> Null List
        let empty_arr = RawBson::Array(RawArrayBuf::new());
        assert_eq!(
            Wrap::<DataType>::from(empty_arr.as_raw_bson_ref()).0,
            DataType::List(Box::new(DataType::Null))
        );

        let int_arr = RawBson::Array(RawArrayBuf::from_iter(vec![
            RawBson::Int32(1),
            RawBson::Int32(2),
        ]));
        assert_eq!(
            Wrap::<DataType>::from(int_arr.as_raw_bson_ref()).0,
            DataType::List(Box::new(DataType::Int32))
        );

        let mixed_arr = RawBson::Array(RawArrayBuf::from_iter(vec![
            RawBson::Int32(1),
            RawBson::Double(2.5),
        ]));
        assert_eq!(
            Wrap::<DataType>::from(mixed_arr.as_raw_bson_ref()).0,
            DataType::List(Box::new(DataType::Float64))
        );
    }

    #[test]
    fn test_anyvalue_primitives_borrowed() {
        assert_eq!(
            Wrap::<AnyValue>::from(&RawBson::Int32(42)).0,
            AnyValue::Int32(42)
        );
        assert_eq!(
            Wrap::<AnyValue>::from(&RawBson::Int64(42)).0,
            AnyValue::Int64(42)
        );
        assert_eq!(
            Wrap::<AnyValue>::from(&RawBson::Double(3.14)).0,
            AnyValue::Float64(3.14)
        );
        assert_eq!(
            Wrap::<AnyValue>::from(&RawBson::Boolean(true)).0,
            AnyValue::Boolean(true)
        );
        assert_eq!(Wrap::<AnyValue>::from(&RawBson::Null).0, AnyValue::Null);
        let oid = mongodb::bson::oid::ObjectId::new();
        assert_eq!(
            Wrap::<AnyValue>::from(&RawBson::ObjectId(oid)).0,
            AnyValue::BinaryOwned(oid.bytes().to_vec())
        );
        let bytes = b"bytes".to_vec();
        let b_bin = RawBson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: bytes.clone(),
        });
        assert_eq!(
            Wrap::<AnyValue>::from(&b_bin).0,
            AnyValue::BinaryOwned(bytes)
        );
    }

    #[test]
    fn test_anyvalue_string_borrowed() {
        let b_str = RawBson::String("hello".into());
        let av: Wrap<AnyValue> = (&b_str).into();

        match av.0 {
            AnyValue::StringOwned(s) => assert_eq!(s.as_str(), "hello"),
            _ => panic!("Expected StringOwned"),
        }
    }

    #[test]
    fn test_anyvalue_datetime() {
        let millis = 1704067200000i64;
        let b_dt = RawBson::DateTime(MongoDateTime::from_millis(millis));

        let av: Wrap<AnyValue> = (&b_dt).into();
        assert_eq!(
            av.0,
            AnyValue::Datetime(millis, TimeUnit::Milliseconds, None)
        );
    }

    #[test]
    fn test_anyvalue_object_id() {
        let oid = ObjectId::new();
        let b_oid = RawBson::ObjectId(oid);

        let av: Wrap<AnyValue> = (&b_oid).into();
        match av.0 {
            AnyValue::BinaryOwned(s) => assert_eq!(s, oid.bytes()),
            _ => panic!("Expected BinaryOwned for ObjectId"),
        }
    }

    #[test]
    fn test_anyvalue_array_transmute() {
        let arr_buf =
            RawArrayBuf::from_iter(vec![RawBson::Int32(1), RawBson::Int32(2)].into_iter());
        let arr = RawBson::Array(arr_buf);
        let av: Wrap<AnyValue> = (&arr).into();

        match av.0 {
            AnyValue::List(series) => {
                assert_eq!(series.len(), 2);
                assert_eq!(series.dtype(), &DataType::Int32);
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_anyvalue_document_to_struct() -> Result<(), Box<dyn std::error::Error>> {
        let doc: Document = doc! {
            "score": 100i32
        };
        let doc = RawBson::Document(RawDocumentBuf::from_document(&doc)?);

        let av: Wrap<AnyValue> = (&doc).into();

        match av.0 {
            AnyValue::StructOwned(box_tuple) => {
                let (vals, fields) = *box_tuple;
                assert_eq!(vals.len(), 1);
                assert_eq!(vals[0], AnyValue::Int32(100));

                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name().as_str(), "score");
                assert_eq!(fields[0].dtype(), &DataType::Int32);
            }
            _ => panic!("Expected StructOwned"),
        }
        Ok(())
    }
}
