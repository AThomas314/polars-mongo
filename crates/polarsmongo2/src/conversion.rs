use polars::prelude::*;

use mongodb::bson::{Bson, Document};

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

impl From<&Document> for Wrap<DataType> {
    fn from(doc: &Document) -> Self {
        let fields = doc.iter().map(|(key, value)| {
            let dtype: Wrap<DataType> = value.into();
            Field::new(key.into(), dtype.0)
        });
        DataType::Struct(fields.collect()).into()
    }
}

impl From<&Bson> for Wrap<DataType> {
    fn from(bson: &Bson) -> Self {
        let dt = match bson {
            Bson::Double(_) => DataType::Float64,
            Bson::String(_) => DataType::String,

            Bson::Array(arr) => {
                use polars::frame::row::coerce_dtype;

                let dtypes: Vec<_> = arr
                    .iter()
                    .map(|doc| {
                        let dt: Self = doc.into();
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
            Bson::Boolean(_) => DataType::Boolean,
            Bson::Null => DataType::Null,
            Bson::Int32(_) => DataType::Int32,
            Bson::Int64(_) => DataType::Int64,
            Bson::Timestamp(_) => DataType::String,
            Bson::Document(doc) => return doc.into(),
            Bson::DateTime(_) => DataType::Datetime(TimeUnit::Milliseconds, None),
            Bson::ObjectId(_) => DataType::Binary,
            Bson::Symbol(_) => DataType::String,
            Bson::Undefined => DataType::Unknown(UnknownKind::Any),
            Bson::Binary(_) => DataType::Binary,
            _ => DataType::String,
        };
        Wrap(dt)
    }
}

impl<'a> From<Bson> for Wrap<AnyValue<'a>> {
    fn from(bson: Bson) -> Self {
        let dt = match bson {
            Bson::Double(v) => AnyValue::Float64(v),
            Bson::String(v) => AnyValue::StringOwned(v.into()),
            Bson::Array(arr) => {
                let vals: Vec<Wrap<AnyValue>> = arr.iter().map(|v| v.into()).collect();
                // Wrap is transparent, so this is safe
                let vals = unsafe { std::mem::transmute::<_, Vec<AnyValue>>(vals) };
                let s = Series::new("".into(), vals);
                AnyValue::List(s)
            }
            Bson::Boolean(b) => AnyValue::Boolean(b),
            Bson::Null | Bson::Undefined => AnyValue::Null,
            Bson::Int32(v) => AnyValue::Int32(v),
            Bson::Int64(v) => AnyValue::Int64(v),
            Bson::Timestamp(v) => AnyValue::StringOwned(format!("{:#?}", v).into()),
            Bson::DateTime(dt) => {
                AnyValue::Datetime(dt.timestamp_millis(), TimeUnit::Milliseconds, None)
            }
            Bson::Binary(b) => AnyValue::BinaryOwned(b.bytes),
            Bson::ObjectId(oid) => AnyValue::BinaryOwned(oid.bytes().to_vec()),
            Bson::Symbol(s) => AnyValue::StringOwned(s.into()),
            v => AnyValue::StringOwned(format!("{:#?}", v).into()),
        };
        Wrap(dt)
    }
}

impl<'a, 'b> From<&'b Bson> for Wrap<AnyValue<'a>> {
    fn from(bson: &'b Bson) -> Self {
        let dt = match bson {
            Bson::Double(v) => AnyValue::Float64(*v),
            Bson::String(v) => AnyValue::StringOwned(v.into()),
            Bson::Array(arr) => {
                let vals: Vec<Wrap<AnyValue>> = arr.iter().map(|v| v.into()).collect();
                // Wrap is transparent, so this is safe
                let vals = unsafe { std::mem::transmute::<_, Vec<AnyValue>>(vals) };
                let s = Series::new("".into(), vals);
                AnyValue::List(s)
            }
            Bson::Boolean(b) => AnyValue::Boolean(*b),
            Bson::Null | Bson::Undefined => AnyValue::Null,
            Bson::Int32(v) => AnyValue::Int32(*v),
            Bson::Int64(v) => AnyValue::Int64(*v),
            Bson::Timestamp(v) => AnyValue::StringOwned(format!("{:#?}", v).into()),
            Bson::Binary(b) => AnyValue::BinaryOwned(b.bytes.clone()),
            Bson::DateTime(dt) => {
                AnyValue::Datetime(dt.timestamp_millis(), TimeUnit::Milliseconds, None)
            }
            Bson::Document(doc) => {
                let vals: (Vec<AnyValue>, Vec<Field>) = doc
                    .into_iter()
                    .map(|(key, value)| {
                        let dt: Wrap<DataType> = value.into();
                        let fld = Field::new(key.into(), dt.0);
                        let av: Wrap<AnyValue<'a>> = value.into();
                        (av.0, fld)
                    })
                    .unzip();

                AnyValue::StructOwned(Box::new(vals))
            }
            Bson::ObjectId(oid) => AnyValue::BinaryOwned(oid.bytes().to_vec()),
            Bson::Symbol(s) => AnyValue::StringOwned(s.into()),
            v => AnyValue::StringOwned(format!("{:#?}", v).into()),
        };
        Wrap(dt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{DateTime as MongoDateTime, doc, oid::ObjectId};

    #[test]
    fn test_dtype_primitives() {
        let cases = vec![
            (Bson::Int32(1), DataType::Int32),
            (Bson::Int64(1), DataType::Int64),
            (Bson::Double(1.0), DataType::Float64),
            (Bson::Boolean(true), DataType::Boolean),
            (Bson::String("test".into()), DataType::String),
            (Bson::Null, DataType::Null),
            (
                Bson::DateTime(MongoDateTime::from_millis(1000)),
                DataType::Datetime(TimeUnit::Milliseconds, None),
            ),
            (
                Bson::Binary(mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: b"123".to_vec(),
                }),
                DataType::Binary,
            ),
            (Bson::ObjectId(ObjectId::new()), DataType::Binary),
        ];

        for (bson_val, expected_dt) in cases {
            let dt: Wrap<DataType> = (&bson_val).into();
            assert_eq!(dt.0, expected_dt, "Failed on {:?}", bson_val);
        }
    }

    #[test]
    fn test_dtype_document() {
        let d = doc! {
            "name": "Alice",
            "age": 30i32
        };

        let dt: Wrap<DataType> = (&d).into();

        let expected = DataType::Struct(vec![
            Field::new("name".into(), DataType::String),
            Field::new("age".into(), DataType::Int32),
        ]);

        assert_eq!(dt.0, expected);
    }

    #[test]
    fn test_dtype_array_coercion() {
        // Empty array -> Null List
        let empty_arr = Bson::Array(vec![]);
        assert_eq!(
            Wrap::<DataType>::from(&empty_arr).0,
            DataType::List(Box::new(DataType::Null))
        );

        let int_arr = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
        assert_eq!(
            Wrap::<DataType>::from(&int_arr).0,
            DataType::List(Box::new(DataType::Int32))
        );

        let mixed_arr = Bson::Array(vec![Bson::Int32(1), Bson::Double(2.5)]);
        assert_eq!(
            Wrap::<DataType>::from(&mixed_arr).0,
            DataType::List(Box::new(DataType::Float64))
        );
    }

    #[test]
    fn test_anyvalue_primitives_borrowed() {
        assert_eq!(
            Wrap::<AnyValue>::from(&Bson::Int32(42)).0,
            AnyValue::Int32(42)
        );
        assert_eq!(
            Wrap::<AnyValue>::from(&Bson::Int64(42)).0,
            AnyValue::Int64(42)
        );
        assert_eq!(
            Wrap::<AnyValue>::from(&Bson::Double(3.14)).0,
            AnyValue::Float64(3.14)
        );
        assert_eq!(
            Wrap::<AnyValue>::from(&Bson::Boolean(true)).0,
            AnyValue::Boolean(true)
        );
        assert_eq!(Wrap::<AnyValue>::from(&Bson::Null).0, AnyValue::Null);
        let oid = mongodb::bson::oid::ObjectId::new();
        assert_eq!(
            Wrap::<AnyValue>::from(&Bson::ObjectId(oid)).0,
            AnyValue::BinaryOwned(oid.bytes().to_vec())
        );
        let bytes = b"bytes".to_vec();
        let b_bin = Bson::Binary(mongodb::bson::Binary {
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
        let b_str = Bson::String("hello".into());
        let av: Wrap<AnyValue> = (&b_str).into();

        match av.0 {
            AnyValue::StringOwned(s) => assert_eq!(s.as_str(), "hello"),
            _ => panic!("Expected StringOwned"),
        }
    }

    #[test]
    fn test_anyvalue_datetime() {
        let millis = 1704067200000i64;
        let b_dt = Bson::DateTime(MongoDateTime::from_millis(millis));

        let av: Wrap<AnyValue> = (&b_dt).into();
        assert_eq!(
            av.0,
            AnyValue::Datetime(millis, TimeUnit::Milliseconds, None)
        );
    }

    #[test]
    fn test_anyvalue_object_id() {
        let oid = ObjectId::new();
        let b_oid = Bson::ObjectId(oid);

        let av: Wrap<AnyValue> = (&b_oid).into();
        match av.0 {
            AnyValue::BinaryOwned(s) => assert_eq!(s, oid.bytes()),
            _ => panic!("Expected BinaryOwned for ObjectId"),
        }
    }

    #[test]
    fn test_anyvalue_array_transmute() {
        let arr = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
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
    fn test_anyvalue_document_to_struct() {
        let doc = Bson::Document(doc! {
            "score": 100i32
        });

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
    }
}
