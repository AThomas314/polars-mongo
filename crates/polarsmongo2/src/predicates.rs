use mongodb::bson::{Bson, Document, doc};
use polars::prelude::*;
use polars_plan::prelude::*;
pub fn parse_expr(predicate: &Option<Expr>) -> Document {
    predicate
        .as_ref()
        .and_then(expr_to_mongo)
        .unwrap_or_default()
}

fn expr_to_mongo(e: &Expr) -> Option<Document> {
    match e {
        Expr::BinaryExpr { left, op, right } if is_logical(op) => {
            let mongo_op = parse_op(op);
            let left_doc = expr_to_mongo(left)?;
            let right_doc = expr_to_mongo(right)?;
            Some(doc! { mongo_op: [left_doc, right_doc] })
        }

        Expr::BinaryExpr { left, op, right } => {
            if let (Expr::Column(name), Expr::Literal(lit)) = (left.as_ref(), right.as_ref()) {
                let val = lit_to_bson(lit)?;
                let mongo_op = parse_op(op);
                if mongo_op == "$eq" {
                    Some(doc! { name.as_str(): val })
                } else {
                    Some(doc! { name.as_str(): { mongo_op: val } })
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

fn is_logical(op: &Operator) -> bool {
    matches!(
        op,
        Operator::And | Operator::Or | Operator::LogicalAnd | Operator::LogicalOr
    )
}

fn lit_to_bson(lit: &LiteralValue) -> Option<Bson> {
    match lit {
        LiteralValue::Scalar(scalar) => match scalar.value() {
            AnyValue::Float32(f) => Some(Bson::Double(*f as f64)),
            AnyValue::Float64(f) => Some(Bson::Double(*f)),
            AnyValue::Int32(i) => Some(Bson::Int32(*i)),
            AnyValue::Int64(i) => Some(Bson::Int64(*i)),
            AnyValue::Boolean(b) => Some(Bson::Boolean(*b)),
            AnyValue::String(s) => Some(Bson::String(s.to_string())),
            AnyValue::StringOwned(s) => Some(Bson::String(s.to_string())),
            AnyValue::Binary(b) => Some(Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: b.to_vec(),
            })),
            AnyValue::BinaryOwned(b) => Some(Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: b.clone(),
            })),
            _ => None,
        },
        LiteralValue::Dyn(d) => match d {
            DynLiteralValue::Str(s) => Some(Bson::String(s.to_string())),
            DynLiteralValue::Float(f) => Some(Bson::Double(*f)),
            _ => None,
        },
        _ => None,
    }
}

fn parse_op(op: &Operator) -> &'static str {
    match op {
        Operator::And | Operator::LogicalAnd => "$and",
        Operator::Or | Operator::LogicalOr => "$or",
        Operator::Gt => "$gt",
        Operator::Lt => "$lt",
        Operator::LtEq => "$lte",
        Operator::GtEq => "$gte",
        Operator::Eq => "$eq",
        Operator::NotEq => "$ne",
        _ => "$unknown",
    }
}
#[cfg(test)]
mod tests {
    use std::ops::Rem;

    use super::*;
    use polars::prelude::{col, lit};
    #[test]
    fn test_parse_expr_eq() {
        let predicate = &Some(col("a").eq(lit("b")));
        let expected = doc! {"a" : "b"};
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_gt() {
        let predicate = &Some(col("a").gt(lit("b")));
        let expected = doc! {"a" :{"$gt": "b"}};
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_gte() {
        let predicate = &Some(col("a").gt_eq(lit("b")));
        let expected = doc! {"a" :{"$gte": "b"}};
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_lt() {
        let predicate = &Some(col("a").lt(lit("b")));
        let expected = doc! {"a" :{"$lt": "b"}};
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_lte() {
        let predicate = &Some(col("a").lt_eq(lit("b")));
        let expected = doc! {"a" :{"$lte": "b"}};
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_ne() {
        let predicate = &Some(col("a").neq(lit("b")));
        let expected = doc! {"a" :{"$ne": "b"}};
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_and() {
        let predicate = &Some((col("a").neq(lit("b"))).and((col("a")).eq(lit("c"))));

        let expected = doc! {
            "$and": [
                { "a": { "$ne": "b" } },
                { "a": "c" }
            ]
        };
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_or() {
        let predicate = &Some((col("a").neq(lit("b"))).or((col("a")).eq(lit("c"))));

        let expected = doc! {
            "$or": [
                { "a": { "$ne": "b" } },
                { "a": "c" }
            ]
        };
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_and_or() {
        let predicate = &Some(
            (col("d").neq(lit("e")).or(col("d").eq(lit("z"))))
                .and(col("a").neq(lit("b")).or(col("a").eq(lit("c")))),
        );

        let expected = doc! {
            "$and": [
                {
                    "$or": [
                        { "d": { "$ne": "e" } },
                        { "d": "z" }
                    ]
                },
                {
                    "$or": [
                        { "a": { "$ne": "b" } },
                        { "a": "c" }
                    ]
                }
            ]
        };
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
    #[test]
    fn test_parse_expr_and_or_unsupported() {
        let predicate = &Some(
            (col("d").neq(lit("e")).or(col("d").eq(lit("z"))))
                .and(col("a").neq(lit("b")).or(col("a").eq(lit("c"))))
                .or(col("c").rem(lit(2)).eq(0)),
        );

        let expected = doc! {};
        let output = parse_expr(predicate);
        assert_eq!(output, expected);
    }
}
