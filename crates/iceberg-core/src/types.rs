use serde::de::{self, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum PrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal { precision: u32, scale: u32 },
    Date,
    Time,
    Timestamp,
    Timestamptz,
    String,
    Uuid,
    Fixed(u32),
    Binary,
}

#[derive(Debug, PartialEq)]
pub enum Type {
    Primitive(PrimitiveType),
    Struct(StructType),
    List(ListType),
    Map(MapType),
}

#[derive(Debug, PartialEq)]
pub struct StructType {
    pub fields: Vec<NestedField>,
}

#[derive(Debug, PartialEq)]
pub struct NestedField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    pub field_type: Box<Type>,
    pub doc: Option<String>, // TODO: default value
}

#[derive(Debug, PartialEq)]
pub struct ListType {
    pub id: i32,
    pub required: bool,
    pub element_type: Box<Type>,
}

#[derive(Debug, PartialEq)]
pub struct MapType {
    pub key_id: i32,
    pub key_type: Box<Type>,
    pub value_id: i32,
    pub value_required: bool,
    pub value_type: Box<Type>,
}

pub struct TableSchema {
    schema_id: i32,
    identifier_field_ids: Option<Vec<i32>>,
    pub fields: StructType,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Transform {
    Identity,
    Bucket(u32),
    Truncate(u32),
    Year,
    Month,
    Day,
    Hour,
    Void,
}

// Implementation based on https://serde.rs/impl-deserialize.html
struct TransformVisitor;

impl<'de> Visitor<'de> for TransformVisitor {
    type Value = Transform;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(
            "a supported Transform type (identity, bucket, truncate, year, month, day, hour, void)",
        )
    }
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match value {
            "identity" => Ok(Transform::Identity),
            "year" => Ok(Transform::Year),
            "month" => Ok(Transform::Month),
            "day" => Ok(Transform::Day),
            "hour" => Ok(Transform::Hour),
            "void" => Ok(Transform::Void),
            s if s.starts_with("bucket[") && s.ends_with("]") => {
                let bucket_size = value[7..s.len() - 1]
                    .parse::<u32>()
                    .map_err(|e| E::custom(format!("Invalid bucket width: {}", e)))?;
                Ok(Transform::Bucket(bucket_size))
            }
            s if s.starts_with("truncate[") && s.ends_with("]") => {
                let width = value[9..s.len() - 1]
                    .parse::<u32>()
                    .map_err(|e| E::custom(format!("Invalid truncate width: {}", e)))?;
                Ok(Transform::Truncate(width))
            }
            _ => Err(E::custom(format!("unknown transform: {}", value))),
        }
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(TransformVisitor)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    source_id: u32,
    field_id: u32,
    name: String,
    transform: Transform,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    spec_id: u32,
    fields: Vec<PartitionField>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn use_primitive_type() {
        assert_eq!(
            Type::Primitive(PrimitiveType::Boolean),
            Type::Primitive(PrimitiveType::Boolean)
        );
    }

    #[test]
    fn deserialize_transform_identity() {
        let raw = r#""identity""#;
        let transform: Transform = serde_json::from_str(raw).unwrap();
        assert_eq!(transform, Transform::Identity);
    }

    #[test]
    fn deserialize_transform_year() {
        let raw = r#""year""#;
        let transform: Transform = serde_json::from_str(raw).unwrap();
        assert_eq!(transform, Transform::Year);
    }

    #[test]
    fn deserialize_transform_month() {
        let raw = r#""month""#;
        let transform: Transform = serde_json::from_str(raw).unwrap();
        assert_eq!(transform, Transform::Month);
    }

    #[test]
    fn deserialize_transform_day() {
        let raw = r#""day""#;
        let transform: Transform = serde_json::from_str(raw).unwrap();
        assert_eq!(transform, Transform::Day);
    }

    #[test]
    fn deserialize_transform_hour() {
        let raw = r#""hour""#;
        let transform: Transform = serde_json::from_str(raw).unwrap();
        assert_eq!(transform, Transform::Hour);
    }

    #[test]
    fn deserialize_transform_void() {
        let raw = r#""void""#;
        let transform: Transform = serde_json::from_str(raw).unwrap();
        assert_eq!(transform, Transform::Void);
    }

    #[test]
    fn deserialize_transform_bucket() {
        let raw = r#""bucket[16]""#;
        let transform: Transform = serde_json::from_str(raw).unwrap();
        assert_eq!(transform, Transform::Bucket(16));
    }

    #[test]
    fn deserialize_transform_truncate(){
        let raw = r#""truncate[20]""#;
        let transform: Transform = serde_json::from_str(raw).unwrap();
        assert_eq!(transform, Transform::Truncate(20));
    }
}
