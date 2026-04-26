use serde::de::{self, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

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

#[derive(Error, Debug, PartialEq, Eq)]
pub enum PrimitiveTypeError {
    #[error("Unknown primitive type: {0}")]
    UnknownType(String),

    #[error("Invalid fixed length: {0}")]
    InvalidFixedLength(String),

    #[error("Invalid decimal format: {0}")]
    InvalidDecimalFormat(String),

    #[error("Decimal precision {0} exceeds maximum of 38")]
    DecimalPrecisionTooLarge(u32),

    #[error("Decimal scale {scale} exceeds precision of {precision}")]
    DecimalScaleExceedsPrecision { scale: u32, precision: u32 },
}

impl FromStr for PrimitiveType {
    type Err = PrimitiveTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "boolean" => Ok(PrimitiveType::Boolean),
            "int" => Ok(PrimitiveType::Int),
            "long" => Ok(PrimitiveType::Long),
            "float" => Ok(PrimitiveType::Float),
            "double" => Ok(PrimitiveType::Double),
            "date" => Ok(PrimitiveType::Date),
            "time" => Ok(PrimitiveType::Time),
            "timestamp" => Ok(PrimitiveType::Timestamp),
            "timestamptz" => Ok(PrimitiveType::Timestamptz),
            "string" => Ok(PrimitiveType::String),
            "uuid" => Ok(PrimitiveType::Uuid),
            "binary" => Ok(PrimitiveType::Binary),

            s if s.starts_with("fixed[") && s.ends_with("]") => {
                let inner = s
                    .strip_prefix("fixed[")
                    .and_then(|s| s.strip_suffix("]"))
                    .ok_or_else(|| PrimitiveTypeError::InvalidFixedLength(s.to_string()))?;
                let length = inner
                    .parse::<u32>()
                    .map_err(|_| PrimitiveTypeError::InvalidFixedLength(inner.to_string()))?;
                Ok(PrimitiveType::Fixed(length))
            }

            s if s.starts_with("decimal(") && s.ends_with(")") => {
                let (p_raw, s_raw) = s
                    .strip_prefix("decimal(")
                    .and_then(|s| s.strip_suffix(")"))
                    .and_then(|s| s.split_once(','))
                    .ok_or_else(|| PrimitiveTypeError::InvalidDecimalFormat(s.to_string()))?;
                let precision = p_raw
                    .trim()
                    .parse::<u32>()
                    .map_err(|_| PrimitiveTypeError::InvalidDecimalFormat(p_raw.to_string()))?;
                let scale = s_raw
                    .trim()
                    .parse::<u32>()
                    .map_err(|_| PrimitiveTypeError::InvalidDecimalFormat(s_raw.to_string()))?;
                if precision > 38 {
                    return Err(PrimitiveTypeError::DecimalPrecisionTooLarge(precision));
                }
                if scale > precision {
                    return Err(PrimitiveTypeError::DecimalScaleExceedsPrecision {
                        scale,
                        precision,
                    });
                }
                Ok(PrimitiveType::Decimal { precision, scale })
            }
            _ => Err(PrimitiveTypeError::UnknownType(s.to_string())),
        }
    }
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct PrimitiveTypeVisitor;

        impl<'de> Visitor<'de> for PrimitiveTypeVisitor {
            type Value = PrimitiveType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a primitive type")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                value.parse::<PrimitiveType>().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(PrimitiveTypeVisitor)
    }
}

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrimitiveType::Boolean => write!(f, "boolean"),
            PrimitiveType::Int => write!(f, "int"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Time => write!(f, "time"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::Timestamptz => write!(f, "timestamptz"),
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Uuid => write!(f, "uuid"),
            PrimitiveType::Binary => write!(f, "binary"),
            PrimitiveType::Decimal { precision, scale } => {
                write!(f, "decimal({},{})", precision, scale)
            }
            PrimitiveType::Fixed(l) => write!(f, "fixed[{}]", l),
        }
    }
}

impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    Primitive(PrimitiveType),
    Struct(StructType),
    List(ListType),
    Map(MapType),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct StructType {
    pub fields: Vec<NestedField>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NestedField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: Box<Type>,
    pub doc: Option<String>, // TODO: default value
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ListType {
    pub id: i32,
    pub required: bool,
    pub element_type: Box<Type>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MapType {
    pub key_id: i32,
    pub key_type: Box<Type>,
    pub value_id: i32,
    pub value_required: bool,
    pub value_type: Box<Type>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableSchema {
    pub schema_id: i32,
    pub identifier_field_ids: Option<Vec<i32>>,
    pub fields: Vec<NestedField>,
}

#[derive(Debug, PartialEq)]
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

impl fmt::Display for Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transform::Identity => write!(f, "identity"),
            Transform::Bucket(n) => write!(f, "bucket[{}]", n),
            Transform::Truncate(n) => write!(f, "truncate[{}]", n),
            Transform::Year => write!(f, "year"),
            Transform::Month => write!(f, "month"),
            Transform::Day => write!(f, "day"),
            Transform::Hour => write!(f, "hour"),
            Transform::Void => write!(f, "void"),
        }
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
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
    pub fields: Vec<PartitionField>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SnapshotOperation {
    Append,
    Replace,
    Overwrite,
    Delete,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotSummary {
    operation: SnapshotOperation,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    snapshot_id: u64,
    parent_snapshot_id: Option<u32>,
    sequence_number: u64,
    timestamp_ms: u64,
    manifest_list: String,
    summary: SnapshotSummary,
    schema_id: Option<u32>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SortOrderDirection {
    Asc,
    Desc,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SortOrderNullOrder {
    NullsFirst,
    NullsLast,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrderField {
    transform: Transform,
    source_id: u32,
    direction: SortOrderDirection,
    null_order: SortOrderNullOrder,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrder {
    order_id: u32,
    fields: Vec<SortOrderField>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLogEntry {
    snapshot_id: u64,
    timestamp_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLogEntry {
    metadata_file: String,
    timestamp_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: u32,
    pub table_uuid: String,
    pub location: String,
    pub last_sequence_number: u64,
    pub last_updated_ms: u64,
    pub last_column_id: u32,
    pub schemas: Vec<TableSchema>,
    pub current_schema_id: u32,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: u32,
    pub last_partition_id: u32,
    pub properties: Option<HashMap<String, String>>,
    pub current_snapshot_id: Option<u64>,
    pub snapshots: Vec<Snapshot>,
    pub snapshot_log: Option<Vec<SnapshotLogEntry>>,
    pub metadata_log: Option<Vec<MetadataLogEntry>>,
    pub sort_orders: Vec<SortOrder>,
    pub default_sort_order_id: u32,
    pub refs: Option<HashMap<String, String>>,
    // statistics
    // partition-statistics
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
    fn test_roundtrip() {
        let variants = vec![
            (Transform::Identity, r#""identity""#),
            (Transform::Bucket(10), r#""bucket[10]""#),
            (Transform::Truncate(15), r#""truncate[15]""#),
            (Transform::Year, r#""year""#),
            (Transform::Month, r#""month""#),
            (Transform::Day, r#""day""#),
            (Transform::Hour, r#""hour""#),
            (Transform::Void, r#""void""#),
        ];
        for (t, expected) in variants {
            let t_str = serde_json::to_string(&t).unwrap();
            assert_eq!(t_str, expected);
            let new_t: Transform = serde_json::from_str(&t_str).unwrap();
            assert_eq!(t, new_t);
        }
    }

    #[test]
    fn test_load_metadata_file() {
        let metadata_raw = r#"
{"location":"file:///my-path","table-uuid":"0782efde-af24-4555-a654-77313ae34f37","last-updated-ms":1776936286421,"last-column-id":2,"schemas":[{"type":"struct","fields":[{"id":1,"name":"id","type":"int","required":true},{"id":2,"name":"name","type":"string","required":false}],"schema-id":0,"identifier-field-ids":[]}],"current-schema-id":0,"partition-specs":[{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,"transform":"identity","name":"id_part"}]}],"default-spec-id":0,"last-partition-id":1000,"properties":{},"snapshots":[],"snapshot-log":[],"metadata-log":[],"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"refs":{},"statistics":[],"partition-statistics":[],"format-version":2,"last-sequence-number":0}
        "#;
        let metadata: TableMetadata = serde_json::from_str(metadata_raw).unwrap();

        assert_eq!(metadata.table_uuid, "0782efde-af24-4555-a654-77313ae34f37")
    }
}
