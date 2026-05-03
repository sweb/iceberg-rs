use serde::de::{self, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

pub const MANIFEST_LIST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "manifest_list",
    "fields": [
        {"name": "manifest_path", "type": "string", "field-id": 500},
        {"name": "manifest_length", "type": "long", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "field-id": 502},
        {"name": "content", "type": "int", "field-id": 517},
        {"name": "sequence_number", "type": "long", "field-id": 515},
        {"name": "min_sequence_number", "type": "long", "field-id": 516},
        {"name": "added_snapshot_id", "type": "long", "field-id": 503},
        {"name": "added_files_count", "type": "int", "field-id": 504},
        {"name": "existing_files_count", "type": "int", "field-id": 505},
        {"name": "deleted_files_count", "type": "int", "field-id": 506},
        {"name": "added_rows_count", "type": "long", "field-id": 507},
        {"name": "existing_rows_count", "type": "long", "field-id": 508},
        {"name": "deleted_rows_count", "type": "long", "field-id": 509},
        {
            "name": "partitions",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "field_summary",
                        "fields": [
                            {"name": "contains_null", "type": "boolean", "field-id": 511},
                            {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 518, "default": null},
                            {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 512, "default": null},
                            {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 513, "default": null}
                        ]
                    }
                }
            ],
            "field-id": 510,
            "default": null
        },
        {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 519, "default": null}
    ]
}
"#;

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

#[derive(Error, Debug, PartialEq, Eq)]
pub enum TransformError {
    #[error("Unknown transform: {0}")]
    UnknownTransform(String),
    #[error("Invalid bucket width: {0}")]
    InvalidBucketWidth(String),
    #[error("Invalid truncate width: {0}")]
    InvalidTruncateWidth(String),
}

impl FromStr for Transform {
    type Err = TransformError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "identity" => Ok(Transform::Identity),
            "year" => Ok(Transform::Year),
            "month" => Ok(Transform::Month),
            "day" => Ok(Transform::Day),
            "hour" => Ok(Transform::Hour),
            "void" => Ok(Transform::Void),
            s if s.starts_with("bucket[") && s.ends_with("]") => {
                let inner = s
                    .strip_prefix("bucket[")
                    .and_then(|s| s.strip_suffix("]"))
                    .ok_or_else(|| TransformError::InvalidBucketWidth(s.to_string()))?;
                let bucket_size = inner
                    .parse::<u32>()
                    .map_err(|_| TransformError::InvalidBucketWidth(s.to_string()))?;
                Ok(Transform::Bucket(bucket_size))
            }
            s if s.starts_with("truncate[") && s.ends_with("]") => {
                let inner = s
                    .strip_prefix("truncate[")
                    .and_then(|s| s.strip_suffix("]"))
                    .ok_or_else(|| TransformError::InvalidTruncateWidth(s.to_string()))?;
                let width = inner
                    .parse::<u32>()
                    .map_err(|_| TransformError::InvalidTruncateWidth(s.to_string()))?;
                Ok(Transform::Truncate(width))
            }
            _ => Err(TransformError::UnknownTransform(s.to_string())),
        }
    }
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
        value.parse::<Transform>().map_err(E::custom)
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

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ManifestListEntry {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: u32,
    pub content: u32,
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: u32,
    pub existing_files_count: u32,
    pub deleted_files_count: u32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
    pub partitions: Option<Vec<FieldSummary>>,
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct FieldSummary {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub lower_bound: Option<Vec<u8>>,
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub upper_bound: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub enum Literal {
    Boolean(bool),
    Int(i32),
    Long(i64),
    String(String),
    Float(f32),
    Double(f64),
    Decimal(i128),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    TimestampTz(i64),
    Fixed(Vec<u8>),
    Binary(Vec<u8>),
    Uuid([u8; 16]),
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ParseLiteralError {
    #[error("Type not defined, expected format `type:value`, instead got: {0}")]
    TypeNotDefined(String),
    #[error("Could not parse value for int: {0}")]
    IntNotParsable(String),
    #[error("Could not parse value for long: {0}")]
    LongNotParsable(String),
    #[error("Could not parse value for float: {0}")]
    FloatNotParsable(String),
    #[error("Could not parse value for double: {0}")]
    DoubleNotParsable(String),
    #[error("Could not parse value for decimal (represented as i128): {0}")]
    DecimalNotParsable(String),
    #[error("Unknown type: {0}")]
    UnknownType(String),
}

impl FromStr for Literal {
    type Err = ParseLiteralError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (t, v) = s
            .split_once(':')
            .ok_or_else(|| ParseLiteralError::TypeNotDefined(s.to_string()))?;
        match t {
            "int" => {
                let parsed = v
                    .parse::<i32>()
                    .map_err(|_| ParseLiteralError::IntNotParsable(v.to_string()))?;
                Ok(Literal::Int(parsed))
            }
            "long" => {
                let parsed = v
                    .parse::<i64>()
                    .map_err(|_| ParseLiteralError::LongNotParsable(v.to_string()))?;
                Ok(Literal::Long(parsed))
            }
            "string" => Ok(Literal::String(v.to_string())),
            "float" => {
                let parsed = v
                    .parse::<f32>()
                    .map_err(|_| ParseLiteralError::FloatNotParsable(v.to_string()))?;
                Ok(Literal::Float(parsed))
            }
            "double" => {
                let parsed = v
                    .parse::<f64>()
                    .map_err(|_| ParseLiteralError::DoubleNotParsable(v.to_string()))?;
                Ok(Literal::Double(parsed))
            }
            "decimal" => {
                let parsed = v
                    .parse::<i128>()
                    .map_err(|_| ParseLiteralError::DecimalNotParsable(v.to_string()))?;
                Ok(Literal::Decimal(parsed))
            }
            _ => Err(ParseLiteralError::UnknownType(t.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{Reader, Schema, Writer, from_value};
    use std::fs::File;
    use std::path::Path;
    use tempfile::NamedTempFile;

    #[test]
    fn test_transform_roundtrip() {
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

    #[test]
    fn test_load_manifest_list() {
        let path = Path::new("../../test-data/manifest-list.avro");
        let file = File::open(path).unwrap();
        let reader = Reader::new(file).unwrap();

        for (i, record) in reader.enumerate() {
            let manifest_list_entry = from_value::<ManifestListEntry>(&record.unwrap()).unwrap();

            assert_eq!(
                manifest_list_entry.manifest_path,
                format!("s3://bucket/path/to/manifest{}.avro", i + 1)
            );
        }
    }

    #[test]
    fn test_manifest_list_roundtrip() {
        let partitions1 = FieldSummary {
            contains_null: false,
            contains_nan: Some(false),
            lower_bound: Some(vec![0, 0]),
            upper_bound: Some(vec![0, 0, 0]),
        };
        let entry1 = ManifestListEntry {
            manifest_path: "/my/path/1".to_string(),
            manifest_length: 1,
            partition_spec_id: 0,
            content: 1,
            sequence_number: 1,
            min_sequence_number: 3,
            added_snapshot_id: 1,
            added_files_count: 1,
            existing_files_count: 1,
            deleted_files_count: 0,
            added_rows_count: 1,
            existing_rows_count: 1,
            deleted_rows_count: 0,
            partitions: Some(vec![partitions1]),
            key_metadata: None,
        };
        let file = NamedTempFile::new().unwrap();
        let file2 = file.reopen().unwrap();
        let schema = Schema::parse_str(MANIFEST_LIST_SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, file);
        writer.append_ser(&entry1).unwrap();
        writer.flush().unwrap();

        let reader = Reader::new(file2).unwrap();

        for record in reader {
            let manifest_list_entry = from_value::<ManifestListEntry>(&record.unwrap()).unwrap();

            assert_eq!(&manifest_list_entry, &entry1);
        }
    }

    #[test]
    fn test_parse_literal_from_str() {
        let test_set = vec![
            ("int:32", Literal::Int(32)),
            ("long:5000", Literal::Long(5000)),
            ("float:23.4", Literal::Float(23.4)),
            ("double:230.4", Literal::Double(230.4)),
            ("decimal:50023", Literal::Decimal(50023)),
            (
                "string:hello,world",
                Literal::String("hello,world".to_string()),
            ),
        ];
        for (input, expected) in test_set {
            let actual = input.parse::<Literal>().unwrap();
            assert_eq!(actual, expected);
        }
    }
}
