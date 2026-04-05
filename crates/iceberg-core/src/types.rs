#[derive(Debug, PartialEq)]
pub enum PrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal {precision: u32, scale: u32 },
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
    pub doc: Option<String>
    // TODO: default value
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn use_primitive_type() {
        assert_eq!(Type::Primitive(PrimitiveType::Boolean), Type::Primitive(PrimitiveType::Boolean));
    }
}
