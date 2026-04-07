import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

# Paths relative to the script location
base_dir = os.path.dirname(os.path.abspath(__file__))
warehouse_path = os.path.join(base_dir, "../test-data/warehouse")
os.makedirs(warehouse_path, exist_ok=True)

catalog = load_catalog(
    "local",
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)

partition_spec = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_part")
)

namespace = "default"
try:
    catalog.create_namespace(namespace)
except Exception:
    pass

table_name = (namespace, "sample_table")
try:
    catalog.drop_table(table_name)
except Exception:
    pass

table = catalog.create_table(
    identifier=table_name,
    schema=schema,
    partition_spec=partition_spec,
    properties={"format-version": "2"}
)

print(f"Metadata location: {table.metadata_location}")
