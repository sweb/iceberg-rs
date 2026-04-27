import os
import sys

try:
    import fastavro
except ImportError:
    print("fastavro not found. Please install it with: pip install fastavro")
    sys.exit(1)

# Iceberg Manifest List Schema (V2)
# Based on: https://iceberg.apache.org/spec/#manifest-lists
MANIFEST_LIST_SCHEMA = {
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
                            {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 518, "default": None},
                            {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 512, "default": None},
                            {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 513, "default": None},
                        ],
                    },
                },
            ],
            "field-id": 510,
            "default": None,
        },
        {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 519, "default": None},
    ],
}

def generate_manifest_list(output_path):
    records = [
        {
            "manifest_path": "s3://bucket/path/to/manifest1.avro",
            "manifest_length": 1024,
            "partition_spec_id": 0,
            "content": 0,  # 0: DATA, 1: DELETES
            "sequence_number": 1,
            "min_sequence_number": 1,
            "added_snapshot_id": 123456789,
            "added_files_count": 10,
            "existing_files_count": 0,
            "deleted_files_count": 0,
            "added_rows_count": 1000,
            "existing_rows_count": 0,
            "deleted_rows_count": 0,
            "partitions": [
                {
                    "contains_null": False,
                    "contains_nan": False,
                    "lower_bound": b"\x01\x00\x00\x00",
                    "upper_bound": b"\x0a\x00\x00\x00",
                }
            ],
            "key_metadata": b"some-metadata",
        },
        {
            "manifest_path": "s3://bucket/path/to/manifest2.avro",
            "manifest_length": 2048,
            "partition_spec_id": 0,
            "content": 0,
            "sequence_number": 2,
            "min_sequence_number": 1,
            "added_snapshot_id": 123456790,
            "added_files_count": 5,
            "existing_files_count": 10,
            "deleted_files_count": 0,
            "added_rows_count": 500,
            "existing_rows_count": 1000,
            "deleted_rows_count": 0,
            "partitions": None,
            "key_metadata": None,
        }
    ]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as out:
        fastavro.writer(out, MANIFEST_LIST_SCHEMA, records)
    print(f"Generated manifest list at: {output_path}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output = os.path.join(base_dir, "../test-data/manifest-list.avro")
    generate_manifest_list(output)
