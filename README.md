# Learning project: Apache Iceberg spec implementation

## Testing with PyIceberg

To test this implementation against a reference, you can generate Iceberg metadata using the provided Python script in `test-gen-python/`. This uses `uv` to run an ephemeral environment with `pyiceberg`.

### Generate Sample Metadata
Run the following command from the root of the project to generate a local Iceberg table and its V2 metadata:

```bash
cd test-gen-python && uv run --with pyiceberg --with sqlalchemy python3 generate_metadata.py
```

The metadata will be generated in `test-data/warehouse/default/sample_table/metadata/`.


