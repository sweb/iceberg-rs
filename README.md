# Learning project: Apache Iceberg spec implementation

## Running the CLI

To test the functionality with real files, [iceberg-cli](crates/iceberg-cli) implements a very simple CLI to interact with Iceberg files.

```
cargo run -- --path <path to a table metadata file>
```


## Testing with PyIceberg

To test this implementation against a reference, you can generate Iceberg metadata using the provided Python script in `test-gen-python/`. This uses `uv` to run an ephemeral environment with `pyiceberg`.

### Generate Sample Metadata
Run the following command from the root of the project to generate a local Iceberg table and its V2 metadata:

```bash
cd test-gen-python && uv run --with pyiceberg --with sqlalchemy python3 generate_metadata.py
```

The metadata will be generated in `test-data/warehouse/default/sample_table/metadata/`.


