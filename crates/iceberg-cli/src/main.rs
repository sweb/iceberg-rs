use clap::Parser;
use iceberg_core::types::TableMetadata;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// Simple CLI to explore iceberg files
#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    /// Path to the iceberg table
    #[arg(short, long)]
    path: String,
}

fn main() {
    let args = Args::parse();

    let path = Path::new(&args.path);
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    let table_metadata: TableMetadata = serde_json::from_reader(reader).unwrap();
    let schemas = table_metadata.schemas;
    if schemas.len() != 1 {
        panic!("More than one schema is not supported yet!");
    }

    let schema = &schemas[0];
    for f in schema.fields.iter() {
        println!("Field: {:?}", f);
    }
}
