// nc_ingestor/src/cli.rs
// Command Line Interface (CLI) specific logic for nc_ingestor.

use std::path::PathBuf;

use clap::Parser;

/// Command Line Interface for the nc_ingestor module.
#[derive(Parser, Debug,)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// The type of database to ingest data into (e.g., "mongodb", "sqlite", "postgres").
    #[clap(subcommand)]
    pub command: Commands,

    /// Halt execution immediately upon encountering any non-recoverable error.
    #[clap(long)]
    pub strict: bool,

    /// Generate a structured error summary report (ingestion_report.json) at the end.
    #[clap(long)]
    pub report: bool,

    /// Number of concurrent files to process.
    #[clap(short, long, default_value_t = 4)]
    pub concurrency: usize,
}

#[derive(Parser, Debug,)]
pub enum Commands {
    /// Ingest data into MongoDB
    Mongo(MongoArgs,),
    /// Ingest data into Neo4j
    Neo4j(Neo4jArgs,),
    /// Ingest data into PostgreSQL
    Postgres(PostgresArgs,),
    /// Ingest data into Qdrant
    Qdrant(QdrantArgs,),

    /// Ingest data into SQLite
    Sqlite(SqliteArgs,),
}

#[derive(Parser, Debug,)]
pub struct CommonIngestorArgs {
    /// Name of the collection or table to ingest data into
    #[clap(long)]
    pub collection_name: Option<String,>,

    /// Size of the vector (for vector databases like Qdrant)
    #[clap(long)]
    pub vector_size: Option<u64,>,

    /// Custom column mappings (e.g., --map source_field:target_column)
    #[clap(long, value_parser = parse_key_val, value_delimiter = ',')]
    pub map: Option<Vec<(String, String,),>,>,

    /// OpenAI API Key for generating embeddings
    #[clap(long, env = "OPENAI_API_KEY")]
    pub openai_api_key: Option<String,>,

    /// Field name to use for generating embeddings
    #[clap(long)]
    pub embed_field: Option<String,>,

    /// JSON string defining relationships for Neo4j (e.g.,
    /// '[{"source_field":"user_id","target_label":"User","target_field":"id","relationship_type":"
    /// BELONGS_TO"}]')
    #[clap(long)]
    pub relationships: Option<String,>,
}

/// Parse a single key-value pair
fn parse_key_val(s: &str,) -> Result<(String, String,), String,> {
    let pos = s
        .find(':',)
        .ok_or_else(|| format!("invalid KEY:VALUE: no `:` found in `{}`", s),)?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string(),),)
}

#[derive(Parser, Debug,)]
pub struct MongoArgs {
    /// Connection string for MongoDB
    #[clap(long, env = "MONGO_URI")]
    pub uri:  String,
    /// Path to the data file or directory to ingest
    #[clap(short, long)]
    pub path: PathBuf,

    #[clap(flatten)]
    pub common: CommonIngestorArgs,
}

#[derive(Parser, Debug,)]
pub struct Neo4jArgs {
    /// Connection string for Neo4j
    #[clap(long, env = "NEO4J_URI")]
    pub uri:  String,
    /// Path to the data file or directory to ingest
    #[clap(short, long)]
    pub path: PathBuf,

    #[clap(flatten)]
    pub common: CommonIngestorArgs,
}

#[derive(Parser, Debug,)]
pub struct PostgresArgs {
    /// Connection string for PostgreSQL
    #[clap(long, env = "PG_URI")]
    pub uri:  String,
    /// Path to the data file or directory to ingest
    #[clap(short, long)]
    pub path: PathBuf,

    #[clap(flatten)]
    pub common: CommonIngestorArgs,
}

#[derive(Parser, Debug,)]
pub struct QdrantArgs {
    /// Connection string for Qdrant
    #[clap(long, env = "QDRANT_URI")]
    pub uri:  String,
    /// Path to the data file or directory to ingest
    #[clap(short, long)]
    pub path: PathBuf,

    #[clap(flatten)]
    pub common: CommonIngestorArgs,
}

#[derive(Parser, Debug,)]
pub struct SqliteArgs {
    /// Path to the SQLite database file
    #[clap(long, env = "SQLITE_DB_PATH")]
    pub db_path: String,
    /// Path to the data file or directory to ingest
    #[clap(short, long)]
    pub path:    PathBuf,

    #[clap(flatten)]
    pub common: CommonIngestorArgs,
}
