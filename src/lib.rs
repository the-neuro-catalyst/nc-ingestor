// nc_ingestor/src/lib.rs
// This file will contain the public API for the nc_ingestor module.

pub mod cli;
pub mod embeddings;
pub mod error;
pub mod ingestor;
pub mod mongo;
pub mod neo4j;
pub mod postgres;
pub mod qdrant;
pub mod retry;
pub mod schema_builder;
pub mod sqlite;

pub const DEFAULT_COLLECTION_NAME: &str = "ingested_nc_collection";
pub const DEFAULT_VECTOR_SIZE: u64 = 4;
pub const DEFAULT_SQL_TABLE_NAME: &str = "ingested_data";
