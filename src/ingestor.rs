// nc_ingestor/src/ingestor.rs
// Core ingestion logic and traits.

use async_trait::async_trait;
use nc_reader::nc_reader_result::DataReaderResult;

use crate::error::Result; // Assuming this path is correct

use std::collections::HashMap;

/// Configuration for an ingestor.
#[derive(Debug, Clone,)]
pub struct IngestorConfig {
    // Common configuration options for all ingestors
    pub database_url:    String,
    pub collection_name: Option<String,>,
    pub vector_size:     Option<u64,>,
    pub mappings:        Option<HashMap<String, String,>,>,
    pub openai_api_key:  Option<String,>,
    pub embed_field:     Option<String,>,
    pub relationships:   Option<Vec<RelationshipConfig>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RelationshipConfig {
    pub source_field: String,      // e.g. "user_id"
    pub target_label: String,      // e.g. "User"
    pub target_field: String,      // e.g. "id"
    pub relationship_type: String, // e.g. "BELONGS_TO"
}

/// Trait for all data ingestors.
#[async_trait]
pub trait Ingestor: Send + Sync {
    /// Creates a new ingestor instance with the given configuration.
    async fn new(config: IngestorConfig,) -> Result<Self,>
    where
        Self: Sized;

    /// Ingests data into the target database.
    async fn ingest(&self, data: DataReaderResult,) -> Result<(),>;
}

// Example concrete ingestor (conceptual)
// pub struct MongoIngestor {
//     config: IngestorConfig,
//     // client: MongoClient,
// }

// #[async_trait]
// impl Ingestor for MongoIngestor {
//     fn new(config: IngestorConfig) -> Result<Self> {
//         // Initialize MongoDB client here
//         Ok(MongoIngestor { config })
//     }

//     async fn ingest(&self, data: DataReaderResult) -> Result<()> {
//         println!("Ingesting data to MongoDB: {:?}", data);
//         // MongoDB ingestion logic here
//         Ok(())
//     }
// }
