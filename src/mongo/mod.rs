// nc_ingestor/src/mongo/mod.rs
// MongoDB specific ingestion logic.

use async_trait::async_trait;
use mongodb::Client;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use nc_reader::nc_reader_result::DataReaderResult;
use tracing::info;

use crate::error::{IngestorError, Result};
use crate::ingestor::{Ingestor, IngestorConfig};
use crate::retry::{execute_with_retry, wrap_error};

pub struct MongoIngestor {
    #[allow(dead_code)]
    config: IngestorConfig,
    client: Client,
}

#[async_trait]
impl Ingestor for MongoIngestor {
    async fn new(config: IngestorConfig,) -> Result<Self,> {
        let client_options = ClientOptions::parse(&config.database_url,)
            .await
            .map_err(|e| {
                IngestorError::ConfigurationError(format!("Failed to parse MongoDB URI: {}", e),)
            },)?;
        let client = Client::with_options(client_options,).map_err(|e| {
            IngestorError::ConnectionError(format!("Failed to create MongoDB client: {}", e),)
        },)?;

        execute_with_retry(|| async {
            client
                .database("admin",)
                .run_command(doc! {"ping": 1}, None,)
                .await
                .map(|_| (),)
                .map_err(|e| {
                    wrap_error(IngestorError::ConnectionError(format!(
                        "Failed to connect to MongoDB: {}",
                        e
                    ),),)
                },)
        },)
        .await?;

        Ok(MongoIngestor { config, client, },)
    }

    async fn ingest(&self, data: DataReaderResult,) -> Result<(),> {
        let database_name = "scm_db"; // Default database name
        let collection_name = self
            .config
            .collection_name
            .as_deref()
            .unwrap_or(crate::DEFAULT_COLLECTION_NAME,);

        let collection = self
            .client
            .database(database_name,)
            .collection(collection_name,);

        let bson_document = mongodb::bson::to_document(&data,).map_err(|e| {
            IngestorError::IngestionError(format!(
                "Failed to serialize DataReaderResult to BSON: {}",
                e
            ),)
        },)?;

        execute_with_retry(|| async {
            collection
                .insert_one(bson_document.clone(), None,)
                .await
                .map(|_| (),)
                .map_err(|e| {
                    wrap_error(IngestorError::IngestionError(format!(
                        "Failed to insert data into MongoDB: {}",
                        e
                    ),),)
                },)
        },)
        .await?;

        info!(
            "Successfully ingested data to MongoDB into collection '{}' in database '{}'.",
            collection_name, database_name
        );
        Ok((),)
    }
}
