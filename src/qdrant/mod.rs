// nc_ingestor/src/qdrant/mod.rs
// Qdrant specific ingestion logic.

use std::collections::HashMap;

use async_trait::async_trait;
use nc_reader::nc_reader_result::DataReaderResult;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CollectionStatus, CreateCollection, Distance, PointStruct, UpsertPoints, VectorParams,
};
use qdrant_client::qdrant::{PointId, point_id::PointIdOptions}; /* Ensure PointIdOptions is
                                                                  * imported */
use tracing::info;
use uuid::Uuid;

use crate::embeddings::{Embedder, OpenAIEmbedder};
use crate::error::{IngestorError, Result};
use crate::ingestor::{Ingestor, IngestorConfig};
use crate::retry::{execute_with_retry, wrap_error};

pub struct QdrantIngestor {
    config:   IngestorConfig,
    client:   Qdrant,
    embedder: Option<Box<dyn Embedder,>,>,
}

#[async_trait]
impl Ingestor for QdrantIngestor {
    async fn new(config: IngestorConfig,) -> Result<Self,> {
        let client = Qdrant::from_url(&config.database_url,)
            .build()
            .map_err(|e| {
                IngestorError::ConnectionError(format!("Failed to create Qdrant client: {}", e),)
            },)?;

        // Basic check: list collections with retry
        execute_with_retry(|| async {
            client.list_collections().await.map(|_| (),).map_err(|e| {
                wrap_error(IngestorError::ConnectionError(format!(
                    "Failed to connect to Qdrant: {}",
                    e
                ),),)
            },)
        },)
        .await?;

        let embedder: Option<Box<dyn Embedder,>,> = config
            .openai_api_key
            .as_ref()
            .map(|key| Box::new(OpenAIEmbedder::new(key.clone(), None,),) as Box<dyn Embedder,>,);

        Ok(QdrantIngestor {
            config,
            client,
            embedder,
        },)
    }

    async fn ingest(&self, data: DataReaderResult,) -> Result<(),> {
        let collection_name = self
            .config
            .collection_name
            .as_deref()
            .unwrap_or(crate::DEFAULT_COLLECTION_NAME,);
        let vector_size = self
            .config
            .vector_size
            .unwrap_or(crate::DEFAULT_VECTOR_SIZE,);

        self.ensure_collection(collection_name, vector_size,)
            .await?;

        match data {
            DataReaderResult::Csv(csv_data, _,) => {
                for row in csv_data.nc_rows {
                    self.ingest_record(row, collection_name, vector_size,)
                        .await?;
                }
            },
            DataReaderResult::Stream(stream, _,) => {
                for record_res in stream {
                    let record =
                        record_res.map_err(|e| IngestorError::IngestionError(e.to_string(),),)?;
                    self.ingest_record(record, collection_name, vector_size,)
                        .await?;
                }
            },
            _ => {
                let json_val = serde_json::to_value(&data,)
                    .map_err(|e| IngestorError::IngestionError(e.to_string(),),)?;
                self.ingest_record(json_val, collection_name, vector_size,)
                    .await?;
            },
        }

        Ok((),)
    }
}

impl QdrantIngestor {
    async fn ensure_collection(&self, collection_name: &str, vector_size: u64,) -> Result<(),> {
        let collection_info = execute_with_retry(|| async {
            self.client
                .collection_info(collection_name,)
                .await
                .map_err(|e| {
                    wrap_error(IngestorError::DatabaseError(format!(
                        "Failed to get Qdrant collection info: {}",
                        e
                    ),),)
                },)
        },)
        .await?;

        if collection_info.result.is_none()
            || collection_info.result.unwrap().status != CollectionStatus::Green as i32
        {
            let create_collection_req = CreateCollection {
                collection_name: collection_name.to_string(),
                vectors_config: Some(qdrant_client::qdrant::VectorsConfig {
                    config: Some(qdrant_client::qdrant::vectors_config::Config::Params(
                        VectorParams {
                            size: vector_size,
                            distance: Distance::Cosine as i32,
                            ..Default::default()
                        },
                    ),),
                },),
                ..Default::default()
            };

            execute_with_retry(|| async {
                self.client
                    .create_collection(create_collection_req.clone(),)
                    .await
                    .map(|_| (),)
                    .map_err(|e| {
                        wrap_error(IngestorError::DatabaseError(format!(
                            "Failed to create Qdrant collection: {}",
                            e
                        ),),)
                    },)
            },)
            .await?;
            info!("Created Qdrant collection: {}", collection_name);
        }
        Ok((),)
    }

    async fn ingest_record(
        &self,
        record: serde_json::Value,
        collection_name: &str,
        vector_size: u64,
    ) -> Result<(),> {
        let mut qdrant_payload = HashMap::new();
        let mut text_to_embed = None;

        if let Some(obj,) = record.as_object() {
            for (key, value,) in obj {
                qdrant_payload.insert(key.clone(), serde_json_value_to_qdrant_value(value,),);

                if let Some(embed_f,) = &self.config.embed_field {
                    if key == embed_f {
                        text_to_embed = value.as_str().map(|s| s.to_string(),);
                    }
                }
            }
        }

        let vector_data = if let (Some(embedder,), Some(text,),) = (&self.embedder, text_to_embed,)
        {
            let embeddings = embedder.generate_embeddings(&[text,],).await?;
            if !embeddings.is_empty() {
                embeddings[0].clone()
            } else {
                vec![0.0; vector_size as usize]
            }
        } else {
            vec![0.1; vector_size as usize]
        };

        let point_id = Uuid::new_v4().to_string();
        let upsert_req = UpsertPoints {
            collection_name: collection_name.to_string(),
            wait: Some(true,),
            points: vec![PointStruct {
                id:      Some(PointId {
                    point_id_options: Some(PointIdOptions::Uuid(point_id.clone(),),),
                },),
                payload: qdrant_payload,
                vectors: Some(vector_data.into(),),
            }],
            ..Default::default()
        };

        execute_with_retry(|| async {
            self.client
                .upsert_points(upsert_req.clone(),)
                .await
                .map(|_| (),)
                .map_err(|e| {
                    wrap_error(IngestorError::IngestionError(format!(
                        "Failed to upsert point to Qdrant: {}",
                        e
                    ),),)
                },)
        },)
        .await?;

        Ok((),)
    }
}

// Helper function to convert serde_json::Value to qdrant_client::qdrant::Value
fn serde_json_value_to_qdrant_value(json_val: &serde_json::Value,) -> qdrant_client::qdrant::Value {
    match json_val {
        serde_json::Value::Null => qdrant_client::qdrant::Value {
            kind: Some(qdrant_client::qdrant::value::Kind::NullValue(0,),),
        }, // NullValue now expects i32
        serde_json::Value::Bool(b,) => qdrant_client::qdrant::Value {
            kind: Some(qdrant_client::qdrant::value::Kind::BoolValue(*b,),),
        },
        serde_json::Value::Number(n,) => {
            if n.is_i64() {
                qdrant_client::qdrant::Value {
                    kind: Some(qdrant_client::qdrant::value::Kind::IntegerValue(
                        n.as_i64().unwrap(),
                    ),),
                }
            } else if n.is_f64() {
                qdrant_client::qdrant::Value {
                    kind: Some(qdrant_client::qdrant::value::Kind::DoubleValue(
                        n.as_f64().unwrap(),
                    ),),
                }
            } else {
                qdrant_client::qdrant::Value {
                    kind: Some(qdrant_client::qdrant::value::Kind::StringValue(
                        n.to_string(),
                    ),),
                } // Fallback to string
            }
        },
        serde_json::Value::String(s,) => qdrant_client::qdrant::Value {
            kind: Some(qdrant_client::qdrant::value::Kind::StringValue(s.clone(),),),
        },
        serde_json::Value::Array(arr,) => {
            let list_values = arr.iter().map(serde_json_value_to_qdrant_value,).collect();
            qdrant_client::qdrant::Value {
                kind: Some(qdrant_client::qdrant::value::Kind::ListValue(
                    qdrant_client::qdrant::ListValue {
                        values: list_values,
                    },
                ),),
            }
        },
        serde_json::Value::Object(obj,) => {
            let fields = obj
                .iter()
                .map(|(k, v,)| (k.clone(), serde_json_value_to_qdrant_value(v,),),)
                .collect();
            qdrant_client::qdrant::Value {
                kind: Some(qdrant_client::qdrant::value::Kind::StructValue(
                    qdrant_client::qdrant::Struct { fields, },
                ),),
            }
        },
    }
}
