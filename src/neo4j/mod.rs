// nc_ingestor/src/neo4j/mod.rs
// Neo4j specific ingestion logic.

use std::collections::HashMap;

use async_trait::async_trait;
use nc_reader::nc_reader_result::DataReaderResult;
use neo4rs::{BoltType, Graph, query};
use tracing::info;

use crate::error::{IngestorError, Result};
use crate::ingestor::{Ingestor, IngestorConfig};
use crate::retry::{execute_with_retry, wrap_error};

pub struct Neo4jIngestor {
    config: IngestorConfig,
    graph:  Graph,
}

#[async_trait]
impl Ingestor for Neo4jIngestor {
    async fn new(config: IngestorConfig,) -> Result<Self,> {
        let uri = &config.database_url;

        let parsed_uri = url::Url::parse(uri,)
            .map_err(|e| IngestorError::ConfigurationError(format!("Invalid Neo4j URI: {}", e),),)?;

        let host = parsed_uri.host_str().unwrap_or("localhost",).to_string();
        let port = parsed_uri.port().unwrap_or(7687,);

        let username = parsed_uri.username();
        let password = parsed_uri.password().unwrap_or_default();

        let host_port = format!("{}:{}", host, port);
        let graph = execute_with_retry(|| async {
            Graph::new(&host_port, username, password,)
                .await
                .map_err(|e| {
                    wrap_error(IngestorError::ConnectionError(format!(
                        "Failed to connect to Neo4j: {:?}",
                        e
                    ),),)
                },)
        },)
        .await?;

        Ok(Neo4jIngestor { config, graph, },)
    }

    async fn ingest(&self, data: DataReaderResult,) -> Result<(),> {
        let label_name = self
            .config
            .collection_name
            .as_deref()
            .unwrap_or("IngestedData",)
            .to_string();

        match data {
            DataReaderResult::Csv(csv_data, _,) => {
                for row in csv_data.nc_rows {
                    self.ingest_record(row, &label_name,).await?;
                }
            },
            DataReaderResult::Stream(stream, _,) => {
                for record_res in stream {
                    let record =
                        record_res.map_err(|e| IngestorError::IngestionError(e.to_string(),),)?;
                    self.ingest_record(record, &label_name,).await?;
                }
            },
            _ => {
                let json_val = serde_json::to_value(&data,)
                    .map_err(|e| IngestorError::IngestionError(e.to_string(),),)?;
                self.ingest_record(json_val, &label_name,).await?;
            },
        }

        info!(
            "Successfully ingested data to Neo4j with label '{}'.",
            label_name
        );
        Ok((),)
    }
}

impl Neo4jIngestor {
    async fn ingest_record(&self, record: serde_json::Value, label: &str,) -> Result<(),> {
        let record_obj = record.as_object().ok_or_else(|| {
            IngestorError::IngestionError("Record must be an object".to_string(),)
        },)?;

        // Find a unique ID for MERGE
        let id_field = if record_obj.contains_key("id",) {
            "id"
        } else if record_obj.contains_key("ID",) {
            "ID"
        } else if record_obj.contains_key("uuid",) {
            "uuid"
        } else {
            ""
        };

        let id_value = if !id_field.is_empty() {
            record_obj
                .get(id_field,)
                .unwrap()
                .to_string()
                .replace("\"", "",)
        } else {
            // Use hash of the record as ID if no ID field found
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            record.to_string().hash(&mut hasher,);
            hasher.finish().to_string()
        };

        let json_data = serde_json::to_string(&record,)
            .map_err(|e| IngestorError::IngestionError(e.to_string(),),)?;
        let bolt_props = json_to_bolt(&record,);

        // MERGE node
        let merge_query = format!(
            "MERGE (n:{} {{_id: $id}}) SET n += $props, n.data = $data",
            label
        );

        execute_with_retry(|| async {
            self.graph
                .run(
                    query(&merge_query,)
                        .param("id", id_value.clone(),)
                        .param("props", bolt_props.clone(),)
                        .param("data", json_data.clone(),),
                )
                .await
                .map(|_| (),)
                .map_err(|e| {
                    wrap_error(IngestorError::IngestionError(format!(
                        "Failed to merge node in Neo4j: {:?}",
                        e
                    ),),)
                },)
        },)
        .await?;

        // Handle relationships
        if let Some(relationships,) = &self.config.relationships {
            for rel in relationships {
                if let Some(source_val,) = record_obj.get(&rel.source_field,) {
                    if source_val.is_null() {
                        continue;
                    }

                    let target_id = source_val.to_string().replace("\"", "",);
                    let rel_query = format!(
                        "MATCH (a:{} {{_id: $source_id}}) 
                         MERGE (b:{} {{_id: $target_id}}) 
                         MERGE (a)-[:{}]->(b)",
                        label, rel.target_label, rel.relationship_type
                    );

                    execute_with_retry(|| async {
                        self.graph
                            .run(
                                query(&rel_query,)
                                    .param("source_id", id_value.clone(),)
                                    .param("target_id", target_id.clone(),),
                            )
                            .await
                            .map(|_| (),)
                            .map_err(|e| {
                                wrap_error(IngestorError::IngestionError(format!(
                                    "Failed to create relationship in Neo4j: {:?}",
                                    e
                                ),),)
                            },)
                    },)
                    .await?;
                }
            }
        }

        Ok((),)
    }
}

fn json_to_bolt(val: &serde_json::Value,) -> BoltType {
    match val {
        serde_json::Value::Null => BoltType::Null(neo4rs::BoltNull {},),
        serde_json::Value::Bool(b,) => BoltType::Boolean(neo4rs::BoltBoolean { value: *b, },),
        serde_json::Value::Number(n,) => {
            if let Some(i,) = n.as_i64() {
                BoltType::Integer(neo4rs::BoltInteger { value: i, },)
            } else {
                BoltType::Float(neo4rs::BoltFloat {
                    value: n.as_f64().unwrap_or(0.0,),
                },)
            }
        },
        serde_json::Value::String(s,) => {
            BoltType::String(neo4rs::BoltString { value: s.clone(), },)
        },
        serde_json::Value::Array(arr,) => {
            let vec: Vec<BoltType,> = arr.iter().map(json_to_bolt,).collect();
            BoltType::List(neo4rs::BoltList { value: vec, },)
        },
        serde_json::Value::Object(obj,) => {
            let mut map = HashMap::new();
            for (k, v,) in obj {
                map.insert(neo4rs::BoltString { value: k.clone(), }, json_to_bolt(v,),);
            }
            BoltType::Map(neo4rs::BoltMap { value: map, },)
        },
    }
}
