use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::{Manager, Pool};
use futures_util::{SinkExt, pin_mut};
use nc_reader::nc_reader_result::{DataReaderResult, RecordStream};
use tokio_postgres::{Config as TokioPgConfig, CopyInSink, NoTls};
use tracing::info;

use crate::error::{IngestorError, Result};
use crate::ingestor::{Ingestor, IngestorConfig};
use crate::retry::{execute_with_retry, wrap_error};
use crate::schema_builder::{SqlDialect, SqlSchemaBuilder};

pub struct PostgresIngestor {
    #[allow(dead_code)]
    config: IngestorConfig,

    pool: Pool,
}

#[async_trait]
impl Ingestor for PostgresIngestor {
    async fn new(config: IngestorConfig,) -> Result<Self,> {
        let pg_config = TokioPgConfig::from_str(&config.database_url,).map_err(|e| {
            IngestorError::ConfigurationError(format!("Invalid PostgreSQL URI: {}", e),)
        },)?;

        let manager = Manager::new(pg_config, NoTls,);
        let pool = Pool::builder(manager,)
            .max_size(16,) // Example max pool size
            .build()
            .map_err(|e| {
                IngestorError::ConnectionError(format!("Failed to create PostgreSQL pool: {}", e),)
            },)?;

        // Test the connection with retry
        execute_with_retry(|| async {
            pool.get().await.map(|_| (),).map_err(|e| {
                wrap_error(IngestorError::ConnectionError(format!(
                    "Failed to get client from pool: {}",
                    e
                ),),)
            },)
        },)
        .await?;

        Ok(PostgresIngestor { config, pool, },)
    }

    async fn ingest(&self, data: DataReaderResult,) -> Result<(),> {
        let table_name = self
            .config
            .collection_name
            .as_deref()
            .unwrap_or(crate::DEFAULT_SQL_TABLE_NAME,)
            .to_string();

        let mappings = self.config.mappings.clone();

        match data {
            DataReaderResult::Csv(csv_data, _metadata,) => {
                if let Some(schema,) = csv_data.inferred_schema {
                    let client = self
                        .pool
                        .get()
                        .await
                        .map_err(|e| IngestorError::ConnectionError(e.to_string(),),)?;
                    let builder = SqlSchemaBuilder::new(SqlDialect::Postgres, mappings.clone(),);
                    let create_query = builder.build_create_table(&table_name, &schema,);

                    execute_with_retry(|| async {
                        client
                            .execute(&create_query, &[],)
                            .await
                            .map(|_| (),)
                            .map_err(|e| wrap_error(IngestorError::DatabaseError(e.to_string(),),),)
                    },)
                    .await?;

                    let mut col_names: Vec<String,> = schema.keys().cloned().collect();
                    col_names.sort();

                    self.ingest_via_copy(
                        csv_data.nc_rows.into_iter(),
                        &table_name,
                        &col_names,
                        mappings,
                    )
                    .await?;
                } else {
                    self.ingest_as_blob(DataReaderResult::Csv(csv_data, _metadata,), &table_name,)
                        .await?;
                }
            },
            DataReaderResult::Stream(stream, _metadata,) => {
                self.batch_ingest_stream(stream, &table_name,).await?;
            },
            _ => {
                self.ingest_as_blob(data, &table_name,).await?;
            },
        }

        info!(
            "Successfully ingested data to PostgreSQL table '{}'.",
            table_name
        );
        Ok((),)
    }
}

impl PostgresIngestor {
    async fn ingest_via_copy(
        &self,
        rows: impl Iterator<Item = serde_json::Value,>,
        table_name: &str,
        col_names: &[String],
        mappings: Option<HashMap<String, String,>,>,
    ) -> Result<(),> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| IngestorError::ConnectionError(e.to_string(),),)?;

        let mapped_cols: Vec<String,> = col_names
            .iter()
            .map(|c| {
                let target = mappings
                    .as_ref()
                    .and_then(|m: &HashMap<String, String,>| m.get(c,),)
                    .unwrap_or(c,);
                format!("\"{}\"", target)
            },)
            .collect();

        let copy_query = format!(
            "COPY \"{}\" ({}) FROM STDIN (FORMAT CSV, HEADER FALSE)",
            table_name,
            mapped_cols.join(", ")
        );

        let sink: CopyInSink<Bytes,> = client
            .copy_in(&copy_query,)
            .await
            .map_err(|e| IngestorError::DatabaseError(e.to_string(),),)?;
        pin_mut!(sink);

        for row in rows {
            if let serde_json::Value::Object(obj,) = row {
                let mut line = String::new();
                for (i, col,) in col_names.iter().enumerate() {
                    if i > 0 {
                        line.push(',',);
                    }
                    let val = obj.get(col,).unwrap_or(&serde_json::Value::Null,);
                    line.push_str(&json_value_to_csv_field(val,),);
                }
                line.push('\n',);
                sink.send(Bytes::from(line,),)
                    .await
                    .map_err(|e: tokio_postgres::Error| {
                        IngestorError::IngestionError(e.to_string(),)
                    },)?;
            }
        }

        sink.close()
            .await
            .map_err(|e: tokio_postgres::Error| IngestorError::IngestionError(e.to_string(),),)?;
        Ok((),)
    }

    async fn batch_ingest_stream(&self, stream: RecordStream, table_name: &str,) -> Result<(),> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| IngestorError::ConnectionError(e.to_string(),),)?;

        let create_table_query = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL
            )",
            table_name
        );

        execute_with_retry(|| async {
            client
                .execute(&create_table_query, &[],)
                .await
                .map(|_| (),)
                .map_err(|e| wrap_error(IngestorError::DatabaseError(e.to_string(),),),)
        },)
        .await?;

        let copy_query = format!(
            "COPY \"{}\" (data) FROM STDIN (FORMAT CSV, HEADER FALSE)",
            table_name
        );
        let sink: CopyInSink<Bytes,> = client
            .copy_in(&copy_query,)
            .await
            .map_err(|e| IngestorError::DatabaseError(e.to_string(),),)?;
        pin_mut!(sink);

        for record_res in stream {
            let record = record_res.map_err(|e| IngestorError::IngestionError(e.to_string(),),)?;
            let json_data = serde_json::to_string(&record,)
                .map_err(|e| IngestorError::IngestionError(e.to_string(),),)?;

            let mut line = json_value_to_csv_field(&serde_json::Value::String(json_data,),);
            line.push('\n',);
            sink.send(Bytes::from(line,),)
                .await
                .map_err(
                    |e: tokio_postgres::Error| IngestorError::IngestionError(e.to_string(),),
                )?;
        }

        sink.close()
            .await
            .map_err(|e: tokio_postgres::Error| IngestorError::IngestionError(e.to_string(),),)?;
        Ok((),)
    }

    async fn ingest_as_blob(&self, data: DataReaderResult, table_name: &str,) -> Result<(),> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| IngestorError::ConnectionError(e.to_string(),),)?;

        let create_table_query = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL
            )",
            table_name
        );

        execute_with_retry(|| async {
            client
                .execute(&create_table_query, &[],)
                .await
                .map(|_| (),)
                .map_err(|e| wrap_error(IngestorError::DatabaseError(e.to_string(),),),)
        },)
        .await?;

        let json_data = serde_json::to_string(&data,)
            .map_err(|e| IngestorError::IngestionError(e.to_string(),),)?;
        let insert_query = format!("INSERT INTO \"{}\" (data) VALUES ($1)", table_name);

        execute_with_retry(|| async {
            client
                .execute(&insert_query, &[&json_data,],)
                .await
                .map(|_| (),)
                .map_err(|e| wrap_error(IngestorError::IngestionError(e.to_string(),),),)
        },)
        .await?;
        Ok((),)
    }
}

fn json_value_to_csv_field(val: &serde_json::Value,) -> String {
    match val {
        serde_json::Value::Null => "".to_string(),
        serde_json::Value::Bool(b,) => b.to_string(),
        serde_json::Value::Number(n,) => n.to_string(),
        serde_json::Value::String(s,) => {
            if s.contains(',',) || s.contains('"',) || s.contains('\n',) || s.contains('\r',) {
                format!("\"{}\"", s.replace("\"", "\"\""))
            } else {
                s.clone()
            }
        },
        serde_json::Value::Array(_,) | serde_json::Value::Object(_,) => {
            let s = val.to_string();
            format!("\"{}\"", s.replace("\"", "\"\""))
        },
    }
}
