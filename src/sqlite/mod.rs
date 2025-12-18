// nc_ingestor/src/sqlite/mod.rs
// SQLite specific ingestion logic.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nc_reader::nc_reader_result::{DataReaderResult, RecordStream};
use rusqlite::{Connection, params};
use tokio::task;
use tracing::info;

use crate::error::{IngestorError, Result};
use crate::ingestor::{Ingestor, IngestorConfig};
use crate::schema_builder::{SqlDialect, SqlSchemaBuilder};

pub struct SqliteIngestor {
    #[allow(dead_code)]
    config: IngestorConfig,
    conn:   Arc<Mutex<Connection,>,>,
}

#[async_trait]
impl Ingestor for SqliteIngestor {
    async fn new(config: IngestorConfig,) -> Result<Self,> {
        let database_url_owned = config.database_url.clone();
        let conn_path = database_url_owned.trim_start_matches("sqlite://",);
        let conn_path_owned = conn_path.to_string();
        let conn = task::spawn_blocking(move || Connection::open(conn_path_owned,),)
            .await
            .map_err(|e| {
                IngestorError::Other(format!(
                    "Failed to spawn blocking task for SQLite connection: {}",
                    e
                ),)
            },)?
            .map_err(|e| {
                IngestorError::ConnectionError(format!("Failed to connect to SQLite: {}", e),)
            },)?;

        let conn_arc = Arc::new(Mutex::new(conn,),);

        Ok(SqliteIngestor {
            config,
            conn: conn_arc,
        },)
    }

    async fn ingest(&self, data: DataReaderResult,) -> Result<(),> {
        let table_name = self
            .config
            .collection_name
            .as_deref()
            .unwrap_or(crate::DEFAULT_SQL_TABLE_NAME,)
            .to_string();

        let conn_clone = Arc::clone(&self.conn,);
        let table_name_clone = table_name.clone();
        let mappings = self.config.mappings.clone();

        match data {
            DataReaderResult::Csv(csv_data, _metadata,) => {
                if let Some(schema) = csv_data.inferred_schema {
                    // Structured Ingestion
                    let builder = SqlSchemaBuilder::new(SqlDialect::Sqlite, mappings.clone());
                    let create_query = builder.build_create_table(&table_name_clone, &schema);
                    
                    task::spawn_blocking(move || {
                        let conn = conn_clone.lock().unwrap();
                        conn.execute(&create_query, [])
                    }).await.map_err(|e| IngestorError::Other(e.to_string()))?
                    .map_err(|e| IngestorError::DatabaseError(e.to_string()))?;

                    // INSERT rows
                    let conn_clone = Arc::clone(&self.conn,);
                    let table_name_for_insert = table_name_clone.clone();
                    
                    // Build Insert Query
                    let mut col_names: Vec<String> = schema.keys().cloned().collect();
                    col_names.sort();
                    
                    let mapped_cols: Vec<String> = col_names.iter().map(|c| {
                        let target = mappings.as_ref().and_then(|m| m.get(c)).unwrap_or(c);
                        format!("`{}`", target)
                    }).collect();
                    
                    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{}", i)).collect();
                    let insert_sql = format!("INSERT INTO `{}` ({}) VALUES ({})", 
                        table_name_for_insert, 
                        mapped_cols.join(", "), 
                        placeholders.join(", ")
                    );

                    let nc_rows = csv_data.nc_rows;
                    task::spawn_blocking(move || {
                        let mut conn = conn_clone.lock().unwrap();
                        let tx = conn.transaction().map_err(|e| IngestorError::DatabaseError(e.to_string()))?;
                        
                        {
                            let mut stmt = tx.prepare(&insert_sql).map_err(|e| IngestorError::DatabaseError(e.to_string()))?;
                            for row in nc_rows {
                                if let serde_json::Value::Object(obj) = row {
                                    let mut params = Vec::new();
                                    for col in &col_names {
                                        let val = obj.get(col).unwrap_or(&serde_json::Value::Null);
                                        // Convert serde_json::Value to rusqlite::types::Value (simplified)
                                        let sql_val = match val {
                                            serde_json::Value::Number(n) => {
                                                if let Some(i) = n.as_i64() { rusqlite::types::Value::Integer(i) }
                                                else { rusqlite::types::Value::Real(n.as_f64().unwrap_or(0.0)) }
                                            }
                                            serde_json::Value::String(s) => rusqlite::types::Value::Text(s.clone()),
                                            serde_json::Value::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
                                            _ => rusqlite::types::Value::Null,
                                        };
                                        params.push(sql_val);
                                    }
                                    stmt.execute(rusqlite::params_from_iter(params)).map_err(|e| IngestorError::IngestionError(e.to_string()))?;
                                }
                            }
                        }
                        tx.commit().map_err(|e| IngestorError::DatabaseError(e.to_string()))
                    }).await.map_err(|e| IngestorError::Other(e.to_string()))??;
                } else {
                    // Fallback to Blob if no schema
                    self.ingest_as_blob(DataReaderResult::Csv(csv_data, _metadata), &table_name_clone).await?;
                }
            },
            DataReaderResult::Stream(stream, _metadata) => {
                self.batch_ingest_stream(stream, &table_name_clone).await?;
            }
            _ => {
                // Fallback for other types
                self.ingest_as_blob(data, &table_name_clone).await?;
            }
        }

        info!("Successfully ingested data to SQLite table '{}'.", table_name);
        Ok(())
    }
}

impl SqliteIngestor {
    async fn batch_ingest_stream(&self, stream: RecordStream, table_name: &str) -> Result<()> {
        let conn_clone = Arc::clone(&self.conn,);
        let table_name_for_create = table_name.to_string();
        
        // Ensure table exists (blob mode for generic stream)
        task::spawn_blocking(move || {
            let conn = conn_clone.lock().unwrap();
            let create_table_query = format!(
                "CREATE TABLE IF NOT EXISTS `{}` (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL
                )",
                table_name_for_create
            );
            conn.execute(&create_table_query, [],)
        },)
        .await
        .map_err(|e| IngestorError::Other(e.to_string()))?
        .map_err(|e| IngestorError::DatabaseError(e.to_string()))?;

        let conn_clone = Arc::clone(&self.conn,);
        let table_name_clone = table_name.to_string();
        let insert_query = format!("INSERT INTO `{}` (data) VALUES (?1)", table_name_clone);

        task::spawn_blocking(move || {
            let mut conn = conn_clone.lock().unwrap();
            let tx = conn.transaction().map_err(|e| IngestorError::DatabaseError(e.to_string()))?;
            
            let mut count = 0;
            {
                let mut stmt = tx.prepare(&insert_query).map_err(|e| IngestorError::DatabaseError(e.to_string()))?;
                for record_res in stream {
                    let record = record_res.map_err(|e: nc_reader::error::DataReaderError| IngestorError::IngestionError(e.to_string()))?;
                    let json_data = serde_json::to_string(&record).map_err(|e| IngestorError::IngestionError(e.to_string()))?;
                    stmt.execute(params![json_data]).map_err(|e| IngestorError::IngestionError(e.to_string()))?;
                    
                    count += 1;
                    if count >= 1000 {
                        // We can't easily commit and continue inside this closure because stmt holds a borrow of tx.
                        // For simplicity, we'll do one big transaction for now, or we could refactor to chunk the stream outside.
                    }
                }
            }
            tx.commit().map_err(|e| IngestorError::DatabaseError(e.to_string()))
        })
        .await
        .map_err(|e| IngestorError::Other(e.to_string()))?
        .map_err(|e| IngestorError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn ingest_as_blob(&self, data: DataReaderResult, table_name: &str) -> Result<()> {
        let conn_clone = Arc::clone(&self.conn,);
        let table_name_for_create = table_name.to_string();
        task::spawn_blocking(move || {
            let conn = conn_clone.lock().unwrap();
            let create_table_query = format!(
                "CREATE TABLE IF NOT EXISTS `{}` (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL
                )",
                table_name_for_create
            );
            conn.execute(&create_table_query, [],)
        },)
        .await
        .map_err(|e| IngestorError::Other(e.to_string()))?
        .map_err(|e| IngestorError::DatabaseError(e.to_string()))?;

        let json_data = serde_json::to_string(&data,).map_err(|e| IngestorError::IngestionError(e.to_string()))?;

        let conn_clone = Arc::clone(&self.conn,);
        let table_name_clone = table_name.to_string();
        task::spawn_blocking(move || {
            let conn = conn_clone.lock().unwrap();
            let insert_query = format!("INSERT INTO `{}` (data) VALUES (?1)", table_name_clone);
            conn.execute(&insert_query, params![json_data],)
        },)
        .await
        .map_err(|e| IngestorError::Other(e.to_string()))?
        .map_err(|e| IngestorError::IngestionError(e.to_string()))?;
        Ok(())
    }
}
