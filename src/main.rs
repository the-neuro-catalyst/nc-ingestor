// nc_ingestor/src/main.rs
// This file will contain the main entry point for the nc_ingestor CLI application.

use std::future::Future;

use clap::Parser;
use nc_ingestor::cli::{Cli, Commands, MongoArgs, Neo4jArgs, PostgresArgs, QdrantArgs, SqliteArgs};
use nc_ingestor::error::{IngestorError, Result};
use nc_ingestor::ingestor::{Ingestor, IngestorConfig};
use nc_ingestor::mongo::MongoIngestor;
use nc_ingestor::neo4j::Neo4jIngestor;
use nc_ingestor::postgres::PostgresIngestor;
use nc_ingestor::qdrant::QdrantIngestor;
use nc_ingestor::sqlite::SqliteIngestor;
use nc_reader::file_reader::{FileReaderOptions, read_file_content};
use nc_reader::output::{OutputFormat, OutputMode};
use serde::Serialize;
use tracing::{error, info};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Serialize,)]
struct ProcessingError {
    path:  String,
    error: String,
}

#[derive(Serialize, Default,)]
struct Report {
    total_files:   usize,
    success_count: usize,
    failure_count: usize,
    errors:        Vec<ProcessingError,>,
}

struct ProcessingRegistry {
    report: std::sync::Mutex<Report,>,
    strict: bool,
}

impl ProcessingRegistry {
    fn new(strict: bool,) -> Self {
        Self {
            report: std::sync::Mutex::new(Report::default(),),
            strict,
        }
    }

    fn record_success(&self,) {
        let mut report = self.report.lock().unwrap();
        report.total_files += 1;
        report.success_count += 1;
    }

    fn record_error(&self, path: &str, err: String,) -> Result<(),> {
        let mut report = self.report.lock().unwrap();
        report.total_files += 1;
        report.failure_count += 1;
        report.errors.push(ProcessingError {
            path:  path.to_string(),
            error: err.clone(),
        },);

        error!("Error at {}: {}", path, err);

        if self.strict {
            return Err(IngestorError::IngestionError(format!(
                "Strict mode enabled. Halting on error at {}: {}",
                path, err
            ),),);
        }
        Ok((),)
    }

    fn save_report(&self,) -> Result<(),> {
        let report = self.report.lock().unwrap();
        let json = serde_json::to_string_pretty(&*report,).map_err(|e| {
            IngestorError::Other(format!("Failed to serialize error report: {}", e),)
        },)?;
        std::fs::write("ingestion_report.json", json,).map_err(|e| {
            IngestorError::Other(format!("Failed to write ingestion_report.json: {}", e),)
        },)?;
        info!("Ingestion report saved to ingestion_report.json");
        Ok((),)
    }
}

#[tokio::main]
async fn main() -> Result<(),> {
    // Initialize tracing
    let file_appender = tracing_appender::rolling::never(".", "ingestor.log",);
    let (non_blocking, _guard,) = tracing_appender::non_blocking(file_appender,);

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info",),),)
        .with(fmt::layer().with_writer(std::io::stderr,),)
        .with(fmt::layer().with_writer(non_blocking,).with_ansi(false,),)
        .init();

    let cli = Cli::parse();
    let registry = std::sync::Arc::new(ProcessingRegistry::new(cli.strict,),);

    let res = match &cli.command {
        Commands::Mongo(args,) => {
            handle_ingestion(
                args,
                MongoIngestor::new,
                std::sync::Arc::clone(&registry,),
                cli.concurrency,
            )
            .await
        },
        Commands::Neo4j(args,) => {
            handle_ingestion(
                args,
                Neo4jIngestor::new,
                std::sync::Arc::clone(&registry,),
                cli.concurrency,
            )
            .await
        },
        Commands::Postgres(args,) => {
            handle_ingestion(
                args,
                PostgresIngestor::new,
                std::sync::Arc::clone(&registry,),
                cli.concurrency,
            )
            .await
        },
        Commands::Qdrant(args,) => {
            handle_ingestion(
                args,
                QdrantIngestor::new,
                std::sync::Arc::clone(&registry,),
                cli.concurrency,
            )
            .await
        },

        Commands::Sqlite(args,) => {
            handle_ingestion(
                args,
                SqliteIngestor::new,
                std::sync::Arc::clone(&registry,),
                cli.concurrency,
            )
            .await
        },
    };

    if cli.report {
        registry.save_report()?;
    }

    res
}

async fn handle_ingestion<T: Ingestor + Send + Sync + 'static, F,>(
    args: &impl IngestionArgs,
    ingestor_factory: impl FnOnce(IngestorConfig,) -> F,
    registry: std::sync::Arc<ProcessingRegistry,>,
    concurrency: usize,
) -> Result<(),>
where
    F: Future<Output = Result<T,>,> + Send + 'static,
{
    let path = args.path();
    let database_url = args.database_url();

    let config = IngestorConfig {
        database_url:    database_url.to_string(),
        collection_name: args.collection_name(),
        vector_size:     args.vector_size(),
        mappings:        args.mappings(),
        openai_api_key:  args.openai_api_key(),
        embed_field:     args.embed_field(),
        relationships:   args.relationships(),
    };

    let ingestor_res = ingestor_factory(config,).await;
    let ingestor = match ingestor_res {
        Ok(i,) => std::sync::Arc::new(i,),
        Err(e,) => {
            registry.record_error(&path.to_string_lossy(), e.to_string(),)?;
            return Ok((),);
        },
    };

    let mut files = Vec::new();
    if path.is_file() {
        files.push(path.to_path_buf(),);
    } else if path.is_dir() {
        for entry in walkdir::WalkDir::new(path,)
            .into_iter()
            .filter_map(|e| e.ok(),)
        {
            if entry.path().is_file() {
                files.push(entry.path().to_path_buf(),);
            }
        }
    }

    info!(
        "Found {} files to process with concurrency {}",
        files.len(),
        concurrency
    );

    let mut join_set = tokio::task::JoinSet::new();
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency,),);

    for file in files {
        let ingestor_task = std::sync::Arc::clone(&ingestor,);
        let registry_task = std::sync::Arc::clone(&registry,);
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        join_set.spawn(async move {
            let _permit = permit; // Hold permit until task is done
            let file_str = file.to_string_lossy().to_string();

            let reader_options = FileReaderOptions {
                head:               None,
                file_type_override: None,
                output_mode:        OutputMode::Default,
                output_format:      OutputFormat::Json,
                recursive:          false,
                filter_exts:        None,
                output_path:        None,
            };

            info!("Processing: {}", file_str);
            let nc_res = read_file_content(&file, reader_options,).await;

            let data = match nc_res {
                Ok(d,) => d,
                Err(e,) => {
                    let _ = registry_task.record_error(&file_str, e.to_string(),);
                    return;
                },
            };

            match ingestor_task.ingest(data,).await {
                Ok(_,) => {
                    registry_task.record_success();
                    info!("Successfully ingested: {}", file_str);
                },
                Err(e,) => {
                    let _ = registry_task.record_error(&file_str, e.to_string(),);
                },
            }
        },);
    }

    while let Some(res,) = join_set.join_next().await {
        if let Err(e,) = res {
            error!("Task panicked: {}", e);
        }
    }

    Ok((),)
}

// Trait to generalize over different database argument types
trait IngestionArgs {
    fn path(&self,) -> &std::path::Path;
    fn database_url(&self,) -> &str;
    fn collection_name(&self,) -> Option<String,>;
    fn vector_size(&self,) -> Option<u64,>;
    fn mappings(&self,) -> Option<std::collections::HashMap<String, String,>,>;
    fn openai_api_key(&self,) -> Option<String,>;
    fn embed_field(&self,) -> Option<String,>;
    fn relationships(&self,) -> Option<Vec<nc_ingestor::ingestor::RelationshipConfig,>,>;
}

fn map_to_hashmap(
    map_vec: &Option<Vec<(String, String,),>,>,
) -> Option<std::collections::HashMap<String, String,>,> {
    map_vec.as_ref().map(|vec| vec.iter().cloned().collect(),)
}

impl IngestionArgs for MongoArgs {
    fn path(&self,) -> &std::path::Path {
        &self.path
    }

    fn database_url(&self,) -> &str {
        &self.uri
    }

    fn collection_name(&self,) -> Option<String,> {
        self.common.collection_name.clone()
    }

    fn vector_size(&self,) -> Option<u64,> {
        self.common.vector_size
    }

    fn mappings(&self,) -> Option<std::collections::HashMap<String, String,>,> {
        map_to_hashmap(&self.common.map,)
    }

    fn openai_api_key(&self,) -> Option<String,> {
        self.common.openai_api_key.clone()
    }

    fn embed_field(&self,) -> Option<String,> {
        self.common.embed_field.clone()
    }

    fn relationships(&self,) -> Option<Vec<nc_ingestor::ingestor::RelationshipConfig,>,> {
        self.common
            .relationships
            .as_ref()
            .and_then(|s| serde_json::from_str(s,).ok(),)
    }
}

impl IngestionArgs for Neo4jArgs {
    fn path(&self,) -> &std::path::Path {
        &self.path
    }

    fn database_url(&self,) -> &str {
        &self.uri
    }

    fn collection_name(&self,) -> Option<String,> {
        self.common.collection_name.clone()
    }

    fn vector_size(&self,) -> Option<u64,> {
        self.common.vector_size
    }

    fn mappings(&self,) -> Option<std::collections::HashMap<String, String,>,> {
        map_to_hashmap(&self.common.map,)
    }

    fn openai_api_key(&self,) -> Option<String,> {
        self.common.openai_api_key.clone()
    }

    fn embed_field(&self,) -> Option<String,> {
        self.common.embed_field.clone()
    }

    fn relationships(&self,) -> Option<Vec<nc_ingestor::ingestor::RelationshipConfig,>,> {
        self.common
            .relationships
            .as_ref()
            .and_then(|s| serde_json::from_str(s,).ok(),)
    }
}

impl IngestionArgs for PostgresArgs {
    fn path(&self,) -> &std::path::Path {
        &self.path
    }

    fn database_url(&self,) -> &str {
        &self.uri
    }

    fn collection_name(&self,) -> Option<String,> {
        self.common.collection_name.clone()
    }

    fn vector_size(&self,) -> Option<u64,> {
        self.common.vector_size
    }

    fn mappings(&self,) -> Option<std::collections::HashMap<String, String,>,> {
        map_to_hashmap(&self.common.map,)
    }

    fn openai_api_key(&self,) -> Option<String,> {
        self.common.openai_api_key.clone()
    }

    fn embed_field(&self,) -> Option<String,> {
        self.common.embed_field.clone()
    }

    fn relationships(&self,) -> Option<Vec<nc_ingestor::ingestor::RelationshipConfig,>,> {
        self.common
            .relationships
            .as_ref()
            .and_then(|s| serde_json::from_str(s,).ok(),)
    }
}

impl IngestionArgs for QdrantArgs {
    fn path(&self,) -> &std::path::Path {
        &self.path
    }

    fn database_url(&self,) -> &str {
        &self.uri
    }

    fn collection_name(&self,) -> Option<String,> {
        self.common.collection_name.clone()
    }

    fn vector_size(&self,) -> Option<u64,> {
        self.common.vector_size
    }

    fn mappings(&self,) -> Option<std::collections::HashMap<String, String,>,> {
        map_to_hashmap(&self.common.map,)
    }

    fn openai_api_key(&self,) -> Option<String,> {
        self.common.openai_api_key.clone()
    }

    fn embed_field(&self,) -> Option<String,> {
        self.common.embed_field.clone()
    }

    fn relationships(&self,) -> Option<Vec<nc_ingestor::ingestor::RelationshipConfig,>,> {
        self.common
            .relationships
            .as_ref()
            .and_then(|s| serde_json::from_str(s,).ok(),)
    }
}

impl IngestionArgs for SqliteArgs {
    fn path(&self,) -> &std::path::Path {
        &self.path
    }

    fn database_url(&self,) -> &str {
        &self.db_path
    }

    fn collection_name(&self,) -> Option<String,> {
        self.common.collection_name.clone()
    }

    fn vector_size(&self,) -> Option<u64,> {
        self.common.vector_size
    }

    fn mappings(&self,) -> Option<std::collections::HashMap<String, String,>,> {
        map_to_hashmap(&self.common.map,)
    }

    fn openai_api_key(&self,) -> Option<String,> {
        self.common.openai_api_key.clone()
    }

    fn embed_field(&self,) -> Option<String,> {
        self.common.embed_field.clone()
    }

    fn relationships(&self,) -> Option<Vec<nc_ingestor::ingestor::RelationshipConfig,>,> {
        self.common
            .relationships
            .as_ref()
            .and_then(|s| serde_json::from_str(s,).ok(),)
    }
}
