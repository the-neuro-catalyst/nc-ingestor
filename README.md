# 🌉 nc_ingestor

**nc_ingestor** is the "Integration" layer of the Neuro-Catalyst stack. It provides a resilient, high-concurrency pipeline for loading data into various storage engines.

## 🚀 Features

- **Parallel Ingestion**: Uses `tokio::task::JoinSet` and semaphores for concurrent file processing.
- **Resilient Retries**: Implements exponential backoff via the `backoff` crate for network-sensitive databases.
- **Bulk Loading**: Optimized Postgres ingestion using the `COPY` protocol.
- **AI/Vector Support**: Automatic embedding generation using OpenAI and ingestion into Qdrant.
- **Graph Inference**: Idempotent node and relationship merging for Neo4j.
- **Multi-DB Support**:
  - SQL: PostgreSQL, SQLite.
  - NoSQL: MongoDB.
  - Graph: Neo4j.
  - Vector: Qdrant.

## 💻 CLI Usage

```bash
# Ingest all CSVs into Postgres with parallel workers
nc_ingestor --input-dir ./data --db-type postgres --concurrency 4

# Generate embeddings and store in Qdrant
nc_ingestor --input-file data.json --db-type qdrant --embed-fields "description"
```

## 🛠️ Architecture: The `Ingestor` Trait

Each database implementation follows a unified async trait:

```rust
#[async_trait]
pub trait Ingestor {
    async fn ingest_record(&self, record: Value, schema: &Schema) -> Result<(), IngestorError>;
    async fn finalize(&self) -> Result<(), IngestorError>;
}
```

## 🛡️ Resilience Strategy

- **Error Registry**: All transient failures are logged and retried.
- **Backpressure**: Semaphore-based concurrency control prevents OOM when processing thousands of files.
- **Idempotency**: Ingestors are designed to be re-run safely without creating duplicate data.
