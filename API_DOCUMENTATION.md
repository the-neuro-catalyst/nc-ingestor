# nc_ingestor API Documentation

**The Integration Layer**
`nc_ingestor` is a resilient, concurrent loading engine responsible for pushing processed data into various storage backends.

## 💻 CLI Interface

### Basic Usage
```bash
nc_ingestor <COMMAND> [OPTIONS]
```

### Global Options
| Option | Description | Default |
| :--- | :--- | :--- |
| `--concurrency <N>` | Number of parallel file processors. | `4` |
| `--strict` | Halt on first error. | `false` |
| `--report` | Generate completion report. | `false` |

### Subcommands & Database Support

#### 1. PostgreSQL (`postgres`)
High-performance bulk loading using `COPY`.

```bash
nc_ingestor postgres \
  --uri "postgres://user:pass@host/db" \
  --path ./data.csv \
  --map "csv_col:table_col"
```

#### 2. MongoDB (`mongo`)
Document-store ingestion.

```bash
nc_ingestor mongo \
  --uri "mongodb://localhost:27017" \
  --path ./data.json \
  --collection-name "my_collection"
```

#### 3. Qdrant (`qdrant`)
Vector search ingestion with automatic embedding generation.

```bash
nc_ingestor qdrant \
  --uri "http://localhost:6333" \
  --path ./docs \
  --embed-field "text_content" \
  --openai-api-key "sk-..."
```

#### 4. Neo4j (`neo4j`)
Graph database ingestion with relationship mapping.

```bash
nc_ingestor neo4j \
  --uri "bolt://localhost:7687" \
  --path ./relations.csv \
  --relationships '[{"source":"user_id", "target":"group_id", "type":"BELONGS_TO"}]'
```

#### 5. SQLite (`sqlite`)
Local database file ingestion.

```bash
nc_ingestor sqlite --db-path ./local.db --path ./data.csv
```

## 🛡️ Resilience Features
- **Exponential Backoff:** Automatically retries failed network requests.
- **Concurrency Control:** Semaphore-based limiting to prevent OOM.
- **Idempotency:** Operations are designed to be safe to re-run.

```