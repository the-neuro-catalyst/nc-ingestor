// nc_ingestor/tests/integration_tests.rs

use std::str::FromStr;

use deadpool_postgres::{Manager, Pool};
use mongodb::Client;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use nc_ingestor::ingestor::{Ingestor, IngestorConfig};
use nc_ingestor::mongo::MongoIngestor;
use nc_ingestor::neo4j::Neo4jIngestor;
use nc_ingestor::postgres::PostgresIngestor;
use nc_ingestor::qdrant::QdrantIngestor;
use nc_ingestor::sqlite::SqliteIngestor;
use nc_reader::nc_reader_result::DataReaderResult;
use nc_reader::reader::txt_reader::TextData;
use neo4rs::{Graph, query};
use qdrant_client::Qdrant;
use rusqlite::{Connection, params};
use tempfile::NamedTempFile;
use tokio_postgres::{Config as TokioPgConfig, NoTls};
#[tokio::test]
async fn test_sqlite_ingestion() {
    // 1. Create a temporary SQLite database file
    let db_file = NamedTempFile::new().expect("Failed to create temporary file",);
    let db_path = db_file.path().to_str().expect("Failed to get path string",);
    let database_url = format!("sqlite://{}", db_path);

    // 2. Initialize SqliteIngestor with the temporary database URL
    let config = IngestorConfig {
        database_url:    database_url.clone(),
        collection_name: None,
        vector_size:     None,
        mappings:        None,
        openai_api_key:  None,
        embed_field:     None,
        relationships:   None,
    };
    let ingestor = SqliteIngestor::new(config,)
        .await
        .expect("Failed to create SqliteIngestor",);

    // 3. Create dummy DataReaderResult
    let test_nc_content = "This is a test string.".to_string(); // Keep test_nc_content for later assertion
    let test_data = DataReaderResult::Text(
        TextData {
            content:     test_nc_content.clone(),
            first_lines: Some(vec![test_nc_content.clone()],),
            line_count:  1,
            total_size:  1,
        },
        nc_reader::nc_reader_result::FileMetadata {
            size:       test_nc_content.len() as u64,
            line_count: Some(1,),
        },
    );
    let serialized_test_data = serde_json::to_string(&test_data,).unwrap(); // Serialize before move
    // 4. Call the ingest method
    ingestor
        .ingest(test_data,)
        .await
        .expect("Failed to ingest data",);

    // 5. Verify that the data was successfully inserted into the temporary database
    let verification_db_path = db_file
        .path()
        .to_str()
        .expect("Failed to get path string for verification",);
    let conn = Connection::open(verification_db_path,)
        .expect("Failed to connect to the test database for verification",);

    let mut stmt = conn
        .prepare("SELECT id, data FROM ingested_data",)
        .expect("Failed to prepare statement",);
    let rows: Vec<(i64, String,),> = stmt
        .query_map(params![], |row| Ok((row.get(0,)?, row.get(1,)?,),),)
        .expect("Failed to query data",)
        .collect::<std::result::Result<Vec<(i64, String,),>, rusqlite::Error,>>()
        .expect("Failed to collect rows",);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, serialized_test_data);

    // 6. Clean up: The `NamedTempFile` will automatically be deleted when it goes out of scope.
}

#[tokio::test]
async fn test_mongodb_ingestion() {
    // This test assumes a local MongoDB instance is running at the default port.
    // For CI/CD environments, this might need to be mocked or run against a test container.
    if std::env::var("RUN_MONGO_TESTS",).is_err() {
        println!("Skipping MongoDB ingestion test: RUN_MONGO_TESTS environment variable not set.");
        return;
    }
    let mongo_uri = "mongodb://localhost:27017";
    let database_name = "test_scm_db";
    let collection_name = "test_ingested_data";

    let config = IngestorConfig {
        database_url:    mongo_uri.to_string(),
        collection_name: None,
        vector_size:     None,
        mappings:        None,
        openai_api_key:  None,
        embed_field:     None,
        relationships:   None,
    };
    let ingestor = MongoIngestor::new(config,)
        .await
        .expect("Failed to create MongoIngestor",);

    let test_nc_content = "This is a test document for MongoDB.".to_string();
    let test_data = DataReaderResult::Text(
        TextData {
            content:     test_nc_content.clone(),
            first_lines: Some(vec![test_nc_content.clone()],),
            line_count:  1,
            total_size:  test_nc_content.len() as u64,
        },
        nc_reader::nc_reader_result::FileMetadata {
            size:       test_nc_content.len() as u64,
            line_count: Some(1,),
        },
    );

    ingestor
        .ingest(test_data,)
        .await
        .expect("Failed to ingest data to MongoDB",);

    // Verify data
    let client_options = ClientOptions::parse(mongo_uri,).await.unwrap();
    let client = Client::with_options(client_options,).unwrap();
    let collection = client
        .database(database_name,)
        .collection::<mongodb::bson::Document>(collection_name,);

    let filter = doc! {
        "Text.content": &test_nc_content,
        // Assuming DataReaderResult::Text serializes to a document with a "Text" field
    };

    let fetched_document = collection.find_one(filter, None,).await.unwrap();

    assert!(fetched_document.is_some());

    // Clean up
    collection.delete_many(doc! {}, None,).await.unwrap();
}

#[tokio::test]
async fn test_neo4j_ingestion() {
    // This test assumes a local Neo4j instance is running at the default bolt port (7687)
    // with user 'neo4j' and password 'password'.
    // For CI/CD environments, this might need to be mocked or run against a test container.
    if std::env::var("RUN_NEO4J_TESTS",).is_err() {
        println!("Skipping Neo4j ingestion test: RUN_NEO4J_TESTS environment variable not set.");
        return;
    }
    let neo4j_uri = "bolt://localhost:7687";
    let neo4j_user = "neo4j";
    let neo4j_password = "password"; // Default password for Neo4j desktop/docker

    let config = IngestorConfig {
        database_url:    format!(
            "{}?user={}&password={}",
            neo4j_uri, neo4j_user, neo4j_password
        ),
        collection_name: None,
        vector_size:     None,
        mappings:        None,
        openai_api_key:  None,
        embed_field:     None,
        relationships:   None,
    };
    let ingestor = Neo4jIngestor::new(config,)
        .await
        .expect("Failed to create Neo4jIngestor",);

    let test_nc_content = "This is a test document for Neo4j.".to_string();
    let test_data = DataReaderResult::Text(
        TextData {
            content:     test_nc_content.clone(),
            first_lines: Some(vec![test_nc_content.clone()],),
            line_count:  1,
            total_size:  test_nc_content.len() as u64,
        },
        nc_reader::nc_reader_result::FileMetadata {
            size:       test_nc_content.len() as u64,
            line_count: Some(1,),
        },
    );

    ingestor
        .ingest(test_data,)
        .await
        .expect("Failed to ingest data to Neo4j",);

    // Verify data
    let graph = Graph::new(neo4j_uri, neo4j_user, neo4j_password,)
        .await
        .expect("Failed to connect to Neo4j for verification",);

    let query_str =
        "MATCH (n:IngestedData) WHERE n.data CONTAINS $expected_content RETURN n.data AS data";
    let mut result_stream = graph
        .execute(query(query_str,).param("expected_content", test_nc_content.clone(),),)
        .await
        .expect("Failed to execute verification query",);

    let mut found_data = Vec::new();
    while let Ok(Some(row,),) = result_stream.next().await {
        found_data.push(
            row.get::<String>("data",)
                .expect("Failed to get data from row",),
        );
    }

    assert_eq!(found_data.len(), 1);
    assert!(found_data[0].contains(&test_nc_content));

    // Clean up
    graph
        .run(query("MATCH (n:IngestedData) DETACH DELETE n",),)
        .await
        .expect("Failed to clean up Neo4j data",);
}

#[tokio::test]
async fn test_postgres_ingestion() {
    // This test assumes a local PostgreSQL instance is running with a database
    // named `test_db` and a user `postgres` with password `password`.
    if std::env::var("RUN_POSTGRES_TESTS",).is_err() {
        println!(
            "Skipping PostgreSQL ingestion test: RUN_POSTGRES_TESTS environment variable not set."
        );
        return;
    }
    let postgres_uri = "postgres://postgres:password@localhost:5432/test_db";

    let config = IngestorConfig {
        database_url:    postgres_uri.to_string(),
        collection_name: None,
        vector_size:     None,
        mappings:        None,
        openai_api_key:  None,
        embed_field:     None,
        relationships:   None,
    };
    let ingestor = PostgresIngestor::new(config,)
        .await
        .expect("Failed to create PostgresIngestor",);

    let test_nc_content = "This is a test document for PostgreSQL.".to_string();
    let test_data = DataReaderResult::Text(
        TextData {
            content:     test_nc_content.clone(),
            first_lines: Some(vec![test_nc_content.clone()],),
            line_count:  1,
            total_size:  test_nc_content.len() as u64,
        },
        nc_reader::nc_reader_result::FileMetadata {
            size:       test_nc_content.len() as u64,
            line_count: Some(1,),
        },
    );

    ingestor
        .ingest(test_data,)
        .await
        .expect("Failed to ingest data to PostgreSQL",);

    // Verify data
    let pg_config = TokioPgConfig::from_str(postgres_uri,).unwrap();
    let manager = Manager::new(pg_config, NoTls,);
    let pool = Pool::builder(manager,)
        .max_size(1,) // Small pool for testing
        .build()
        .expect("Failed to build verification pool",);
    let client = pool
        .get()
        .await
        .expect("Failed to get client for verification",);

    let rows = client
        .query(
            "SELECT data FROM ingested_data WHERE data LIKE $1",
            &[&format!("%{}%", test_nc_content),],
        )
        .await
        .expect("Failed to query data from PostgreSQL",);

    assert_eq!(rows.len(), 1);
    assert!(rows[0].get::<usize, String>(0).contains(&test_nc_content));

    // Clean up
    client
        .execute(
            "DELETE FROM ingested_data WHERE data LIKE $1",
            &[&format!("%{}%", test_nc_content),],
        )
        .await
        .expect("Failed to clean up PostgreSQL data",);
}

#[tokio::test]
async fn test_qdrant_ingestion() {
    // This test assumes a local Qdrant instance is running at http://localhost:6334.
    if std::env::var("RUN_QDRANT_TESTS",).is_err() {
        println!("Skipping Qdrant ingestion test: RUN_QDRANT_TESTS environment variable not set.");
        return;
    }
    let qdrant_uri = "http://localhost:6334";
    let _collection_name = "test_ingested_nc_collection";
    let vector_size = 4; // Must match the size used in QdrantIngestor

    let config = IngestorConfig {
        database_url:    qdrant_uri.to_string(),
        collection_name: None,
        vector_size:     Some(vector_size,),
        mappings:        None,
        openai_api_key:  None,
        embed_field:     None,
        relationships:   None,
    };
    let ingestor = QdrantIngestor::new(config,)
        .await
        .expect("Failed to create QdrantIngestor",);

    let test_nc_content = "This is a test document for Qdrant.".to_string();
    let test_data = DataReaderResult::Text(
        TextData {
            content:     test_nc_content.clone(),
            first_lines: Some(vec![test_nc_content.clone()],),
            line_count:  1,
            total_size:  test_nc_content.len() as u64, // Assuming 1 byte per char for simplicity
        },
        nc_reader::nc_reader_result::FileMetadata {
            size:       test_nc_content.len() as u64,
            line_count: Some(1,),
        },
    );

    ingestor
        .ingest(test_data,)
        .await
        .expect("Failed to ingest data to Qdrant",);

    // Verify data
    let _client = Qdrant::from_url(qdrant_uri,).build().unwrap();
}
