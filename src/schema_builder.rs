use std::collections::HashMap;
use nc_schema::DataType;

pub enum SqlDialect {
    Postgres,
    Sqlite,
}

pub struct SqlSchemaBuilder {
    dialect: SqlDialect,
    mappings: HashMap<String, String>,
}

impl SqlSchemaBuilder {
    pub fn new(dialect: SqlDialect, mappings: Option<HashMap<String, String>>) -> Self {
        Self {
            dialect,
            mappings: mappings.unwrap_or_default(),
        }
    }

    pub fn map_type(&self, nc_type: &DataType) -> String {
        match (nc_type, &self.dialect) {
            (DataType::String, SqlDialect::Postgres) => "TEXT".to_string(),
            (DataType::String, SqlDialect::Sqlite) => "TEXT".to_string(),
            
            (DataType::Integer, SqlDialect::Postgres) => "BIGINT".to_string(),
            (DataType::Integer, SqlDialect::Sqlite) => "INTEGER".to_string(),
            
            (DataType::Float, SqlDialect::Postgres) => "DOUBLE PRECISION".to_string(),
            (DataType::Float, SqlDialect::Sqlite) => "REAL".to_string(),
            
            (DataType::Number, SqlDialect::Postgres) => "NUMERIC".to_string(),
            (DataType::Number, SqlDialect::Sqlite) => "REAL".to_string(),
            
            (DataType::Boolean, SqlDialect::Postgres) => "BOOLEAN".to_string(),
            (DataType::Boolean, SqlDialect::Sqlite) => "INTEGER".to_string(), // SQLite uses 0/1
            
            (DataType::Null, _) => "TEXT".to_string(), // Default fallback
            
            (DataType::Array(_), SqlDialect::Postgres) => "JSONB".to_string(),
            (DataType::Array(_), SqlDialect::Sqlite) => "TEXT".to_string(), // Store as JSON string
            
            (DataType::Object(_), SqlDialect::Postgres) => "JSONB".to_string(),
            (DataType::Object(_), SqlDialect::Sqlite) => "TEXT".to_string(),
            
            (DataType::Union(variants), _dialect) => {
                // If Union contains Null, we ignore it for type mapping (it just makes the column nullable)
                // We pick the first non-null type for mapping
                if let Some(non_null) = variants.iter().find(|t| !matches!(t, DataType::Null)) {
                    self.map_type(non_null)
                } else {
                    "TEXT".to_string()
                }
            }
            
            _ => "TEXT".to_string(),
        }
    }

    pub fn build_create_table(&self, table_name: &str, schema: &HashMap<String, DataType>) -> String {
        let mut columns = Vec::new();
        
        // Sort keys for deterministic SQL generation
        let mut keys: Vec<&String> = schema.keys().collect();
        keys.sort();

        for key in keys {
            let nc_type = schema.get(key).unwrap();
            let sql_type = self.map_type(nc_type);
            
            // Apply renaming mapping if exists
            let column_name = self.mappings.get(key).unwrap_or(key);
            
            // Quote column name to handle reserved words
            let quoted_name = match self.dialect {
                SqlDialect::Postgres => format!("\"{}\"", column_name),
                SqlDialect::Sqlite => format!("`{}`", column_name),
            };
            
            columns.push(format!("{} {}", quoted_name, sql_type));
        }

        format!("CREATE TABLE IF NOT EXISTS \"{}\" ({})", table_name, columns.join(", "))
    }
}
