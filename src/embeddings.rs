use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::error::{IngestorError, Result};

#[async_trait]
pub trait Embedder: Send + Sync {
    async fn generate_embeddings(&self, texts: &[String],) -> Result<Vec<Vec<f32,>,>,>;
}

pub struct OpenAIEmbedder {
    client:  Client,
    api_key: String,
    model:   String,
}

impl OpenAIEmbedder {
    pub fn new(api_key: String, model: Option<String,>,) -> Self {
        Self {
            client: Client::new(),
            api_key,
            model: model.unwrap_or_else(|| "text-embedding-3-small".to_string(),),
        }
    }
}

#[derive(Serialize,)]
struct OpenAIRequest {
    input: Vec<String,>,
    model: String,
}

#[derive(Deserialize,)]
struct OpenAIResponse {
    data: Vec<EmbeddingData,>,
}

#[derive(Deserialize,)]
struct EmbeddingData {
    embedding: Vec<f32,>,
}

#[async_trait]
impl Embedder for OpenAIEmbedder {
    async fn generate_embeddings(&self, texts: &[String],) -> Result<Vec<Vec<f32,>,>,> {
        if texts.is_empty() {
            return Ok(vec![],);
        }

        let response = self
            .client
            .post("https://api.openai.com/v1/embeddings",)
            .header("Authorization", format!("Bearer {}", self.api_key),)
            .json(&OpenAIRequest {
                input: texts.to_vec(),
                model: self.model.clone(),
            },)
            .send()
            .await
            .map_err(|e| IngestorError::Other(format!("OpenAI API error: {}", e),),)?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(IngestorError::Other(format!(
                "OpenAI API error: {} - {}",
                status, error_text
            ),),);
        }

        let result: OpenAIResponse = response.json().await.map_err(|e| {
            IngestorError::Other(format!("Failed to parse OpenAI response: {}", e),)
        },)?;

        Ok(result.data.into_iter().map(|d| d.embedding,).collect(),)
    }
}
