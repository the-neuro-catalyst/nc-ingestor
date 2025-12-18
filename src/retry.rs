use backoff::{ExponentialBackoff, future::retry};
use crate::error::{IngestorError, Result};
use std::future::Future;
use tracing::warn;

pub async fn execute_with_retry<F, Fut, T>(operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = std::result::Result<T, backoff::Error<IngestorError>>>,
{
    let backoff = ExponentialBackoff::default();
    
    retry(backoff, operation).await
}

/// Helper to wrap an IngestorError into a backoff::Error.
/// Decisions on what is transient vs permanent should be made here or at call site.
pub fn transient_error(err: IngestorError) -> backoff::Error<IngestorError> {
    warn!("Transient error encountered, retrying: {}", err);
    backoff::Error::transient(err)
}

pub fn permanent_error(err: IngestorError) -> backoff::Error<IngestorError> {
    backoff::Error::permanent(err)
}

pub fn wrap_error(err: IngestorError) -> backoff::Error<IngestorError> {
    if err.is_transient() {
        transient_error(err)
    } else {
        permanent_error(err)
    }
}
