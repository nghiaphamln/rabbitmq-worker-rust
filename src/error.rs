
use thiserror::Error;

/// Generic error type for the RabbitMQ worker library.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Error originating from the underlying `lapin` library.
    #[error("RabbitMQ communication error: {0}")]
    Lapin(#[from] lapin::Error),

    /// Error during message deserialization.
    #[error("Failed to deserialize message: {0}")]
    Deserialization(#[from] serde_json::Error),

    /// Custom messaging-related error.
    #[error("Messaging setup or configuration error: {message}")]
    MessagingError {
        message: String,
    },

    /// Error from the message handler logic.
    #[error("Message handler failed: {0}")]
    HandlerError(#[from] Box<dyn std::error::Error + Send + Sync>),

    /// A generic error for DLQ operations.
    #[error("Dead letter queue operation failed: {0}")]
    DlqError(String),
}

// Allow converting from a string-like type into a MessagingError
impl From<&str> for WorkerError {
    fn from(s: &str) -> Self {
        WorkerError::MessagingError { message: s.to_string() }
    }
}

impl From<String> for WorkerError {
    fn from(s: String) -> Self {
        WorkerError::MessagingError { message: s }
    }
}
