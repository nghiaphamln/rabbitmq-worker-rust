
//! # RabbitMQ Worker
//! A generic RabbitMQ worker library with built-in retry and dead-letter queue (DLQ) logic.

pub mod dlq;
pub mod error;
pub mod handler;
pub mod retry;
pub mod worker;

// Re-export key components for easy access
pub use dlq::DeadLetterQueueHandler;
pub use error::WorkerError;
pub use handler::MessageHandler;
pub use retry::{MessageRetryInfo, RetryConfig};
pub use worker::{GenericRabbitMQWorker, WorkerConfig};
