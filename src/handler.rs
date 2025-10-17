
//! Defines the core trait for message handling logic.

use crate::error::WorkerError;
use async_trait::async_trait;
use serde::Deserialize;

/// A trait for processing messages from a RabbitMQ queue.
///
/// Implement this trait for your specific message type and business logic.
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// The type of the message that this handler can process.
    /// Must be deserializable from JSON.
    type MessageType: for<'de> Deserialize<'de> + Send + Sync;

    /// Processes a single deserialized message.
    ///
    /// # Arguments
    /// * `message` - The deserialized message payload.
    ///
    /// # Returns
    /// `Ok(())` if the message was processed successfully, or a `WorkerError` if not.
    async fn handle_message(&self, message: Self::MessageType) -> Result<(), WorkerError>;

    /// A name for the handler, used for logging and identification.
    fn handler_name(&self) -> &str;
}
