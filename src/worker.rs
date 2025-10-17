//! The generic RabbitMQ worker and its configuration.

use crate::dlq::DeadLetterQueueHandler;
use crate::error::WorkerError;
use crate::handler::MessageHandler;
use crate::retry::{MessageRetryInfo, RetryConfig};
use futures_util::TryStreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind,
};
use std::sync::Arc;

/// Configuration for a `GenericRabbitMQWorker`.
///
/// Use the `WorkerConfig::builder()` method to construct this struct.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// The name of the queue to consume messages from.
    pub queue_name: String,
    /// The name of the exchange to bind the queue to.
    pub exchange_name: String,
    /// The routing key for the binding between the exchange and the queue.
    pub routing_key: String,
    /// A unique identifier for the consumer on this queue.
    pub consumer_tag: String,
    /// The AMQP URL for connecting to the RabbitMQ broker.
    pub rabbitmq_url: String,
    /// The configuration for message retry behavior.
    pub retry_config: RetryConfig,
    /// The number of messages to fetch from the server at a time (QoS prefetch count).
    pub prefetch_count: u16,
}

impl WorkerConfig {
    /// Creates a new `WorkerConfigBuilder` to start building the worker configuration.
    ///
    /// # Arguments
    /// * `queue_name` - The name of the queue to consume from.
    /// * `rabbitmq_url` - The connection URL for the RabbitMQ broker.
    pub fn builder(queue_name: String, rabbitmq_url: String) -> WorkerConfigBuilder {
        WorkerConfigBuilder::new(queue_name, rabbitmq_url)
    }
}

/// A builder for creating `WorkerConfig` instances.
pub struct WorkerConfigBuilder {
    queue_name: String,
    rabbitmq_url: String,
    exchange_name: Option<String>,
    routing_key: Option<String>,
    consumer_tag: Option<String>,
    retry_config: Option<RetryConfig>,
    prefetch_count: Option<u16>,
}

impl WorkerConfigBuilder {
    /// Creates a new builder with the required fields.
    fn new(queue_name: String, rabbitmq_url: String) -> Self {
        Self {
            queue_name,
            rabbitmq_url,
            exchange_name: None,
            routing_key: None,
            consumer_tag: None,
            retry_config: None,
            prefetch_count: None,
        }
    }

    /// Sets a custom exchange name.
    /// Defaults to `{queue_name}_exchange` if not set.
    pub fn exchange_name(mut self, exchange_name: String) -> Self {
        self.exchange_name = Some(exchange_name);
        self
    }

    /// Sets a custom routing key.
    /// Defaults to `{queue_name}.process` if not set.
    pub fn routing_key(mut self, routing_key: String) -> Self {
        self.routing_key = Some(routing_key);
        self
    }

    /// Sets a custom consumer tag.
    /// Defaults to `{queue_name}_consumer` if not set.
    pub fn consumer_tag(mut self, consumer_tag: String) -> Self {
        self.consumer_tag = Some(consumer_tag);
        self
    }

    /// Sets a custom retry configuration.
    /// Defaults to `RetryConfig::masstransit_default()`.
    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = Some(retry_config);
        self
    }

    /// Sets a custom prefetch count (QoS).
    /// Defaults to 1.
    ///
    /// **Warning:** Setting this to a value greater than 1 means your `MessageHandler`
    /// may be called concurrently. Ensure your handler is thread-safe.
    pub fn prefetch_count(mut self, count: u16) -> Self {
        self.prefetch_count = Some(count);
        self
    }

    /// Builds the final `WorkerConfig`, applying defaults for any unset options.
    pub fn build(self) -> WorkerConfig {
        let queue_name = self.queue_name;
        WorkerConfig {
            exchange_name: self.exchange_name.unwrap_or_else(|| format!("{}_exchange", queue_name)),
            routing_key: self.routing_key.unwrap_or_else(|| format!("{}.process", queue_name)),
            consumer_tag: self.consumer_tag.unwrap_or_else(|| format!("{}_consumer", queue_name)),
            retry_config: self.retry_config.unwrap_or_default(),
            prefetch_count: self.prefetch_count.unwrap_or(1),
            queue_name,
            rabbitmq_url: self.rabbitmq_url,
        }
    }
}


/// A generic, reusable RabbitMQ worker that processes messages from a queue.
pub struct GenericRabbitMQWorker<H: MessageHandler> {
    handler: Arc<H>,
    config: WorkerConfig,
}

impl<H: MessageHandler + 'static> GenericRabbitMQWorker<H> {
    /// Creates a new worker.
    pub fn new(handler: Arc<H>, config: WorkerConfig) -> Self {
        Self { handler, config }
    }

    /// Connects to RabbitMQ, sets up the consumer, and runs the message processing loop.
    ///
    /// This function will run until the connection is lost or the consumer is cancelled.
    /// The application is responsible for handling reconnection and graceful shutdown.
    pub async fn run(&self) -> Result<(), WorkerError> {
        log::info!(
            "Connecting to RabbitMQ and setting up worker for queue '{}'...",
            self.config.queue_name
        );

        let connection = Connection::connect(&self.config.rabbitmq_url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        let dlq_handler = DeadLetterQueueHandler::new(
            channel.clone(),
            &self.config.queue_name,
            self.config.retry_config.clone(),
        )
        .await?;

        self.setup_infrastructure(&channel).await?;

        channel.basic_qos(self.config.prefetch_count, BasicQosOptions::default()).await?;
        log::info!("QoS prefetch count set to {}", self.config.prefetch_count);

        let consumer = channel
            .basic_consume(
                &self.config.queue_name,
                &self.config.consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        log::info!(
            "Consumer started with tag '{}'. Waiting for messages...",
            consumer.tag().as_str()
        );

        let handler = self.handler.clone();
        consumer
            .try_for_each(move |delivery| {
                let handler = handler.clone();
                let dlq_handler = dlq_handler.clone();
                async move {
                    if let Err(e) = Self::process_message(delivery, handler, Arc::new(dlq_handler)).await {
                        log::error!("Message processing failed with a recoverable error: {}", e);
                    }
                    Ok(())
                }
            })
            .await?;

        Ok(())
    }

    /// Declares the primary exchange and queue, and binds them together.
    async fn setup_infrastructure(&self, channel: &lapin::Channel) -> Result<(), WorkerError> {
        channel
            .exchange_declare(
                &self.config.exchange_name,
                ExchangeKind::Topic,
                ExchangeDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await?;

        channel
            .queue_declare(
                &self.config.queue_name,
                QueueDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await?;

        channel
            .queue_bind(
                &self.config.queue_name,
                &self.config.exchange_name,
                &self.config.routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        log::info!("Queue '{}' and exchange '{}' are set up and bound.", self.config.queue_name, self.config.exchange_name);
        Ok(())
    }

    /// The core logic for processing a single delivered message.
    async fn process_message(
        delivery: Delivery,
        handler: Arc<H>,
        dlq_handler: Arc<DeadLetterQueueHandler>,
    ) -> Result<(), WorkerError> {
        let delivery_tag = delivery.delivery_tag;
        let message_body = &delivery.data;

        let retry_info = Self::extract_retry_info_from_headers(&delivery.properties);

        let message: H::MessageType = match serde_json::from_slice(message_body) {
            Ok(msg) => msg,
            Err(e) => {
                log::error!("Failed to parse message, sending to DLQ. Tag: {}, Error: {}", delivery_tag, e);
                dlq_handler.handle_failed_message(
                    message_body,
                    delivery.routing_key.as_str(),
                    delivery.exchange.as_str(),
                    &e.to_string(),
                    retry_info,
                ).await.map_err(|e| WorkerError::DlqError(e.to_string()))?;

                delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await?;
                return Ok(());
            }
        };

        match handler.handle_message(message).await {
            Ok(_) => {
                delivery.ack(BasicAckOptions::default()).await?;
                log::info!("Message processed successfully. Tag: {}", delivery_tag);
            }
            Err(e) => {
                log::error!("Failed to process message. Tag: {}, Error: {}", delivery_tag, e);
                let retry_action = dlq_handler.handle_failed_message(
                    message_body,
                    delivery.routing_key.as_str(),
                    delivery.exchange.as_str(),
                    &e.to_string(),
                    retry_info,
                ).await.map_err(|e| WorkerError::DlqError(e.to_string()))?;

                // Nack the message *without* requeueing, as the DLQ handler has already republished it for retry or sent it to the DLQ.
                delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await?;
                log::info!("Message nacked after failure. Retry action: {:?}", retry_action);
            }
        }

        Ok(())
    }

    /// Extracts retry information from message headers.
    fn extract_retry_info_from_headers(properties: &lapin::BasicProperties) -> Option<MessageRetryInfo> {
        use chrono::{DateTime, Utc};
        use lapin::types::AMQPValue;

        let headers = properties.headers().as_ref()?;

        let retry_count = match headers.inner().get("x-retry-count") {
            Some(AMQPValue::LongLongInt(count)) => *count as u32,
            Some(AMQPValue::LongInt(count)) => *count as u32,
            Some(AMQPValue::ShortInt(count)) => *count as u32,
            _ => return None, // Not a retried message
        };

        let last_exception = headers
            .inner()
            .get("x-last-exception")
            .and_then(|v| v.as_long_string())
            .map(|s| s.to_string());

        let first_delivery_time = headers
            .inner()
            .get("x-first-delivery")
            .and_then(|v| v.as_long_string())
            .and_then(|s| DateTime::parse_from_rfc3339(s.to_string().as_str()).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        let last_retry_time = headers
            .inner()
            .get("x-last-retry-time")
            .and_then(|v| v.as_long_string())
            .and_then(|s| DateTime::parse_from_rfc3339(s.to_string().as_str()).ok())
            .map(|dt| dt.with_timezone(&Utc));

        Some(MessageRetryInfo {
            retry_count,
            last_exception,
            first_delivery_time,
            last_retry_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::RetryConfig;
    use std::time::Duration;

    #[test]
    fn test_worker_config_builder_defaults() {
        let queue_name = "test_queue".to_string();
        let url = "amqp://localhost".to_string();
        let config = WorkerConfig::builder(queue_name.clone(), url.clone()).build();

        assert_eq!(config.queue_name, queue_name);
        assert_eq!(config.rabbitmq_url, url);
        assert_eq!(config.exchange_name, "test_queue_exchange");
        assert_eq!(config.routing_key, "test_queue.process");
        assert_eq!(config.consumer_tag, "test_queue_consumer");
        assert_eq!(config.prefetch_count, 1);
        assert_eq!(config.retry_config.immediate_retries, 1); // Default from RetryConfig
    }

    #[test]
    fn test_worker_config_builder_custom_values() {
        let queue_name = "test_queue".to_string();
        let url = "amqp://localhost".to_string();
        let retry_config = RetryConfig::new(5, vec![Duration::from_secs(1)]);

        let config = WorkerConfig::builder(queue_name.clone(), url.clone())
            .exchange_name("custom_exchange".to_string())
            .routing_key("custom.key".to_string())
            .consumer_tag("custom_consumer".to_string())
            .prefetch_count(10)
            .retry_config(retry_config.clone())
            .build();

        assert_eq!(config.exchange_name, "custom_exchange");
        assert_eq!(config.routing_key, "custom.key");
        assert_eq!(config.consumer_tag, "custom_consumer");
        assert_eq!(config.prefetch_count, 10);
        assert_eq!(config.retry_config.immediate_retries, 5);
    }
}