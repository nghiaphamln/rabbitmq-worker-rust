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
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// The name of the queue to consume messages from.
    pub queue_name: String,
    /// The name of the exchange to bind the queue to.
    /// Defaults to `{queue_name}_exchange` if not specified.
    pub exchange_name: Option<String>,
    /// The routing key for the binding between the exchange and the queue.
    /// Defaults to `{queue_name}.process` if not specified.
    pub routing_key: Option<String>,
    /// A unique identifier for the consumer on this queue.
    /// Defaults to `{queue_name}_consumer`.
    pub consumer_tag: String,
    /// The AMQP URL for connecting to the RabbitMQ broker.
    pub rabbitmq_url: String,
    /// The configuration for message retry behavior.
    /// Defaults to `RetryConfig::masstransit_default()`.
    pub retry_config: RetryConfig,
    /// The number of messages to fetch from the server at a time (QoS prefetch count).
    /// A value of 1 ensures that messages are processed one at a time.
    /// Defaults to 1.
    pub prefetch_count: u16,
}

impl WorkerConfig {
    /// Creates a simple configuration using convention-based names and default settings.
    pub fn new(queue_name: String, rabbitmq_url: String) -> Self {
        let consumer_tag = format!("{}_consumer", queue_name);
        Self {
            queue_name,
            exchange_name: None,
            routing_key: None,
            consumer_tag,
            rabbitmq_url,
            retry_config: RetryConfig::default(),
            prefetch_count: 1,
        }
    }

    /// Attaches a custom retry configuration.
    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Sets a custom prefetch count (QoS).
    ///
    /// **Warning:** Setting this to a value greater than 1 means your `MessageHandler`
    /// may be called concurrently from multiple tasks. Ensure your handler is thread-safe
    /// and prepared for concurrent execution.
    pub fn with_prefetch_count(mut self, count: u16) -> Self {
        self.prefetch_count = count;
        self
    }

    /// Gets the exchange name, generating it from the queue name if not provided.
    pub fn get_exchange_name(&self) -> String {
        self.exchange_name.clone().unwrap_or_else(|| format!("{}_exchange", self.queue_name))
    }

    /// Gets the routing key, generating it from the queue name if not provided.
    pub fn get_routing_key(&self) -> String {
        self.routing_key.clone().unwrap_or_else(|| format!("{}.process", self.queue_name))
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
        let exchange_name = self.config.get_exchange_name();
        let routing_key = self.config.get_routing_key();

        channel
            .exchange_declare(
                &exchange_name,
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
                &exchange_name,
                &routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        log::info!("Queue '{}' and exchange '{}' are set up and bound.", self.config.queue_name, exchange_name);
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
