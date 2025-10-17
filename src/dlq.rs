
//! Dead Letter Queue (DLQ) and retry scheduling logic.

use crate::error::WorkerError;
use crate::retry::{MessageRetryInfo, RetryConfig};
use chrono::Utc;
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, ExchangeKind,
};
use serde_json::json;
use std::time::Duration;

/// Defines the names for the DLQ and delayed exchange infrastructure.
#[derive(Clone, Debug)]
struct DlxNames {
    dlq_exchange: String,
    dlq_queue: String,
    dlq_routing_key: String,
    delayed_exchange: String,
}

impl DlxNames {
    fn new(base_name: &str) -> Self {
        Self {
            dlq_exchange: format!("{}_dlx", base_name),
            dlq_queue: format!("{}_dlq", base_name),
            dlq_routing_key: format!("{}.failed", base_name),
            delayed_exchange: format!("{}_delayed_exchange", base_name),
        }
    }
}

/// Manages the infrastructure and logic for handling failed messages,
/// including retries and sending to a Dead Letter Queue.
#[derive(Clone)]
pub struct DeadLetterQueueHandler {
    channel: Channel,
    retry_config: RetryConfig,
    names: DlxNames,
}

impl DeadLetterQueueHandler {
    /// Creates a new DLQ handler and sets up the required RabbitMQ infrastructure.
    pub async fn new(
        channel: Channel,
        base_queue_name: &str,
        retry_config: RetryConfig,
    ) -> Result<Self, WorkerError> {
        let names = DlxNames::new(base_queue_name);
        let handler = Self {
            channel,
            retry_config,
            names,
        };

        handler.setup_infrastructure().await?;
        Ok(handler)
    }

    /// Declares the DLQ, the delayed message exchange, and their bindings.
    async fn setup_infrastructure(&self) -> Result<(), WorkerError> {
        // Declare DLQ exchange
        self.channel
            .exchange_declare(
                &self.names.dlq_exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await?;

        // Declare DLQ queue
        self.channel
            .queue_declare(
                &self.names.dlq_queue,
                QueueDeclareOptions { durable: true, ..Default::default() },
                FieldTable::default(),
            )
            .await?;

        // Bind DLQ queue
        self.channel
            .queue_bind(
                &self.names.dlq_queue,
                &self.names.dlq_exchange,
                &self.names.dlq_routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        // Declare delayed message exchange (requires rabbitmq-delayed-message-exchange plugin)
        let mut delayed_exchange_args = FieldTable::default();
        delayed_exchange_args.insert("x-delayed-type".into(), AMQPValue::LongString("topic".into()));

        self.channel
            .exchange_declare(
                &self.names.delayed_exchange,
                ExchangeKind::Custom("x-delayed-message".to_string()),
                ExchangeDeclareOptions { durable: true, ..Default::default() },
                delayed_exchange_args,
            )
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to declare delayed message exchange. Ensure the `rabbitmq-delayed-message-exchange` plugin is installed on the broker. Error: {}",
                    e
                );
                e
            })?;

        log::info!("DLQ and delayed message infrastructure setup completed for base queue '{}'", self.names.dlq_queue);
        Ok(())
    }

    /// Decides whether to retry a message or send it to the DLQ.
    pub async fn handle_failed_message(
        &self,
        original_message: &[u8],
        original_routing_key: &str,
        original_exchange: &str,
        error_message: &str,
        retry_info: Option<MessageRetryInfo>,
    ) -> Result<RetryAction, WorkerError> {
        let mut retry_info = retry_info.unwrap_or_default();
        retry_info.increment_retry(Some(error_message.to_string()));

        let retry_count = retry_info.retry_count;

        // Decide if we should retry at all
        if !self.retry_config.should_retry(retry_count) {
            log::warn!(
                "Message sent to DLQ after exhausting all {} retries. Error: {}",
                self.retry_config.max_retry_count, error_message
            );
            self.send_to_dlq(original_message, original_routing_key, original_exchange, &retry_info).await?;
            return Ok(RetryAction::SentToDlq);
        }

        // Decide if the retry should be delayed
        if let Some(delay) = self.retry_config.get_retry_delay(retry_count) {
            if delay.is_zero() {
                self.republish_with_retry_headers(original_message, original_routing_key, original_exchange, &retry_info).await?;
                log::info!(
                    "Message republished for immediate retry (attempt {}).",
                    retry_count
                );
            } else {
                self.schedule_delayed_retry(original_message, original_routing_key, delay, &retry_info).await?;
                log::info!(
                    "Message scheduled for delayed retry (attempt {}, delay: {:?}).",
                    retry_count, delay
                );
            }
            return Ok(RetryAction::ScheduledRetry);
        }

        // Fallback: if get_retry_delay returns None (should be covered by should_retry, but as a safeguard)
        log::error!("Message sent to DLQ due to unexpected retry logic failure.");
        self.send_to_dlq(original_message, original_routing_key, original_exchange, &retry_info).await?;
        Ok(RetryAction::SentToDlq)
    }

    /// Publishes a message to the Dead Letter Queue.
    async fn send_to_dlq(
        &self,
        original_message: &[u8],
        original_routing_key: &str,
        original_exchange: &str,
        retry_info: &MessageRetryInfo,
    ) -> Result<(), lapin::Error> {
        let dlq_message = json!({
            "original_message": String::from_utf8_lossy(original_message),
            "original_routing_key": original_routing_key,
            "original_exchange": original_exchange,
            "retry_info": retry_info,
            "dlq_timestamp": Utc::now(),
        });

        let properties = BasicProperties::default().with_headers(self.create_dlq_headers(retry_info));

        self.channel
            .basic_publish(
                &self.names.dlq_exchange,
                &self.names.dlq_routing_key,
                BasicPublishOptions::default(),
                dlq_message.to_string().as_bytes(),
                properties,
            )
            .await?;

        Ok(())
    }

    /// Republishes a message to its original exchange for an immediate retry, adding retry headers.
    async fn republish_with_retry_headers(
        &self,
        original_message: &[u8],
        original_routing_key: &str,
        original_exchange: &str,
        retry_info: &MessageRetryInfo,
    ) -> Result<(), lapin::Error> {
        let properties = BasicProperties::default().with_headers(self.create_retry_headers(retry_info, None));

        self.channel
            .basic_publish(
                original_exchange,
                original_routing_key,
                BasicPublishOptions::default(),
                original_message,
                properties,
            )
            .await?;

        Ok(())
    }

    /// Schedules a message for a delayed retry using the delayed-message exchange.
    async fn schedule_delayed_retry(
        &self,
        original_message: &[u8],
        original_routing_key: &str,
        delay: Duration,
        retry_info: &MessageRetryInfo,
    ) -> Result<(), lapin::Error> {
        let delay_ms = delay.as_millis() as i64;
        let headers = self.create_retry_headers(retry_info, Some(delay_ms));
        let properties = BasicProperties::default().with_headers(headers);

        // Publish to the delayed exchange, which will route it back to the original queue after the delay.
        self.channel
            .basic_publish(
                &self.names.delayed_exchange,
                original_routing_key, // The exchange routes based on this key
                BasicPublishOptions::default(),
                original_message,
                properties,
            )
            .await?;

        Ok(())
    }

    /// Creates the AMQP headers for a message being sent to the DLQ.
    fn create_dlq_headers(&self, retry_info: &MessageRetryInfo) -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert("x-retry-count".into(), AMQPValue::LongLongInt(retry_info.retry_count as i64));
        headers.insert("x-first-delivery".into(), AMQPValue::LongString(retry_info.first_delivery_time.to_rfc3339().into()));
        headers.insert("x-dlq-reason".into(), AMQPValue::LongString("max-retries-exceeded".into()));

        if let Some(ref exception) = retry_info.last_exception {
            headers.insert("x-last-exception".into(), AMQPValue::LongString(exception.clone().into()));
        }
        headers
    }

    /// Creates the AMQP headers for a message being retried.
    fn create_retry_headers(&self, retry_info: &MessageRetryInfo, delay_ms: Option<i64>) -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert("x-retry-count".into(), AMQPValue::LongLongInt(retry_info.retry_count as i64));
        headers.insert("x-first-delivery".into(), AMQPValue::LongString(retry_info.first_delivery_time.to_rfc3339().into()));
        headers.insert("x-retry-type".into(), AMQPValue::LongString(self.retry_config.get_retry_type(retry_info.retry_count).into()));

        if let Some(ref exception) = retry_info.last_exception {
            headers.insert("x-last-exception".into(), AMQPValue::LongString(exception.clone().into()));
        }

        if let Some(delay) = delay_ms {
            headers.insert("x-delay".into(), AMQPValue::LongLongInt(delay));
        }

        headers
    }
}

/// Represents the outcome of a failed message handling attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryAction {
    /// The message was republished for an immediate retry.
    ImmediateRetry,
    /// The message was sent to a delayed exchange for a future retry.
    ScheduledRetry,
    /// The message has exceeded all retries and was sent to the DLQ.
    SentToDlq,
}
