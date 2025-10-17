
//! Retry configuration and logic for message processing.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for message redelivery attempts, inspired by MassTransit's policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub immediate_retries: u32,
    pub delayed_intervals: Vec<Duration>,
    pub max_retry_count: u32,
}

impl RetryConfig {
    /// Creates a new retry configuration.
    ///
    /// # Arguments
    /// * `immediate_retries` - Number of immediate retries before any delay.
    /// * `delayed_intervals` - A vector of `Duration`s for subsequent delayed retries.
    pub fn new(immediate_retries: u32, delayed_intervals: Vec<Duration>) -> Self {
        let max_retry_count = immediate_retries + delayed_intervals.len() as u32;
        Self {
            immediate_retries,
            delayed_intervals,
            max_retry_count,
        }
    }

    /// A default configuration that mimics common MassTransit settings.
    /// Includes 1 immediate retry and 5 delayed retries with exponential backoff
    /// (1, 2, 4, 8, 16 minutes).
    pub fn masstransit_default() -> Self {
        Self::new(
            1, // 1 immediate retry
            vec![
                Duration::from_secs(60),   // 1 minute
                Duration::from_secs(120),  // 2 minutes
                Duration::from_secs(240),  // 4 minutes
                Duration::from_secs(480),  // 8 minutes
                Duration::from_secs(960),  // 16 minutes
            ],
        )
    }

    /// Returns the delay duration for a given retry attempt number.
    /// Returns `None` if the `retry_count` exceeds the configured maximum retries.
    pub fn get_retry_delay(&self, retry_count: u32) -> Option<Duration> {
        if retry_count == 0 {
            return Some(Duration::from_secs(0)); // First attempt, no delay
        }

        if retry_count <= self.immediate_retries {
            return Some(Duration::from_secs(0)); // Immediate retry
        }

        let delayed_index = (retry_count - self.immediate_retries - 1) as usize;
        self.delayed_intervals.get(delayed_index).copied()
    }

    /// Checks if a message should be retried based on the current `retry_count`.
    pub fn should_retry(&self, retry_count: u32) -> bool {
        retry_count <= self.max_retry_count
    }

    /// Returns a string slice describing the type of retry for logging purposes.
    pub fn get_retry_type(&self, retry_count: u32) -> &'static str {
        if retry_count == 0 {
            "initial"
        } else if retry_count <= self.immediate_retries {
            "immediate"
        } else {
            "delayed"
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::masstransit_default()
    }
}

/// Holds the state of a message's retry attempts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRetryInfo {
    pub retry_count: u32,
    pub last_exception: Option<String>,
    pub first_delivery_time: chrono::DateTime<chrono::Utc>,
    pub last_retry_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl MessageRetryInfo {
    /// Creates a new `MessageRetryInfo` for a message being processed for the first time.
    pub fn new() -> Self {
        Self {
            retry_count: 0,
            last_exception: None,
            first_delivery_time: chrono::Utc::now(),
            last_retry_time: None,
        }
    }

    /// Increments the retry count and records the error message and time.
    pub fn increment_retry(&mut self, error_message: Option<String>) {
        self.retry_count += 1;
        self.last_exception = error_message;
        self.last_retry_time = Some(chrono::Utc::now());
    }
}

impl Default for MessageRetryInfo {
    fn default() -> Self {
        Self::new()
    }
}
