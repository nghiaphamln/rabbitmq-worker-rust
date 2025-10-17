use async_trait::async_trait;
use rabbitmq_worker::{GenericRabbitMQWorker, MessageHandler, WorkerConfig, WorkerError};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

// 1. Define your message struct
#[derive(Deserialize, Debug, Clone)]
struct MyMessage {
    content: String,
    id: u32,
}

// 2. Implement the `MessageHandler` trait for your logic
struct MyMessageHandler;

#[async_trait]
impl MessageHandler for MyMessageHandler {
    type MessageType = MyMessage;

    fn handler_name(&self) -> &str {
        "MyTestMessageHandler"
    }

    async fn handle_message(&self, message: Self::MessageType) -> Result<(), WorkerError> {
        log::info!(
            "Received message with ID: {}. Content: '{}'",
            message.id,
            message.content
        );

        if message.id == 99 {
            log::info!("Simulating a long-running task...");
            tokio::time::sleep(Duration::from_secs(10)).await;
            log::info!("Long-running task finished.");
        }

        if message.id % 2 != 0 {
            log::warn!("Simulating a processing failure for message {}", message.id);
            return Err(WorkerError::from(format!(
                "Failed to process message with odd ID: {}",
                message.id
            )));
        }

        log::info!("Successfully processed message {}", message.id);
        Ok(())
    }
}

// 3. Configure and run the worker
#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let rabbitmq_url = std::env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".to_string());

    log::info!("Using RabbitMQ at {}", rabbitmq_url);

    let handler = Arc::new(MyMessageHandler);

    // Configure the worker using the new builder pattern
    let config = WorkerConfig::builder("my_app_queue".to_string(), rabbitmq_url)
        .prefetch_count(5)
        .build();
    let worker = Arc::new(GenericRabbitMQWorker::new(handler, config));

    let reconnect_delay = Duration::from_secs(5);

    // The application is now in control of the run loop.
    loop {
        tokio::select! {
            // Listen for Ctrl+C for graceful shutdown
            _ = tokio::signal::ctrl_c() => {
                log::info!("Ctrl+C received. Shutting down.");
                break;
            },

            // Run the worker
            result = worker.run() => {
                match result {
                    Ok(_) => {
                        log::info!("Worker finished unexpectedly. Will not reconnect.");
                        break;
                    }
                    Err(e) => {
                        log::error!("Worker failed: {}. Reconnecting in {:?}...", e, reconnect_delay);
                        tokio::time::sleep(reconnect_delay).await;
                    }
                }
            }
        }
    }

    log::info!("Application has shut down.");
}