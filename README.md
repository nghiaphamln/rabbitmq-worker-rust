# Generic RabbitMQ Worker for Rust

**Important:** This library relies on the `rabbitmq-delayed-message-exchange` plugin for its delayed retry functionality. Please ensure it is enabled on your RabbitMQ broker.

A flexible, generic RabbitMQ worker library for Rust, inspired by the ease of use of MassTransit in .NET. It provides the core building blocks for creating robust message consumers with built-in support for automatic retries (immediate and delayed) and a dead-letter queue (DLQ).

## Philosophy

This library provides a `GenericRabbitMQWorker` that handles a single, resilient connection. It is designed to be run inside a loop that your application controls. This gives you full authority over the lifecycle, including:

-   **Graceful Shutdown**: You decide how to listen for shutdown signals (like `Ctrl+C`) and stop the worker.
-   **Reconnect Strategy**: You control the delay and logic for reconnecting after a failure.

## Features

-   **Generic Worker**: Implement the `MessageHandler` trait for any message type.
-   **Convention over Configuration**: Sensible defaults for queue, exchange, and routing key names.
-   **Automatic Retries**: Built-in support for MassTransit-style delayed redelivery.
-   **Dead-Letter Queue (DLQ)**: Failed messages are automatically sent to a DLQ after all retry attempts are exhausted.
-   **Configurable QoS**: Set the prefetch count to control message throughput.
-   **Delayed Message Support**: Uses the `rabbitmq-delayed-message-exchange` plugin for efficient delayed messaging.
-   **Async First**: Built on top of `lapin` and `tokio`.

## Prerequisites

This library requires the `rabbitmq-delayed-message-exchange` plugin to be enabled on your RabbitMQ broker. You can enable it with the following command:

```sh
rabbitmq-plugins enable rabbitmq_delayed_message_exchange
```

## Usage

1.  **Add the dependency** to your `Cargo.toml`:

    ```toml
    [dependencies]
    rabbitmq-worker = "1.0.0"
    serde = { version = "1.0", features = ["derive"] }
    tokio = { version = "1", features = ["full"] }
    log = "0.4"
    env_logger = "0.9"
    ```

2.  **Define your message struct**:

    ```rust
    use serde::Deserialize;

    #[derive(Deserialize, Debug)]
    struct MyMessage {
        content: String,
        id: u32,
    }
    ```

3.  **Implement the `MessageHandler` trait**:

    ```rust
    use async_trait::async_trait;
    use rabbitmq_worker::{MessageHandler, WorkerError};
    use std::sync::Arc;

    struct MyMessageHandler;

    #[async_trait]
    impl MessageHandler for MyMessageHandler {
        type MessageType = MyMessage;

        fn handler_name(&self) -> &str {
            "MyMessageHandler"
        }

        async fn handle_message(&self, message: Self::MessageType) -> Result<(), WorkerError> {
            log::info!("Received message: {:?}", message);
            // Your business logic here...
            Ok(())
        }
    }
    ```

4.  **Build the run loop** in your `main.rs`:

    The application is responsible for managing the worker's lifecycle. This allows for flexible shutdown and reconnect strategies.

    ```rust
    use rabbitmq_worker::{GenericRabbitMQWorker, WorkerConfig};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::main]
    async fn main() {
        env_logger::init();

        let rabbitmq_url = std::env::var("RABBITMQ_URL")
            .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".to_string());

        let handler = Arc::new(MyMessageHandler);

        // Configure the worker using the builder pattern
        let config = WorkerConfig::builder("my_queue".to_string(), rabbitmq_url)
            .prefetch_count(10)
            .build();

        // The worker can be shared across tasks
        let worker = Arc::new(GenericRabbitMQWorker::new(handler, config));
        let reconnect_delay = Duration::from_secs(5);

        // Run the worker in a loop, handling reconnects and shutdown
        loop {
            tokio::select! {
                // Listen for Ctrl+C for graceful shutdown
                _ = tokio::signal::ctrl_c() => {
                    log::info!("Ctrl+C received. Shutting down.");
                    break;
                },

                // Run the worker and handle results
                result = worker.run() => {
                    if let Err(e) = result {
                        log::error!("Worker failed: {}. Reconnecting in {:?}...", e, reconnect_delay);
                        tokio::time::sleep(reconnect_delay).await;
                    }
                }
            }
        }

        log::info!("Application has shut down.");
    }
    ```

## How It Works

-   **Worker**: The `GenericRabbitMQWorker` is a lightweight struct that holds the configuration and message handler. Its `run()` method attempts to connect and process messages in a single, long-lived session.
-   **Run Loop**: The application creates a `loop` that continuously calls `worker.run()`. The `tokio::select!` macro allows the application to simultaneously wait for the worker to finish (or fail) and listen for external shutdown signals (like `Ctrl+C`).
-   **Auto-Reconnect**: If `worker.run()` returns an `Err` (e.g., the connection is lost), the application logs the error, waits for a configurable period, and the `loop` attempts to call `run()` again.
-   **Graceful Shutdown**: If the shutdown signal is received, the `loop` is broken, and the application can terminate cleanly.

## License

This project is licensed under the Apache-2.0 License.