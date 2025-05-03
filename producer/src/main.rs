#![allow(
    clippy::wildcard_imports,
    clippy::missing_errors_doc, // TODO: docs
    clippy::let_underscore_untyped,
    clippy::module_name_repetitions,
    clippy::multiple_crate_versions, // TODO: check later
)]
use chrono::Local;
use mqi::Properties;
use mqi::connect_options::{ApplName, Credentials};
use mqi::prelude::*;
use mqi::types::QueueName;
use mqi::types::{MQCMHO, MQSMPO};
use mqi::{QueueManager, ThreadNone, mqstr};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(EnvFilter::from_default_env()) // Use environment variable for configuration
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    let queue_name = QueueName(mqstr!("DEV.QUEUE.1"));

    // User credentials and application name.
    // MQI will use the C API defaults of using MQSERVER environment variable
    let connect_options = (
        ApplName(mqstr!("producer example")),
        Credentials::user("admin", "passw0rd"),
    );

    // Connect to the queue manager. Make all MQ warnings as a rust Result::Err
    let queue_manager = mqi::connect::<ThreadNone>(&connect_options).warn_as_error()?;

    info!("Producer is running...");

    // Setup signal handling using ctrlc
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    // Main producer loop

    while running.load(Ordering::SeqCst) {
        let qm = queue_manager.connection_ref();
        let current_time = Local::now();
        let message = json!({
            "message": "hello",
            "time": current_time.to_rfc3339()
        });
        let message_str = message.to_string(); // Convert message to string
        // Add custom application headers
        let msg = Properties::new(qm, MQCMHO::default()).expect("message created");
        msg.set_property("wally", "test", MQSMPO::default())
            .warn_as_error()
            .expect("property set should not fail");

        // Put a message with headers and properties. Discard any warnings.
        queue_manager
            .put_message(&queue_name, &(), message_str.as_str())
            .discard_warning()?;

        info!("Sent message with headers: {}", message);
        thread::sleep(Duration::from_millis(10)); // Adjust the interval as needed
    }

    info!("Producer has stopped.");
    Ok(())
}
