// #![forbid(unsafe_code)]
#![allow(
    clippy::wildcard_imports,
    clippy::missing_errors_doc, // TODO: docs
    clippy::let_underscore_untyped,
    clippy::module_name_repetitions,
    clippy::multiple_crate_versions, // TODO: check later
    
)]
use anyhow::{Context, Result};
use mqi::MqStruct;
use mqi::Properties;
use mqi::StrCcsidOwned;
use mqi::Syncpoint;
use mqi::connect_options::{ApplName, Credentials};
use mqi::sys;
use mqi::types::MQIMPO;
use mqi::types::{MQCMHO, QueueName};
use mqi::{Object, ThreadNone, constants, mqstr, prelude::*};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

// Simple configuration with minimal options
struct Config {
    queue_name: String,
    username: String,
    password: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue_name: "DEV.QUEUE.1".to_string(),
            username: "admin".to_string(),
            password: "passw0rd".to_string(),
        }
    }
}

// Simple function to process a message
fn process_message(message: &str) -> Result<()> {
    if message.is_empty() {
        // Handle empty message case
        error!("Received empty message");
        Err(anyhow::anyhow!("Empty message"))
    } else {
        // Simulate processing the message
        info!("Processing message: {}", message);
        Ok(())
    } 
}

// Main function with simplified IBM MQ consumer
fn main() -> Result<()> {
    // Setup logging
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Use default configuration
    let config = Config::default();

    // Set up MQ connection using string slices instead of String references
    let connect_options = (
        ApplName(mqstr!("MQ Consumer")),
        Credentials::user(config.username.as_str(), config.password.as_str()),
    );

    // Connect to the queue manager
    let queue_manager = mqi::connect::<ThreadNone>(&connect_options)
        .warn_as_error()
        .context("Failed to connect to queue manager")?;

    info!("Connected to MQ. Consumer is starting...");

    // Set up signal handling for clean shutdown
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        info!("Received interrupt signal, shutting down");
        r.store(false, Ordering::SeqCst);
    })?;

    // Main message processing loop
    // Add a scope here so we can drop the queue object and properties at the end
    // of the loop, allowing us to safely disconnect from the queue manager
    // without any active references
    {
        // Create these in a new scope so they're automatically dropped at the end
        let queue_name = QueueName::from_str(config.queue_name.as_str())?;
        let queue_obj = Object::open(
            queue_manager.connection_ref(),
            &(
                queue_name,
                constants::MQOO_INPUT_AS_Q_DEF | constants::MQOO_SAVE_ALL_CONTEXT,
            ),
        )
        .warn_as_error()?;

        let mut properties = Properties::new(&queue_manager, MQCMHO::default())?;

        // Main loop
        while running.load(Ordering::SeqCst) {
            // Create buffer for message data
            let mut buffer = Vec::<u8>::with_capacity(20 * 1024);
            let buf_write = buffer.spare_capacity_mut();

            // Create a new syncpoint for this message cycle
            let syncpoint = Syncpoint::new(queue_manager.connection_ref());

            let message: Option<(_, MqStruct<sys::MQMD2>)> = queue_obj
                .get_data_with(
                    &(
                        constants::MQGMO_SYNCPOINT | constants::MQGMO_WAIT,
                        &mut properties,
                    ),
                    buf_write,
                )
                .warn_as_error()
                .context("Unable to get a message")?;

            if let Some((msg_data, _md)) = message {
                // Use the initialized msg_data directly
                let len = msg_data.len();
                // This is where the bug is. We need to use set_len to inform Rust about
                // the valid data that was written into the buffer's spare capacity.
                // Safety: This is safe because the IBM MQ API has already written valid
                // data of length `len` into the buffer's spare capacity.
                #[allow(unsafe_code)]
                unsafe {
                    buffer.set_len(len);
                }

                // Get the custom header
                let head: Option<StrCcsidOwned> = properties
                    .property("wally", MQIMPO::default())
                    .warn_as_error()?;
                info!("Found custom header - wally: {:?}", head);

                info!("Received message of size {}", len);

                // Convert the message buffer to a string
                match std::str::from_utf8(&buffer) {
                    Ok(message_str) => {
                        // Process the message with the string content
                        if let Err(e) = process_message(message_str) {
                            // If there was an error processing, backout the message
                            error!("Error processing message: {}", e);
                            syncpoint
                                .backout()
                                .warn_as_error()
                                .context("unable to backout")?;
                            warn!("Message rolled back");
                        } else {
                            // Commit the message if processed successfully
                            syncpoint
                                .commit()
                                .warn_as_error()
                                .context("unable to commit")?;
                            info!("Message committed");
                        }
                    }
                    Err(e) => {
                        // Handle case where message is not valid UTF-8
                        error!("Invalid UTF-8 in message: {}", e);
                        syncpoint
                            .backout()
                            .warn_as_error()
                            .context("unable to backout")?;
                        warn!("Message rolled back due to invalid UTF-8");
                    }
                }
            }
            // No message received case is handled implicitly by continuing the loop
        }

        // These will be dropped automatically at the end of this scope
    }

    // Now we can safely disconnect since all borrowers are gone
    queue_manager.disconnect().discard_warning()?;
    info!("Consumer stopped");
    Ok(())
}
