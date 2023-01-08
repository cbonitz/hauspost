//! This is a simple topic-based in-process message exchange for use in [Tokio](https://tokio.rs/) applications.
//! It is intended for use-cases where it is convenient to use the communication patterns provided by a message broker,
//! but persistence and state shared across multiple instances of a program are not required.
//!
//! ## Usage Example
//!
//! In this contrived toy example, simulate passing data between requests based on dynamic request attributes, and between requests to a background proess
//! to create useless location-based service with a visitor counter.
//! ```rust
//! #
//! # tokio_test::block_on(async {
//! # use message_exchange::exchange::{MessageExchange, MessageExchangeConnection, ReceiveStatus};
//! # use tokio::time::Duration;
//! let mut connection = MessageExchange::new().run_in_background();
//! async fn location_based_service(
//!     connection: &MessageExchangeConnection<String>,
//!     location: String,
//!     user_name: String,
//! ) -> String {
//!     // Check if someone was here recently.
//!     let response = match connection
//!         .receive_message(location.clone(), Some(Duration::from_millis(10)))
//!         .await
//!     {
//!         ReceiveStatus::Received(previous_user_name) => {
//!             if previous_user_name != user_name {
//!                 format!("{} was here.", previous_user_name)
//!             } else {
//!                 "You were the last person at this location.".to_string()
//!             }
//!         }
//!         ReceiveStatus::InternalError => "Internal error".to_string(),
//!         ReceiveStatus::Timeout => "Nobody was here recently.".to_string(),
//!     };
//!     // Let others know we were here.
//!     connection
//!         .send_message_nonblocking(user_name, location, Some(Duration::from_secs(10)))
//!         .await;
//!     connection
//!         .send_message_nonblocking("anonymized".to_string(), "__user_counter".to_string(), Some(Duration::from_secs(10)))
//!         .await;
//!     response
//! }
//! connection.subscribe("__user_counter".to_string());
//! // Simulate making requests in a typical Rust web framework using Connection as shared state.
//! assert_eq!(
//!     location_based_service(
//!         &connection,
//!         "a random place".to_string(),
//!         "Alice".to_string()
//!     )
//!     .await,
//!     "Nobody was here recently."
//! );
//!
//! assert_eq!(
//!     location_based_service(&connection, "a random place".to_string(), "Bob".to_string())
//!         .await,
//!     "Alice was here."
//! );
//!
//! let mut visitor_counter = 0;
//! while let Ok(Some(_)) = connection
//!     .subscriptions_recv(Duration::from_millis(10))
//!     .await {
//!     visitor_counter += 1;
//! }
//! assert_eq!(visitor_counter, 2);
//! # });
//! ```
pub mod exchange;
pub mod queue;
pub mod requests;
