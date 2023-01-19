//! _Hauspost_ is a simple topic-based in-process message broker for use in [Tokio](https://tokio.rs/) applications.
//! It is intended for use-cases where it is convenient to use the communication patterns provided by a message broker,
//! but persistence and state shared across multiple instances of a program are not required.
//!
//! ## Usage Example
//!
//! In this toy example shows how message passing can be used to communicate asynchronously between different request
//! handler invocations, as well as between request handlers and a background thread.
//! ```rust
//! #
//! # tokio_test::block_on(async {
//! # use hauspost::broker::{MessageBroker, ReceiveStatus};
//! # use hauspost::connection::{MessageBrokerConnection};
//! # use tokio::time::Duration;
//! let mut connection = MessageBroker::new().run_in_background();
//! async fn location_based_service(
//!     connection: &MessageBrokerConnection<String>,
//!     location: String,
//!     user_name: String,
//! ) -> String {
//!     // Check if someone was here recently.
//!     let location_topic = format!("__location-{}", location);
//!     let response = match connection.peek_message(location_topic.clone()).await {
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
//!         .send_message_nonblocking(user_name, location_topic, Some(Duration::from_secs(10)))
//!         .await;
//!     connection
//!         .send_message_nonblocking(
//!             "".to_string(),
//!             "__recent_visitor_counter".to_string(),
//!             Some(Duration::from_secs(10)),
//!         )
//!         .await;
//!     response
//! }
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
//! connection
//!     .subscribe("__recent_visitor_counter".to_string())
//!     .unwrap();
//! let mut recent_visitor_counter = 0;
//! while let Ok(Some(_)) = connection.subscriptions_recv(Duration::default()).await {
//!     recent_visitor_counter += 1;
//! }
//! assert_eq!(recent_visitor_counter, 2);
//! # });
//! ```
pub mod broker;
pub mod connection;
pub mod queue;
pub(crate) mod requests;
