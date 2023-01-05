# Simple In-Process Message Exchange

This is a simple topic-based in-process message exchange for use in [Tokio](https://tokio.rs/) applications.

## Usage Example

Pass data between requests based on dynamic request attributes to create a location-based service.

```rust
let connection = MessageExchange::new().run_in_background();
async fn location_based_service(
    connection: &MessageExchangeConnection<String>,
    location: String,
    user_name: String,
) -> String {
    let response = match connection
        .receive_message(location.clone(), Some(Duration::from_millis(10)))
        .await
    {
        ReceiveStatus::Received(previous_user_name) => {
            if previous_user_name != user_name {
                format!("{} was here.", previous_user_name)
            } else {
                "You were the last person at this location.".to_string()
            }
        }
        ReceiveStatus::InternalError => "Internal error".to_string(),
        ReceiveStatus::Timeout => "Nobody was here recently.".to_string(),
    };
    connection
        .send_message_nonblocking(user_name, location, None)
        .await;
    response
}

assert_eq!(
    location_based_service(
        &connection,
        "a random place".to_string(),
        "Alice".to_string()
    )
    .await,
    "Nobody was here recently."
);

assert_eq!(
    location_based_service(&connection, "a random place".to_string(), "Bob".to_string())
        .await,
    "Alice was here."
);
```
