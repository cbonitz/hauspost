# Subscription MVP

As a developer, I want to subscribe to a queue and receive messages.

## Implementation details

- The Connection has an `mpsc` sender/receiver pair
- A request describing a subscription is sent to the message exchange and processed in the queue
  - The method creating a subscription sends a copy of the sender to the queue.
  - The connection has a receive method that calls the receive method of the receiver.
- The subscription lasts until this receiver is dropped.
- The subscription is added to the queue like a normal `RequestReceicve`
- If the subscription is at the head of the queue and receives a message, it is pushed back to the end of the queue.

## Out of Scope

- Backpressure Handling - can be done later by optionally using a bounded sender and using `try_send`.
- Expiring subscriptions after a given period of time.
- Explicit unsubscribe.
