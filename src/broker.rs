//! Module containing the message broker.
use core::panic;
use std::{
    collections::HashMap,
    fmt::{self},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{sleep, Instant},
};

use tracing::{info, warn};

use crate::{
    connection::MessageBrokerConnection,
    queue::{Queue, QueueStatus},
    requests::{MessageSubscription, RequestReceive, RequestSend},
};

/// Requirements for messages
pub trait Message: Send + 'static {}
impl<T> Message for T where T: Send + 'static {}

/// Result of a receive operation
#[derive(PartialEq, Eq)]
pub enum ReceiveStatus<T>
where
    T: Message,
{
    /// A result was received.
    Received(T),
    /// An internal error has occured.
    InternalError,
    /// The message request has timed out.
    Timeout,
}

impl<T> From<ReceiveStatus<T>> for Option<T>
where
    T: Message,
{
    fn from(value: ReceiveStatus<T>) -> Self {
        match value {
            ReceiveStatus::Received(t) => Some(t),
            ReceiveStatus::InternalError => None,
            ReceiveStatus::Timeout => None,
        }
    }
}

impl<T> ReceiveStatus<T>
where
    T: Message,
{
    pub fn ok(self) -> Option<T> {
        self.into()
    }
}

impl<T> fmt::Debug for ReceiveStatus<T>
where
    T: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InternalError => write!(f, "InternalError"),
            Self::Received(_) => f.debug_tuple("Received").finish(),
            Self::Timeout => write!(f, "Timeout"),
        }
    }
}

/// Result of a send operation
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SendStatus {
    /// An internal error has occured.
    InternalError,
    /// The message was delivered.
    Delivered,
    /// The message was enqueued for delivery.
    Enqueued,
    /// The request has timed out.
    Timeout,
}

#[derive(Debug)]
pub(crate) enum Request<T>
where
    T: Message,
{
    Receive(RequestReceive<T>),
    Send(RequestSend<T>),
    Subscribe(MessageSubscription<T>),
}

/// A simple queue-based message broker.
pub struct MessageBroker<T>
where
    T: Message,
{
    queues: HashMap<String, Queue<MessageSubscription<T>, RequestReceive<T>, RequestSend<T>>>,
    request_sender: Option<mpsc::UnboundedSender<Request<T>>>,
    request_receiver: mpsc::UnboundedReceiver<Request<T>>,
    tick_sender: mpsc::UnboundedSender<Instant>,
    tick_receiver: mpsc::UnboundedReceiver<Instant>,
}

impl<T> MessageBroker<T>
where
    T: Message,
{
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
    const MAX_TIMEOUT: Duration = Duration::from_secs(10);

    /// Create a message broker together with a [MessageBrokerConnection].
    pub fn new() -> Self {
        let (request_sender, request_receiver) = mpsc::unbounded_channel();
        let (tick_sender, tick_receiver) = mpsc::unbounded_channel();

        Self {
            queues: HashMap::new(),
            request_sender: Some(request_sender),
            request_receiver,
            tick_sender,
            tick_receiver,
        }
    }

    #[tracing::instrument(skip(self, request), fields(request_id = %request.id, queue=%request.queue))]
    async fn receive(&mut self, request: RequestReceive<T>) {
        let queue_name = request.queue.clone();
        let id = request.id;
        let timeout_at = request.timeout.timeout_at;
        match self.queues.get_mut(&request.queue) {
            Some(queue) => {
                if queue.receive(request, id, timeout_at) == QueueStatus::Empty {
                    info!(queue_name = queue_name, "remove empty queue");
                    self.queues.remove(&queue_name);
                }
            }
            None => {
                let mut queue = Queue::new(&request.queue);
                queue.receive(request, id, timeout_at);
                self.queues.insert(queue_name, queue);
            }
        }
    }

    #[tracing::instrument(skip(self, request),fields(message_id = %request.id, queue=%request.queue))]
    async fn send(&mut self, request: RequestSend<T>) {
        let queue_name = request.queue.clone();
        let id = request.id;
        let timeout_at = request.timeout.timeout_at;
        match self.queues.get_mut(&request.queue) {
            Some(queue) => {
                if queue.send(request, id, timeout_at) == QueueStatus::Empty {
                    info!(queue_name = queue_name, "remove empty queue");
                    self.queues.remove(&queue_name);
                }
            }
            None => {
                let mut queue = Queue::new(&request.queue);
                queue.send(request, id, timeout_at);
                self.queues.insert(queue_name, queue);
            }
        }
    }

    #[tracing::instrument(skip(self, request), fields(request_id = %request.id, queue=%request.queue))]
    fn subscribe(&mut self, request: MessageSubscription<T>) {
        let queue_name = request.queue.clone();
        let id = request.id;
        match self.queues.get_mut(&request.queue) {
            Some(queue) => {
                queue.subscribe(request, id);
            }
            None => {
                let mut queue = Queue::new(&request.queue);
                queue.subscribe(request, id);
                self.queues.insert(queue_name, queue);
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn process_timeout_at(&mut self, now: Instant) {
        let mut remove = vec![];
        for (queue_name, queue) in self.queues.iter_mut() {
            if queue.process_timeout_at(now) == QueueStatus::Empty {
                remove.push(queue_name.clone());
            }
        }
        for queue_name in remove {
            info!(queue_name = queue_name, "remove empty queue");
            self.queues.remove(&queue_name);
        }
    }

    async fn process_request(&mut self, request: Request<T>) {
        info!("Received request");
        match request {
            Request::Receive(request) => {
                self.process_timeout_at(request.timeout.created_at).await;
                self.receive(request).await
            }
            Request::Send(message) => {
                self.process_timeout_at(message.timeout.created_at).await;
                self.send(message).await
            }
            Request::Subscribe(request) => self.subscribe(request),
        }
    }

    /// Run the exchange in the background. Returns a cloneable [MessageBrokerConnection].
    /// Background tasks will stop when the last clone of the connection is dropped.
    #[tracing::instrument(skip(self))]
    pub fn run_in_background(mut self) -> MessageBrokerConnection<T> {
        let tick_sender = self.tick_sender.clone();
        tokio::spawn(async move {
            loop {
                match tick_sender.send(Instant::now()) {
                    Ok(_) => {}
                    Err(_) => break,
                };
                sleep(Duration::from_millis(10)).await;
            }
        });
        let connection = MessageBrokerConnection::new(
            self.request_sender.take().unwrap(),
            Self::DEFAULT_TIMEOUT,
            Self::MAX_TIMEOUT,
        );
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    tick = self.tick_receiver.recv() => {
                        match tick {
                            Some(tick) => self.process_timeout_at(tick).await,
                            None => return,
                        }

                    }
                    message = self.request_receiver.recv() => {
                        match message {
                            Some(request) => self.process_request(request).await,
                            None => {
                                info!("all connections dropped");
                                return;
                            }
                        }
                    }
                }
            }
        });
        connection
    }
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Once};

    use tokio::time::{self};
    use tracing_subscriber::fmt::format::FmtSpan;

    use crate::connection::BlockingSendStatus;

    use super::*;

    static INITIALIZE_LOGGER: Once = Once::new();

    fn initialize_logger() {
        if let Ok(val) = env::var("TRACE_TESTS") {
            if val == "1" {
                INITIALIZE_LOGGER.call_once(|| {
                    tracing_subscriber::fmt()
                        .with_span_events(FmtSpan::ACTIVE)
                        .init();
                });
            }
        }
    }

    #[tokio::test]
    async fn test_send_receive_single_message() {
        initialize_logger();
        let connection = MessageBroker::<String>::new().run_in_background();
        let send_connection = connection.clone();

        tokio::spawn(async move {
            send_connection
                .send_message_blocking("hello, world".to_string(), "greetings".to_string(), None)
                .await;
        });

        assert_eq!(
            connection
                .receive_message("greetings".to_string(), None)
                .await
                .ok()
                .unwrap(),
            "hello, world"
        )
    }

    #[tokio::test]
    async fn test_send_peek_single_message() {
        initialize_logger();
        let connection = MessageBroker::<String>::new().run_in_background();

        connection
            .send_message_nonblocking("hello, world".to_string(), "greetings".to_string(), None)
            .await;

        assert_eq!(
            connection
                .peek_message("greetings".to_string())
                .await
                .ok()
                .unwrap(),
            "hello, world"
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_receive_large_amount_of_messages_sequential() {
        initialize_logger();
        let connection = MessageBroker::<String>::new().run_in_background();
        let count = 10_000;

        let send_connection = connection.clone();
        tokio::spawn(async move {
            for i in 1..count {
                send_connection
                    .send_message_nonblocking(
                        format!("message {}", i),
                        format!("messages {}", i % 10),
                        Some(Duration::from_secs(15)),
                    )
                    .await;
            }
        });

        // Receive must be in-order within topic
        for i in 1..count {
            assert_eq!(
                connection
                    .receive_message(
                        format!("messages {}", i % 10),
                        Some(Duration::from_secs(15)),
                    )
                    .await
                    .ok()
                    .unwrap(),
                format!("message {}", i)
            )
        }
    }

    #[tokio::test]
    async fn test_send_receive_multiple_messages_with_nonblocking_send() {
        initialize_logger();
        let connection = MessageBroker::<String>::new().run_in_background();

        let send_connection_1 = connection.clone();
        tokio::spawn(async move {
            for i in 1..2 {
                send_connection_1
                    .send_message_nonblocking(
                        format!("Goodbye, {}", i),
                        "farewells".to_string(),
                        None,
                    )
                    .await;
            }
        });
        let send_connection_2 = connection.clone();
        tokio::spawn(async move {
            for i in 1..2 {
                send_connection_2
                    .send_message_nonblocking(
                        format!("Hello, {}", i),
                        "greetings".to_string(),
                        None,
                    )
                    .await;
            }
        });

        // Receive must be in-order within topic
        for i in 1..2 {
            assert_eq!(
                connection
                    .receive_message("greetings".to_string(), None)
                    .await,
                ReceiveStatus::Received(format!("Hello, {}", i))
            )
        }
        for i in 1..2 {
            assert_eq!(
                connection
                    .receive_message("farewells".to_string(), None)
                    .await,
                ReceiveStatus::Received(format!("Goodbye, {}", i))
            )
        }
    }

    #[tokio::test]
    async fn test_send_timeout() {
        initialize_logger();
        let connection = MessageBroker::<String>::new().run_in_background();

        let send_result = tokio::spawn(async move {
            connection
                .send_message_blocking(
                    "should time out".to_string(),
                    "topic".to_string(),
                    Some(Duration::from_millis(1)),
                )
                .await
        });
        assert_eq!(send_result.await.unwrap(), BlockingSendStatus::Timeout);
    }

    #[tokio::test]
    async fn test_receive_timeout() {
        initialize_logger();
        let connection = MessageBroker::<String>::new().run_in_background();

        let receive_result = tokio::spawn(async move {
            connection
                .receive_message("topic".to_string(), Some(Duration::from_millis(1)))
                .await
        });

        assert_eq!(receive_result.await.unwrap(), ReceiveStatus::Timeout);
    }

    #[tokio::test]
    async fn test_subsequent_send_and_receive_timeouts() {
        initialize_logger();
        let connection = MessageBroker::<String>::new().run_in_background();

        let send_connection = connection.clone();
        let send_result = tokio::spawn(async move {
            send_connection
                .send_message_blocking(
                    "should time out".to_string(),
                    "topic".to_string(),
                    Some(Duration::from_millis(1)),
                )
                .await
        });
        assert_eq!(send_result.await.unwrap(), BlockingSendStatus::Timeout);

        let receive_result = tokio::spawn(async move {
            connection
                .receive_message("topic".to_string(), Some(Duration::from_millis(10)))
                .await
        });
        assert_eq!(receive_result.await.unwrap(), ReceiveStatus::Timeout);
    }

    #[tokio::test]
    async fn test_one_message_timed_out_one_message_received() {
        initialize_logger();
        time::pause();
        let connection = MessageBroker::<String>::new().run_in_background();

        connection
            .send_message_nonblocking(
                "should time out".to_string(),
                "topic".to_string(),
                Some(Duration::from_secs(1)),
            )
            .await;
        connection
            .send_message_nonblocking(
                "should be received".to_string(),
                "topic".to_string(),
                Some(Duration::from_secs(3)),
            )
            .await;

        time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            connection.receive_message("topic".to_string(), None).await,
            ReceiveStatus::Received("should be received".to_string())
        )
    }

    #[tokio::test]
    async fn test_send_timeout_many_messages_before_delivering_many() {
        initialize_logger();
        time::pause();
        let connection = MessageBroker::<String>::new().run_in_background();
        let count = 5_000;

        for _ in 1..count {
            connection
                .send_message_nonblocking(
                    "should time out".to_string(),
                    "topic".to_string(),
                    Some(Duration::from_secs(1)),
                )
                .await;
        }
        for i in 1..count {
            connection
                .send_message_nonblocking(
                    format!("should be received {}", i),
                    "topic".to_string(),
                    Some(Duration::from_secs(3)),
                )
                .await;
        }

        // Receive must be in-order within topic
        time::advance(Duration::from_secs(2)).await;
        for i in 1..count {
            assert_eq!(
                connection.receive_message("topic".to_string(), None).await,
                ReceiveStatus::Received(format!("should be received {}", i).to_string())
            )
        }
    }

    #[tokio::test]
    async fn test_timed_out_messages_interleaved_with_non_timed_out_messages() {
        initialize_logger();
        time::pause();
        let connection = MessageBroker::<String>::new().run_in_background();
        let count = 5_000;

        for i in 1..count {
            connection
                .send_message_nonblocking(
                    "should time out".to_string(),
                    "topic".to_string(),
                    Some(Duration::from_secs(1)),
                )
                .await;
            connection
                .send_message_nonblocking(
                    format!("should be received {}", i),
                    "topic".to_string(),
                    Some(Duration::from_secs(3)),
                )
                .await;
        }

        // Receive must be in-order within topic
        time::advance(Duration::from_secs(2)).await;
        for i in 1..count {
            assert_eq!(
                connection.receive_message("topic".to_string(), None).await,
                ReceiveStatus::Received(format!("should be received {}", i).to_string())
            )
        }
    }

    #[tokio::test]
    async fn test_send_subscribe_single_message() {
        initialize_logger();
        let mut connection = MessageBroker::<String>::new().run_in_background();
        let send_connection = connection.clone();

        connection.subscribe("greetings".to_string()).unwrap();
        tokio::spawn(async move {
            send_connection
                .send_message_blocking("hello, world".to_string(), "greetings".to_string(), None)
                .await;
        });
        assert_eq!(
            connection
                .subscriptions_recv(Duration::from_millis(100))
                .await
                .unwrap()
                .unwrap(),
            "hello, world".to_string()
        );
        connection.unsubscribe("greetings");
        assert_eq!(
            connection
                .send_message_blocking(
                    "hello, world".to_string(),
                    "greetings".to_string(),
                    Some(Duration::from_millis(10)),
                )
                .await,
            BlockingSendStatus::Timeout,
        );
    }

    #[tokio::test]
    async fn doc_test() {
        let mut connection = MessageBroker::new().run_in_background();
        async fn location_based_service(
            connection: &MessageBrokerConnection<String>,
            location: String,
            user_name: String,
        ) -> String {
            // Check if someone was here recently.
            let location_topic = format!("__location-{}", location);
            let response = match connection.peek_message(location_topic.clone()).await {
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
            // Let others know we were here.
            connection
                .send_message_nonblocking(user_name, location_topic, Some(Duration::from_secs(10)))
                .await;
            connection
                .send_message_nonblocking(
                    "".to_string(),
                    "__recent_visitor_counter".to_string(),
                    Some(Duration::from_secs(10)),
                )
                .await;
            response
        }
        // Simulate making requests in a typical Rust web framework using Connection as shared state.
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

        connection
            .subscribe("__recent_visitor_counter".to_string())
            .unwrap();
        let mut recent_visitor_counter = 0;
        while let Ok(Some(_)) = connection.subscriptions_recv(Duration::default()).await {
            recent_visitor_counter += 1;
        }
        assert_eq!(recent_visitor_counter, 2);
    }
}
