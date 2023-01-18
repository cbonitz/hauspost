//! The message broker and its connection.
use anyhow::anyhow;
use core::panic;
use std::{
    cmp::min,
    collections::HashMap,
    fmt::{self, Display},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{self, error::Elapsed, sleep, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::{event, info, span, warn, Level};

use crate::{
    queue::{Queue, QueueStatus},
    requests::{MessageSubscription, RequestReceive, RequestSend, TimeoutStamp},
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

impl Into<BlockingSendStatus> for SendStatus {
    fn into(self) -> BlockingSendStatus {
        match self {
            SendStatus::InternalError => BlockingSendStatus::InternalError,
            SendStatus::Delivered => BlockingSendStatus::Delivered,
            SendStatus::Enqueued => BlockingSendStatus::InternalError,
            SendStatus::Timeout => BlockingSendStatus::Timeout,
        }
    }
}

impl Into<NonblockingSendStatus> for SendStatus {
    fn into(self) -> NonblockingSendStatus {
        match self {
            SendStatus::InternalError => NonblockingSendStatus::InternalError,
            SendStatus::Delivered => NonblockingSendStatus::InternalError,
            SendStatus::Enqueued => NonblockingSendStatus::Enqueued,
            SendStatus::Timeout => NonblockingSendStatus::Timeout,
        }
    }
}

/// Result of a blocking send operation
#[derive(Debug, PartialEq, Eq)]
pub enum BlockingSendStatus {
    /// An internal error has occured.
    InternalError,
    /// The message was delivered.
    Delivered,
    /// The request has timed out.
    Timeout,
}

/// Result of a nonblocking send operation
#[derive(Debug, PartialEq, Eq)]
pub enum NonblockingSendStatus {
    /// An internal error has occured.
    InternalError,
    /// The message was enqueued for delivery.
    Enqueued,
    /// The request has timed out.
    Timeout,
}

#[derive(Debug)]
enum Request<T>
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
            tick_receiver: tick_receiver,
        }
    }

    #[tracing::instrument(skip(self, request), fields(request_id = %request.id, queue=%request.queue))]
    async fn receive(&mut self, request: RequestReceive<T>) {
        let queue_name = request.queue.clone();
        let id = request.id.clone();
        let timeout_at = request.timeout.timeout_at.clone();
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
        let id = request.id.clone();
        let timeout_at = request.timeout.timeout_at.clone();
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
        let id = request.id.clone();
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

/// A cloneable connection that clients can use to interact with a [MessageBroker]
pub struct MessageBrokerConnection<T>
where
    T: Message,
{
    request_sender: mpsc::UnboundedSender<Request<T>>,
    subscription_receiver: mpsc::UnboundedReceiver<T>,
    subscription_sender: mpsc::UnboundedSender<T>,
    subscriptions: HashMap<String, CancellationToken>,
    default_timeout: Duration,
    max_timeout: Duration,
}

impl<T> Display for MessageBrokerConnection<T>
where
    T: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageBrokerConnection")
    }
}

impl<T> MessageBrokerConnection<T>
where
    T: Message,
{
    fn new(
        request_sender: mpsc::UnboundedSender<Request<T>>,
        default_timeout: Duration,
        max_timeout: Duration,
    ) -> Self {
        let (subscription_sender, subscription_receiver) = mpsc::unbounded_channel();
        Self {
            request_sender,
            subscription_receiver,
            subscription_sender,
            subscriptions: HashMap::new(),
            default_timeout,
            max_timeout,
        }
    }
}

impl<T> Clone for MessageBrokerConnection<T>
where
    T: Message,
{
    fn clone(&self) -> Self {
        let (subscription_sender, subscription_receiver) = mpsc::unbounded_channel();
        Self {
            request_sender: self.request_sender.clone(),
            subscription_receiver,
            subscription_sender,
            subscriptions: HashMap::new(),
            default_timeout: self.default_timeout.clone(),
            max_timeout: self.max_timeout.clone(),
        }
    }
}

impl<T> MessageBrokerConnection<T>
where
    T: Message,
{
    /// Send `message` to `topic` in a blocking fashion.
    ///
    /// The method will return when the message was delivered, the operation has timed out or an error occured and return the corresponding [BlockingSendStatus].
    /// If `timeout` is not specified, the [MessageBroker]'s default timeout will be applied.
    ///
    /// Messages from subsequent calls to this function on the same connection will be delivered in-order.
    pub async fn send_message_blocking(
        &self,
        message: T,
        topic: String,
        timeout: Option<Duration>,
    ) -> BlockingSendStatus {
        self.send_message_internal(message, topic, true, timeout)
            .await
            .into()
    }

    /// Send `message` to `topic` in a nonblocking fashion.
    ///
    /// The method will return when the message was enqueued, or an error has occured doing so.
    /// After the timeout, the message will be discarded.
    /// If `timeout` is not specified, the [MessageBroker]'s default timeout will be applied.
    ///
    /// Messages from subsequent calls to this function on the same connection will be delivered in-order.
    pub async fn send_message_nonblocking(
        &self,
        message: T,
        topic: String,
        timeout: Option<Duration>,
    ) -> BlockingSendStatus {
        self.send_message_internal(message, topic, false, timeout)
            .await
            .into()
    }

    async fn send_message_internal(
        &self,
        message: T,
        topic: String,
        block: bool,
        timeout: Option<Duration>,
    ) -> SendStatus {
        let timeout = TimeoutStamp::new(min(
            timeout.unwrap_or(self.default_timeout),
            self.max_timeout,
        ));
        let (receiver, request) = RequestSend::new(topic, timeout, message, block);
        let span = span!(
            Level::INFO,
            "send_message",
            queue = request.queue,
            message_id = request.id.to_string()
        );
        let _guard = span.enter();
        let message = Request::Send(request);
        let result = match self.request_sender.send(message) {
            Ok(()) => match receiver {
                Some(receiver) => match receiver.await {
                    Ok(status) => status,
                    Err(_) => SendStatus::InternalError,
                },
                None => SendStatus::Enqueued,
            },
            Err(_) => SendStatus::InternalError,
        };
        event!(Level::INFO, result = ?result);
        result
    }

    /// Register to receive a single message from `topic`. If `timeout` is not specified, the [MessageBroker]'s default timeout will be applied.
    pub async fn receive_message(
        &self,
        topic: String,
        timeout: Option<Duration>,
    ) -> ReceiveStatus<T> {
        let timeout = TimeoutStamp::new(min(
            timeout.unwrap_or(self.default_timeout),
            self.max_timeout,
        ));
        let (receiver, request) = RequestReceive::new(topic, timeout);
        let span = span!(
            Level::INFO,
            "receive_message",
            queue = request.queue,
            message_id = request.id.to_string()
        );
        let _guard = span.enter();
        let request = Request::Receive(request);
        let result = match self.request_sender.send(request) {
            Ok(()) => match receiver.await {
                Ok(status) => status,
                Err(_) => {
                    warn!("Receive failed (result sender dropped)");
                    ReceiveStatus::InternalError
                }
            },
            Err(_) => {
                warn!("Send failed");
                ReceiveStatus::InternalError
            }
        };
        event!(Level::INFO, result = ?result);
        result
    }

    /// Receive a single message from `topic` if immediately available.
    pub async fn peek_message(&self, topic: String) -> ReceiveStatus<T> {
        self.receive_message(topic, Some(Duration::default())).await
    }

    /// Subscribe to topic. Messages will be received via an [`mpsc::UnboundedReceiver`] as long as the subscription is active.
    /// To consume the messages, call `subscriptions_recv`.
    ///
    /// Currently, the subscription receiver is unbounded, so no backpressure is implemented.
    pub fn subscribe(&mut self, topic: String) -> Result<(), anyhow::Error> {
        let (cancel, subscription) =
            MessageSubscription::new(topic.clone(), self.subscription_sender.clone());
        self.request_sender
            .send(Request::Subscribe(subscription))
            .map_err(|_| anyhow!("could not subscribe"))?;
        self.subscriptions.insert(topic, cancel);
        Ok(())
    }

    /// Unsubscribe from a topic. No more messages will be added to the subscription receiver after this method was called.
    pub fn unsubscribe(&mut self, topic: &str) {
        match self.subscriptions.remove(topic) {
            Some(cancel) => cancel.cancel(),
            None => {}
        }
    }

    /// Receive one of the messages in the subscription receiver with timeout.
    pub async fn subscriptions_recv(&mut self, timeout: Duration) -> Result<Option<T>, Elapsed> {
        time::timeout(timeout, self.subscription_receiver.recv()).await
    }
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Once};

    use tokio::time::{self};
    use tracing_subscriber::fmt::format::FmtSpan;

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
        connection.unsubscribe(&"greetings");
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
