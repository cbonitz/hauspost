use std::{cmp::min, collections::HashMap, fmt, time::Duration};

use tokio::{
    sync::mpsc,
    time::{sleep, Instant},
};
use tracing::{event, info, span, warn, Level};

use crate::{
    queue::{Queue, QueueStatus},
    requests::{RequestReceive, RequestSend, TimeoutStamp},
};

pub trait Message: Send + 'static {}
impl<T> Message for T where T: Send + 'static {}

#[derive(PartialEq, Eq)]
pub enum RecieveStatus<T>
where
    T: Message,
{
    InternalError,
    Received(T),
    Timeout,
}

impl<T> fmt::Debug for RecieveStatus<T>
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

#[derive(Debug, PartialEq, Eq)]
pub enum SendStatus {
    InternalError,
    Received,
    Enqueued,
    Timeout,
}

#[derive(Debug)]
enum Request<T>
where
    T: Message,
{
    Receive(RequestReceive<T>),
    Send(RequestSend<T>),
}
pub struct MessageExchange<T>
where
    T: Message,
{
    queues: HashMap<String, Queue<RequestReceive<T>, RequestSend<T>>>,
    request_receiver: mpsc::UnboundedReceiver<Request<T>>,
    tick_sender: mpsc::UnboundedSender<Instant>,
    tick_receiver: Option<mpsc::UnboundedReceiver<Instant>>,
}

impl<T> MessageExchange<T>
where
    T: Message,
{
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
    const MAX_TIMEOUT: Duration = Duration::from_secs(10);

    pub fn new() -> (MessageExchangeConnection<T>, Self) {
        let (request_sender, request_receiver) = mpsc::unbounded_channel();
        let (tick_sender, tick_receiver) = mpsc::unbounded_channel();
        (
            MessageExchangeConnection {
                request_sender,
                default_timeout: Self::DEFAULT_TIMEOUT,
                max_timeout: Self::MAX_TIMEOUT,
            },
            Self {
                queues: HashMap::new(),
                request_receiver,
                tick_sender,
                tick_receiver: Some(tick_receiver),
            },
        )
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
        };
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) {
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
        let mut tick_receiver = self.tick_receiver.take().expect("Can only run once");
        loop {
            tokio::select! {
                tick = tick_receiver.recv() => {
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
    }
}

#[derive(Clone)]
pub struct MessageExchangeConnection<T>
where
    T: Message,
{
    request_sender: mpsc::UnboundedSender<Request<T>>,
    default_timeout: Duration,
    max_timeout: Duration,
}

impl<T> MessageExchangeConnection<T>
where
    T: Message,
{
    pub async fn send_message(
        &self,
        message: T,
        queue: String,
        block: bool,
        timeout: Option<Duration>,
    ) -> SendStatus {
        let timeout = TimeoutStamp::new(min(
            timeout.unwrap_or(self.default_timeout),
            self.max_timeout,
        ));
        let (receiver, request) = RequestSend::new(queue, timeout, message, block);
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

    pub async fn receive_message(
        &self,
        queue: String,
        timeout: Option<Duration>,
    ) -> RecieveStatus<T> {
        let timeout = TimeoutStamp::new(min(
            timeout.unwrap_or(self.default_timeout),
            self.max_timeout,
        ));
        let (receiver, request) = RequestReceive::new(queue, timeout);
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
                    RecieveStatus::InternalError
                }
            },
            Err(_) => {
                warn!("Send failed");
                RecieveStatus::InternalError
            }
        };
        event!(Level::INFO, result = ?result);
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Once};

    use tokio::time;
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
    async fn test_send_receive_single() {
        initialize_logger();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });
        let send_connection = connection.clone();
        tokio::spawn(async move {
            send_connection
                .send_message(
                    "hello, world".to_string(),
                    "greetings".to_string(),
                    true,
                    None,
                )
                .await;
        });
        assert_eq!(
            connection
                .receive_message("greetings".to_string(), None)
                .await,
            RecieveStatus::Received("hello, world".to_string())
        )
    }

    #[tokio::test]
    async fn test_send_receive_multiple() {
        initialize_logger();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });
        let send_connection = connection.clone();
        tokio::spawn(async move {
            for i in 1..2 {
                send_connection
                    .send_message(format!("Hello, {}", i), "greetings".to_string(), true, None)
                    .await;
            }
            for i in 1..2 {
                send_connection
                    .send_message(
                        format!("Goodbye, {}", i),
                        "farewells".to_string(),
                        true,
                        None,
                    )
                    .await;
            }
        });
        for i in 1..2 {
            assert_eq!(
                connection
                    .receive_message("greetings".to_string(), None)
                    .await,
                RecieveStatus::Received(format!("Hello, {}", i))
            )
        }
        for i in 1..2 {
            assert_eq!(
                connection
                    .receive_message("farewells".to_string(), None)
                    .await,
                RecieveStatus::Received(format!("Goodbye, {}", i))
            )
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_receive_large_amount() {
        initialize_logger();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });
        let count = 10_000;
        let send_connection = connection.clone();
        tokio::spawn(async move {
            for i in 1..count {
                send_connection
                    .send_message(
                        format!("message {}", i),
                        format!("messages {}", i % 10),
                        false,
                        Some(Duration::from_secs(15)),
                    )
                    .await;
            }
        });
        for i in 1..count {
            assert_eq!(
                connection
                    .receive_message(
                        format!("messages {}", i % 10),
                        Some(Duration::from_secs(15)),
                    )
                    .await,
                RecieveStatus::Received(format!("message {}", i))
            )
        }
    }

    #[tokio::test]
    async fn test_send_receive_multiple_nonblocking() {
        initialize_logger();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });
        let send_connection = connection.clone();
        tokio::spawn(async move {
            for i in 1..2 {
                send_connection
                    .send_message(
                        format!("Goodbye, {}", i),
                        "farewells".to_string(),
                        false,
                        None,
                    )
                    .await;
            }
            for i in 1..2 {
                send_connection
                    .send_message(
                        format!("Hello, {}", i),
                        "greetings".to_string(),
                        false,
                        None,
                    )
                    .await;
            }
        });
        for i in 1..2 {
            assert_eq!(
                connection
                    .receive_message("greetings".to_string(), None)
                    .await,
                RecieveStatus::Received(format!("Hello, {}", i))
            )
        }
        for i in 1..2 {
            assert_eq!(
                connection
                    .receive_message("farewells".to_string(), None)
                    .await,
                RecieveStatus::Received(format!("Goodbye, {}", i))
            )
        }
    }

    #[tokio::test]
    async fn test_timeout_status_send() {
        initialize_logger();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });

        let send_result = tokio::spawn(async move {
            connection
                .send_message(
                    "forget".to_string(),
                    "queue".to_string(),
                    true,
                    Some(Duration::from_millis(1)),
                )
                .await
        });
        assert_eq!(send_result.await.unwrap(), SendStatus::Timeout);
    }

    #[tokio::test]
    async fn test_timeout_status_receive() {
        initialize_logger();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });

        let receive_result = tokio::spawn(async move {
            connection
                .receive_message("queue".to_string(), Some(Duration::from_millis(1)))
                .await
        });
        assert_eq!(receive_result.await.unwrap(), RecieveStatus::Timeout);
    }

    #[tokio::test]
    async fn test_timeout_status_send_receive() {
        initialize_logger();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });
        let send_connection = connection.clone();
        let send_result = tokio::spawn(async move {
            send_connection
                .send_message(
                    "forget".to_string(),
                    "queue".to_string(),
                    true,
                    Some(Duration::from_millis(1)),
                )
                .await
        });
        assert_eq!(send_result.await.unwrap(), SendStatus::Timeout);

        let receive_result = tokio::spawn(async move {
            connection
                .receive_message("queue".to_string(), Some(Duration::from_millis(10)))
                .await
        });
        assert_eq!(receive_result.await.unwrap(), RecieveStatus::Timeout);
    }

    #[tokio::test]
    async fn test_send_timeout() {
        initialize_logger();
        time::pause();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });
        connection
            .send_message(
                "forget".to_string(),
                "queue".to_string(),
                false,
                Some(Duration::from_secs(1)),
            )
            .await;
        connection
            .send_message(
                "remember".to_string(),
                "queue".to_string(),
                false,
                Some(Duration::from_secs(3)),
            )
            .await;
        time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            connection.receive_message("queue".to_string(), None).await,
            RecieveStatus::Received("remember".to_string())
        )
    }

    #[tokio::test]
    async fn test_send_timeout_multiple() {
        initialize_logger();
        time::pause();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });
        for _ in 1..5_000 {
            connection
                .send_message(
                    "forget".to_string(),
                    "queue".to_string(),
                    false,
                    Some(Duration::from_secs(1)),
                )
                .await;
        }
        for _ in 1..5_000 {
            connection
                .send_message(
                    "remember".to_string(),
                    "queue".to_string(),
                    false,
                    Some(Duration::from_secs(3)),
                )
                .await;
        }
        time::advance(Duration::from_secs(2)).await;
        for _ in 1..5_000 {
            assert_eq!(
                connection.receive_message("queue".to_string(), None).await,
                RecieveStatus::Received("remember".to_string())
            )
        }
    }
}
