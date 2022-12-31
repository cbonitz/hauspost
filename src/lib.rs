use std::{
    cmp::{min, Reverse},
    collections::{BinaryHeap, HashMap, LinkedList},
    fmt,
    time::Duration,
};

use debug_ignore::DebugIgnore;
use tokio::{
    sync::{mpsc, oneshot},
    time::{sleep, Instant},
};
use tracing::{event, info, span, warn, Level};
use uuid::Uuid;

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

#[derive(Debug)]
struct TimeoutStamp {
    timeout_at: Instant,
    created_at: Instant,
}

impl TimeoutStamp {
    pub fn new(timeout_duration: Duration) -> Self {
        let created_at = Instant::now();
        Self {
            timeout_at: created_at + timeout_duration,
            created_at,
        }
    }
}

#[derive(Debug)]
struct RequestReceive<T>
where
    T: Message,
{
    id: Uuid,
    response_sender: DebugIgnore<Option<oneshot::Sender<RecieveStatus<T>>>>,
    queue: String,
    timeout: TimeoutStamp,
}

impl<T> RequestReceive<T>
where
    T: Message,
{
    #[tracing::instrument(skip(self), fields(request_id = %self.id))]
    pub fn reply(&mut self, status: RecieveStatus<T>) {
        if let Some(sender) = self.response_sender.0.take() {
            match sender.send(status) {
                Ok(_) => {}
                Err(_) => warn!(
                    message_id = self.id.to_string(),
                    "Failed to send send status",
                ),
            }
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
struct SendMessage<T>
where
    T: Message,
{
    id: Uuid,
    response_sender: DebugIgnore<Option<oneshot::Sender<SendStatus>>>,
    message: DebugIgnore<T>,
    queue: String,
    timeout: TimeoutStamp,
}

impl<T> SendMessage<T>
where
    T: Message,
{
    #[tracing::instrument(skip(self), fields(message_id = %self.id))]
    pub fn reply(&mut self, status: SendStatus) {
        if let Some(sender) = self.response_sender.0.take() {
            match sender.send(status) {
                Ok(_) => {}
                Err(_) => warn!(
                    message_id = self.id.to_string(),
                    "Failed to send send status",
                ),
            }
        }
    }
}

#[derive(Debug)]
enum Request<T>
where
    T: Message,
{
    Receive(RequestReceive<T>),
    Send(SendMessage<T>),
}
pub struct MessageExchange<T>
where
    T: Message,
{
    request_queues: HashMap<String, LinkedList<Uuid>>,
    request_timeouts: BinaryHeap<Reverse<(Instant, Uuid)>>,
    requests_by_sequence_number: HashMap<Uuid, RequestReceive<T>>,
    message_queues: HashMap<String, LinkedList<Uuid>>,
    message_timeouts: BinaryHeap<Reverse<(Instant, Uuid)>>,
    messages_by_sequence_number: HashMap<Uuid, SendMessage<T>>,
    request_receiver: mpsc::UnboundedReceiver<Request<T>>,
    tick_sender: mpsc::UnboundedSender<Instant>,
    tick_receiver: Option<mpsc::UnboundedReceiver<Instant>>,
}

impl<T> MessageExchange<T>
where
    T: Message,
{
    pub fn new() -> (MessageExchangeConnection<T>, Self) {
        let (request_sender, request_receiver) = mpsc::unbounded_channel();
        let (tick_sender, tick_receiver) = mpsc::unbounded_channel();
        (
            MessageExchangeConnection { request_sender },
            Self {
                request_queues: HashMap::new(),
                request_timeouts: BinaryHeap::new(),
                requests_by_sequence_number: HashMap::new(),
                message_queues: HashMap::new(),
                message_timeouts: BinaryHeap::new(),
                messages_by_sequence_number: HashMap::new(),
                request_receiver,
                tick_sender,
                tick_receiver: Some(tick_receiver),
            },
        )
    }

    #[tracing::instrument(skip(self, request), fields(request_id = %request.id, queue=%request.queue))]
    async fn receive(&mut self, request: RequestReceive<T>) {
        info!("Receive request {}", request.id);
        let queue = request.queue.clone();
        match self.request_queues.get_mut(queue.as_str()) {
            Some(queue) => queue.push_back(request.id),
            None => {
                self.request_queues
                    .insert(queue.clone(), LinkedList::from([request.id]));
                ()
            }
        }
        self.request_timeouts
            .push(Reverse((request.timeout.timeout_at, request.id.clone())));
        self.requests_by_sequence_number.insert(request.id, request);
        self.make_matches(&queue).await;
    }

    #[tracing::instrument(skip(self, message),fields(message_id = %message.id, queue=%message.queue))]
    async fn send(&mut self, message: SendMessage<T>) {
        info!("Send request {}", message.id);
        let queue = message.queue.clone();
        match self.message_queues.get_mut(queue.as_str()) {
            Some(queue) => queue.push_back(message.id),
            None => {
                self.message_queues
                    .insert(queue.clone(), LinkedList::from([message.id]));
                ()
            }
        }
        self.message_timeouts
            .push(Reverse((message.timeout.timeout_at, message.id.clone())));
        self.messages_by_sequence_number.insert(message.id, message);
        self.make_matches(queue.as_str()).await;
    }

    #[tracing::instrument(skip(request, message), fields(request_id = %request.id, message_id=%message.id))]
    async fn process_match(mut request: RequestReceive<T>, mut message: SendMessage<T>) {
        message.reply(SendStatus::Received);
        request.reply(RecieveStatus::Received(message.message.0));
    }

    #[tracing::instrument(skip(self))]
    fn pop_timed_out(&mut self, queue: &str) {
        let request_queue = self.request_queues.get_mut(queue);
        let mut remove_request_queue = false;
        if let Some(request_queue) = request_queue {
            loop {
                let front = request_queue.front();
                match front {
                    Some(elem) => {
                        if !self.requests_by_sequence_number.contains_key(elem) {
                            request_queue.pop_front();
                        } else {
                            break;
                        }
                    }
                    None => {}
                }
                if request_queue.is_empty() {
                    remove_request_queue = true;
                    break;
                }
            }
        }
        if remove_request_queue {
            self.request_queues.remove(queue);
        }
        let message_queue = self.message_queues.get_mut(queue);
        let mut remove_message_queue = false;
        if let Some(message_queue) = message_queue {
            loop {
                let front = message_queue.front();
                match front {
                    Some(elem) => {
                        if !self.messages_by_sequence_number.contains_key(elem) {
                            message_queue.pop_front();
                        } else {
                            break;
                        }
                    }
                    None => {}
                }
                if message_queue.is_empty() {
                    remove_message_queue = true;
                    break;
                }
            }
        }
        if remove_message_queue {
            self.message_queues.remove(queue);
        }
    }

    #[tracing::instrument(skip(self))]
    async fn make_matches(&mut self, queue: &str) {
        loop {
            self.pop_timed_out(queue);
            let request_queue = self.request_queues.get_mut(queue);
            let message_queue = self.message_queues.get_mut(queue);
            match (request_queue, message_queue) {
                (Some(request_queue), Some(message_queue)) => {
                    let request_id = request_queue
                        .pop_front()
                        .expect("No empty request queue should ever be in requests map");
                    if request_queue.is_empty() {
                        self.request_queues.remove(queue);
                    }
                    let request = self
                        .requests_by_sequence_number
                        .remove(&request_id)
                        .expect("Queues and request lists must match");
                    let message_id = message_queue
                        .pop_front()
                        .expect("No empty request queue should ever be in requests map");
                    if message_queue.is_empty() {
                        self.message_queues.remove(queue);
                    }
                    let message = self
                        .messages_by_sequence_number
                        .remove(&message_id)
                        .expect("Queues and message lists must match");
                    Self::process_match(request, message).await;
                }

                _ => {
                    break;
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn process_timeout_at(&mut self, now: Instant) {
        loop {
            let timed_out = self
                .message_timeouts
                .peek()
                .filter(|Reverse((timeout_at, _))| timeout_at < &now)
                .is_some();
            if timed_out {
                let Reverse((_, id)) = self.message_timeouts.pop().unwrap();
                event!(Level::INFO, message_id = id.to_string(), "message_timeout");
                if let Some(mut message) = self.messages_by_sequence_number.remove(&id) {
                    message.reply(SendStatus::Timeout)
                }
            } else {
                break;
            }
        }
        loop {
            let timed_out = self
                .request_timeouts
                .peek()
                .filter(|Reverse((timeout_at, _))| timeout_at < &now)
                .is_some();
            if timed_out {
                let Reverse((_, id)) = self.request_timeouts.pop().unwrap();
                event!(Level::INFO, request_id = id.to_string(), "request_timeout");
                if let Some(mut sender) = self.requests_by_sequence_number.remove(&id) {
                    sender.reply(RecieveStatus::Timeout)
                }
            } else {
                break;
            }
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
}

impl<T> MessageExchangeConnection<T>
where
    T: Message,
{
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
    const MAX_TIMEOUT: Duration = Duration::from_secs(10);

    pub async fn send_message(
        &self,
        message: T,
        queue: String,
        block: bool,
        timeout: Option<Duration>,
    ) -> SendStatus {
        let (response_sender, receiver) = if block {
            let (response_sender, receiver) = oneshot::channel();
            (Some(response_sender), Some(receiver))
        } else {
            (None, None)
        };
        let id = Uuid::new_v4();
        let span = span!(
            Level::INFO,
            "send_message",
            queue = queue,
            message_id = id.to_string()
        );
        let _guard = span.enter();
        let message = Request::Send(SendMessage {
            id,
            response_sender: DebugIgnore(response_sender),
            message: DebugIgnore(message),
            queue,
            timeout: TimeoutStamp::new(min(
                timeout.unwrap_or(Self::DEFAULT_TIMEOUT),
                Self::MAX_TIMEOUT,
            )),
        });
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
        let (response_sender, receiver) = oneshot::channel();
        let id = Uuid::new_v4();
        let span = span!(
            Level::INFO,
            "receive_message",
            queue = queue,
            message_id = id.to_string()
        );
        let _guard = span.enter();
        let request = Request::Receive(RequestReceive {
            id,
            response_sender: DebugIgnore(Some(response_sender)),
            queue,
            timeout: TimeoutStamp::new(min(
                timeout.unwrap_or(Self::DEFAULT_TIMEOUT),
                Self::MAX_TIMEOUT,
            )),
        });
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
    async fn test_send_receive() {
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
                        None,
                    )
                    .await;
            }
        });
        for i in 1..count {
            assert_eq!(
                connection
                    .receive_message(format!("messages {}", i % 10), None)
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
