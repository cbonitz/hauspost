use std::{
    collections::{HashMap, LinkedList},
    fmt,
};

use debug_ignore::DebugIgnore;
use tokio::sync::{mpsc, oneshot};
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
}

impl<T> fmt::Debug for RecieveStatus<T>
where
    T: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InternalError => write!(f, "InternalError"),
            Self::Received(_) => f.debug_tuple("Received").finish(),
        }
    }
}

#[derive(Debug)]
struct RequestReceive<T>
where
    T: Message,
{
    id: Uuid,
    response_sender: DebugIgnore<oneshot::Sender<RecieveStatus<T>>>,
    queue: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendStatus {
    InternalError,
    Received,
    Enqueued,
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
    requests_by_sequence_number: HashMap<Uuid, RequestReceive<T>>,
    message_queues: HashMap<String, LinkedList<Uuid>>,
    messages_by_sequence_number: HashMap<Uuid, SendMessage<T>>,
    request_receiver: mpsc::UnboundedReceiver<Request<T>>,
}

impl<T> MessageExchange<T>
where
    T: Message,
{
    pub fn new() -> (MessageExchangeConnection<T>, Self) {
        let (request_sender, request_receiver) = mpsc::unbounded_channel();
        (
            MessageExchangeConnection { request_sender },
            Self {
                request_queues: HashMap::new(),
                requests_by_sequence_number: HashMap::new(),
                message_queues: HashMap::new(),
                messages_by_sequence_number: HashMap::new(),
                request_receiver,
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
        self.messages_by_sequence_number.insert(message.id, message);
        self.make_matches(queue.as_str()).await;
    }

    #[tracing::instrument(skip(request, message), fields(request_id = %request.id, message_id=%message.id))]
    async fn process_match(request: RequestReceive<T>, message: SendMessage<T>) {
        match request
            .response_sender
            .0
            .send(RecieveStatus::Received(message.message.0))
        {
            Ok(()) => {
                message.response_sender.0.map(|sender| {
                    if sender.send(SendStatus::Received).is_err() {
                        warn!("Error sending success result");
                    }
                });
            }
            Err(_) => {
                warn!("Error sending message");
                message.response_sender.0.map(|sender| {
                    if sender.send(SendStatus::InternalError).is_err() {
                        warn!("Error sending error result");
                    }
                });
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn make_matches(&mut self, queue: &str) {
        loop {
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

    async fn process_request(&mut self, request: Request<T>) {
        info!("Received request");
        match request {
            Request::Receive(request) => self.receive(request).await,
            Request::Send(message) => self.send(message).await,
        };
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) {
        loop {
            match self.request_receiver.recv().await {
                Some(request) => self.process_request(request).await,
                None => {
                    info!("all connections dropped");
                    return;
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
    pub async fn send_message(&self, message: T, queue: String, block: bool) -> SendStatus {
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

    pub async fn receive_message(&self, queue: String) -> RecieveStatus<T> {
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
            response_sender: DebugIgnore(response_sender),
            queue,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_receive() {
        initialize_logger();
        let (connection, mut exchange) = MessageExchange::<String>::new();
        tokio::spawn(async move {
            exchange.run().await;
        });
        let send_connection = connection.clone();
        tokio::spawn(async move {
            send_connection
                .send_message("hello, world".to_string(), "greetings".to_string(), true)
                .await;
        });
        assert_eq!(
            connection.receive_message("greetings".to_string()).await,
            RecieveStatus::Received("hello, world".to_string())
        )
    }

    #[tokio::test(flavor = "multi_thread")]
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
                    .send_message(format!("Hello, {}", i), "greetings".to_string(), true)
                    .await;
            }
            for i in 1..2 {
                send_connection
                    .send_message(format!("Goodbye, {}", i), "farewells".to_string(), true)
                    .await;
            }
        });
        for i in 1..2 {
            assert_eq!(
                connection.receive_message("greetings".to_string()).await,
                RecieveStatus::Received(format!("Hello, {}", i))
            )
        }
        for i in 1..2 {
            assert_eq!(
                connection.receive_message("farewells".to_string()).await,
                RecieveStatus::Received(format!("Goodbye, {}", i))
            )
        }
    }

    #[tokio::test(flavor = "multi_thread")]
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
                    .send_message(format!("Goodbye, {}", i), "farewells".to_string(), false)
                    .await;
            }
            for i in 1..2 {
                send_connection
                    .send_message(format!("Hello, {}", i), "greetings".to_string(), false)
                    .await;
            }
        });
        for i in 1..2 {
            assert_eq!(
                connection.receive_message("greetings".to_string()).await,
                RecieveStatus::Received(format!("Hello, {}", i))
            )
        }
        for i in 1..2 {
            assert_eq!(
                connection.receive_message("farewells".to_string()).await,
                RecieveStatus::Received(format!("Goodbye, {}", i))
            )
        }
    }
}
