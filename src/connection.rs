//! The cessage brokers's connection.
use anyhow::anyhow;

use std::{
    cmp::min,
    collections::HashMap,
    fmt::{self, Display},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{self, error::Elapsed},
};
use tokio_util::sync::CancellationToken;
use tracing::{event, span, warn, Level};

use crate::{
    broker::{Message, ReceiveStatus, Request, SendStatus},
    requests::{MessageSubscription, RequestReceive, RequestSend, TimeoutStamp},
};

impl From<SendStatus> for BlockingSendStatus {
    fn from(val: SendStatus) -> Self {
        match val {
            SendStatus::InternalError => BlockingSendStatus::InternalError,
            SendStatus::Delivered => BlockingSendStatus::Delivered,
            SendStatus::Enqueued => BlockingSendStatus::InternalError,
            SendStatus::Timeout => BlockingSendStatus::Timeout,
        }
    }
}

impl From<SendStatus> for NonblockingSendStatus {
    fn from(val: SendStatus) -> Self {
        match val {
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
    pub(crate) fn new(
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
            default_timeout: self.default_timeout,
            max_timeout: self.max_timeout,
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
