//! Requests and responses used in the message exchange.
use debug_ignore::DebugIgnore;
use std::time::Duration;
use tokio::{
    sync::{
        mpsc::{error::SendError, UnboundedSender},
        oneshot::{self, Receiver},
    },
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::{
    exchange::{Message, ReceiveStatus, SendStatus},
    queue::{RespondWith, RespondWithTimeout, Subscription, SubscriptionDelivery},
};

/// Request to receive a message.
#[derive(Debug)]
pub struct RequestReceive<T>
where
    T: Message,
{
    pub id: Uuid,
    pub queue: String,
    pub timeout: TimeoutStamp,
    pub(crate) response_sender: DebugIgnore<Option<oneshot::Sender<ReceiveStatus<T>>>>,
}

impl<T> RequestReceive<T>
where
    T: Message,
{
    pub fn new(queue: String, timeout: TimeoutStamp) -> (Receiver<ReceiveStatus<T>>, Self) {
        let (response_sender, receiver) = oneshot::channel();
        let id = Uuid::new_v4();
        let request = Self {
            id,
            response_sender: DebugIgnore(Some(response_sender)),
            queue,
            timeout,
        };
        (receiver, request)
    }

    #[tracing::instrument(skip(self), fields(request_id = %self.id))]
    fn reply(&mut self, status: ReceiveStatus<T>) {
        if let Some(sender) = self.response_sender.0.take() {
            if sender.send(status).is_err() {
                warn!(
                    message_id = self.id.to_string(),
                    "Failed to send send status",
                );
            }
        }
    }
}

impl<T> RespondWith<RequestSend<T>> for RequestReceive<T>
where
    T: Message,
{
    #[tracing::instrument(skip(self, message), fields(request_id = %self.id, message_id=%message.id))]
    fn respond_with(mut self, mut message: RequestSend<T>) {
        message.reply(SendStatus::Delivered);
        self.reply(ReceiveStatus::Received(message.consume()));
    }
}

impl<T> RespondWithTimeout for RequestReceive<T>
where
    T: Message,
{
    fn respond_with_timeout(mut self) {
        self.reply(ReceiveStatus::Timeout);
    }
}

/// Request to send a message.
#[derive(Debug)]
pub struct RequestSend<T>
where
    T: Message,
{
    pub id: Uuid,
    pub queue: String,
    pub timeout: TimeoutStamp,
    pub(crate) response_sender: DebugIgnore<Option<oneshot::Sender<SendStatus>>>,
    message: DebugIgnore<Option<T>>,
}

impl<T> RequestSend<T>
where
    T: Message,
{
    pub(crate) fn new(
        queue: String,
        timeout: TimeoutStamp,
        message: T,
        block: bool,
    ) -> (Option<Receiver<SendStatus>>, Self) {
        let (response_sender, receiver) = if block {
            let (response_sender, receiver) = oneshot::channel();
            (Some(response_sender), Some(receiver))
        } else {
            (None, None)
        };
        let id = Uuid::new_v4();
        (
            receiver,
            Self {
                id,
                queue,
                timeout,
                response_sender: DebugIgnore(response_sender),
                message: DebugIgnore(Some(message)),
            },
        )
    }
    #[tracing::instrument(skip(self), fields(message_id = %self.id))]
    pub(crate) fn reply(&mut self, status: SendStatus) {
        if let Some(sender) = self.response_sender.0.take() {
            if sender.send(status).is_err() {
                warn!(
                    message_id = self.id.to_string(),
                    "Failed to send send status",
                );
            }
        }
    }

    pub(crate) fn take(mut self) -> (T, EmptyRequestSend<T>) {
        let payload = self.message.take().unwrap();
        (payload, EmptyRequestSend { request: self })
    }

    pub(crate) fn consume(mut self) -> T {
        self.message.take().unwrap()
    }
}

pub(crate) struct EmptyRequestSend<T>
where
    T: Message,
{
    request: RequestSend<T>,
}

impl<T> EmptyRequestSend<T>
where
    T: Message,
{
    pub(crate) fn with_message(mut self, payload: T) -> RequestSend<T> {
        self.request.message.replace(payload);
        self.request
    }

    pub(crate) fn reply(mut self, status: SendStatus) {
        self.request.reply(status);
    }
}

impl<T> RespondWithTimeout for RequestSend<T>
where
    T: Message,
{
    fn respond_with_timeout(mut self) {
        self.reply(SendStatus::Timeout);
    }
}

#[derive(Debug)]
pub struct MessageSubscription<T>
where
    T: Message,
{
    pub id: Uuid,
    pub queue: String,
    sender: UnboundedSender<T>,
    cancellation_token: CancellationToken,
}

impl<T> MessageSubscription<T>
where
    T: Message,
{
    pub fn new(queue: String, sender: UnboundedSender<T>) -> (CancellationToken, Self) {
        let token = CancellationToken::new();
        let result = Self {
            id: Uuid::new_v4(),
            queue,
            sender,
            cancellation_token: token.clone(),
        };
        (token, result)
    }
}

impl<T> Subscription<RequestSend<T>> for MessageSubscription<T>
where
    T: Message,
{
    fn try_deliver(&mut self, response: RequestSend<T>) -> SubscriptionDelivery<RequestSend<T>> {
        if self.cancellation_token.is_cancelled() {
            return SubscriptionDelivery::ExpiredSubscriptionCannotDeliver(response);
        }
        let (response, empty_request) = response.take();
        match self.sender.send(response) {
            Ok(()) => {
                empty_request.reply(SendStatus::Delivered);
                SubscriptionDelivery::DeliveryAccepted
            }
            Err(SendError(message)) => SubscriptionDelivery::ExpiredSubscriptionCannotDeliver(
                empty_request.with_message(message),
            ),
        }
    }
}

/// Encapsulates information to process and trace timeouts.
#[derive(Debug)]
pub struct TimeoutStamp {
    pub timeout_at: Instant,
    pub created_at: Instant,
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
