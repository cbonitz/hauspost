//! Requests and responses used in the message exchange.
use std::time::Duration;

use debug_ignore::DebugIgnore;
use tokio::{
    sync::oneshot::{self, Receiver},
    time::Instant,
};
use tracing::warn;
use uuid::Uuid;

use crate::{
    exchange::{Message, ReceiveStatus, SendStatus},
    queue::{RespondWith, RespondWithTimeout},
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

impl<T> RespondWith<RequestSend<T>> for RequestReceive<T>
where
    T: Message,
{
    #[tracing::instrument(skip(self, message), fields(request_id = %self.id, message_id=%message.id))]
    fn respond_with(mut self, mut message: RequestSend<T>) {
        message.reply(SendStatus::Delivered);
        self.reply(ReceiveStatus::Received(message.message.0));
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
    pub(crate) message: DebugIgnore<T>,
}

impl<T> RequestSend<T>
where
    T: Message,
{
    pub fn new(
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
                message: DebugIgnore(message),
            },
        )
    }
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

impl<T> RespondWithTimeout for RequestSend<T>
where
    T: Message,
{
    fn respond_with_timeout(mut self) {
        self.reply(SendStatus::Timeout);
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
