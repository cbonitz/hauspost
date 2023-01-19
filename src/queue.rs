//! Implmementation of a queue and its associated traits.
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
};

use tokio::time::Instant;
use tracing::{event, warn, Level};
use uuid::Uuid;

/// The ability to respond with a response of type `T`
pub trait RespondWith<T> {
    fn respond_with(self, response: T);
}

/// The ability to respond with a timeout
pub trait RespondWithTimeout {
    fn respond_with_timeout(self);
}

pub enum SubscriptionDelivery<T> {
    DeliveryAccepted,
    ExpiredSubscriptionCannotDeliver(T),
}

/// A subscription
pub trait Subscription<T> {
    fn try_deliver(&mut self, response: T) -> SubscriptionDelivery<T>;
}

/// Status of a queue
#[derive(Eq, PartialEq, Debug)]
pub enum QueueStatus {
    /// The queue is empty
    Empty,
    /// The queue is nonempty
    Nonempty,
}

enum QueueItem<Subscription, RequestSingleReceive> {
    Subscription(Subscription),
    RequestSingleReceive(RequestSingleReceive),
}

/// A queue abstraction that will match requests of type `Req` to responses of type `Msg` in a FIFO matter.
///
/// At the core, there are
/// * a pair of HashMaps to store the requests indexed by their IDs.
/// * a pair of queues for send and receive requests.
/// * a pair of heaps for their timeouts.
/// After each send/receive request, an attempt will be made to match the new send/recieive request with a corresponding receive/send request.
/// Timeouts will remove the request from its HashMap, and pop all expired ids from the front of the send/receive queues.
///
/// The following invariant is maintained after each possible operation:
/// 1. Both the send and receive request queues are either empty, or their head is a non-expired request, i.e. the request is still in the corresponding HashMap.
/// 2. At most one of the two queues is nonempty.
pub struct Queue<Subscription, RequestSingleReceive, Msg>
where
    Msg: RespondWithTimeout,
    Subscription: crate::queue::Subscription<Msg>,
    RequestSingleReceive: RespondWith<Msg> + RespondWithTimeout,
{
    name: String,
    receive_request_queue: VecDeque<Uuid>,
    receive_request_timeouts: BinaryHeap<Reverse<(Instant, Uuid)>>,
    receive_requests: HashMap<Uuid, QueueItem<Subscription, RequestSingleReceive>>,
    message_request_queue: VecDeque<Uuid>,
    message_request_timeouts: BinaryHeap<Reverse<(Instant, Uuid)>>,
    message_requests: HashMap<Uuid, Msg>,
}

impl<Subscription, RequestSingleReceive, Msg> Queue<Subscription, RequestSingleReceive, Msg>
where
    Msg: RespondWithTimeout,
    Subscription: crate::queue::Subscription<Msg>,
    RequestSingleReceive: RespondWith<Msg> + RespondWithTimeout,
{
    /// Creates a queue
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            receive_request_queue: VecDeque::new(),
            receive_request_timeouts: BinaryHeap::new(),
            receive_requests: HashMap::new(),
            message_request_queue: VecDeque::new(),
            message_request_timeouts: BinaryHeap::new(),
            message_requests: HashMap::new(),
        }
    }

    /// Removes all IDs at the head of of `queue` that have no curresponding requests in `requests_by_id`
    fn remove_ids_without_request_in<U>(
        queue: &mut VecDeque<Uuid>,
        requests_by_id: &HashMap<Uuid, U>,
    ) {
        loop {
            let front = queue.front();
            match front {
                Some(elem) => {
                    if !requests_by_id.contains_key(elem) {
                        queue.pop_front();
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }
    }

    /// This method ensures invariant 1 of the struct documentation.
    fn remove_requests_without_id(&mut self) -> QueueStatus {
        Self::remove_ids_without_request_in(
            &mut self.message_request_queue,
            &self.message_requests,
        );
        Self::remove_ids_without_request_in(
            &mut self.receive_request_queue,
            &self.receive_requests,
        );
        if self.receive_request_queue.is_empty() && self.message_request_queue.is_empty() {
            QueueStatus::Empty
        } else {
            QueueStatus::Nonempty
        }
    }

    fn pop_front<T>(queue: &mut VecDeque<Uuid>, map: &mut HashMap<Uuid, T>) -> Option<(Uuid, T)> {
        loop {
            match queue.pop_front() {
                Some(id) => match map.remove(&id) {
                    Some(request) => return Some((id, request)),
                    None => continue,
                },
                None => return None,
            }
        }
    }

    fn push_back<T>(queue: &mut VecDeque<Uuid>, map: &mut HashMap<Uuid, T>, id: Uuid, request: T) {
        queue.push_back(id);
        map.insert(id, request);
    }

    fn push_front<T>(queue: &mut VecDeque<Uuid>, map: &mut HashMap<Uuid, T>, id: Uuid, request: T) {
        queue.push_front(id);
        map.insert(id, request);
    }

    /// Try to match a request to a response after a send or receive request was enqueued.
    /// This method directly ensures invariant 2, and invariant 1 by calling another method,
    /// so it needs to be called after every send or receive operation.
    fn make_match(&mut self) -> QueueStatus {
        if self.receive_request_queue.is_empty() && self.message_request_queue.is_empty() {
            return QueueStatus::Empty;
        }
        if self.receive_request_queue.is_empty() || self.message_request_queue.is_empty() {
            return QueueStatus::Nonempty;
        }
        while let Some((msg_req_id, send_message)) =
            Self::pop_front(&mut self.message_request_queue, &mut self.message_requests)
        {
            if !self.try_find_receiver(send_message, msg_req_id) {
                break;
            }
        }
        self.remove_requests_without_id()
    }

    fn try_find_receiver(&mut self, mut send_message: Msg, msg_req_id: Uuid) -> bool {
        loop {
            match Self::pop_front(&mut self.receive_request_queue, &mut self.receive_requests) {
                Some((request_id, request)) => {
                    match request {
                        QueueItem::Subscription(mut subscription) => {
                            match subscription.try_deliver(send_message) {
                                SubscriptionDelivery::DeliveryAccepted => {
                                    Self::push_back(
                                        &mut self.receive_request_queue,
                                        &mut self.receive_requests,
                                        request_id,
                                        QueueItem::Subscription(subscription),
                                    );
                                    return true;
                                }
                                SubscriptionDelivery::ExpiredSubscriptionCannotDeliver(message) => {
                                    send_message = message
                                }
                            }
                        }
                        QueueItem::RequestSingleReceive(request) => {
                            request.respond_with(send_message);
                            return true;
                        }
                    };
                }
                None => {
                    Self::push_front(
                        &mut self.message_request_queue,
                        &mut self.message_requests,
                        msg_req_id,
                        send_message,
                    );
                    return false;
                }
            }
        }
    }

    /// Process potential timeouts at instant `now`.
    pub fn process_timeout_at(&mut self, now: Instant) -> QueueStatus {
        loop {
            let timed_out = self
                .message_request_timeouts
                .peek()
                .filter(|Reverse((timeout_at, _))| timeout_at < &now)
                .is_some();
            if timed_out {
                let Reverse((_, id)) = self.message_request_timeouts.pop().unwrap();
                event!(Level::INFO, msg_req_id = id.to_string(), "message_timeout");
                if let Some(message) = self.message_requests.remove(&id) {
                    message.respond_with_timeout();
                }
            } else {
                break;
            }
        }
        loop {
            let timed_out = self
                .receive_request_timeouts
                .peek()
                .filter(|Reverse((timeout_at, _))| timeout_at < &now)
                .is_some();
            if timed_out {
                let Reverse((_, id)) = self.receive_request_timeouts.pop().unwrap();
                event!(Level::INFO, rcv_req_id = id.to_string(), "request_timeout");
                if let Some(sender) = self.receive_requests.remove(&id) {
                    match sender {
                        QueueItem::Subscription(_) => warn!("Subscriptions should not time out."),
                        QueueItem::RequestSingleReceive(sender) => sender.respond_with_timeout(),
                    };
                }
            } else {
                break;
            }
        }
        self.remove_requests_without_id()
    }

    /// Process a receive request.
    #[tracing::instrument(skip(self, receive_request), fields(queue=self.name, rcv_req_id=%id))]
    pub fn receive(
        &mut self,
        receive_request: RequestSingleReceive,
        id: Uuid,
        timeout_at: Instant,
    ) -> QueueStatus {
        self.receive_request_queue.push_back(id);
        self.receive_request_timeouts
            .push(Reverse((timeout_at, id)));
        self.receive_requests
            .insert(id, QueueItem::RequestSingleReceive(receive_request));
        self.make_match()
    }

    /// Process a message request.
    #[tracing::instrument(skip(self, message_request), fields(queue=self.name, msg_req_id=%id))]
    pub fn send(&mut self, message_request: Msg, id: Uuid, timeout_at: Instant) -> QueueStatus {
        self.message_request_queue.push_back(id);
        self.message_request_timeouts
            .push(Reverse((timeout_at, id)));
        self.message_requests.insert(id, message_request);
        self.make_match()
    }

    /// Process a subscription.
    #[tracing::instrument(skip(self, subscription), fields(queue=self.name, msg_req_id=%id))]
    pub fn subscribe(&mut self, subscription: Subscription, id: Uuid) -> QueueStatus {
        self.receive_request_queue.push_back(id);
        self.receive_requests
            .insert(id, QueueItem::Subscription(subscription));
        self.make_match()
    }
}

#[cfg(test)]
mod tests {
    use super::{Queue, QueueStatus};
    use crate::{
        broker::{Message, ReceiveStatus, SendStatus},
        requests::{MessageSubscription, RequestReceive, RequestSend, TimeoutStamp},
    };
    use distinct_permutations::distinct_permutations;
    use itertools::Itertools;
    use std::cmp::Ord;
    use std::{env, iter::repeat, sync::Once, time::Duration};
    use tokio::{
        sync::{mpsc, oneshot},
        time::{self, Instant},
    };
    use tracing_subscriber::fmt::format::FmtSpan;

    const DEFAULT_QUEUE_NAME: &str = "foo";

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

    fn q() -> Queue<MessageSubscription<u64>, RequestReceive<u64>, RequestSend<u64>> {
        Queue::new(DEFAULT_QUEUE_NAME)
    }

    const TIMEOUT: Duration = Duration::from_millis(1);

    fn receive() -> (oneshot::Receiver<ReceiveStatus<u64>>, RequestReceive<u64>) {
        RequestReceive::new(
            DEFAULT_QUEUE_NAME.to_string(),
            TimeoutStamp::new(TIMEOUT),
        )
    }
    fn send(message: u64) -> (oneshot::Receiver<SendStatus>, RequestSend<u64>) {
        let (receiver, request) = RequestSend::new(
            DEFAULT_QUEUE_NAME.to_string(),
            TimeoutStamp::new(TIMEOUT),
            message,
            true,
        );
        (receiver.unwrap(), request)
    }

    impl<T> Queue<MessageSubscription<T>, RequestReceive<T>, RequestSend<T>>
    where
        T: Message,
    {
        pub fn send_receive_request(&mut self, request: RequestReceive<T>) -> QueueStatus {
            let timeout_at = request.timeout.timeout_at;
            let send_id = request.id;
            self.receive(request, send_id, timeout_at)
        }

        pub fn send_send_request(&mut self, request: RequestSend<T>) -> QueueStatus {
            let timeout_at = request.timeout.timeout_at;
            let send_id = request.id;
            self.send(request, send_id, timeout_at)
        }

        pub fn send_subscribe_request(
            &mut self,
            subscription: MessageSubscription<T>,
        ) -> QueueStatus {
            let subscription_id = subscription.id;
            self.subscribe(subscription, subscription_id)
        }
    }

    async fn assert_send_received(receiver: oneshot::Receiver<SendStatus>) {
        assert_eq!(receiver.await.unwrap(), SendStatus::Delivered);
    }
    async fn assert_receive_received<T>(receiver: oneshot::Receiver<ReceiveStatus<T>>, message: T)
    where
        T: Message + PartialEq,
    {
        assert_eq!(receiver.await.unwrap(), ReceiveStatus::Received(message));
    }

    #[tokio::test]
    async fn test_send_then_receive() {
        initialize_logger();
        time::pause();
        let mut q = q();
        let (send_status_receiver, send_request) = send(1);
        let (receive_status_receiver, receive_request) = receive();

        assert_eq!(q.send_send_request(send_request), QueueStatus::Nonempty);
        assert_eq!(q.send_receive_request(receive_request), QueueStatus::Empty);

        assert_send_received(send_status_receiver).await;
        assert_receive_received(receive_status_receiver, 1).await;
    }

    #[tokio::test]
    async fn test_receive_then_send() {
        initialize_logger();

        time::pause();
        let mut q = q();
        let (send_status_receiver, send_request) = send(1);
        let (receive_status_receiver, receive_request) = receive();

        assert_eq!(
            q.send_receive_request(receive_request),
            QueueStatus::Nonempty
        );
        assert_eq!(q.send_send_request(send_request), QueueStatus::Empty);

        assert_send_received(send_status_receiver).await;
        assert_receive_received(receive_status_receiver, 1).await;
    }

    #[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
    pub enum SendReceive {
        Send,
        Receive,
    }

    /// Interleaves send/receive requests in order and validates correct send/receive status
    async fn send_then_receive_interleaved(number_of_pairs: usize, steps: Vec<SendReceive>) {
        let mut q = q();
        let mut send_receivers = vec![];
        let mut send_requests = vec![];
        let mut receive_receivers = vec![];
        let mut receive_requests = vec![];
        for i in 0..number_of_pairs {
            let (send_receiver, send_request) = send(i as u64);
            send_receivers.push(send_receiver);
            send_requests.push(send_request);
            let (receive_receiver, receive_request) = receive();
            receive_receivers.push(receive_receiver);
            receive_requests.push(receive_request);
        }
        let mut last_queue_status = QueueStatus::Nonempty;
        for step in steps {
            last_queue_status = match step {
                SendReceive::Send => q.send_send_request(send_requests.remove(0)),
                SendReceive::Receive => q.send_receive_request(receive_requests.remove(0)),
            };
        }
        assert_eq!(last_queue_status, QueueStatus::Empty);
        for send_receiver in send_receivers.into_iter() {
            assert_send_received(send_receiver).await;
        }
        for (i, receive_receiver) in receive_receivers.into_iter().enumerate() {
            assert_receive_received(receive_receiver, i as u64).await;
        }
    }

    #[tokio::test]
    async fn test_send_then_receive_interleaved() {
        initialize_logger();
        time::pause();
        let number_of_pairs = 5;
        let mut steps = vec![];
        steps.append(
            &mut repeat(SendReceive::Receive)
                .take(number_of_pairs)
                .collect_vec(),
        );
        steps.append(
            &mut repeat(SendReceive::Send)
                .take(number_of_pairs)
                .collect_vec(),
        );
        for permutation in distinct_permutations(steps) {
            send_then_receive_interleaved(number_of_pairs, permutation).await;
        }
    }

    #[tokio::test]
    async fn test_subscribe_receive_then_cancel() {
        initialize_logger();
        time::pause();
        let mut q = q();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let (cancel, subscription) =
            MessageSubscription::new(DEFAULT_QUEUE_NAME.to_string(), sender);
        let (send_status_receiver_1, send_request_1) = send(1);
        let (send_status_receiver_2, send_request_2) = send(2);
        let (send_status_receiver_3, send_request_3) = send(3);

        q.send_subscribe_request(subscription);
        q.send_send_request(send_request_1);
        assert_send_received(send_status_receiver_1).await;
        assert_eq!(receiver.recv().await.unwrap(), 1);
        q.send_send_request(send_request_2);
        assert_send_received(send_status_receiver_2).await;
        assert_eq!(receiver.recv().await.unwrap(), 2);
        cancel.cancel();
        q.send_send_request(send_request_3);
        time::advance(TIMEOUT * 2).await;
        q.process_timeout_at(Instant::now());
        assert_eq!(send_status_receiver_3.await.unwrap(), SendStatus::Timeout);
        assert!(receiver.recv().await.is_none());
    }
}
