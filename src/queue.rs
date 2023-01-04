//! Implmementation of a queue and its associated traits.
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
};

use tokio::time::Instant;
use tracing::{event, Level};
use uuid::Uuid;

/// The ability to respond with a response of type `T`
pub trait RespondWith<T> {
    fn respond_with(self, response: T);
}

/// The ability to respond with a timeout
pub trait RespondWithTimeout {
    fn respond_with_timeout(self);
}

/// Status of a queue
#[derive(Eq, PartialEq, Debug)]
pub enum QueueStatus {
    /// The queue is empty
    Empty,
    /// The queue is nonempty
    Nonempty,
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
pub struct Queue<Req, Msg>
where
    Req: RespondWith<Msg>,
{
    name: String,
    receive_request_queue: VecDeque<Uuid>,
    receive_request_timeouts: BinaryHeap<Reverse<(Instant, Uuid)>>,
    receive_requests: HashMap<Uuid, Req>,
    message_request_queue: VecDeque<Uuid>,
    message_request_timeouts: BinaryHeap<Reverse<(Instant, Uuid)>>,
    message_requests: HashMap<Uuid, Msg>,
}

impl<'a, 'b, Req, Msg> Queue<Req, Msg>
where
    Msg: RespondWithTimeout,
    Req: RespondWith<Msg> + RespondWithTimeout,
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

    /// Try to match a request to a response after a send or receive request was enqueued.
    /// This method directly ensures invariant 2, and invariant 1 by calling another method,
    /// so it needs to be called after every send or receive operation.
    #[tracing::instrument(skip(self), fields(queue=self.name))]
    fn make_match(&mut self) -> QueueStatus {
        if self.receive_request_queue.is_empty() && self.message_request_queue.is_empty() {
            return QueueStatus::Empty;
        }
        if self.receive_request_queue.is_empty() || self.message_request_queue.is_empty() {
            return QueueStatus::Nonempty;
        }
        let rcv_req_id = self
            .receive_request_queue
            .pop_front()
            .expect("No empty request queue should ever be in requests map");
        let request = self
            .receive_requests
            .remove(&rcv_req_id)
            .expect("Queues and request lists must match");
        let msg_req_id = self
            .message_request_queue
            .pop_front()
            .expect("No empty request queue should ever be in requests map");
        let message = self
            .message_requests
            .remove(&msg_req_id)
            .expect("Queues and message lists must match");
        request.respond_with(message);
        self.remove_requests_without_id()
    }

    /// Process potential timeouts at instant `now`.
    #[tracing::instrument(skip(self), fields(queue=self.name))]
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
                    sender.respond_with_timeout();
                }
            } else {
                break;
            }
        }
        self.remove_requests_without_id()
    }

    /// Process a receive request.
    #[tracing::instrument(skip(self, receive_request), fields(queue=self.name, rcv_req_id=%id))]
    pub fn receive(&mut self, receive_request: Req, id: Uuid, timeout_at: Instant) -> QueueStatus {
        self.receive_request_queue.push_back(id.clone());
        self.receive_request_timeouts
            .push(Reverse((timeout_at, id.clone())));
        self.receive_requests.insert(id, receive_request);
        self.make_match()
    }

    /// Process a message request.
    #[tracing::instrument(skip(self, message_request), fields(queue=self.name, msg_req_id=%id))]
    pub fn send(&mut self, message_request: Msg, id: Uuid, timeout_at: Instant) -> QueueStatus {
        self.message_request_queue.push_back(id.clone());
        self.message_request_timeouts
            .push(Reverse((timeout_at, id.clone())));
        self.message_requests.insert(id, message_request);
        self.make_match()
    }
}

#[cfg(test)]
mod tests {
    use super::{Queue, QueueStatus};
    use crate::{
        exchange::{Message, ReceiveStatus, SendStatus},
        requests::{RequestReceive, RequestSend, TimeoutStamp},
    };
    use distinct_permutations::distinct_permutations;
    use itertools::Itertools;
    use std::cmp::Ord;
    use std::{env, iter::repeat, sync::Once, time::Duration};
    use tokio::{sync::oneshot, time};
    use tracing_subscriber::fmt::format::FmtSpan;

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

    fn q() -> Queue<RequestReceive<u64>, RequestSend<u64>> {
        Queue::new("foo")
    }

    const TIMEOUT: Duration = Duration::from_millis(1);

    fn receive() -> (oneshot::Receiver<ReceiveStatus<u64>>, RequestReceive<u64>) {
        RequestReceive::new("foo".to_string(), TimeoutStamp::new(TIMEOUT.clone()))
    }
    fn send(message: u64) -> (oneshot::Receiver<SendStatus>, RequestSend<u64>) {
        let (receiver, request) = RequestSend::new(
            "foo".to_string(),
            TimeoutStamp::new(TIMEOUT.clone()),
            message,
            true,
        );
        (receiver.unwrap(), request)
    }

    impl<T> Queue<RequestReceive<T>, RequestSend<T>>
    where
        T: Message,
    {
        pub fn send_receive_request(&mut self, request: RequestReceive<T>) -> QueueStatus {
            let timeout_at = request.timeout.timeout_at.clone();
            let send_id = request.id.clone();
            self.receive(request, send_id, timeout_at)
        }

        pub fn send_send_request(&mut self, request: RequestSend<T>) -> QueueStatus {
            let timeout_at = request.timeout.timeout_at.clone();
            let send_id = request.id.clone();
            self.send(request, send_id, timeout_at)
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
}
