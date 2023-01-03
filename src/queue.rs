use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, LinkedList},
};

use tokio::time::Instant;
use tracing::{event, Level};
use uuid::Uuid;

pub trait RespondWith<T> {
    fn respond_with(self, response: T);
}

pub trait RespondWithTimeout {
    fn respond_with_timeout(self);
}

#[derive(Eq, PartialEq, Debug)]
pub enum QueueStatus {
    Empty,
    Nonempty,
}

pub struct Queue<Req, Res>
where
    Req: RespondWith<Res>,
{
    name: String,
    request_queue: LinkedList<Uuid>,
    request_timeouts: BinaryHeap<Reverse<(Instant, Uuid)>>,
    requests_by_sequence_number: HashMap<Uuid, Req>,
    message_queue: LinkedList<Uuid>,
    message_timeouts: BinaryHeap<Reverse<(Instant, Uuid)>>,
    messages_by_sequence_number: HashMap<Uuid, Res>,
}

impl<'a, 'b, Req, Res> Queue<Req, Res>
where
    Res: RespondWithTimeout,
    Req: RespondWith<Res> + RespondWithTimeout,
{
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            request_queue: LinkedList::new(),
            request_timeouts: BinaryHeap::new(),
            requests_by_sequence_number: HashMap::new(),
            message_queue: LinkedList::new(),
            message_timeouts: BinaryHeap::new(),
            messages_by_sequence_number: HashMap::new(),
        }
    }

    fn pop_timed_out_in<U>(
        queue: &mut LinkedList<Uuid>,
        entities_by_sequence_number: &HashMap<Uuid, U>,
    ) {
        loop {
            let front = queue.front();
            match front {
                Some(elem) => {
                    if !entities_by_sequence_number.contains_key(elem) {
                        queue.pop_front();
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }
    }

    fn pop_timed_out(&mut self) -> QueueStatus {
        Self::pop_timed_out_in(&mut self.message_queue, &self.messages_by_sequence_number);
        Self::pop_timed_out_in(&mut self.request_queue, &self.requests_by_sequence_number);
        if self.request_queue.is_empty() && self.message_queue.is_empty() {
            QueueStatus::Empty
        } else {
            QueueStatus::Nonempty
        }
    }

    #[tracing::instrument(skip(self), fields(queue=self.name))]
    fn make_matches(&mut self) -> QueueStatus {
        if self.request_queue.is_empty() && self.message_queue.is_empty() {
            return QueueStatus::Empty;
        }
        if self.request_queue.is_empty() || self.message_queue.is_empty() {
            return QueueStatus::Nonempty;
        }
        let request_id = self
            .request_queue
            .pop_front()
            .expect("No empty request queue should ever be in requests map");
        let request = self
            .requests_by_sequence_number
            .remove(&request_id)
            .expect("Queues and request lists must match");
        let message_id = self
            .message_queue
            .pop_front()
            .expect("No empty request queue should ever be in requests map");
        let message = self
            .messages_by_sequence_number
            .remove(&message_id)
            .expect("Queues and message lists must match");
        request.respond_with(message);
        self.pop_timed_out()
    }

    #[tracing::instrument(skip(self), fields(queue=self.name))]
    pub fn process_timeout_at(&mut self, now: Instant) -> QueueStatus {
        loop {
            let timed_out = self
                .message_timeouts
                .peek()
                .filter(|Reverse((timeout_at, _))| timeout_at < &now)
                .is_some();
            if timed_out {
                let Reverse((_, id)) = self.message_timeouts.pop().unwrap();
                event!(Level::INFO, message_id = id.to_string(), "message_timeout");
                if let Some(message) = self.messages_by_sequence_number.remove(&id) {
                    message.respond_with_timeout();
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
                if let Some(sender) = self.requests_by_sequence_number.remove(&id) {
                    sender.respond_with_timeout();
                }
            } else {
                break;
            }
        }
        self.pop_timed_out()
    }

    #[tracing::instrument(skip(self, request), fields(queue=self.name, request_id=%id))]
    pub fn receive(&mut self, request: Req, id: Uuid, timeout_at: Instant) -> QueueStatus {
        self.request_queue.push_back(id.clone());
        self.request_timeouts
            .push(Reverse((timeout_at, id.clone())));
        self.requests_by_sequence_number.insert(id, request);
        self.make_matches()
    }

    #[tracing::instrument(skip(self, message), fields(queue=self.name, message_id=%id))]
    pub fn send(&mut self, message: Res, id: Uuid, timeout_at: Instant) -> QueueStatus {
        self.message_queue.push_back(id.clone());
        self.message_timeouts
            .push(Reverse((timeout_at, id.clone())));
        self.messages_by_sequence_number.insert(id, message);
        self.make_matches()
    }
}

#[cfg(test)]
mod tests {
    use super::{Queue, QueueStatus};
    use crate::{
        exchange::{Message, RecieveStatus, SendStatus},
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

    fn receive() -> (oneshot::Receiver<RecieveStatus<u64>>, RequestReceive<u64>) {
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
        assert_eq!(receiver.await.unwrap(), SendStatus::Received);
    }
    async fn assert_receive_received<T>(receiver: oneshot::Receiver<RecieveStatus<T>>, message: T)
    where
        T: Message + PartialEq,
    {
        assert_eq!(receiver.await.unwrap(), RecieveStatus::Received(message));
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
