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

#[derive(Eq, PartialEq)]
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
