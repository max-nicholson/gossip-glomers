use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::Context;
use gossip_glomers::{InitBody, Message, MessageID, Node, Output};
use serde::{Deserialize, Serialize};

const BROADCAST_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct AddBody {
    delta: u64,
}

#[derive(Debug, Serialize, PartialEq)]
struct ReadOkBody {
    value: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct BroadcastBody {
    values: HashMap<String, u64>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseType {
    AddOk,
    ReadOk(ReadOkBody),
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageType {
    Add(AddBody),
    Read,
    Broadcast(BroadcastBody),
}

type MessageBody = gossip_glomers::MessageBody<MessageType>;

type ResponseBody = gossip_glomers::ResponseBody<ResponseType>;

type Response = gossip_glomers::Response<ResponseType>;

struct GrowOnlyCounterNode {
    msg_id: MessageID,
    node_id: String,
    counters: HashMap<String, u64>,
    last_broadcast: Instant,
}

impl Node<MessageType> for GrowOnlyCounterNode {
    fn init(message: InitBody) -> Self {
        Self {
            msg_id: 1,
            node_id: message.node_id.clone(),
            counters: message
                .node_ids
                .iter()
                .map(|node_id| (node_id.clone(), 0))
                .chain([(message.node_id.clone(), 0)])
                .collect(),
            last_broadcast: Instant::now(),
        }
    }

    fn on_message(
        &mut self,
        message: Message<MessageType>,
        output: &mut Output,
    ) -> anyhow::Result<()> {
        match message.body.kind {
            MessageType::Add(body) => {
                *self.counters.get_mut(&self.node_id).unwrap() += body.delta;

                let reply = Response {
                    src: self.node_id.clone(),
                    dst: message.src.clone(),
                    body: ResponseBody {
                        kind: ResponseType::AddOk,
                        msg_id: Some(self.msg_id),
                        in_reply_to: message.body.msg_id,
                    },
                };

                reply
                    .serialize(&mut *output)
                    .context("serializing add_ok response")?;

                self.msg_id += 1;
            }
            MessageType::Read => {
                let total = self.counters.values().sum::<u64>();

                let reply = Response {
                    src: self.node_id.clone(),
                    dst: message.src.clone(),
                    body: ResponseBody {
                        kind: ResponseType::ReadOk(ReadOkBody { value: total }),
                        msg_id: Some(self.msg_id),
                        in_reply_to: message.body.msg_id,
                    },
                };

                reply
                    .serialize(&mut *output)
                    .context("serializing read_ok response")?;

                self.msg_id += 1;
            }
            MessageType::Broadcast(body) => {
                body.values
                    .iter()
                    .filter(|(k, _)| **k != self.node_id)
                    .for_each(|(node_id, incoming)| match self.counters.get_mut(node_id) {
                        Some(current) => {
                            if incoming > current {
                                *current = *incoming;
                            }
                        }
                        None => {
                            self.counters.insert(node_id.clone(), *incoming);
                        }
                    });
            }
        }

        if self.last_broadcast.elapsed() >= BROADCAST_INTERVAL {
            self.last_broadcast = Instant::now();

            for dst in self.counters.keys().filter(|k| **k != self.node_id) {
                let message = Message {
                    src: self.node_id.clone(),
                    dst: dst.clone(),
                    body: MessageBody {
                        kind: MessageType::Broadcast(BroadcastBody {
                            values: self.counters.clone(),
                        }),
                        msg_id: None,
                    },
                };

                message
                    .serialize(&mut *output)
                    .context("serializing broadcast message")?;
            }
        }

        Ok(())
    }
}

pub fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<GrowOnlyCounterNode, MessageType>()
}
