use std::collections::{HashMap, HashSet};

use anyhow::Context;
use gossip_glomers::{InitBody, Message, MessageID, Node, Output};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct BroadcastBody {
    message: u64,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct TopologyBody {
    topology: HashMap<String, Vec<String>>,
}

#[derive(Debug, Serialize, PartialEq)]
struct ReadOkBody {
    messages: HashSet<u64>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseType {
    BroadcastOk,
    ReadOk(ReadOkBody),
    TopologyOk,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageType {
    Broadcast(BroadcastBody),
    Read,
    Topology(TopologyBody),
}

type MessageBody = gossip_glomers::MessageBody<MessageType>;

type ResponseBody = gossip_glomers::ResponseBody<ResponseType>;

type Response = gossip_glomers::Response<ResponseType>;

struct BroadcastNode {
    msg_id: MessageID,
    node_id: String,
    neighbours: Vec<String>,
    messages: HashSet<u64>,
}

impl Node<MessageType> for BroadcastNode {
    fn init(message: InitBody) -> Self {
        Self {
            msg_id: 1,
            node_id: message.node_id.clone(),
            neighbours: message
                .node_ids
                .iter()
                .filter(|&n| n != &message.node_id)
                .cloned()
                .collect(),
            messages: HashSet::new(),
        }
    }

    fn handle(&mut self, message: Message<MessageType>, output: &mut Output) -> anyhow::Result<()> {
        match message.body.kind {
            MessageType::Broadcast(body) => {
                if !self.messages.insert(body.message) {
                    return Ok(());
                }

                for neighbour in &self.neighbours {
                    let reply = Message {
                        src: self.node_id.clone(),
                        dst: neighbour.clone(),
                        body: MessageBody {
                            kind: MessageType::Broadcast(BroadcastBody {
                                message: body.message,
                            }),
                            msg_id: None,
                        },
                    };

                    reply
                        .serialize(&mut *output)
                        .context("serializing broadcast_ok response")?;
                }

                if message.body.msg_id.is_some() {
                    let reply = Response {
                        src: self.node_id.clone(),
                        dst: message.src,
                        body: ResponseBody {
                            kind: ResponseType::BroadcastOk,
                            msg_id: Some(self.msg_id),
                            in_reply_to: message.body.msg_id,
                        },
                    };
                    reply
                        .serialize(output)
                        .context("serializing broadcast_ok response")?;
                    self.msg_id += 1;
                }

                Ok(())
            },
            MessageType::Read => {
                let reply = Response {
                    src: self.node_id.clone(),
                    dst: message.src,
                    body: ResponseBody {
                        kind: ResponseType::ReadOk(ReadOkBody {
                            messages: self.messages.clone(),
                        }),
                        msg_id: Some(self.msg_id),
                        in_reply_to: message.body.msg_id,
                    },
                };

                reply
                    .serialize(output)
                    .context("serializing read_ok response")?;
                self.msg_id += 1;

                Ok(())
            }
            MessageType::Topology(body) => {
                let reply = Response {
                    src: self.node_id.clone(),
                    dst: message.src,
                    body: ResponseBody {
                        kind: ResponseType::TopologyOk,
                        msg_id: Some(self.msg_id),
                        in_reply_to: message.body.msg_id,
                    },
                };

                if let Some(neighbours) = body.topology.get(&self.node_id) {
                    self.neighbours = neighbours.clone()
                }

                reply
                    .serialize(output)
                    .context("serializing topology_ok response")?;
                self.msg_id += 1;

                Ok(())
            }
        }
    }
}

pub fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<BroadcastNode, MessageType>()
}
