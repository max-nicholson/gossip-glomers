use anyhow::Context;
use serde_json::ser::Formatter;
use std::{
    collections::{HashMap, HashSet},
    io::{self, Write},
};
use uuid::Uuid;

use serde::{Deserialize, Serialize};

type MessageID = u64;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct EchoBody {
    echo: String,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct InitBody {
    node_id: String,
    node_ids: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct BroadcastBody {
    message: u64,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct TopologyBody {
    topology: HashMap<String, Vec<String>>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageType {
    Echo(EchoBody),
    // {
    //     "type":     "init",
    //     "msg_id":   1,
    //     "node_id":  "n3",
    //     "node_ids": ["n1", "n2", "n3"]
    // }
    Init(InitBody),
    // {
    // "type": "generate"
    // }
    Generate,
    Broadcast(BroadcastBody),
    Read,
    Topology(TopologyBody),
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct MessageBody {
    #[serde(rename = "type", flatten)]
    kind: MessageType,
    msg_id: Option<MessageID>,
}

// {
//   "src": "c1",
//   "dest": "n1",
//   "body": {
//     "type": "echo",
//     "msg_id": 1,
//     "echo": "Please echo 35"
//   }
// }
#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: MessageBody,
}

#[derive(Debug, Serialize, PartialEq)]
struct EchoOkBody {
    echo: String,
}

#[derive(Debug, Serialize, PartialEq)]
struct GenerateOkBody {
    id: Uuid,
}

#[derive(Debug, Serialize, PartialEq)]
struct ReadOkBody {
    messages: HashSet<u64>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseType {
    EchoOk(EchoOkBody),
    InitOk,
    GenerateOk(GenerateOkBody),
    BroadcastOk,
    ReadOk(ReadOkBody),
    TopologyOk,
}

#[derive(Debug, PartialEq, Serialize)]
struct ResponseBody {
    #[serde(rename = "type", flatten)]
    kind: ResponseType,
    msg_id: Option<MessageID>,
    in_reply_to: Option<MessageID>,
}

// {
//   "src": "n1",
//   "dest": "c1",
//   "body": {
//     "type": "echo_ok",
//     "msg_id": 1,
//     "in_reply_to": 1,
//     "echo": "Please echo 35"
//   }
// }
#[derive(Debug, PartialEq, Serialize)]
struct Response {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: ResponseBody,
}

enum State {
    Initialized(InitializedState),
    Uninitialized(UninitializedState),
}

impl State {
    pub fn new() -> Self {
        State::Uninitialized(UninitializedState { msg_id: 0 })
    }
}

struct InitializedState {
    pub msg_id: MessageID,
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub neighbours: Vec<String>,
    pub messages: HashSet<u64>,
}

struct UninitializedState {
    msg_id: MessageID,
}

impl UninitializedState {
    pub fn init(&mut self, node_id: String, node_ids: Vec<String>) -> InitializedState {
        InitializedState {
            msg_id: self.msg_id,
            node_id: node_id.clone(),
            neighbours: node_ids
                .iter()
                .filter(|&n| n != &node_id)
                .cloned()
                .collect(),
            node_ids,
            messages: HashSet::new(),
        }
    }
}

#[derive(Default)]
struct JSONLFormatter {
    depth: usize,
}

impl Formatter for JSONLFormatter {
    fn begin_object<W: ?Sized + Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.depth += 1;
        w.write_all(b"{")
    }

    fn end_object<W: ?Sized + Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.depth -= 1;

        if self.depth == 0 {
            w.write_all(b"}\n")
        } else {
            w.write_all(b"}")
        }
    }
}

fn main() -> anyhow::Result<()> {
    let input = std::io::stdin().lock();

    let mut output =
        serde_json::Serializer::with_formatter(std::io::stdout().lock(), JSONLFormatter::default());

    let mut state = State::new();

    for message in serde_json::Deserializer::from_reader(input).into_iter::<Message>() {
        let message = message.context("could not deserialize Maelstrom input")?;

        match state {
            State::Initialized(ref mut state) => {
                match message.body.kind {
                    MessageType::Init(_) => {
                        // this would be an error
                        unimplemented!()
                    }
                    MessageType::Echo(body) => {
                        let reply = Response {
                            src: state.node_id.clone(),
                            dst: message.src,
                            body: ResponseBody {
                                kind: ResponseType::EchoOk(EchoOkBody { echo: body.echo }),
                                msg_id: Some(state.msg_id),
                                in_reply_to: message.body.msg_id,
                            },
                        };

                        reply
                            .serialize(&mut output)
                            .context("serializing echo_ok response")?;
                        state.msg_id += 1;
                    }
                    MessageType::Generate => {
                        let reply = Response {
                            src: state.node_id.clone(),
                            dst: message.src,
                            body: ResponseBody {
                                kind: ResponseType::GenerateOk(GenerateOkBody {
                                    id: Uuid::now_v7(),
                                }),
                                msg_id: Some(state.msg_id),
                                in_reply_to: message.body.msg_id,
                            },
                        };

                        reply
                            .serialize(&mut output)
                            .context("serializing generate_ok response")?;
                        state.msg_id += 1;
                    }
                    MessageType::Broadcast(body) => {
                        if !state.messages.insert(body.message) {
                            continue;
                        }

                        for neighbour in &state.neighbours {
                            let reply = Message {
                                src: state.node_id.clone(),
                                dst: neighbour.clone(),
                                body: MessageBody {
                                    kind: MessageType::Broadcast(BroadcastBody {
                                        message: body.message,
                                    }),
                                    msg_id: None,
                                },
                            };

                            reply
                                .serialize(&mut output)
                                .context("serializing broadcast_ok response")?;
                        }

                        if message.body.msg_id.is_some() {
                            let reply = Response {
                                src: state.node_id.clone(),
                                dst: message.src,
                                body: ResponseBody {
                                    kind: ResponseType::BroadcastOk,
                                    msg_id: Some(state.msg_id),
                                    in_reply_to: message.body.msg_id,
                                },
                            };
                            reply
                                .serialize(&mut output)
                                .context("serializing broadcast_ok response")?;
                            state.msg_id += 1;
                        }
                    }
                    MessageType::Read => {
                        let reply = Response {
                            src: state.node_id.clone(),
                            dst: message.src,
                            body: ResponseBody {
                                kind: ResponseType::ReadOk(ReadOkBody {
                                    messages: state.messages.clone(),
                                }),
                                msg_id: Some(state.msg_id),
                                in_reply_to: message.body.msg_id,
                            },
                        };

                        reply
                            .serialize(&mut output)
                            .context("serializing broadcast_ok response")?;
                        state.msg_id += 1;
                    }
                    MessageType::Topology(body) => {
                        let reply = Response {
                            src: state.node_id.clone(),
                            dst: message.src,
                            body: ResponseBody {
                                kind: ResponseType::TopologyOk,
                                msg_id: Some(state.msg_id),
                                in_reply_to: message.body.msg_id,
                            },
                        };

                        if let Some(neighbours) = body.topology.get(&state.node_id) {
                            state.neighbours = neighbours.clone()
                        }

                        reply
                            .serialize(&mut output)
                            .context("serializing topology_ok response")?;
                        state.msg_id += 1;
                    }
                }
            }
            State::Uninitialized(mut s) => {
                match message.body.kind {
                    MessageType::Init(body) => {
                        let initialized_state = s.init(body.node_id, body.node_ids);

                        let reply = Response {
                            src: initialized_state.node_id.clone(),
                            dst: message.src,
                            body: ResponseBody {
                                kind: ResponseType::InitOk,
                                msg_id: None,
                                in_reply_to: message.body.msg_id,
                            },
                        };

                        reply
                            .serialize(&mut output)
                            .context("serializing init_ok response")?;

                        state = State::Initialized(initialized_state);
                    }
                    // This would be an error
                    _ => unimplemented!(),
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_deserialize() {
        let request = r#"{
        "src": "n1",
        "dest": "n2",
        "body": {
            "type": "init",
            "msg_id": 1,
            "node_id": "n3",
            "node_ids": ["n1", "n2", "n3"]
        }
    }"#;

        assert_eq!(
            serde_json::from_str::<Message>(request).unwrap(),
            Message {
                dst: "n2".to_string(),
                src: "n1".to_string(),
                body: MessageBody {
                    msg_id: Some(1),
                    kind: MessageType::Init(InitBody {
                        node_id: "n3".to_string(),
                        node_ids: vec!["n1".to_string(), "n2".to_string(), "n3".to_string()]
                    })
                }
            }
        );
    }

    #[test]
    fn test_echo_deserialize() {
        let request = r#"{
        "src": "n1",
        "dest": "n2",
        "body": {
            "type": "echo",
            "msg_id": 1,
            "echo": "foo"
        }
    }"#;

        assert_eq!(
            serde_json::from_str::<Message>(request).unwrap(),
            Message {
                dst: "n2".to_string(),
                src: "n1".to_string(),
                body: MessageBody {
                    msg_id: Some(1),
                    kind: MessageType::Echo(EchoBody {
                        echo: "foo".to_string()
                    })
                }
            }
        );
    }

    #[test]
    fn test_echo_ok_serialize() {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                serde_json::to_string(&Response {
                    src: "n1".to_string(),
                    dst: "n2".to_string(),
                    body: ResponseBody {
                        msg_id: Some(1),
                        in_reply_to: Some(1),
                        kind: ResponseType::EchoOk(EchoOkBody {
                            echo: "hello".to_string(),
                        })
                    }
                })
                .unwrap()
                .as_str()
            )
            .unwrap(),
            serde_json::from_str::<serde_json::Value>(
                r#"{
                    "src": "n1",
                    "dest": "n2",
                    "body": {
                        "type": "echo_ok",
                        "echo": "hello",
                        "msg_id": 1,
                        "in_reply_to": 1
                    }
                }"#
            )
            .unwrap()
        );
    }
}
