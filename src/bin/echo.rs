use anyhow::Context;
use serde::{Deserialize, Serialize};

use gossip_glomers::{InitBody, Message, MessageID, Node, Output};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct EchoBody {
    echo: String,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageType {
    Echo(EchoBody),
}

#[derive(Debug, Serialize, PartialEq)]
struct EchoOkBody {
    echo: String,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseType {
    EchoOk(EchoOkBody),
}

#[allow(dead_code)]
type MessageBody = gossip_glomers::MessageBody<MessageType>;

type ResponseBody = gossip_glomers::ResponseBody<ResponseType>;

type Response = gossip_glomers::Response<ResponseType>;

struct EchoNode {
    msg_id: MessageID,
    node_id: String,
}

impl Node<MessageType> for EchoNode {
    fn init(message: InitBody) -> Self {
        Self {
            msg_id: 1,
            node_id: message.node_id,
        }
    }

    fn handle(&mut self, message: Message<MessageType>, output: &mut Output) -> anyhow::Result<()> {
        match message.body.kind {
            MessageType::Echo(body) => {
                let reply = Response {
                    src: self.node_id.clone(),
                    dst: message.src,
                    body: ResponseBody {
                        kind: ResponseType::EchoOk(EchoOkBody { echo: body.echo }),
                        msg_id: Some(self.msg_id),
                        in_reply_to: message.body.msg_id,
                    },
                };

                reply
                    .serialize(output)
                    .context("serializing echo_ok response")?;
                self.msg_id += 1;

                Ok(())
            }
        }
    }
}

pub fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<EchoNode, MessageType>()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
            serde_json::from_str::<Message<MessageType>>(request).unwrap(),
            Message {
                dst: "n2".to_string(),
                src: "n1".to_string(),
                body: MessageBody {
                    msg_id: Some(1),
                    kind: MessageType::Echo(EchoBody {
                        echo: "foo".to_string(),
                    })
                },
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
