use anyhow::Context;
use gossip_glomers::{InitBody, Message, MessageID, Node, Output};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, PartialEq)]
struct GenerateOkBody {
    id: Uuid,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseType {
    GenerateOk(GenerateOkBody),
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageType {
    // {
    // "type": "generate"
    // }
    Generate,
}

type ResponseBody = gossip_glomers::ResponseBody<ResponseType>;

type Response = gossip_glomers::Response<ResponseType>;

struct UniqueIDNode {
    msg_id: MessageID,
    node_id: String,
}

impl Node<MessageType> for UniqueIDNode {
    fn init(message: InitBody) -> Self {
        Self {
            msg_id: 1,
            node_id: message.node_id,
        }
    }

    fn on_message(&mut self, message: Message<MessageType>, output: &mut Output) -> anyhow::Result<()> {
        match message.body.kind {
            MessageType::Generate => {
                let reply = Response {
                    src: self.node_id.clone(),
                    dst: message.src,
                    body: ResponseBody {
                        kind: ResponseType::GenerateOk(GenerateOkBody { id: Uuid::now_v7() }),
                        msg_id: Some(self.msg_id),
                        in_reply_to: message.body.msg_id,
                    },
                };

                reply
                    .serialize(output)
                    .context("serializing generate_ok response")?;
                self.msg_id += 1;

                Ok(())
            }
        }
    }
}

pub fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<UniqueIDNode, MessageType>()?;

    Ok(())
}
