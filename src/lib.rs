use anyhow::Context;
use serde_json::{Serializer, ser::Formatter};
use std::io::{self, BufRead, StdoutLock, Write};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub type MessageID = u64;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct InitBody {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageType {
    // {
    //     "type":     "init",
    //     "msg_id":   1,
    //     "node_id":  "n3",
    //     "node_ids": ["n1", "n2", "n3"]
    // }
    Init(InitBody),
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct MessageBody<Type> {
    #[serde(rename = "type", flatten)]
    pub kind: Type,
    pub msg_id: Option<MessageID>,
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
pub struct Message<Type> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: MessageBody<Type>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseType {
    InitOk,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct ResponseBody<Type> {
    #[serde(rename = "type", flatten)]
    pub kind: Type,
    pub msg_id: Option<MessageID>,
    pub in_reply_to: Option<MessageID>,
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
pub struct Response<Type> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: ResponseBody<Type>,
}

#[derive(Default)]
pub struct JSONLFormatter {
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

pub type Output<'a> = Serializer<StdoutLock<'a>, JSONLFormatter>;

pub trait Node<MessageType> {
    fn init(message: InitBody) -> Self;
    fn handle(&mut self, message: Message<MessageType>, output: &mut Output) -> anyhow::Result<()>;
}

pub fn run<N, Type>() -> anyhow::Result<()>
where
    N: Node<Type>,
    Type: DeserializeOwned,
{
    let mut input = std::io::stdin().lock();

    let mut output =
        serde_json::Serializer::with_formatter(std::io::stdout().lock(), JSONLFormatter::default());

    let mut buf = String::new();
    input.read_line(&mut buf).context("reading init message")?;
    let message: Message<MessageType> =
        serde_json::from_str(&buf).context("deserializing init message")?;

    let MessageType::Init(init_body) = message.body.kind;

    let reply = Response {
        src: init_body.node_id.clone(),
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

    let mut node: N = Node::init(init_body);

    for message in serde_json::Deserializer::from_reader(input).into_iter::<Message<Type>>() {
        let message = message.context("could not deserialize Maelstrom input")?;

        node.handle(message, &mut output)?;
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
            serde_json::from_str::<Message<MessageType>>(request).unwrap(),
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
}
