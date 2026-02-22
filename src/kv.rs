use serde::{Deserialize, Serialize};

use crate::{Message, MessageBody};

#[derive(Debug, PartialEq, Serialize)]
pub struct ReadBody {
    key: String,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct CompareAndSwapBody<T> {
    key: String,
    from: T,
    to: T,
    create_if_not_exists: bool,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageType<T> {
    Read(ReadBody),
    Write,
    #[serde(rename = "cas")]
    CompareAndSwap(CompareAndSwapBody<T>),
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ReadOkBody {
    pub value: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseType {
    ReadOk(ReadOkBody),
    WriteOk,
    #[serde(rename = "cas_ok")]
    CompareAndSwapOk,
}

pub struct Sequential {
    node_id: String,
}

impl Sequential {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    pub fn read<T>(&self, key: &str) -> Message<MessageType<T>> {
        Message::<MessageType<T>> {
            src: self.node_id.clone(),
            dst: "seq-kv".to_string(),
            body: MessageBody::<MessageType<T>> {
                kind: MessageType::Read(ReadBody {
                    key: key.to_string(),
                }),
                msg_id: None,
            },
        }
    }

    pub fn compare_and_swap<T>(
        &self,
        key: &str,
        from: T,
        to: T,
        create_if_not_exists: bool,
    ) -> Message<MessageType<T>> {
        Message::<MessageType<T>> {
            src: self.node_id.clone(),
            dst: "seq-kv".to_string(),
            body: MessageBody::<MessageType<T>> {
                kind: MessageType::CompareAndSwap(CompareAndSwapBody::<T> {
                    key: key.to_string(),
                    from,
                    to,
                    create_if_not_exists,
                }),
                msg_id: None,
            },
        }
    }
}
