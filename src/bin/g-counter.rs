use std::sync::LazyLock;
use std::{collections::HashMap, io::stderr};

use anyhow::Context;
use gossip_glomers::{
    ErrorBody, ErrorMessageType, InitBody, Message, MessageID, Node, Output, Service, kv,
};
use regex::Regex;
use serde::{Deserialize, Serialize};

const GLOBAL_COUNTER_KEY: &str = "counter";

#[derive(Debug)]
struct ReadRequest {
    src: String,
    msg_id: Option<MessageID>,
}

#[derive(Debug)]
struct WriteRequest {
    src: String,
    msg_id: Option<MessageID>,
    delta: u64,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct AddBody {
    delta: u64,
}

#[derive(Debug, Serialize, PartialEq)]
struct ReadOkBody {
    value: u64,
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
}

type ResponseBody = gossip_glomers::ResponseBody<ResponseType>;

type Response = gossip_glomers::Response<ResponseType>;

struct GrowOnlyCounterNode {
    msg_id: MessageID,
    node_id: String,
    reads: HashMap<MessageID, ReadRequest>,
    writes: HashMap<MessageID, WriteRequest>,
    last_read: u64,
    kv: kv::Sequential,
}

impl Node<MessageType> for GrowOnlyCounterNode {
    fn init(message: InitBody) -> Self {
        Self {
            msg_id: 1,
            node_id: message.node_id.clone(),
            kv: kv::Sequential::new(message.node_id.clone()),
            last_read: 0,
            reads: HashMap::new(),
            writes: HashMap::new(),
        }
    }

    fn on_error(
        &mut self,
        message: Message<ErrorMessageType>,
        output: &mut Output,
    ) -> anyhow::Result<()> {
        let ErrorMessageType::Error(body) = message.body.kind;

        if message.src != "seq-kv" {
            return Ok(());
        };

        match body.code {
            20 => {
                if let Some(msg_id) = body.in_reply_to
                    && let Some(request) = self.reads.get(&msg_id)
                {
                    let reply = Response {
                        src: self.node_id.clone(),
                        dst: request.src.clone(),
                        body: ResponseBody {
                            kind: ResponseType::ReadOk(ReadOkBody { value: 0 }),
                            msg_id: Some(self.msg_id),
                            in_reply_to: request.msg_id,
                        },
                    };

                    reply
                        .serialize(output)
                        .context("serializing read_ok response")?;
                    self.msg_id += 1;

                    self.reads.remove(&msg_id);
                }
            }
            22 => {
                if let Some(msg_id) = body.in_reply_to
                    && let Some(write_request) = self.writes.remove(&msg_id)
                {
                    static RE: LazyLock<Regex> =
                        LazyLock::new(|| Regex::new(r"current value (\d+) is not \d+").unwrap());

                    if let Some(captures) = RE.captures(&body.text)
                        && let Some(last_read) = captures
                            .get(1)
                            .and_then(|m| str::parse::<u64>(m.as_str()).ok())
                    {
                        self.last_read = last_read
                    }

                    let mut request = self.kv.compare_and_swap::<u64>(
                        GLOBAL_COUNTER_KEY,
                        self.last_read,
                        self.last_read + write_request.delta,
                        self.last_read == 0,
                    );
                    request.body.msg_id = Some(self.msg_id);

                    self.writes.insert(self.msg_id, write_request);

                    request
                        .serialize(output)
                        .context("serializing compare_and_swap request")?;

                    self.msg_id += 1;
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn on_service(&mut self, service: Service, output: &mut Output) -> anyhow::Result<()> {
        match service {
            Service::KeyValue(message) => match message.body.kind {
                kv::ResponseType::ReadOk(kv::ReadOkBody { value }) => {
                    let value = match value {
                        Some(value) => serde_json::from_value::<u64>(value)
                            .context("deserializing read_ok value as u64")?,
                        None => 0,
                    };

                    if value >= self.last_read {
                        self.last_read = value
                    }

                    if let Some(msg_id) = message.body.in_reply_to
                        && let Some(request) = self.reads.get(&msg_id)
                    {
                        let reply = Response {
                            src: self.node_id.clone(),
                            dst: request.src.clone(),
                            body: ResponseBody {
                                kind: ResponseType::ReadOk(ReadOkBody { value }),
                                msg_id: Some(self.msg_id),
                                in_reply_to: request.msg_id,
                            },
                        };

                        reply
                            .serialize(output)
                            .context("serializing read_ok response")?;
                        self.msg_id += 1;

                        self.reads.remove(&msg_id);
                    }

                    Ok(())
                }
                kv::ResponseType::CompareAndSwapOk => {
                    if let Some(msg_id) = message.body.in_reply_to
                        && let Some(request) = self.writes.get(&msg_id)
                    {
                        let reply = Response {
                            src: self.node_id.clone(),
                            dst: request.src.clone(),
                            body: ResponseBody {
                                kind: ResponseType::AddOk,
                                msg_id: Some(self.msg_id),
                                in_reply_to: request.msg_id,
                            },
                        };
                        reply
                            .serialize(output)
                            .context("serializing add_ok response")?;
                        self.msg_id += 1;

                        self.writes.remove(&msg_id);
                    }

                    Ok(())
                }
                _ => Ok(()),
            },
        }
    }

    fn on_message(
        &mut self,
        message: Message<MessageType>,
        output: &mut Output,
    ) -> anyhow::Result<()> {
        match message.body.kind {
            MessageType::Add(body) => {
                if body.delta == 0 {
                    // No need to involve KV store
                    // Effectively a no-op
                    let reply = Response {
                        src: self.node_id.clone(),
                        dst: message.src,
                        body: ResponseBody {
                            kind: ResponseType::AddOk,
                            msg_id: Some(self.msg_id),
                            in_reply_to: message.body.msg_id,
                        },
                    };

                    reply
                        .serialize(output)
                        .context("serializing add_ok response")?;
                    self.msg_id += 1;

                    return Ok(());
                }

                self.writes.insert(
                    self.msg_id,
                    WriteRequest {
                        src: message.src,
                        msg_id: message.body.msg_id,
                        delta: body.delta,
                    },
                );

                let mut request = self.kv.compare_and_swap::<u64>(
                    GLOBAL_COUNTER_KEY,
                    self.last_read,
                    self.last_read + body.delta,
                    self.last_read == 0,
                );
                request.body.msg_id = Some(self.msg_id);

                request
                    .serialize(output)
                    .context("serializing compare_and_swap request")?;

                self.msg_id += 1;

                Ok(())
            }
            MessageType::Read => {
                self.reads.insert(
                    self.msg_id,
                    ReadRequest {
                        src: message.src,
                        msg_id: message.body.msg_id,
                    },
                );

                let mut request = self.kv.compare_and_swap::<u64>(
                    GLOBAL_COUNTER_KEY,
                    self.last_read,
                    self.last_read,
                    self.last_read == 0,
                );
                request.body.msg_id = Some(self.msg_id);

                request
                    .serialize(output)
                    .context("serializing read request")?;

                self.msg_id += 1;

                Ok(())
            }
        }
    }
}

pub fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<GrowOnlyCounterNode, MessageType>()
}

#[cfg(test)]
mod tests {
    use super::*;
}
