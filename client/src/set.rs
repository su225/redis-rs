use std::time::{Duration, Instant};
use common::RESPFrame;
use crate::client::{RedisClientError, RedisCommand};

pub struct Set;

#[derive(Debug)]
pub enum SetOption {
    Default,
    OnlyIfExists,
    OnlyIfNotExists,
}

#[derive(Debug)]
pub enum TTLOption {
    ExpireAfter(Duration),
    ExpireAt(Instant),
    KeepTTL,
}

#[derive(Debug)]
pub struct SetRequest {
    key: String,
    value: String,
    set_option: SetOption,
    should_return_old_string_at_key: bool,
    ttl_option: TTLOption,
}

#[derive(Debug)]
pub enum SetResponse {
    Ok,
    Response(Option<String>),
}

impl RedisCommand for Set {
    type Request = SetRequest;
    type Response = SetResponse;
}

impl TryInto<RESPFrame> for SetRequest {
    type Error = RedisClientError;

    fn try_into(self) -> Result<RESPFrame, Self::Error> {
        todo!()
    }
}

impl TryFrom<RESPFrame> for SetResponse {
    type Error = RedisClientError;

    fn try_from(value: RESPFrame) -> Result<Self, Self::Error> {
        todo!()
    }
}