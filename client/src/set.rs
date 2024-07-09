use std::time::{Duration, Instant};

pub enum SetOption {
    Default,
    OnlyIfExists,
    OnlyIfNotExists,
}

pub enum TTLOption {
    ExpireAfter(Duration),
    ExpireAt(Instant),
    KeepTTL,
}

pub struct SetRequest {
    key: String,
    value: String,
    set_option: SetOption,
    should_return_old_string_at_key: bool,
    ttl_option: TTLOption,
}

pub enum SetResponse {
    Ok,
    Response(Option<String>),
}