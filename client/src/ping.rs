use common::RESPFrame;
use crate::client::{RedisClientError, RedisCommand};

pub struct PingPong;

pub struct PingRequest {
    message: Option<String>,
}

impl PingRequest {
    fn new() -> Self {
        PingRequest { message: None }
    }

    fn with_message(msg: String) -> Self {
        PingRequest { message: Some(msg) }
    }
}

impl TryInto<RESPFrame> for PingRequest {
    type Error = RedisClientError;

    fn try_into(self) -> Result<RESPFrame, Self::Error> {
        let mut frames = Vec::with_capacity(2);
        frames.push(RESPFrame::BulkString("ping".to_string()));
        if let Some(message) = self.message {
            frames.push(RESPFrame::BulkString(message));
        }
        Ok(RESPFrame::Array(frames))
    }
}

pub struct PongResponse {
    message: String,
}

impl TryFrom<RESPFrame> for PongResponse {
    type Error = RedisClientError;

    fn try_from(value: RESPFrame) -> Result<Self, Self::Error> {
        println!("pong response: {:?}", value);
        Ok(PongResponse { message: value.try_into()? })
    }
}

impl RedisCommand for PingPong {
    type Request = PingRequest;
    type Response = PongResponse;
}

#[cfg(test)]
mod ping_pong_test {
    use crate::client::RedisClient;
    use super::*;

    #[tokio::test]
    async fn test_ping_without_message() {
        let address = "127.0.0.1:6379";
        let mut client_res = RedisClient::connect(address.to_string()).await.unwrap();
        let pong_response = client_res.execute::<PingPong>(PingRequest::new()).await.unwrap();
        assert_eq!(pong_response.message.to_lowercase(), "pong");
    }

    #[tokio::test]
    async fn test_ping_with_message() {
        let address = "127.0.0.1:6379";
        let mut client_res = RedisClient::connect(address.to_string()).await.unwrap();
        let pong_response = client_res.execute::<PingPong>(PingRequest::with_message("hello".to_string())).await.unwrap();
        assert_eq!(pong_response.message.to_lowercase(), "hello");
    }
}