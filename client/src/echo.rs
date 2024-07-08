use common::RESPFrame;

use crate::client::{RedisClientError, RedisCommand};

pub struct Echo;

pub struct EchoRequest {
    message: String,
}

impl EchoRequest {
    fn new(msg: String) -> EchoRequest {
        EchoRequest { message: msg }
    }
}

impl TryInto<RESPFrame> for EchoRequest {
    type Error = RedisClientError;

    fn try_into(self) -> Result<RESPFrame, Self::Error> {
        Ok(RESPFrame::Array(vec![
            RESPFrame::BulkString("echo".to_string()),
            RESPFrame::BulkString(self.message),
        ]))
    }
}

pub struct EchoResponse {
    message: String,
}

impl TryFrom<RESPFrame> for EchoResponse {
    type Error = RedisClientError;

    fn try_from(value: RESPFrame) -> Result<Self, Self::Error> {
        Ok(EchoResponse { message: value.try_into()? })
    }
}

impl RedisCommand for Echo {
    type Request = EchoRequest;
    type Response = EchoResponse;
}

#[cfg(test)]
mod echo_test {
    use crate::client::RedisClient;
    use crate::echo::{Echo, EchoRequest};

    #[tokio::test]
    async fn test_echo() {
        let address = "127.0.0.1:6379";
        let mut client_res = RedisClient::connect(address.to_string()).await.unwrap();

        let pong_response = client_res.execute::<Echo>(EchoRequest::new("helloWorld".to_string())).await.unwrap();
        assert_eq!(pong_response.message, "helloWorld");

        let pong_response = client_res.execute::<Echo>(EchoRequest::new("byebyeWorld".to_string())).await.unwrap();
        assert_eq!(pong_response.message, "byebyeWorld");
    }
}