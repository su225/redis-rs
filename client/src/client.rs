use std::fmt::{Debug, Display, Formatter};
use std::io::Error;

use futures::sink::SinkExt;
use futures::StreamExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use common::{RESPEncodeError, RESPFrame};
use common::resp_codec::RESPCodec;
use common::RESPFrame::BulkString;

pub struct RedisClient {
    pub address: String,
    w: FramedWrite<OwnedWriteHalf, RESPCodec>,
    r: FramedRead<OwnedReadHalf, RESPCodec>,
}

const CLIENT_RESP_VERSION: &'static str = "3";

#[derive(Debug)]
pub enum RedisClientError {
    Inner(String),
    RequestConversionError(String),
    RequestEncodeError(String),
    ResponseError(String),
    ResponseDecodeError(String),
}

impl From<Error> for RedisClientError {
    fn from(value: Error) -> Self {
        RedisClientError::Inner(value.to_string())
    }
}

impl From<RESPEncodeError> for RedisClientError {
    fn from(value: RESPEncodeError) -> Self {
        RedisClientError::RequestEncodeError(format!("{value:?}"))
    }
}

impl Display for RedisClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&*self.to_string())
    }
}

impl std::error::Error for RedisClientError {}

trait RedisCommand {
    type Request: TryInto<RESPFrame, Error = RedisClientError>;
    type Response: TryFrom<RESPFrame, Error = RedisClientError>;
}



pub struct Handshake;

impl RedisCommand for Handshake {
    type Request = ClientSettings;
    type Response = ServerHelloResponse;
}

pub struct ClientCredentials {
    pub username: String,
    pub password: String,
}

pub struct ClientSettings {
    pub protocol: u8,
    pub credentials: Option<ClientCredentials>,
    pub client_name: Option<String>,
}

impl ClientSettings {
    fn new() -> ClientSettings {
        ClientSettings {
            protocol: 3,
            credentials: None,
            client_name: None,
        }
    }

    fn with_credentials(cred: ClientCredentials) -> ClientSettings {
        ClientSettings {
            protocol: 3,
            credentials: Some(cred),
            client_name: None,
        }
    }
}

impl TryInto<RESPFrame> for ClientSettings {
    type Error = RedisClientError;

    fn try_into(mut self) -> Result<RESPFrame, Self::Error> {
        if self.protocol == 0 {
            self.protocol = 3u8;
        }
        if let Some(ref cred) = self.credentials {
            if cred.username.is_empty() {
                return Err(RedisClientError::RequestConversionError("empty username".to_string()));
            }
            if cred.password.is_empty() {
                return Err(RedisClientError::RequestConversionError("empty password".to_string()));
            }
        }
        let mut frames = vec![
            BulkString("HELLO".to_string()),
            BulkString(format!("{}", self.protocol).to_string()),
        ];
        if let Some(cred) = self.credentials {
            frames.push(BulkString("AUTH".to_string()));
            frames.push(BulkString(cred.username));
            frames.push(BulkString(cred.password));
        }
        if let Some(cname) = self.client_name {
            frames.push(BulkString("SETNAME".to_string()));
            frames.push(BulkString(cname));
        }
        Ok(RESPFrame::Array(frames))
    }
}

pub struct ServerHelloResponse {
    pub protocol: u8,
    pub server: u8,
    pub server_version: String,
    pub client_id: i64,
    pub server_mode: String,
    pub server_role: String,
    pub modules_loaded: Vec<String>,
}

impl TryFrom<RESPFrame> for ServerHelloResponse {
    type Error = RedisClientError;

    fn try_from(response_frame: RESPFrame) -> Result<Self, Self::Error> {
        match response_frame {
            RESPFrame::Map(kv_pairs) => {
                let mut resp = ServerHelloResponse {
                    protocol: 0,
                    server: 0,
                    server_version: "".to_string(),
                    client_id: 0,
                    server_mode: "".to_string(),
                    server_role: "".to_string(),
                    modules_loaded: vec![],
                };
                for (k, v) in kv_pairs.into_iter() {
                    todo!() // implement key value parsing
                }
                Ok(resp)
            },
            _ => RedisClientError::ResponseDecodeError("expected map".to_string()).into(),
        }
    }
}


impl RedisClient {
    pub async fn connect(addr: String) -> Result<RedisClient, RedisClientError> {
        let stream = TcpStream::connect(&addr).await?;
        let (reader, writer) = stream.into_split();
        let mut resp_framed_writer = FramedWrite::new(writer, RESPCodec::new());
        let mut resp_framed_reader = FramedRead::new(reader, RESPCodec::new());
        Ok(RedisClient {
            address: addr,
            w: resp_framed_writer,
            r: resp_framed_reader,
        })
    }

    pub async fn execute<C: RedisCommand>(&mut self, req: C::Request) -> Result<C::Response, Box<dyn std::error::Error>> {
        self.w.send(req.try_into()?).await?;
        if let Some(resp) = self.r.next().await {
            match resp {
                Ok(resp_frames) => {
                    let resp_conv_result = resp_frames.try_into();
                    resp_conv_result.map_err(|e| RedisClientError::ResponseDecodeError(format!("{e:?}")).into())
                },
                Err(e) => Err(RedisClientError::ResponseError(format!("response error: {e:?}")).into())
            }
        } else {
            Err(RedisClientError::Inner("no response".to_string()).into())
        }
    }
}

#[cfg(test)]
mod client {
    use super::*;

    #[tokio::test]
    async fn test_redis_client_connection_hello() {
        let address = "127.0.0.1:6379";
        let client_res = RedisClient::connect(address.to_string()).await;
        assert!(client_res.is_ok())
    }
}