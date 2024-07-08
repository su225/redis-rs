use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::io::Error;
use std::str::FromStr;
use futures::sink::SinkExt;
use futures::StreamExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use common::{DeploymentMode, NodeRole, RESPConversionError, RESPEncodeError, RESPFrame, RESPParseError};
use common::resp_codec::RESPCodec;

pub struct RedisClient {
    pub address: String,
    w: FramedWrite<OwnedWriteHalf, RESPCodec>,
    r: FramedRead<OwnedReadHalf, RESPCodec>,
    s: Option<ServerHelloResponse>,
}

const CLIENT_RESP_VERSION: u8 = 3u8;

#[derive(Debug)]
pub enum RedisClientError {
    IOError(Error),
    Encode(RESPEncodeError),
    Decode(RESPParseError),
    NoResponseReceived,
    RequestValidation(String),
    ResponseValidation(String),
    ResponseFormat(RESPConversionError),
    UnsupportedProtocolVersion(u8),
    InvalidDeploymentMode(String),
    InvalidNodeRole(String),
}

// -- start of RedisCommand machinery --

pub trait RedisCommand {
    type Request: TryInto<RESPFrame, Error=RedisClientError>;
    type Response: TryFrom<RESPFrame, Error=RedisClientError>;
}

// -- end of RedisCommand machinery --

impl From<Error> for RedisClientError {
    fn from(value: Error) -> Self {
        RedisClientError::IOError(value)
    }
}

impl From<RESPEncodeError> for RedisClientError {
    fn from(value: RESPEncodeError) -> Self {
        RedisClientError::Encode(value)
    }
}

impl From<RESPParseError> for RedisClientError {
    fn from(value: RESPParseError) -> Self {
        RedisClientError::Decode(value)
    }
}

impl From<RESPConversionError> for RedisClientError {
    fn from(value: RESPConversionError) -> Self {
        RedisClientError::ResponseFormat(value)
    }
}

impl Display for RedisClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&*self.to_string())
    }
}

impl std::error::Error for RedisClientError {}

// -- messages for the handshake protocol goes here --

pub struct Handshake;

pub struct ClientCredentials {
    pub username: String,
    pub password: String,
}

pub struct ClientSettings {
    pub protocol: u8,
    pub credentials: Option<ClientCredentials>,
    pub client_name: Option<String>,
}

#[derive(Debug)]
pub struct ServerHelloResponse {
    pub protocol: u8,
    pub server: String,
    pub server_version: String,
    pub client_id: i64,
    pub server_mode: DeploymentMode,
    pub server_role: NodeRole,
    pub modules_loaded: Vec<String>,
    pub additional: HashMap<String, RESPFrame>,
}

impl RedisCommand for Handshake {
    type Request = ClientSettings;
    type Response = ServerHelloResponse;
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
                return Err(RedisClientError::RequestValidation("empty username".to_string()));
            }
            if cred.password.is_empty() {
                return Err(RedisClientError::RequestValidation("empty password".to_string()));
            }
        }
        let mut frames = vec![
            RESPFrame::BulkString("HELLO".to_string()),
            RESPFrame::BulkString(format!("{}", self.protocol).to_string()),
        ];
        if let Some(cred) = self.credentials {
            frames.push(RESPFrame::BulkString("AUTH".to_string()));
            frames.push(RESPFrame::BulkString(cred.username));
            frames.push(RESPFrame::BulkString(cred.password));
        }
        if let Some(cname) = self.client_name {
            frames.push(RESPFrame::BulkString("SETNAME".to_string()));
            frames.push(RESPFrame::BulkString(cname));
        }
        Ok(RESPFrame::Array(frames))
    }
}

impl TryFrom<RESPFrame> for ServerHelloResponse {
    type Error = RedisClientError;

    fn try_from(response_frame: RESPFrame) -> Result<Self, Self::Error> {
        match response_frame {
            RESPFrame::Map(kv_pairs) => Self::parse_server_info(kv_pairs),
            _ => Err(RedisClientError::ResponseValidation("expected map".to_string())),
        }
    }
}

impl ServerHelloResponse {
    fn parse_server_info(kv_pairs: Vec<(RESPFrame, RESPFrame)>) -> Result<ServerHelloResponse, RedisClientError> {
        let mut resp = ServerHelloResponse {
            protocol: 0,
            server: "".to_string(),
            server_version: "".to_string(),
            client_id: 0,
            server_mode: DeploymentMode::Standalone,
            server_role: NodeRole::Master,
            modules_loaded: vec![],
            additional: HashMap::new(),
        };
        for (k, v) in kv_pairs.into_iter() {
            let key: String = k.try_into()?;
            match key.as_str() {
                "server" => { resp.server = v.try_into()?; },
                "version" => { resp.server_version = v.try_into()?; },
                "proto" => {
                    let pv: i64 = v.try_into()?;
                    let proto_version = pv as u8;
                    if proto_version < CLIENT_RESP_VERSION {
                        return Err(RedisClientError::UnsupportedProtocolVersion(proto_version));
                    }
                    resp.protocol = proto_version;
                },
                "id" => { resp.client_id = v.try_into()?; },
                "mode" => {
                    let val: String = v.try_into()?;
                    let mode_res = DeploymentMode::from_str(val.as_str());
                    resp.server_mode = mode_res.map_err(|_| RedisClientError::InvalidDeploymentMode(val))?;
                },
                "role" => {
                    let val: String = v.try_into()?;
                    let role_res = NodeRole::from_str(val.as_str());
                    resp.server_role = role_res.map_err(|_| RedisClientError::InvalidNodeRole(val))?;
                }
                "modules" => {
                    if let RESPFrame::Array(modules) = v {
                        let mut modules_loaded = Vec::with_capacity(modules.len());
                        for md in modules.into_iter() {
                            let module_name: String = md.try_into()?;
                            modules_loaded.push(module_name);
                        }
                        resp.modules_loaded = modules_loaded;
                    } else {
                        return Err(RedisClientError::ResponseValidation("modules should return array of strings".to_string()));
                    }
                }
                _ => { resp.additional.insert(key, v); },
            }
        }
        Ok(resp)
    }
}

// -- end of messages/commands for handshake client --

impl RedisClient {
    pub async fn connect(addr: String) -> Result<RedisClient, RedisClientError> {
        let stream = TcpStream::connect(&addr).await?;
        let (reader, writer) = stream.into_split();
        let resp_framed_writer = FramedWrite::new(writer, RESPCodec::new());
        let resp_framed_reader = FramedRead::new(reader, RESPCodec::new());
        
        let mut client = RedisClient {
            address: addr,
            w: resp_framed_writer,
            r: resp_framed_reader,
            s: None,
        };
        let client_settings = ClientSettings::new();
        let server_resp = client.execute::<Handshake>(client_settings).await?;
        client.s = Some(server_resp);
        Ok(client)
    }

    pub async fn execute<C: RedisCommand>(&mut self, req: C::Request) -> Result<C::Response, RedisClientError> {
        self.w.send(req.try_into()?).await?;
        if let Some(resp) = self.r.next().await {
            let resp_frame = resp.map_err(|e| RedisClientError::Decode(e))?;
            C::Response::try_from(resp_frame)
        } else {
            Err(RedisClientError::NoResponseReceived)
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