use futures::sink::SinkExt;
use futures::StreamExt;
use tokio::io;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use common::resp_codec::{RESPCodec, RESPFrame};

pub struct RedisClient {
    pub address: String,
}

const CLIENT_RESP_VERSION: &'static str = "3";

impl RedisClient {
    pub fn new(addr: String) -> RedisClient {
        RedisClient { address: addr }
    }

    pub async fn execute(&self) -> io::Result<()> {
        let stream = TcpStream::connect(&self.address).await?;
        let (reader, writer) = stream.into_split();
        let mut resp_framed_writer = FramedWrite::new(writer, RESPCodec::new());
        let mut resp_framed_reader = FramedRead::new(reader, RESPCodec::new());

        resp_framed_writer.send(RESPFrame::Array(vec![
           RESPFrame::BulkString("hello".to_string()),
           RESPFrame::BulkString(CLIENT_RESP_VERSION.to_string()),
        ])).await?;
        if let Some(resp) = resp_framed_reader.next().await {
            match resp {
                Ok(response_frames) => {
                    println!("received response from the server: {:?}", response_frames);
                }
                Err(err) => {
                    eprintln!("received error from the server: {:?}", err);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod client_connection {

}