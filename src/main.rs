mod resp_codec;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    loop {
        let (mut socket, remote_address) = listener.accept().await?;
        tokio::spawn(async move {
            println!("accepted connection from {:?}", remote_address);
            let mut buffer = vec![0; 1024];
            match socket.read(&mut buffer).await {
                Ok(0) => return,
                Ok(n) => {
                    println!("read message = {:?}", std::str::from_utf8(&buffer[0..n]));
                    if let Err(e) = socket.write_all(&buffer[0..n]).await {
                        eprintln!("failed to write to socket; err = {:?}", e);
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("failed to read from the socket: err = {:?}", e);
                }
            }
        });
    }
}
