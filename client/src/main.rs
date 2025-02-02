use clap::Parser;

mod client;
mod ping;
mod echo;
mod set;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None, disable_help_flag = true)]
struct RedisClientArgs {
    #[arg(short, long)]
    host: String,

    #[arg(short, long, default_value_t = 6379)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = RedisClientArgs::parse();
    let redis_addr = format!("{}:{}", args.host, args.port);
    let redis_client = client::RedisClient::connect(redis_addr).await?;
    Ok(())
}
