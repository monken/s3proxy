use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;
use clap::Parser;

mod router;
mod s3_handler;
mod xml_writer;
mod credentials;

use crate::s3_handler::S3Handler;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The endpoint to use for S3 requests
    #[arg(long, short, env)]
    endpoint: String,
    #[arg(long, short, default_value="3000", env)]
    port: u16,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("s3proxy=info,aws_smithy_runtime=error").init();

    let args = Args::parse();
    info!("{:?}", args);

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));

    let s3 = Arc::new(
        S3Handler::new(&args.endpoint)
            .await
            .expect("Failed to create S3Handler"),
    );
    let make_svc = make_service_fn(|_conn| {
        let s3 = s3.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                router::route_request(req, s3.clone())
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    info!("Server running on port 3000");
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}
