use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

mod router;
mod s3_handler;
mod xml_writer;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));

    let s3 = Arc::new(
        s3_handler::S3Handler::new()
            .await
            .expect("Failed to create S3Handler"),
    );
    let make_svc = make_service_fn(move |_conn| {
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
