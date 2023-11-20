use std::sync::Arc;

use hyper::{header::HeaderValue, Body, Method, Request, Response, StatusCode};
use serde::Deserialize;
use serde_urlencoded;

use tracing::{info, instrument};

use crate::s3_handler::S3Handler;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct SearchParameters {
    list_type: Option<u8>,
    prefix: Option<String>,
    continuation_token: Option<String>,
    start_after: Option<String>,
}

#[instrument(skip_all, fields(http.method = req.method().to_string(), http.path = req.uri().path_and_query().unwrap().to_string()))]
pub async fn route_request(
    req: Request<Body>,
    s3: Arc<S3Handler>,
) -> Result<Response<Body>, hyper::Error> {
    let query = match serde_urlencoded::from_str::<SearchParameters>(
        req.uri().query().or(Some("")).unwrap(),
    ) {
        Ok(q) => q,
        Err(e) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("Failed to parse query string: {}", e)))
                .unwrap());
        }
    };
    let parts: Vec<&str> = req.uri().path().splitn(3, '/').collect();
    let bucket = parts[1];
    let key = req
        .uri()
        .path()
        .get(bucket.len() + 2..)
        .or(Some(""))
        .unwrap();

    // measure the time it takes to handle the request
    let start = std::time::Instant::now();

    let res = match (req.method(), req.uri().path(), query.list_type) {
        (&Method::GET, "/stream", _) => {
            use tokio::fs::File;
            use tokio::io::AsyncWriteExt; // for write_all()

            let (sender, body) = Body::channel();

            // write the current time to sender every second
            tokio::spawn(async move {
                let mut f = File::create("test.txt").await.unwrap();
                let mut sender = sender;
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(1000));
                loop {
                    interval.tick().await;
                    info!("sending data");
                    let chunk = "foobar\n".as_bytes();
                    match sender.send_data(chunk.into()).await {
                        Ok(_) => f.write(chunk).await.unwrap(),
                        Err(e) => {
                            info!("error sending data: {}", e);
                            break;
                        }
                    };
                }
            });

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain")
                .body(body)
                .unwrap())
        }
        (&Method::GET, _, Some(2)) => {
            let prefix = query.prefix.unwrap_or_default();
            let continuation_token = query.continuation_token;
            let start_after = query.start_after;
            s3.list_objects(bucket, &prefix, continuation_token, start_after)
                .await
        }
        (&Method::GET, _, _) => {
            let range = req.headers().get("range");
            s3.get_object(bucket, key, range).await
        }
        (&Method::HEAD, _, _) => s3.head_object(bucket, key).await,
        // Handle other routes and methods accordingly.
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not found.\n"))
            .unwrap()),
    };
    let cl_zero = &HeaderValue::from_static("0");
    let cl = res
        .as_ref()
        .unwrap()
        .headers()
        .get("content-length")
        .unwrap_or(cl_zero);
    let elapsed = start.elapsed();
    info!(
        status = res.as_ref().unwrap().status().as_u16(),
        took_ms = elapsed.as_micros() as f64 / 1000.0,
        content_length = cl.to_str().unwrap(),
    );

    res
}
