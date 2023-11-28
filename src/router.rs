use std::sync::Arc;

use hyper::{header::HeaderValue, Body, Method, Request, Response, StatusCode};
use serde::Deserialize;
use serde_urlencoded;

use tracing::{info, instrument};

use crate::credentials::Credentials;
use crate::s3_handler::S3Handler;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct SearchParameters {
    list_type: Option<u8>,
    prefix: Option<String>,
    continuation_token: Option<String>,
    start_after: Option<String>,
    max_keys: Option<i32>,
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

    let token = match Credentials::token_from_headers(req.headers()) {
        Ok(t) => t,
        Err(e) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("{}", e)))
                .unwrap());
        }
    };

    let credentials = match s3.get_credentials(&token).await {
        Ok(c) => c,
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(Body::from("Unauthorized\n"))
                .unwrap());
        }
    };

    let res = match (req.method(), req.uri().path(), query.list_type) {
        (&Method::GET, _, Some(2)) => {
            let prefix = query.prefix.unwrap_or_default();
            s3.list_objects(
                &credentials,
                bucket,
                &prefix,
                query.continuation_token,
                query.start_after,
                query.max_keys,
            )
            .await
        }
        (&Method::GET, _, _) => {
            let range: Option<&HeaderValue> = req.headers().get("range");
            s3.get_object(&credentials, bucket, key, range).await
        }
        (&Method::HEAD, _, _) => s3.head_object(&credentials, bucket, key).await,
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
