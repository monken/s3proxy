use futures_util::TryFutureExt;
use hyper::{http, StatusCode};
use hyper::{Body, Response};
use sha2::{Digest, Sha256};
use std::str::FromStr;
use std::sync::RwLock;
use std::time::SystemTime;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::try_join;
use tokio_util::io::ReaderStream;
use tracing::{info, instrument};

use crate::credentials::{CredentialsError, CredentialsManager};
use crate::xml_writer::ListBucketResult;

pub struct S3Handler {
    // config: Builder,
    credentials: CredentialsManager,
    size_cache: RwLock<std::collections::HashMap<String, i64>>,
    http_client: reqwest::Client,
    endpoint: String,
}

impl S3Handler {
    pub fn new(endpoint: &str) -> Self {
        let client = reqwest::Client::builder().http1_only().build().unwrap();

        let size_cache = std::collections::HashMap::new();
        S3Handler {
            // config: s3config,
            size_cache: RwLock::new(size_cache),
            credentials: CredentialsManager::new(&endpoint),
            http_client: client,
            endpoint: endpoint.to_string(),
        }
    }

    pub(crate) fn handle_sdk_error(e: reqwest::Error) -> Result<Response<Body>, hyper::Error> {
        Ok(Response::builder()
            .status(e.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
            .body(Body::from(""))
            .unwrap())
    }

    pub async fn get_credentials(
        &self,
        token: &str,
    ) -> Result<aws_credential_types::Credentials, CredentialsError> {
        let credentials = self.credentials.get_credentials(&token).await?;
        Ok(aws_credential_types::Credentials::new(
            credentials.access_key_id,
            credentials.secret_access_key,
            Some(credentials.session_token),
            None,
            "PLTR",
        ))
    }

    async fn request(
        &self,
        method: reqwest::Method,
        credentials: &aws_credential_types::Credentials,
        uri: &str,
        headers: Option<Vec<(&str, &str)>>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings};
        use aws_sigv4::sign::v4;
        use http::{HeaderName, HeaderValue};

        let signing_settings = SigningSettings::default();
        let creds = credentials.clone().into();

        let signer = v4::SigningParams::builder()
            .identity(&creds)
            .region("foundry")
            .name("s3")
            .settings(signing_settings)
            .time(SystemTime::now())
            .build()
            .unwrap();
        let signable_request = SignableRequest::new(
            method.as_str(),
            uri,
            headers.clone().unwrap_or_default().into_iter(),
            SignableBody::Bytes(&[]),
        )
        .expect("signable request");
        let signed =
            aws_sigv4::http_request::sign(signable_request, &signer.into()).expect("sign request");
        let (x, _) = signed.into_parts();
        let (signed_headers, _) = x.into_parts();
        let mut request = reqwest::Request::new(method, reqwest::Url::parse(uri).unwrap());
        let request_headers = request.headers_mut();
        for header in headers.clone().unwrap_or_default().into_iter() {
            request_headers.insert(
                HeaderName::from_str(header.0).unwrap(),
                HeaderValue::from_str(header.1).unwrap(),
            );
        }
        for header in signed_headers.into_iter() {
            request_headers.insert(
                header.name(),
                HeaderValue::from_str(header.value()).unwrap(),
            );
        }
        self.http_client.execute(request).await
    }

    #[instrument(skip(self, credentials))]
    pub async fn head_object(
        &self,
        credentials: &aws_credential_types::Credentials,
        bucket: &str,
        key: &str,
    ) -> Result<Response<Body>, hyper::Error> {
        {
            let size_cache = self.size_cache.read().unwrap();
            match size_cache.get(key) {
                Some(size) => {
                    return Ok(Response::builder()
                        .status(200)
                        .header("content-length", size.to_string())
                        .body(Body::from(""))
                        .unwrap())
                }
                None => {}
            }
        }
        let uri = format!("{}{}/{}", self.endpoint, bucket, key,);
        let resp = self
            .request(reqwest::Method::HEAD, credentials, &uri, None)
            .await;
        match resp {
            Ok(obj) => {
                info!("Got object: {:?}", obj.headers());
                let cl = obj
                    .headers()
                    .get("content-length")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<i64>()
                    .unwrap();
                self.size_cache.write().unwrap().insert(key.to_string(), cl);
                Ok(Response::builder()
                    .status(200)
                    .header("content-length", cl)
                    .body(Body::from(""))
                    .unwrap())
            }
            Err(e) => S3Handler::handle_sdk_error(e),
        }
    }

    fn hash_filename(bucket: &str, key: &str, range: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{}/{}/{}", bucket, key, range));
        let result = hasher.finalize();
        format!("{:x}", result)
    }

    #[instrument(skip(self, credentials))]
    pub async fn get_object(
        &self,
        credentials: &aws_credential_types::Credentials,
        bucket: &str,
        key: &str,
        range: Option<&http::HeaderValue>,
    ) -> Result<Response<Body>, hyper::Error> {
        let fname = S3Handler::hash_filename(
            bucket,
            key,
            range.map(|r| r.to_str().unwrap()).unwrap_or_default(),
        );

        if let Ok(f) = tokio::fs::metadata(format!("data/{}", fname)).await {
            let file = File::open(format!("data/{}", fname)).await.unwrap();
            let stream = ReaderStream::with_capacity(file, 16_384);
            let body = Body::wrap_stream(stream);
            return Ok(Response::builder()
                .status(200)
                .header("content-length", f.len())
                .body(body)
                .unwrap());
        }

        let (sender, body) = hyper::Body::channel();

        let uri = format!("{}{}/{}", self.endpoint, bucket, key,);
        let headers = range.map(|r| vec![("range", r.to_str().unwrap())]);
        let resp = match self
            .request(reqwest::Method::GET, credentials, &uri, headers)
            .await
        {
            Ok(resp) => resp,
            Err(e) => return S3Handler::handle_sdk_error(e),
        };

        use futures_util::StreamExt;
        let cl = resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut obj_body = resp.bytes_stream();

        let mut file = File::create(format!("data/.{}", fname)).await.unwrap();
        tokio::spawn(async move {
            let mut sender = sender;
            while let Some(buf) = obj_body.next().await {
                let bytes = buf.unwrap();

                try_join!(
                    sender
                        .send_data(bytes.clone())
                        .map_err(|_| std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "failed to send data"
                        )),
                    file.write(&bytes),
                )
                .unwrap();
            }

            tokio::fs::rename(format!("data/.{}", fname), format!("data/{}", fname))
                .await
                .unwrap();
        });

        Ok(Response::builder()
            .status(200)
            .header("content-length", cl)
            .body(body)
            .unwrap())
    }

    #[instrument(skip(self, credentials))]
    pub async fn list_objects(
        &self,
        credentials: &aws_credential_types::Credentials,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: Option<i32>,
    ) -> Result<Response<Body>, hyper::Error> {
        let uri = format!(
            "{}{}?list-type=2&prefix={}&continuation-token={}&start-after={}&max-keys={}",
            self.endpoint,
            bucket,
            prefix,
            continuation_token.unwrap_or_default(),
            start_after.unwrap_or_default(),
            max_keys.map(|k| k.to_string()).unwrap_or_default(),
        );
        let resp = self
            .request(reqwest::Method::GET, credentials, &uri, None)
            .await;
        if let Err(err) = resp {
            return S3Handler::handle_sdk_error(err);
        }

        let status = resp.as_ref().unwrap().status();
        let body = resp.unwrap().text().await.unwrap();

        if status.is_success() {
            let result = ListBucketResult::from_str(body.as_str()).unwrap();

            let mut size_cache = self.size_cache.write().unwrap();
            result.contents.unwrap_or_default().iter().for_each(|obj| {
                size_cache.insert(obj.key.clone(), obj.size);
            });
        }

        Ok(Response::builder()
            .status(status)
            .header("content-length", body.len())
            .body(Body::from(body))
            .unwrap())
    }
}
