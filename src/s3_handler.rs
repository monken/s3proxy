use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::SdkBody;
use aws_sdk_s3::{config::Region, Client, Config, Error};
use bytes::BytesMut;
use futures_util::TryFutureExt;
use http::HeaderValue;
use hyper::http;
use hyper::{Body, Response};
use sha2::{Digest, Sha256};
use std::sync::RwLock;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::try_join;
use tokio_util::io::ReaderStream;
use tracing::{info, instrument};

use crate::credentials::{CredentialsError, CredentialsManager};
use crate::xml_writer::list_bucket_objects_to_xml;

pub struct S3Handler {
    config: Config,
    credentials: CredentialsManager,
    size_cache: RwLock<std::collections::HashMap<String, i64>>,
}

impl S3Handler {
    pub async fn new() -> Result<Self, Error> {
        let s3config = Config::builder()
            .region(Region::new("foundry"))
            .endpoint_url("https://ecosystem.athinia.com/io/s3/")
            .force_path_style(true)
            .build();
        let size_cache = std::collections::HashMap::new();
        Ok(S3Handler {
            config: s3config,
            size_cache: RwLock::new(size_cache),
            credentials: CredentialsManager::new(),
        })
    }

    pub(crate) fn handle_sdk_error<E>(
        e: SdkError<E, http::response::Response<SdkBody>>,
    ) -> Result<Response<Body>, hyper::Error> {
        Ok(Response::builder()
            .status(match e.raw_response() {
                Some(resp) => resp.status(),
                None => http::StatusCode::INTERNAL_SERVER_ERROR,
            })
            .body(Body::from(""))
            .unwrap())
    }

    #[instrument(skip_all)]
    pub async fn get_client(&self, token: &str) -> Result<Client, CredentialsError> {
        let credentials = self.credentials.get_credentials(token.to_string()).await?;
        let aws_credentials = aws_sdk_s3::config::SharedCredentialsProvider::new(
            aws_sdk_s3::config::Credentials::new(
                credentials.access_key_id,
                credentials.secret_access_key,
                Some(credentials.session_token),
                None,
                "PLTR",
            ),
        );
        let client = Client::from_conf(
            self.config
                .to_builder()
                .set_credentials_provider(Some(aws_credentials))
                .clone()
                .build(),
        );
        Ok(client)
    }

    #[instrument(skip(self, client))]
    pub async fn head_object(
        &self,
        client: &Client,
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
        let resp = client.head_object().bucket(bucket).key(key).send().await;
        match resp {
            Ok(obj) => {
                self.size_cache
                    .write()
                    .unwrap()
                    .insert(key.to_string(), obj.content_length);
                Ok(Response::builder()
                    .status(200)
                    .header("content-length", obj.content_length)
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

    #[instrument(skip(self, client))]
    pub async fn get_object(
        &self,
        client: &Client,
        bucket: &str,
        key: &str,
        range: Option<&HeaderValue>,
    ) -> Result<Response<Body>, hyper::Error> {
        let fname = S3Handler::hash_filename(bucket, key, range.unwrap().to_str().unwrap());

        if let Ok(f) = tokio::fs::metadata(format!("data/{}", fname)).await {
            let file = File::open(format!("data/{}", fname)).await.unwrap();
            let stream = ReaderStream::with_capacity(file, 16_384);
            let body = hyper::Body::wrap_stream(stream);
            return Ok(Response::builder()
                .status(200)
                .header("content-length", f.len())
                .body(body)
                .unwrap());
        }

        let (sender, body) = hyper::Body::channel();

        let mut req = client.get_object().bucket(bucket).key(key);
        if range.is_some() {
            req = req.range(range.unwrap().to_str().unwrap());
        }
        let resp = req.send().await;
        if let Err(err) = resp {
            return S3Handler::handle_sdk_error(err);
        }
        let cl = resp.as_ref().unwrap().content_length;
        let mut obj_body = resp.unwrap().body.into_async_read();

        let mut file = File::create(format!("data/.{}", fname)).await.unwrap();
        tokio::spawn(async move {
            let mut sender = sender;
            // let mut obj_body = obj_body;
            loop {
                let mut buf = BytesMut::with_capacity(16_384);
                let n = obj_body.read_buf(&mut buf).await.unwrap();
                let mut buf = buf.freeze();
                info!("read {} bytes", buf.len());
                if n == 0 {
                    break;
                }

                try_join!(
                    sender
                        .send_data(buf.clone())
                        .map_err(|_| std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "failed to send data"
                        )),
                    file.write_all_buf(&mut buf),
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

    #[instrument(skip(self, client))]
    pub async fn list_objects(
        &self,
        client: &Client,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<Response<Body>, hyper::Error> {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if continuation_token.is_some() {
            req = req.continuation_token(continuation_token.unwrap());
        }
        if start_after.is_some() {
            req = req.start_after(start_after.unwrap());
        }
        let res = req.clone().send().await;
        if let Err(err) = res {
            return S3Handler::handle_sdk_error(err);
        }

        {
            let mut size_cache = self.size_cache.write().unwrap();
            res.as_ref().unwrap().contents().iter().for_each(|obj| {
                size_cache.insert(obj.key.clone().unwrap(), obj.size);
            });
        }

        match list_bucket_objects_to_xml(req.as_input().clone().build().unwrap(), res.unwrap()) {
            Ok(xml) => Ok(Response::builder()
                .status(200)
                .header("content-length", xml.len())
                .body(Body::from(xml))
                .unwrap()),
            Err(_) => Ok(Response::builder()
                .status(500)
                .body(Body::from(""))
                .unwrap()),
        }
    }
}
