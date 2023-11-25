use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use hyper::header::{HeaderMap, HeaderValue};
use quick_xml;
use serde::Deserialize;
use serde_json;

use tracing::{info, instrument};

use reqwest;

#[derive(Debug, Deserialize, Clone)]
struct UserAttributes {
    #[serde(rename = "multipass:organization-rid")]
    organization_rid: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserInfo {
    pub username: String,
    pub id: String,
    attributes: UserAttributes,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expiration: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleWithWebIdentityResult {
    credentials: Credentials,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleWithWebIdentityResponse {
    assume_role_with_web_identity_result: AssumeRoleWithWebIdentityResult,
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CredentialsError {
    #[error("Failed to parse credentials")]
    CredentialsParse(),
    #[error("Token missing")]
    TokenMissing(),
    #[error("Request failed with status code {:?}", .0.status())]
    RequestFailed(#[from] reqwest::Error),
}

impl UserInfo {
    pub async fn from_token(token: String) -> Result<UserInfo, CredentialsError> {
        let client = reqwest::Client::new();
        let mut headers = HeaderMap::new();
        headers.append(
            "Authorization",
            HeaderValue::from_str(format!("Bearer {}", token).as_str()).unwrap(),
        );
        let res = client
            .get("https://ecosystem.athinia.com/multipass/api/me")
            .headers(headers)
            .send()
            .await?;

        if !res.status().is_success() {
            return Err(CredentialsError::RequestFailed(
                res.error_for_status().unwrap_err(),
            ));
        }

        let text = res.text().await?;
        let res: UserInfo = serde_json::from_str(&text).unwrap();
        Ok(res)
    }

    pub fn organization_rid(&self) -> &str {
        &self.attributes.organization_rid[0]
    }
}

impl Credentials {
    #[instrument(skip_all)]
    pub fn token_from_headers(
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<String, CredentialsError> {
        let mut token = headers.get("x-amz-security-token");
        if token.is_none() {
            token = headers.get("authorization");
        }
        let mut token = token
            .ok_or(CredentialsError::TokenMissing())?
            .to_str()
            .map_err(|_| CredentialsError::TokenMissing())?;
        if token.to_ascii_lowercase().starts_with("bearer ") {
            token = token.get(7..).unwrap();
        }
        Ok(token.to_string())
    }

    #[instrument(skip_all)]
    pub async fn from_token(
        endpoint: &str,
        token: String,
    ) -> Result<Credentials, CredentialsError> {
        let client = reqwest::Client::new();
        let res = client
            .post(endpoint)
            .query(&[
                ("Action", "AssumeRoleWithWebIdentity"),
                ("WebIdentityToken", token.as_str()),
            ])
            .send()
            .await?;

        if !res.status().is_success() {
            return Err(CredentialsError::RequestFailed(
                res.error_for_status().unwrap_err(),
            ));
        }

        let text = res.text().await?;
        let res: AssumeRoleWithWebIdentityResponse = quick_xml::de::from_str(&text).unwrap();
        Ok(res.assume_role_with_web_identity_result.credentials)
    }

    pub fn is_expired(&self) -> bool {
        self.expiration < Utc::now()
    }
}

struct CredentialsCacheValue(tokio::sync::watch::Receiver<Option<Credentials>>);

pub struct CredentialsManager {
    endpoint: String,
    cache: RwLock<std::collections::HashMap<blake3::Hash, Arc<CredentialsCacheValue>>>,
}

impl CredentialsManager {
    pub fn new(endpoint: &str) -> Self {
        CredentialsManager {
            endpoint: endpoint.to_string(),
            cache: RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub async fn get_credentials(&self, token: String) -> Result<Credentials, CredentialsError> {
        let hash = blake3::hash(token.as_bytes());
        loop {
            let item = self.cache.read().unwrap().get(&hash).cloned();
            match item {
                None => {
                    info!("Cache miss for token");
                    let (sender, receiver) = tokio::sync::watch::channel(None);
                    self.cache
                        .write()
                        .unwrap()
                        .insert(hash.clone(), Arc::new(CredentialsCacheValue(receiver)));
                    let creds = Credentials::from_token(&self.endpoint, token).await;
                    match creds {
                        Ok(creds) => {
                            sender.send(Some(creds.clone())).unwrap();
                            return Ok(creds);
                        }
                        Err(e) => return Err(e),
                    };
                }
                Some(item) => {
                    let mut receiver = item.0.clone();
                    let creds = receiver.wait_for(|c| c.is_some()).await;
                    match creds {
                        Err(_) => return Err(CredentialsError::CredentialsParse()),
                        Ok(creds) => match creds.clone() {
                            Some(creds) if { creds.is_expired() } => {
                                self.cache.write().unwrap().remove(&hash)
                            }
                            Some(creds) => return Ok(creds),
                            None => panic!("Should not happen"),
                        },
                    };
                }
            }
        }
    }
}

#[cfg(test)]
use tokio::sync::Barrier;

// mod tests {
//     #[tokio::test]
//     async fn test_credentials_manager_concurrent_get_credentials() {
//         use super::*;
//         tracing_subscriber::fmt().init();

//         let token = std::env::var("TOKEN").unwrap();

//         let credentials_manager = Arc::new(CredentialsManager::new());

//         // Create a barrier to synchronize the tasks
//         let barrier = Arc::new(Barrier::new(2));

//         // Spawn two tasks that call `get_credentials` concurrently
//         let task1 = tokio::spawn({
//             let barrier = barrier.clone();
//             let credentials_manager = credentials_manager.clone();
//             let token = token.clone();
//             async move {
//                 barrier.wait().await;
//                 credentials_manager.get_credentials(token).await.unwrap()
//             }
//         });

//         let task2 = tokio::spawn({
//             let barrier = barrier.clone();
//             let credentials_manager = credentials_manager.clone();
//             let token = token.clone();
//             async move {
//                 barrier.wait().await;
//                 credentials_manager.get_credentials(token).await.unwrap()
//             }
//         });

//         // Wait for both tasks to complete
//         let (result1, result2) = tokio::try_join!(task1, task2).unwrap();

//         // Assert that the future was only called once
//         assert_eq!(result1, result2);

//         // Assert that the cache contains only one entry
//         let cache_read = credentials_manager.cache.read().unwrap();
//         assert_eq!(cache_read.len(), 1);
//     }
// }
