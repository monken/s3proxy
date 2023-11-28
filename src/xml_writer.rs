use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Content {
    pub key: String,
    pub last_modified: String,
    pub e_tag: String,
    pub size: i64,
    pub storage_class: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListBucketResult {
    pub name: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub key_count: i32,
    pub is_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_after: Option<String>,
    pub contents: Option<Vec<Content>>,
}

impl ListBucketResult {
    pub fn from_str(s: &str) -> Result<ListBucketResult, quick_xml::de::DeError> {
        quick_xml::de::from_str(s)
    }
}
