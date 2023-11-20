use std::convert::Infallible;

use aws_sdk_s3::{
    operation::list_objects_v2::{ListObjectsV2Input, ListObjectsV2Output},
    primitives::DateTimeFormat,
};
use quick_xml::se::to_string;
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Content {
    key: String,
    last_modified: String,
    e_tag: String,
    size: i64,
    storage_class: String,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    name: String,
    prefix: String,
    delimiter: String,
    key_count: i32,
    is_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_after: Option<String>,
    contents: Vec<Content>,
}

pub fn list_bucket_objects_to_xml(
    req: ListObjectsV2Input,
    res: ListObjectsV2Output,
) -> Result<String, Infallible> {
    let result = ListBucketResult {
        name: req.bucket().unwrap_or_default().to_string(),
        prefix: req.prefix().unwrap_or_default().to_string(),
        delimiter: req.delimiter().unwrap_or_default().to_string(),
        key_count: res.key_count(),
        is_truncated: res.is_truncated(),
        continuation_token: req.continuation_token().map(|s| s.to_string()),
        next_continuation_token: res.next_continuation_token().map(|s| s.to_string()),
        start_after: req.start_after().map(|s| s.to_string()),
        contents: res
            .contents()
            .iter()
            .map(|object| Content {
                key: object.key().unwrap_or_default().to_string(),
                last_modified: object
                    .last_modified()
                    .unwrap()
                    .fmt(DateTimeFormat::DateTime)
                    .unwrap(),
                e_tag: object.e_tag().unwrap_or_default().to_string(),
                size: object.size(),
                storage_class: "STANDARD".to_string(),
            })
            .collect(),
    };
    let xml = to_string(&result).unwrap();

    Ok(xml)
}
