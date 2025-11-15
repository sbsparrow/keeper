use serde::Deserialize;
use worker::{Fetch, Url, console_log};

const API_URL: &str = "https://api.acearchive.lgbt/v0";
const ARTIFACTS_PAGE_SIZE: usize = 50;

#[derive(Debug, Clone, Deserialize)]
pub struct FileResponse {
    pub name: String,
    pub filename: String,
    pub media_type: Option<String>,
    pub hash: String,
    pub hash_algorithm: String,
    pub url: String,
    pub lang: Option<String>,
    pub hidden: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LinkResponse {
    pub name: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArtifactResponse {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub description: Option<String>,
    pub url: String,
    pub files: Vec<FileResponse>,
    pub links: Vec<LinkResponse>,
    pub people: Vec<String>,
    pub identities: Vec<String>,
    pub from_year: u32,
    pub to_year: Option<u32>,
    pub decades: Vec<u32>,
    pub collections: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ArtifactsResponse {
    items: Vec<ArtifactResponse>,
    next_cursor: Option<String>,
}

async fn fetch_artifacts_page(cursor: Option<&str>) -> worker::Result<ArtifactsResponse> {
    let mut params = vec![("limit", ARTIFACTS_PAGE_SIZE.to_string())];

    if let Some(cursor) = cursor {
        params.push(("cursor", cursor.to_string()));
    }

    let url = Url::parse_with_params(&format!("{API_URL}/artifacts"), &params)?;

    Fetch::Url(url)
        .send()
        .await?
        .json::<ArtifactsResponse>()
        .await
}

pub async fn fetch_all_artifacts() -> worker::Result<Vec<ArtifactResponse>> {
    let mut all_artifacts = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let response = fetch_artifacts_page(cursor.as_deref()).await?;
        all_artifacts.extend(response.items);

        if let Some(next_cursor) = response.next_cursor {
            cursor = Some(next_cursor);
        } else {
            break;
        }
    }

    console_log!("Fetched {} artifacts from upstream.", all_artifacts.len());

    Ok(all_artifacts)
}
