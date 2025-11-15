use serde::Serialize;
use sha2::{Digest, Sha256};
use worker::wasm_bindgen::JsValue;

use crate::api::ArtifactResponse;

// These model types mirror the metadata format used by the backup client; they are not necessarily
// 1:1 with the upstream API response. The shape of these objects must be consistent across both
// this worker and the backup client in order for the backup checksum to match.

#[derive(Debug, Clone, Serialize)]
struct FileMetadata {
    name: String,
    filename: String,
    media_type: Option<String>,
    hash: String,
    hash_algorithm: String,
    url: String,
    lang: Option<String>,
    hidden: bool,
}

#[derive(Debug, Clone, Serialize)]
struct LinkMetadata {
    name: String,
    url: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactMetadata {
    id: String,
    url: String,
    title: String,
    summary: String,
    description: Option<String>,
    files: Vec<FileMetadata>,
    links: Vec<LinkMetadata>,
    people: Vec<String>,
    identities: Vec<String>,
    from_year: u32,
    to_year: Option<u32>,
    decades: Vec<u32>,
    collections: Vec<String>,
}

impl From<crate::api::ArtifactResponse> for ArtifactMetadata {
    fn from(artifact: crate::api::ArtifactResponse) -> Self {
        Self {
            id: artifact.id,
            url: artifact.url,
            title: artifact.title,
            summary: artifact.summary,
            description: artifact.description,
            files: artifact
                .files
                .into_iter()
                .map(|file| FileMetadata {
                    name: file.name,
                    filename: file.filename,
                    media_type: file.media_type,
                    hash: file.hash,
                    hash_algorithm: file.hash_algorithm,
                    url: file.url,
                    lang: file.lang,
                    hidden: file.hidden,
                })
                .collect(),
            links: artifact
                .links
                .into_iter()
                .map(|link| LinkMetadata {
                    name: link.name,
                    url: link.url,
                })
                .collect(),
            people: artifact.people,
            identities: artifact.identities,
            from_year: artifact.from_year,
            to_year: artifact.to_year,
            decades: artifact.decades,
            collections: artifact.collections,
        }
    }
}

fn compute_canonicalized_checksum<T: Serialize>(object: T) -> worker::Result<String> {
    serde_json_canonicalizer::to_vec(&object)
        .map_err(|e| worker::Error::from(JsValue::from_str(&format!("Serialization error: {}", e))))
        .map(|canonicalized_json| {
            let digest = Sha256::digest(&canonicalized_json);
            hex::encode(digest)
        })
}

// We need to compute a checksum of the backup which is deterministic, stable, and agnostic to the
// on-disk backup format.
//
// To accomplish this, we assemble a list of all the artifact metadata in the backup, canonicalize
// it via RFC 8785 (JSON Canonicalization Scheme), and hash the canonicalized JSON representation.
//
// The JCS format ensures that two semantically identical JSON objects will always serialize to the
// same byte sequence, regardless of field ordering, number formatting, whitespace, etc. We sort
// the array of artifact metadata objects lexicographically by artifact ID.
//
// Because the artifact metadata already includes a hash of each file, we only need to hash the
// metadata.
pub fn compute_backup_checksum(api_response: &[ArtifactResponse]) -> worker::Result<String> {
    let mut sorted_metadata = api_response
        .iter()
        .cloned()
        .map(ArtifactMetadata::from)
        .collect::<Vec<_>>();

    sorted_metadata.sort_by_key(|artifact| artifact.id.clone());

    compute_canonicalized_checksum(&sorted_metadata)
}
