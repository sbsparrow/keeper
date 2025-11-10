use std::{fmt, sync::Arc};

use axum::{
    Router,
    extract::{Json, State},
    http::StatusCode,
    response::NoContent,
    routing::post,
};
use serde::Deserialize;
use tower_service::Service;
use worker::{wasm_bindgen::JsValue, *};

const CHECKSUM_BYTES: usize = 32;
const KEEPER_ID_LEN: usize = 6;

struct AppState {
    pub db: D1Database,
}

impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState").finish_non_exhaustive()
    }
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/backups", post(post_backup))
        .with_state(Arc::new(state))
}

#[event(fetch)]
async fn fetch(
    req: HttpRequest,
    env: Env,
    _ctx: Context,
) -> Result<axum::http::Response<axum::body::Body>> {
    let state = AppState { db: env.d1("DB")? };
    Ok(router(state).call(req).await?)
}

#[derive(Debug, Deserialize)]
struct BackupRequest {
    keeper_id: String,
    checksum: String,
    size: u64,
    email: Option<String>,
}

fn is_valid_keeper_id(keeper_id: &str) -> bool {
    if keeper_id.len() != KEEPER_ID_LEN {
        return false;
    }

    keeper_id.parse::<u32>().is_ok()
}

fn is_valid_checksum(checksum: &str) -> bool {
    if checksum.len() != CHECKSUM_BYTES * 2 {
        return false;
    }

    checksum.chars().all(|c| c.is_ascii_hexdigit())
}

#[axum::debug_handler]
#[worker::send]
async fn post_backup(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BackupRequest>,
) -> std::result::Result<NoContent, StatusCode> {
    if !is_valid_keeper_id(&body.keeper_id) {
        return Err(StatusCode::BAD_REQUEST);
    }

    if !is_valid_checksum(&body.checksum) {
        return Err(StatusCode::BAD_REQUEST);
    }

    state
        .db
        .prepare(
            r#"
            INSERT INTO backups (keeper_id, checksum, size, contact, contact_type)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(&[
            body.keeper_id.clone().into(),
            body.checksum.to_ascii_lowercase().into(),
            // An f64 can only losslessly represent integers up to 2^53. In context, that's 8 PiB
            // (pebibytes). We don't need to worry about it.
            (body.size as f64).into(),
            match &body.email {
                Some(email) => email.into(),
                None => JsValue::null(),
            },
            match body.email {
                Some(_) => "email".into(),
                None => JsValue::null(),
            },
        ])
        .or(Err(StatusCode::INTERNAL_SERVER_ERROR))?
        .run()
        .await
        .or(Err(StatusCode::INTERNAL_SERVER_ERROR))?;

    console_log!(
        "Inserted backup record.\nKeeper ID: {}\nChecksum: {}\nSize: {}\nContact: {}",
        body.keeper_id,
        body.checksum,
        body.size,
        body.email.as_deref().unwrap_or_default(),
    );

    Ok(NoContent)
}
