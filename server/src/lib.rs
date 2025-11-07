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

#[axum::debug_handler]
#[worker::send]
async fn post_backup(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BackupRequest>,
) -> std::result::Result<NoContent, StatusCode> {
    state
        .db
        .prepare(
            r#"
            INSERT INTO backups (keeper_id, checksum, size, contact, contact_type)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(&[
            body.keeper_id.into(),
            body.checksum.into(),
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

    Ok(NoContent)
}
