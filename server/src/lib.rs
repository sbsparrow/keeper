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
            JsValue::from_str(&body.keeper_id),
            JsValue::from_str(&body.checksum),
            // The alternative is to use `JsValue::from_f64`, except an f64 cannot losslessly hold
            // a u64.
            JsValue::bigint_from_str(&body.size.to_string()),
            match &body.email {
                Some(email) => JsValue::from_str(email),
                None => JsValue::null(),
            },
            match body.email {
                Some(_) => JsValue::from_str("email"),
                None => JsValue::null(),
            },
        ])
        .or(Err(StatusCode::INTERNAL_SERVER_ERROR))?
        .run()
        .await
        .or(Err(StatusCode::INTERNAL_SERVER_ERROR))?;

    Ok(NoContent)
}
