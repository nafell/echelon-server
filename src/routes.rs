use axum::{
    routing::{get, post},
    Router,
    extract::State,
    Json,
    http::StatusCode,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::ServerAppState; // main.rs で定義されている ServerAppState をインポート

// プロトコルの状態を取得するハンドラ (main.rs から移動)
async fn status_handler(State(protocol_state): State<ServerAppState>) -> String {
    // TODO: 実際の状態取得ロジックを実装
    // 例: let count = protocol_state.active_connections_count().await;
    // format!("Protocol status: {} active connections", count)
    tracing::debug!("Accessing status endpoint");
    "Protocol status endpoint (implementation pending)".to_string()
}

// HTTP経由でメッセージを送信するハンドラ (main.rs から移動)
#[derive(Deserialize)]
struct SendRequest {
    target_addr: String,
    message: String,
}

async fn send_handler(
    State(protocol_state): State<ServerAppState>,
    Json(payload): Json<SendRequest>,
) -> Result<String, (StatusCode, String)> {
    tracing::debug!("Accessing send endpoint for {}", payload.target_addr);
    match protocol_state.send(&payload.target_addr, payload.message.as_bytes()).await {
        Ok(_) => Ok(format!("Message queued for {}", payload.target_addr)),
        Err(e) => {
            tracing::error!("Failed to send message to {}: {}", payload.target_addr, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to send message: {}", e),
            ))
        }
    }
}

// API ルーターを作成する関数
pub fn create_api_routes() -> Router<ServerAppState> {
    Router::new()
        .route("/status", get(status_handler))
        .route("/send", post(send_handler))
        // 他のAPIエンドポイントをここに追加
}
