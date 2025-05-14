use axum::{
    routing::{get, post},
    Router,
    extract::State,
    Json,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
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

#[derive(Serialize)]
struct MeasurementNodeJson {
    equipment_id: String,
    equipment_version: String,
    facility_name: String,
    machine_type: String,
    measurement_date: i64,
    result: String,
}

async fn nodes_handler() -> Json<Vec<MeasurementNodeJson>> {
    // TODO: 実際のデータ取得ロジックを実装
    // 現在はプレースホルダーデータを返す
    let nodes = vec![
        MeasurementNodeJson {
            equipment_id: "ccbb2c60-d3aa-4947-a335-3c87fa9f7805".to_string(),
            equipment_version: "1.0".to_string(),
            facility_name: "大阪工場".to_string(),
            machine_type: "ギアトレイン".to_string(),
            measurement_date: 1747202641,
            result: "ok".to_string(),
        },
        MeasurementNodeJson {
            equipment_id: "fe7263bf-5700-4bc8-a090-18ebb03898d5".to_string(),
            equipment_version: "1.0".to_string(),
            facility_name: "北九州工場".to_string(),
            machine_type: "ギアトレイン".to_string(),
            measurement_date: 1747203242,
            result: "ng".to_string(),
        },
        MeasurementNodeJson {
            equipment_id: "626ee256-e47e-40c3-b721-5ab673d4a320".to_string(),
            equipment_version: "1.0".to_string(),
            facility_name: "北九州工場".to_string(),
            machine_type: "ギアボックス".to_string(),
            measurement_date: 1747203843,
            result: "na".to_string(),
        },
    ];
    Json(nodes)
}

// API ルーターを作成する関数
pub fn create_api_routes() -> Router<ServerAppState> {
    Router::new()
        .route("/status", get(status_handler))
        .route("/send", post(send_handler))
        .route("/nodes", get(nodes_handler))
        // 他のAPIエンドポイントをここに追加
}
