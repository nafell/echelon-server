use axum::{
    extract::State, http::StatusCode, routing::{get, post}, Error, Json, Router
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use influxdb::{Client, InfluxDbWriteable, ReadQuery, Timestamp};
use crate::{model::WearReading, model::wear_string, model::calc_wear, ServerAppState}; // main.rs で定義されている ServerAppState をインポート

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
    measurement_date: String,
    result: String,
}

async fn nodes_handler() -> Result<Json<Vec<MeasurementNodeJson>>, StatusCode> {
    // TODO: 実際のデータ取得ロジックを実装
    // 現在はプレースホルダーデータを返す
    let client = Client::new("http://localhost:8086", "test");

    let read_query = ReadQuery::new("SELECT * FROM \"PI1000-A001\" ORDER BY time DESC LIMIT 1");

    let read_result = client.json_query(read_query).await.and_then(|mut res| res.deserialize_next::<WearReading>());
    
    match read_result {
        Ok(read_result) => {
            let series = read_result.series.into_iter();
            for sery in series {
                let values = sery.values;
                for value in values {
                    let wear_result = calc_wear(&value);
                    return Ok(Json(
                        vec![
                            MeasurementNodeJson {
                                equipment_id: value.equipment_id,
                                equipment_version: value.equipment_version,
                                facility_name: value.facility_name,
                                machine_type: value.machine_type,
                                measurement_date: value.time.to_string(),
                                result: wear_string(wear_result),
                            }
                        ]
                    ));
                }
            }
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        Err(e) => {
            tracing::error!("Failed to read data from DB: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // let nodes = vec![
    //     MeasurementNodeJson {
    //         equipment_id: "ccbb2c60-d3aa-4947-a335-3c87fa9f7805".to_string(),
    //         equipment_version: "1.0".to_string(),
    //         facility_name: "大阪工場".to_string(),
    //         machine_type: "ギアトレイン".to_string(),
    //         measurement_date: 1747202641,
    //         result: "ok".to_string(),
    //     },
    //     MeasurementNodeJson {
    //         equipment_id: "fe7263bf-5700-4bc8-a090-18ebb03898d5".to_string(),
    //         equipment_version: "1.0".to_string(),
    //         facility_name: "北九州工場".to_string(),
    //         machine_type: "ギアトレイン".to_string(),
    //         measurement_date: 1747203242,
    //         result: "ng".to_string(),
    //     },
    //     MeasurementNodeJson {
    //         equipment_id: "626ee256-e47e-40c3-b721-5ab673d4a320".to_string(),
    //         equipment_version: "1.0".to_string(),
    //         facility_name: "北九州工場".to_string(),
    //         machine_type: "ギアボックス".to_string(),
    //         measurement_date: 1747203843,
    //         result: "na".to_string(),
    //     },
    // ];
    // Json(nodes)
}

// API ルーターを作成する関数
pub fn create_api_routes() -> Router<ServerAppState> {
    Router::new()
        .route("/status", get(status_handler))
        .route("/send", post(send_handler))
        .route("/nodes", get(nodes_handler))
        // 他のAPIエンドポイントをここに追加
}


