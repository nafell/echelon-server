// use std::net::{TcpListener, TcpStream};

// fn handle_client(_stream: TcpStream) {
// }
mod protocol;

use axum::{routing::get, Router, extract::State};
use std::net::SocketAddr;
use std::sync::Arc; // std::sync::Arc を使用
use tokio::sync::Mutex; // Mutex は Tokio のものを使用
use std::time::Duration;

// 注意: protocol モジュール内の NoiseResilientProtocol 及び関連する型は、
//       Tokio ベース (async/await, tokio::net::UdpSocket, tokio::time::sleep, tokio::spawn)
//       へ修正されている必要があります。
use protocol::{NoiseResilientProtocol, ConnectionConfig}; // 必要に応じて ConnectionConfig もインポート

// DB保存処理のプレースホルダー (非同期関数として定義)
async fn save_document_to_db(peer_addr: SocketAddr, data: Vec<u8>) {
    // TODO: ここに実際のDB保存処理を実装する
    // この関数は非同期である必要があるかもしれません (例: DBへの非同期I/O)
    tracing::info!("[ECHELON] Received data from {}: {} bytes. Saving to DB (placeholder)...", peer_addr, data.len());
    // 非同期処理の例 (プレースホルダー)
    // tokio::time::sleep(Duration::from_millis(10)).await;
    // println!("DB save complete for {}", peer_addr);
}

// アプリケーションの状態
// NoiseResilientProtocol を Tokio の Mutex で保護し、Arc で共有
type AppState = Arc<Mutex<NoiseResilientProtocol>>;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // tracing の初期化 (ロギング用)
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let udp_bind_addr = "127.0.0.1:12345"; // UDPサーバーのアドレス
    let web_bind_addr = "127.0.0.1:8080"; // Webサーバーのアドレス

    // --- NoiseResilientProtocol の準備 ---
    // 注意: このコードが正しく動作するには、`protocol.rs` 内の NoiseResilientProtocol が
    //       Tokio の非同期機能 (`tokio::net::UdpSocket`, `tokio::time::sleep`, `tokio::spawn`, `async fn`)
    //       を使用するように大幅に修正されている必要があります。
    //       以下の `new`, `start_server`, `start_maintenance`, `start_receiver` は
    //       非同期に対応したシグネチャを持つ想定です。

    // 設定を作成 (例)
    let config = ConnectionConfig::default(); // 必要に応じて設定をカスタマイズ

    // NoiseResilientProtocol のインスタンスを作成 (修正後の `new` を想定)
    let protocol = NoiseResilientProtocol::with_config(udp_bind_addr, config).await?; // `new` が async になっている場合

    // Arc<Mutex<...>> でラップして共有可能にする
    let shared_protocol: AppState = Arc::new(Mutex::new(protocol));

    // --- プロトコル処理の非同期タスクを開始 ---

    // サーバー処理タスク
    let protocol_clone_server = Arc::clone(&shared_protocol);
    tokio::spawn(async move {
        tracing::info!("Starting protocol server task...");
        // ロックを取得してサーバーを開始 (start_server は async fn である想定)
        let mut protocol_guard = protocol_clone_server.lock().await;
        if let Err(e) = protocol_guard.start_server().await { // `start_server` が async fn になっている想定
            tracing::error!("Failed to start protocol server: {}", e);
        }
        // ロックを解放
        drop(protocol_guard);
        tracing::info!("Protocol server task finished/exited."); // 通常はループするためここには来ないはず
    });

    // メンテナンス処理タスク
    let protocol_clone_maintenance = Arc::clone(&shared_protocol);
    tokio::spawn(async move {
        tracing::info!("Starting protocol maintenance task...");
        // ロックを取得してメンテナンスを開始 (start_maintenance は async fn である想定)
        let mut protocol_guard = protocol_clone_maintenance.lock().await;
        if let Err(e) = protocol_guard.start_maintenance().await { // `start_maintenance` が async fn になっている想定
             tracing::error!("Failed to start protocol maintenance: {}", e);
        }
         // ロックを解放
        drop(protocol_guard);
        tracing::info!("Protocol maintenance task finished/exited."); // 通常はループするためここには来ないはず
    });

    // データ受信処理タスク
    let protocol_clone_receiver = Arc::clone(&shared_protocol);
    tokio::spawn(async move {
        tracing::info!("Starting protocol receiver task...");
        // ロックを取得して受信ループを開始 (start_receiver は async fn で、非同期コールバックを受け取る想定)
        let mut protocol_guard = protocol_clone_receiver.lock().await;

        // 非同期コールバックを定義
        // FnMut を使う場合、コールバック内で非同期処理を行うために `async move` ブロックと `tokio::spawn` が必要になることが多い
        let callback = move |addr: SocketAddr, data: Vec<u8>| {
            // save_document_to_db は async なので、新しいタスクで実行
            tokio::spawn(async move {
                save_document_to_db(addr, data).await;
            });
        };

        // start_receiver を呼び出す (非同期コールバックを渡す想定)
        if let Err(e) = protocol_guard.start_receiver(callback).await { // `start_receiver` が async fn になっている想定
            tracing::error!("Failed to start protocol receiver: {}", e);
        }
         // ロックを解放
        drop(protocol_guard);
        tracing::info!("Protocol receiver task finished/exited."); // 通常はループするためここには来ないはず
    });

    // --- Axum Webサーバーの設定 ---
    let app = Router::new()
        .route("/", get(root_handler))
        // .route("/status", get(status_handler)) // 例: プロトコルの状態を表示するエンドポイント
        // .route("/send", post(send_handler))   // 例: デバイスにメッセージを送るエンドポイント
        .with_state(Arc::clone(&shared_protocol)); // プロトコルの状態を共有 (Arc をクローンして渡す)

    // サーバーを起動
    tracing::info!("Web server listening on http://{}", web_bind_addr);
    let listener = tokio::net::TcpListener::bind(web_bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ルートハンドラの例
async fn root_handler() -> &'static str {
    "Welcome to the Echelon Server (Axum)!"
}

/* --- 以下、必要に応じて追加するハンドラの例 ---

// プロトコルの状態を取得するハンドラ
async fn status_handler(State(protocol_state): State<AppState>) -> String {
    let protocol = protocol_state.lock().await;
    // protocol から接続状態などを取得して返す (例)
    // format!("Protocol status: {} active connections", protocol.active_connections_count())
    "Protocol status endpoint (implementation pending)".to_string()
}

// HTTP経由でメッセージを送信するハンドラ
use axum::Json;
use serde::Deserialize;

#[derive(Deserialize)]
struct SendRequest {
    target_addr: String,
    message: String,
}

async fn send_handler(
    State(protocol_state): State<AppState>,
    Json(payload): Json<SendRequest>,
) -> Result<String, (axum::http::StatusCode, String)> { // エラーハンドリングを改善
    let mut protocol = protocol_state.lock().await;
    // send メソッドも async になっている想定
    match protocol.send(&payload.target_addr, payload.message.as_bytes()).await {
        Ok(_) => Ok(format!("Message queued for {}", payload.target_addr)),
        Err(e) => {
            tracing::error!("Failed to send message to {}: {}", payload.target_addr, e);
            Err((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to send message: {}", e),
            ))
        }
    }
}

*/