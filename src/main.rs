// use std::net::{TcpListener, TcpStream};

// fn handle_client(_stream: TcpStream) {
// }
mod protocol;

use axum::{routing::get, Router, extract::State};
use std::net::SocketAddr;
use std::sync::Arc; // std::sync::Arc を使用
use tokio::sync::Mutex; // Mutex は Tokio のものを使用
use std::time::Duration;
use clap::Parser; // clap をインポート

// 注意: protocol モジュール内の NoiseResilientProtocol 及び関連する型は、
//       Tokio ベース (async/await, tokio::net::UdpSocket, tokio::time::sleep, tokio::spawn)
//       へ修正されている必要があります。
use protocol::{NoiseResilientProtocol, ConnectionConfig}; // 必要に応じて ConnectionConfig もインポート

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// DB保存処理のプレースホルダー (非同期関数として定義)
async fn save_document_to_db(peer_addr: SocketAddr, data: Vec<u8>) {
    // TODO: ここに実際のDB保存処理を実装する
    // この関数は非同期である必要があるかもしれません (例: DBへの非同期I/O)
    tracing::info!("[ECHELON] Received data from {}: {} bytes. Saving to DB (placeholder)...", peer_addr, data.len());
    // 非同期処理の例 (プレースホルダー)
    // tokio::time::sleep(Duration::from_millis(10)).await;
    // println!("DB save complete for {}", peer_addr);
}

// アプリケーションの状態 (サーバー用)
type ServerAppState = Arc<NoiseResilientProtocol>; // サーバーは Arc<NoiseResilientProtocol> 全体を持つ

// --- コマンドライン引数の定義 ---
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// サーバーモードで起動します
    Server {
        /// サーバーがリッスンするUDPアドレス:ポート
        #[arg(short, long, default_value = "127.0.0.1:12345")]
        bind_addr: String,
        /// Web UIサーバーがリッスンするTCPアドレス:ポート
        #[arg(short, long, default_value = "127.0.0.1:8080")]
        web_addr: String,
    },
    /// クライアントモードで起動し、メッセージを送信します
    Client {
        /// 接続先のサーバーアドレス:ポート
        #[arg(short, long, default_value = "127.0.0.1:12345")]
        server_addr: String,
        /// 送信するメッセージ
        #[arg(short, long, default_value = "Hello, protocol!")]
        message: String,
        /// クライアントがバインドするローカルUDPアドレス:ポート (0で自動割当)
        #[arg(long, default_value = "0.0.0.0:0")]
        local_addr: String,
    },
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // tracing の初期化 (ロギング用)
    tracing_subscriber::registry()
    .with(
      tracing_subscriber::EnvFilter::try_from_default_env()
      .unwrap_or_else(|_| "debug".into()),
    )
    .with(tracing_subscriber::fmt::layer())
    .init();
    // ログレベル設定の参考サイト: https://blog.ojisan.io/rust-tracing/

    // コマンドライン引数をパース
    let args = Args::parse();

    match args.command {
        Commands::Server { bind_addr, web_addr } => {
            run_server(&bind_addr, &web_addr).await?;
        }
        Commands::Client { server_addr, message, local_addr } => {
            run_client(&server_addr, &message, &local_addr).await?;
        }
    }

    Ok(())
}

// --- サーバー実行関数 ---
async fn run_server(udp_bind_addr: &str, web_bind_addr: &str) -> std::io::Result<()> {
    tracing::info!("Starting server mode...");
    tracing::info!("UDP Listening on: {}", udp_bind_addr);
    tracing::info!("Web UI Listening on: {}", web_bind_addr);

    // --- NoiseResilientProtocol の準備 ---
    let config = ConnectionConfig::default();
    let protocol = NoiseResilientProtocol::with_config(udp_bind_addr, config).await?;
    let shared_protocol: ServerAppState = Arc::new(protocol); // Mutex 不要、Protocol内部で管理

    // --- プロトコル処理の非同期タスクを開始 ---
    // Arc をクローンして各タスクに渡す
    let protocol_clone_server = Arc::clone(&shared_protocol);
    tokio::spawn(async move {
        tracing::info!("Starting protocol server task...");
        if let Err(e) = protocol_clone_server.start_server().await {
            tracing::error!("Protocol server task failed: {}", e);
        }
         tracing::info!("Protocol server task finished."); // 通常は終了しない
    });

    let protocol_clone_maintenance = Arc::clone(&shared_protocol);
    tokio::spawn(async move {
        tracing::info!("Starting protocol maintenance task...");
        if let Err(e) = protocol_clone_maintenance.start_maintenance().await {
             tracing::error!("Protocol maintenance task failed: {}", e);
        }
        tracing::info!("Protocol maintenance task finished."); // 通常は終了しない
    });

    let protocol_clone_receiver = Arc::clone(&shared_protocol);
    tokio::spawn(async move {
        tracing::info!("Starting protocol receiver task (for server data)...");
        let callback = move |addr: SocketAddr, data: Vec<u8>| {
            // サーバー側で受信したデータをDBに保存
            tokio::spawn(async move {
                save_document_to_db(addr, data).await;
            });
        };
        if let Err(e) = protocol_clone_receiver.start_receiver(callback).await {
            tracing::error!("Protocol receiver task failed: {}", e);
        }
         tracing::info!("Protocol receiver task finished."); // 通常は終了しない
    });

    // --- Axum Webサーバーの設定 ---
    let app = Router::new()
        .route("/", get(root_handler))
        // .route("/status", get(status_handler))
        // .route("/send", post(send_handler))
        .with_state(Arc::clone(&shared_protocol)); // 状態共有

    // サーバーを起動
    tracing::info!("Starting Web server on http://{}", web_bind_addr);
    let listener = tokio::net::TcpListener::bind(web_bind_addr).await?;
    axum::serve(listener, app).await?;

    // 必要に応じてプロトコルの停止処理を追加
    // shared_protocol.stop().await;

    Ok(())
}

// --- クライアント実行関数 ---
async fn run_client(server_addr_str: &str, message: &str, local_addr: &str) -> std::io::Result<()> {
    tracing::info!("Starting client mode...");
    tracing::info!("Connecting to server: {}", server_addr_str);
    tracing::info!("Binding local UDP to: {}", local_addr);
    tracing::info!("Message to send: {}", message);

    // --- NoiseResilientProtocol の準備 (クライアント用) ---
    // クライアントも自身のConfigを持つことができる
    let client_config = ConnectionConfig {
        // 必要であればクライアント固有の設定を調整
        connection_timeout: Duration::from_secs(10), // クライアントは少し短めにしても良いかも
        ..Default::default()
    };
    // クライアントは任意のポート (例: 0.0.0.0:0) でリッスン開始
    let protocol = NoiseResilientProtocol::with_config(local_addr, client_config).await?;
    // クライアントは Arc<Mutex<_>> で共有する必要はないが、タスクに渡すために Arc 化
    let protocol = Arc::new(protocol);

    // --- クライアント用タスクの開始 ---
    // クライアントもACK受信やタイムアウト処理のためにレシーバーとメンテナンスタスクが必要
    let protocol_clone_maintenance = Arc::clone(&protocol);
    tokio::spawn(async move {
        tracing::info!("Starting client maintenance task...");
        if let Err(e) = protocol_clone_maintenance.start_maintenance().await {
             tracing::error!("Client maintenance task failed: {}", e);
        }
         tracing::info!("Client maintenance task finished.");
    });

    let protocol_clone_receiver = Arc::clone(&protocol);
    tokio::spawn(async move {
        tracing::info!("Starting client receiver task...");
        // クライアント側での受信データ処理 (例: サーバーからの応答など)
        let callback = move |addr: SocketAddr, data: Vec<u8>| {
             tracing::info!("Client received data from {}: {} bytes", addr, data.len());
             // 必要ならここで応答データを処理
        };
        if let Err(e) = protocol_clone_receiver.start_receiver(callback).await {
            tracing::error!("Client receiver task failed: {}", e);
        }
         tracing::info!("Client receiver task finished.");
    });


    // --- 接続試行 ---
    tracing::info!("Attempting to connect to {}...", server_addr_str);
    if let Err(e) = protocol.connect(server_addr_str).await {
        tracing::error!("Failed to initiate connection: {}", e);
        protocol.stop().await; // 他のタスクも停止させる
        return Err(e);
    }

    // --- 接続完了待機 ---
    let connect_timeout = Duration::from_secs(5); // 接続試行のタイムアウト
    let start_time = tokio::time::Instant::now();
    loop {
        match protocol.is_connected(server_addr_str).await {
            Ok(true) => {
                tracing::info!("Successfully connected to {}", server_addr_str);
                break;
            }
            Ok(false) => {
                // 接続中...
            }
            Err(e) => {
                 tracing::error!("Error checking connection status: {}", e);
                 protocol.stop().await;
                 return Err(e);
            }
        }

        if start_time.elapsed() > connect_timeout {
            tracing::error!("Connection attempt timed out.");
             protocol.stop().await;
             return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Connection timed out"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await; // 少し待機
    }

    // --- データ送信 ---
    tracing::info!("Sending message: {}", message);
    if let Err(e) = protocol.send(server_addr_str, message.as_bytes()).await {
         tracing::error!("Failed to send message: {}", e);
         // 送信失敗しても切断は試みる
    } else {
         tracing::info!("Message sent successfully.");
         // ACKの到着を少し待つ (任意)
         tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // --- 切断 ---
    tracing::info!("Disconnecting from {}...", server_addr_str);
    if let Err(e) = protocol.disconnect(server_addr_str).await {
        tracing::error!("Failed to send disconnect message: {}", e);
    } else {
        tracing::info!("Disconnect message sent. Waiting briefly...");
        // 切断処理がある程度進むのを待つ（任意）
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // --- プロトコル停止 ---
    tracing::info!("Stopping client protocol tasks...");
    protocol.stop().await; // メンテナンスタスクなどを停止させる
    tracing::info!("Client finished.");

    Ok(())
}

// ルートハンドラの例
async fn root_handler() -> &'static str {
    "Welcome to the Echelon Server (Axum)!"
}

/* --- 以下、必要に応じて追加するハンドラの例 ---

// プロトコルの状態を取得するハンドラ
async fn status_handler(State(protocol_state): State<ServerAppState>) -> String { // State 型を更新
    // let protocol = protocol_state.lock().await; // Mutexがなくなったのでロック不要
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
    State(protocol_state): State<ServerAppState>, // State 型を更新
    Json(payload): Json<SendRequest>,
) -> Result<String, (axum::http::StatusCode, String)> { // エラーハンドリングを改善
    // let mut protocol = protocol_state.lock().await; // Mutexがなくなったのでロック不要
    match protocol_state.send(&payload.target_addr, payload.message.as_bytes()).await { // protocol_state を直接使用
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