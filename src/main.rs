// use std::net::{TcpListener, TcpStream};

// fn handle_client(_stream: TcpStream) {
// }
mod protocol;
mod routes;
mod ftp_client;

use axum::{routing::get, Router, extract::State};
use std::net::SocketAddr;
use std::sync::Arc; // std::sync::Arc を使用
use tokio::sync::Mutex; // Mutex は Tokio のものを使用
use std::time::Duration;
use clap::Parser; // clap をインポート
use anyhow::Context; // Using anyhow for error handling consistency
use std::path::PathBuf;
use crate::ftp_client::{FtpReader, FtpReaderConfig}; // Import FtpReader and its config

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
async fn main() -> anyhow::Result<()> {
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
            run_server(&bind_addr, &web_addr).await
                .context("Failed to run server")?;
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
    let api_routes = routes::create_api_routes();

    let app = Router::new()
        .route("/", get(root_handler))
        .merge(api_routes)
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
async fn run_client(server_addr_str: &str, _message: &str, local_addr: &str) -> anyhow::Result<()> {
    tracing::info!("Starting client mode...");
    tracing::info!("Connecting to Echelon server: {}", server_addr_str);
    tracing::info!("Binding local UDP to: {}", local_addr);
    // tracing::info!("Original message (will be ignored): {}", message); // Message is now from FTP

    // --- NoiseResilientProtocol の準備 (クライアント用) ---
    let client_config = ConnectionConfig {
        connection_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let protocol = NoiseResilientProtocol::with_config(local_addr, client_config).await
        .context("Failed to initialize protocol")?; // Use context for error
    let protocol = Arc::new(protocol);

    // --- クライアント用タスクの開始 (Protocol Maintenance & Receiver) ---
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
        let callback = move |addr: SocketAddr, data: Vec<u8>| {
            tracing::info!("Client received data from {}: {} bytes", addr, data.len());
            // Handle potential responses from the server if needed
        };
        if let Err(e) = protocol_clone_receiver.start_receiver(callback).await {
            tracing::error!("Client receiver task failed: {}", e);
        }
        tracing::info!("Client receiver task finished.");
    });

    // --- 接続試行 (To Echelon Server) ---
    tracing::info!("Attempting to connect Echelon server {}...", server_addr_str);
    protocol.connect(server_addr_str).await
        .with_context(|| format!("Failed to initiate connection to Echelon server {}", server_addr_str))?;

    // --- 接続完了待機 (To Echelon Server) ---
    let connect_timeout = Duration::from_secs(15); // Increased timeout slightly
    let start_time = tokio::time::Instant::now();
    loop {
        match protocol.is_connected(server_addr_str).await {
            Ok(true) => {
                tracing::info!("Successfully connected to Echelon server {}", server_addr_str);
                break;
            }
            Ok(false) => {
                // Still connecting...
            }
            Err(e) => {
                tracing::error!("Error checking Echelon server connection status: {}", e);
                protocol.stop().await; // Stop protocol tasks
                return Err(e).context("Failed while checking Echelon server connection");
            }
        }

        if start_time.elapsed() > connect_timeout {
            tracing::error!("Echelon server connection attempt timed out.");
            protocol.stop().await; // Stop protocol tasks
            return Err(anyhow::anyhow!("Echelon server connection timed out")); // Use anyhow error
        }
        tokio::time::sleep(Duration::from_millis(200)).await; // Slightly longer sleep
    }

    // --- FTP Reader の設定と開始 ---
    // TODO: Get these from command line arguments or a config file
    let ftp_config = FtpReaderConfig {
        host: "your_ftp_host".to_string(), // <-- Replace with actual FTP host
        port: 21,
        user: "your_ftp_user".to_string(), // <-- Replace with actual FTP user
        pass: "your_ftp_password".to_string(), // <-- Replace with actual FTP password
        remote_dir: "/path/to/remote/dir".to_string(), // <-- Replace with actual remote directory
        state_file: PathBuf::from("ftp_reader_state.json"), // State file path
        verbose: true, // Enable verbose logging for FTP reader
        connect_timeout: Duration::from_secs(15),
        read_timeout: Duration::from_secs(60), // Longer timeout for reading potentially large files
        passive_mode: true, // Usually required
    };

    // Clone protocol and server_addr_str for the line handler closure
    let protocol_for_handler = Arc::clone(&protocol);
    let server_addr_for_handler = server_addr_str.to_string(); // Clone server address string

    // Define the line handler: Sends each line via NoiseResilientProtocol
    let line_handler = move |line: String| -> anyhow::Result<()> {
        let proto_clone = Arc::clone(&protocol_for_handler);
        let addr_clone = server_addr_for_handler.clone();
        // Spawn a new task to handle the asynchronous send operation
        // This allows the FtpReader's loop to continue processing quickly.
        tokio::spawn(async move {
            tracing::debug!("FTP Read Line: Sending '{}' to {}", line, addr_clone);
            match proto_clone.send(&addr_clone, line.as_bytes()).await {
                Ok(_) => {
                    tracing::debug!("Successfully sent line to {}", addr_clone);
                }
                Err(e) => {
                    // Log the error, but don't propagate it back to FtpReader
                    // to avoid stopping the reader on a single send failure.
                    tracing::error!("Failed to send line to {}: {}", addr_clone, e);
                }
            }
        });
        // The handler itself returns Ok immediately ("fire and forget")
        Ok(())
    };

    // Create and start the FtpReader
    let mut ftp_reader = FtpReader::new(ftp_config, line_handler)
        .await
        .context("Failed to create FtpReader")?;

    ftp_reader.start().context("Failed to start FtpReader task")?;

    // --- クライアントメインループ (待機) ---
    // The original simple message send is replaced by the FtpReader.
    // The client now just needs to keep running while the FtpReader works.
    // We can wait for a signal (e.g., Ctrl+C) or run indefinitely.
    tracing::info!("FTP Reader started. Client is running and sending lines from FTP.");
    tracing::info!("Press Ctrl+C to stop.");

    // Wait for shutdown signal (Ctrl+C)
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            tracing::info!("Ctrl+C received, shutting down.");
        }
        Err(err) => {
             tracing::error!("Failed to listen for ctrl_c signal: {}", err);
        }
    }

    // --- シャットダウン処理 ---
    tracing::info!("Stopping FTP reader task...");
    ftp_reader.stop().await; // Stop the FTP reader first
    tracing::info!("FTP reader stopped.");

    // Disconnect from Echelon server (optional, but good practice)
    tracing::info!("Disconnecting from Echelon server {}...", server_addr_str);
    if let Err(e) = protocol.disconnect(server_addr_str).await {
        tracing::error!("Failed to send disconnect message: {}", e);
    } else {
        tracing::info!("Disconnect message sent. Waiting briefly...");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Stop the core protocol tasks (maintenance, receiver)
    tracing::info!("Stopping client protocol tasks...");
    protocol.stop().await;
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