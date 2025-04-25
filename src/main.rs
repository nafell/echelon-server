// use std::net::{TcpListener, TcpStream};

// fn handle_client(_stream: TcpStream) {
// }
mod protocol;
mod routes;
mod ftp_client;

use axum::{routing::get, Router};
use std::net::SocketAddr;
use std::sync::Arc; // std::sync::Arc を使用
use std::time::Duration;
use clap::Parser; // clap をインポート
use ftp_client::{FtpReaderConfig, run_ftp_client_task};
use tokio::sync::{mpsc, watch};
use std::path::PathBuf;

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
async fn run_client(server_addr_str: &str, _message: &str, local_addr: &str) -> std::io::Result<()> {
    tracing::info!("Starting client mode (with FTP Reader)...");
    tracing::info!("Connecting to server: {}", server_addr_str);
    tracing::info!("Binding local UDP to: {}", local_addr);

    // --- NoiseResilientProtocol の準備 (クライアント用) ---
    let client_config = ConnectionConfig {
        connection_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let protocol = NoiseResilientProtocol::with_config(local_addr, client_config).await?;
    let protocol = Arc::new(protocol);

    // --- クライアント用タスクの開始 ---
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
        protocol.stop().await;
        return Err(e);
    }

    // --- 接続完了待機 ---
    let connect_timeout = Duration::from_secs(5);
    let start_time = tokio::time::Instant::now();
    loop {
        match protocol.is_connected(server_addr_str).await {
            Ok(true) => {
                tracing::info!("Successfully connected to {}", server_addr_str);
                break;
            }
            Ok(false) => { /* 接続中... */ }
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // --- FTP Reader タスクの準備と開始 ---
    let (line_tx, mut line_rx) = mpsc::channel::<String>(100); // FTPからの行を受け取るチャンネル
    let (shutdown_ftp_tx, shutdown_ftp_rx) = watch::channel(()); // FTPリーダー停止用チャンネル

    // TODO: FTP接続情報は設定ファイルやコマンドライン引数から取得するようにする
    let ftp_config = FtpReaderConfig {
        host: "10.192.144.1:21".to_string(), // 例: "192.168.1.100:21"
        user: "FTP_test".to_string(),
        pass: "qwerty".to_string(),
        remote_dir: "/path/to/your/logs".to_string(), // 例: "/" や "/data"
        state_file: PathBuf::from("ftp_state.json"), // 状態ファイル名
        line_sender: line_tx,
        shutdown_rx: shutdown_ftp_rx,
    };

    // FTP Readerを別タスクで実行
    let ftp_handle = tokio::spawn(run_ftp_client_task(ftp_config));


    // --- FTPから受信した行をサーバーに送信するループ ---
    tracing::info!("Starting loop to forward FTP lines to server...");
    let protocol_clone_sender = Arc::clone(&protocol); // 送信用に Arc をクローン
    let server_addr_str_clone = server_addr_str.to_string(); // ループ内で使うため clone

    let forward_handle = tokio::spawn(async move {
        while let Some(line) = line_rx.recv().await {
            if protocol_clone_sender.is_connected(&server_addr_str_clone).await.unwrap_or(false) {
                tracing::debug!("Forwarding line to server: {}", line);
                if let Err(e) = protocol_clone_sender.send(&server_addr_str_clone, line.as_bytes()).await {
                    tracing::error!("Failed to send message via protocol: {}", e);
                    // ここで送信失敗時のリカバリー処理が必要かもしれない (例: 再接続試行など)
                    // 今回はエラーログのみ
                }
                 // ACKを少し待つか、流量制御が必要なら sleep を入れる
                 // tokio::time::sleep(Duration::from_millis(10)).await;
            } else {
                tracing::warn!("Not connected to server, cannot forward line: {}", line);
                // サーバーへの再接続が必要かもしれない
                // 簡単にするため、今回は破棄
            }
        }
        tracing::info!("FTP line forwarding loop finished (channel closed).");
    });


    // --- クライアント終了処理 ---
    // 例えば Ctrl+C を受け取るまで待機する
    tracing::info!("Client running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    tracing::info!("Ctrl+C received. Shutting down...");

    // 1. FTPリーダーに停止信号を送る
    tracing::info!("Sending shutdown signal to FTP reader...");
    let _ = shutdown_ftp_tx.send(()); // エラーは無視

    // 2. FTPリーダータスクの終了を待つ（タイムアウト付き）
    tracing::info!("Waiting for FTP reader task to finish...");
    match tokio::time::timeout(Duration::from_secs(5), ftp_handle).await {
        Ok(Ok(_)) => tracing::info!("FTP reader task finished gracefully."),
        Ok(Err(e)) => tracing::error!("FTP reader task panicked: {:?}", e),
        Err(_) => tracing::warn!("FTP reader task did not finish within timeout."),
    }

    // 3. 転送ループタスクの終了を待つ (チャンネルが閉じれば終わるはず)
    tracing::info!("Waiting for forwarding task to finish...");
     match tokio::time::timeout(Duration::from_secs(2), forward_handle).await {
        Ok(Ok(_)) => tracing::info!("Forwarding task finished."),
        Ok(Err(e)) => tracing::error!("Forwarding task panicked: {:?}", e),
        Err(_) => tracing::warn!("Forwarding task did not finish within timeout."),
    }


    // 4. サーバーから切断
    tracing::info!("Disconnecting from {}...", server_addr_str);
    if let Err(e) = protocol.disconnect(server_addr_str).await {
        tracing::error!("Failed to send disconnect message: {}", e);
    } else {
        tracing::info!("Disconnect message sent. Waiting briefly...");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 5. プロトコルタスクを停止
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