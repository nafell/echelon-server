
はい、承知いたしました。`src/main.rs` のコードに基づいた詳細な仕様書を作成します。このサーバアプリケーションの全体構成と処理内容に焦点を当てて記述します。

---

# Echelonサーバーアプリケーション 仕様書

## 1. 概要

本アプリケーションは、UDPベースのカスタムプロトコル（`NoiseResilientProtocol`）を使用してクライアント・サーバー間のデータ通信を行うRust製アプリケーションです。サーバーモードとクライアントモードの2つの動作モードを持ちます。

*   **サーバーモード**: クライアントからのデータを受信し、指定された処理（現状はDBへの保存処理のプレースホルダー）を実行します。また、Axumを利用したWeb UI/APIサーバーを起動し、状態表示や操作インターフェースを提供することを目的としています（現状はルートハンドラのみ）。
*   **クライアントモード**: 指定されたFTPサーバーからデータを読み取り、カスタムプロトコルを使用してEchelonサーバーに送信します。

ロギングには `tracing` ライブラリを使用し、コマンドライン引数の解析には `clap` ライブラリを使用しています。

## 2. 起動コマンドと引数

アプリケーションの起動は、サブコマンド `server` または `client` と共に、以下のオプションを指定して行います。

### 2.1. サーバーモード (`server`)

サーバーとしてアプリケーションを起動します。

*   `--bind-addr <アドレス:ポート>` (または `-b <アドレス:ポート>`)
    *   サーバーがUDP通信をリッスンするアドレスとポートを指定します。
    *   デフォルト値: `127.0.0.1:12345`
*   `--web-addr <アドレス:ポート>` (または `-w <アドレス:ポート>`)
    *   Web UI/APIサーバーがリッスンするTCPアドレスとポートを指定します。
    *   デフォルト値: `127.0.0.1:8080`

### 2.2. クライアントモード (`client`)

クライアントとしてアプリケーションを起動し、メッセージ（現在はFTP経由のデータ）をサーバーに送信します。

*   `--server-addr <アドレス:ポート>` (または `-s <アドレス:ポート>`)
    *   接続先のEchelonサーバーのアドレスとポートを指定します。
    *   デフォルト値: `127.0.0.1:12345`
*   `--message <メッセージ>` (または `-m <メッセージ>`)
    *   送信するメッセージを指定します。（現状のFTPクライアントモードでは直接使用されていませんが、引数としては存在します。）
    *   デフォルト値: `"Hello, protocol!"`
*   `--local-addr <アドレス:ポート>`
    *   クライアントがUDP通信をバインドするローカルアドレスとポートを指定します。`0` を指定するとOSが自動的に割り当てます。
    *   デフォルト値: `0.0.0.0:0`

## 3. 全体構成

### 3.1. サーバーモード

```mermaid
graph TD
    subgraph Echelon Server Application
        A[UDP Listener (NoiseResilientProtocol)] -- 受信データ --> B{データ処理};
        B -- ログ保存 --> C[(Database Placeholder)];
        D[Axum Web Server] -- HTTPリクエスト --> E{API Handlers};
        E -- プロトコル操作/状態参照 --> A;
        F[Protocol Maintenance Task] --> A;
        G[Protocol Server Task] --> A;
    end

    Client1 --> A;
    Client2 --> A;
    User/Browser --> D;
```

*   **`NoiseResilientProtocol` (UDP通信)**:
    *   指定されたUDPアドレス（`bind_addr`）でクライアントからの接続を待ち受けます。
    *   `protocol_clone_server`タスク: プロトコルのメインループを処理し、クライアントとの接続管理やデータ受信を行います。
    *   `protocol_clone_maintenance`タスク: 定期的な接続維持処理（キープアライブなど）を行います。
    *   `protocol_clone_receiver`タスク: 受信したデータを処理するコールバック関数を呼び出します。このコールバック内で、`save_document_to_db`関数が非同期に実行され、受信データがDBに保存される想定です（現在はプレースホルダー）。
    *   アプリケーションの状態(`ServerAppState`)として `Arc<NoiseResilientProtocol>` が共有され、Webサーバーのハンドラなどからもアクセス可能です。
*   **Axum Webサーバー**:
    *   指定されたTCPアドレス（`web_addr`）でHTTPリクエストを待ち受けます。
    *   ルート (`/`) ハンドラ: `root_handler` がウェルカムメッセージを返します。
    *   APIルート: `routes::create_api_routes()` で定義されるAPIエンドポイントを提供します（現状、具体的なAPI実装はコメントアウトされています）。
    *   `NoiseResilientProtocol` の状態を共有し、API経由での操作や状態取得を可能にします。
*   **非同期タスク**:
    *   `tokio::spawn` を使用して、UDPプロトコル処理（サーバー、メンテナンス、受信）、および各受信データごとのDB保存処理が独立したタスクとして実行されます。

### 3.2. クライアントモード

```mermaid
graph TD
    subgraph Echelon Client Application
        FTP[FTP Server] -- ファイル行データ --> FTPReader[FtpReaderTask];
        FTPReader -- mpsc channel (line_rx) --> Forwarder[Forwarding Loop Task];
        Forwarder -- NoiseResilientProtocol.send() --> EchelonSrv[Echelon Server];
        NRPClient[NoiseResilientProtocol Client Instance];
        NRPClientMaintenance[Protocol Maintenance Task] --> NRPClient;
        NRPClientReceiver[Protocol Receiver Task] --> NRPClient;
        NRPClient -- 接続/データ送受信 --> EchelonSrv;
        UserCtrlC[User (Ctrl+C)] -- シャットダウンシグナル --> MainTask;
        MainTask -- shutdown_ftp_tx (watch channel) --> FTPReader;
        MainTask -- protocol.disconnect() --> EchelonSrv;
        MainTask -- protocol.stop() --> NRPClientMaintenance;
        MainTask -- protocol.stop() --> NRPClientReceiver;
    end
```

*   **`NoiseResilientProtocol` (クライアントインスタンス)**:
    *   指定されたローカルUDPアドレス (`local_addr`) にバインドし、Echelonサーバー (`server_addr`) との通信を行います。
    *   `connect()`メソッドでサーバーへの接続を開始します。
    *   `is_connected()`で接続状態を確認します。
    *   `send()`メソッドでサーバーにデータを送信します。
    *   `disconnect()`メソッドでサーバーからの切断処理を開始します。
    *   `stop()`メソッドでプロトコル関連のタスクを停止します。
    *   `protocol_clone_maintenance`タスク: 定期的な接続維持処理を行います。
    *   `protocol_clone_receiver`タスク: サーバーからのデータを受信し、ログに出力します。
*   **FTP Reader (`run_ftp_client_task`)**:
    *   設定ファイル (`ftp_config`) に基づき、指定されたFTPサーバーに接続します。
    *   監視対象のディレクトリからファイルの新しい行を読み取ります。
    *   読み取った行は `mpsc::channel` (`line_tx`) を通じて転送ループタスクに送信されます。
    *   `ftp_state.json` ファイルを使用して、最後に読み取ったファイルの状態を保存し、再開時に続きから読み取れるようにします。
    *   `watch::channel` (`shutdown_ftp_rx`) を介してメインタスクからのシャットダウンシグナルを受け取ると、処理を停止します。
*   **転送ループタスク (`forward_handle`)**:
    *   `mpsc::channel` (`line_rx`) からFTP Readerが読み取った行データを受信します。
    *   `NoiseResilientProtocol` インスタンスを使用して、受信した行データをEchelonサーバーに送信します。
    *   サーバーとの接続が確立している場合のみ送信処理を行います。
    *   送信失敗時の詳細なリカバリー処理は現状実装されていません（エラーログ出力のみ）。
*   **シャットダウン処理**:
    *   ユーザーが `Ctrl+C` を入力すると、シャットダウン処理が開始されます。
    1.  FTP Readerタスクに停止シグナルを送信。
    2.  FTP Readerタスクの終了をタイムアウト付きで待機。
    3.  データ転送ループタスクの終了をタイムアウト付きで待機（チャンネルが閉じられると終了する想定）。
    4.  Echelonサーバーに切断メッセージを送信。
    5.  クライアント側のプロトコルタスク（メンテナンス、受信）を停止。

## 4. 主要モジュールと機能詳細

### 4.1. `protocol` (`NoiseResilientProtocol`)

カスタムUDP通信プロトコルを実装するコアモジュール。

*   **設定 (`ConnectionConfig`)**: タイムアウト時間などの接続パラメータを設定可能。
*   **サーバー機能**:
    *   `with_config(bind_addr, config)`: 指定アドレスでリッスンするサーバーインスタンスを生成。
    *   `start_server()`: サーバーのメインループを開始し、クライアントからの接続要求やデータパケットを処理。
    *   `start_maintenance()`: 定期的なメンテナンス処理（アクティブな接続の確認、タイムアウト処理など）を開始。
    *   `start_receiver(callback)`: データ受信時に呼び出される非同期コールバック関数を登録し、受信処理ループを開始。コールバックは `(SocketAddr, Vec<u8>)` を引数に取る。
*   **クライアント機能**:
    *   `with_config(local_addr, config)`: 指定のローカルアドレスにバインドするクライアントインスタンスを生成。
    *   `connect(server_addr)`: 指定されたサーバーアドレスへの接続を開始。
    *   `is_connected(server_addr)`: 指定サーバーとの接続状態を返す。
    *   `send(server_addr, data)`: 指定サーバーにデータを送信。
    *   `disconnect(server_addr)`: 指定サーバーとの接続を切断。
    *   `start_maintenance()`: (クライアント側でも)定期的なメンテナンス処理を開始。
    *   `start_receiver(callback)`: (クライアント側でも)データ受信時にコールバックを実行。
    *   `stop()`: プロトコル関連の全タスクを停止。
*   **共有**: `Arc<NoiseResilientProtocol>` として複数のタスク間で共有され、非同期に操作されます。内部で状態管理を行うため、外部からの `Mutex` による排他制御は不要です。

### 4.2. `ftp_client` (`FtpReaderConfig`, `run_ftp_client_task`)

FTPサーバーからデータを読み取るためのモジュール。

*   **設定 (`FtpReaderConfig`)**:
    *   `host`: FTPサーバーのホスト名とポート。
    *   `user`: FTPユーザー名。
    *   `pass`: FTPパスワード。
    *   `remote_dir`: 監視対象のリモートディレクトリ。
    *   `state_file`: 読み取り状態を保存するJSONファイルのパス。
    *   `line_sender`: 読み取った行を送信するための `mpsc::Sender<String>`。
    *   `shutdown_rx`: シャットダウンシグナルを受信するための `watch::Receiver<()>`。
*   **処理 (`run_ftp_client_task`)**:
    *   設定に基づきFTPサーバーに接続。
    *   指定されたリモートディレクトリ内のファイルを監視し、新しい行を検出。
    *   検出した行を `line_sender` 経由で送信。
    *   `state_file` に現在の読み取り位置（ファイル名、オフセットなど）を定期的に保存し、再起動時にその位置から処理を再開する。
    *   `shutdown_rx` からのシグナルを受信すると、FTP接続をクローズしタスクを終了する。

### 4.3. `routes` (`create_api_routes`)

Axum Webサーバー用のAPIルートを定義するモジュール。

*   `create_api_routes()`: APIルートを含む `Router` を返します。
*   現状では、`src/main.rs` 内で `create_api_routes()` が呼び出されていますが、具体的なAPIエンドポイント（例: `/status` や `/send`）の実装はコメントアウトされています。これらが有効化されると、HTTP経由でプロトコルの状態確認やメッセージ送信が可能になります。

### 4.4. `main.rs` (エントリーポイント)

アプリケーションの起動、コマンドライン引数の処理、モードに応じた初期化とタスクの実行調整を行います。

*   **コマンドライン引数解析**: `clap` を使用して `Args`構造体と`Commands` enum にパース。
*   **モード分岐**: `Commands::Server` または `Commands::Client` に応じて `run_server` または `run_client` 関数を呼び出し。
*   **`run_server`**:
    1.  `NoiseResilientProtocol` のサーバーインスタンスを生成。
    2.  プロトコルのサーバータスク、メンテナンスタスク、受信タスクをそれぞれ `tokio::spawn` で起動。受信タスクのコールバックでは `save_document_to_db` を呼び出す。
    3.  Axumルーターを設定し、`NoiseResilientProtocol` のインスタンスを状態として共有。
    4.  Axumサーバーを指定された `web_bind_addr` で起動。
*   **`run_client`**:
    1.  `NoiseResilientProtocol` のクライアントインスタンスを生成。
    2.  プロトコルのメンテナンスタスク、受信タスクを `tokio::spawn` で起動。受信タスクのコールバックでは受信データをログ出力。
    3.  指定された `server_addr_str` に `protocol.connect()` で接続試行。
    4.  `protocol.is_connected()` で接続完了を待機（タイムアウトあり）。
    5.  FTP Readerタスク (`run_ftp_client_task`) を `tokio::spawn` で起動。FTP設定は現在ハードコード。
    6.  FTP Readerから `mpsc::channel` 経由で受信した行データを、`NoiseResilientProtocol` を使ってサーバーに転送するループタスクを `tokio::spawn` で起動。
    7.  `tokio::signal::ctrl_c()` でCtrl+Cを待ち受け、受信したらシャットダウン処理を開始。
        *   FTP Readerに停止信号送信、終了待機。
        *   転送ループタスクの終了待機。
        *   サーバーに切断メッセージ送信。
        *   プロトコルタスク停止。

## 5. データフロー

### 5.1. サーバーモード (データ受信)

1.  クライアントがEchelonサーバーのUDPポートにデータを送信。
2.  `NoiseResilientProtocol` (`protocol_clone_server`タスク) がデータを受信。
3.  `protocol_clone_receiver`タスクに登録されたコールバックが呼び出される。
4.  コールバック内で `save_document_to_db(peer_addr, data)` が非同期に実行される。
5.  `save_document_to_db` (現状プレースホルダー) がデータを永続化ストレージ（DBなど）に保存。

### 5.2. クライアントモード (FTPデータ -> サーバー)

1.  `run_ftp_client_task` がFTPサーバー (`ftp_config.host`) から指定ディレクトリ (`ftp_config.remote_dir`) のファイルの新しい行を読み取る。
2.  読み取った行 (String) は `mpsc::channel` (`line_tx`) を通じて転送ループタスクに送信される。
3.  転送ループタスク (`forward_handle`) は `line_rx.recv().await` で行データを受信する。
4.  転送ループタスクは、`protocol_clone_sender.is_connected()` でEchelonサーバーとの接続を確認。
5.  接続されていれば、`protocol_clone_sender.send(server_addr_str, line.as_bytes())` を使用して行データをEchelonサーバーに送信。
6.  Echelonサーバーは受信したデータを処理 (上記 5.1. 参照)。

## 6. エラーハンドリングとロギング

*   関数の戻り値には `anyhow::Result` や `std::io::Result` が使用され、エラー伝播と集約的なエラーハンドリングが行われます。
*   `context()` メソッド (`anyhow`由来) により、エラーにコンテキスト情報が付加されます。
*   `tracing` ライブラリが広範囲で使用され、`info!`, `debug!`, `warn!`, `error!` マクロにより詳細なログが出力されます。ログレベルは環境変数 `RUST_LOG` (例: `RUST_LOG=debug`) で制御可能です。
*   クライアントモードの接続処理やタスク終了待機では、`tokio::time::timeout` を使用したタイムアウト処理が実装されています。

## 7. シャットダウン処理 (クライアントモード)

クライアントモードでは、`Ctrl+C` シグナルによるグレースフルシャットダウンが実装されています。

1.  `tokio::signal::ctrl_c().await` でシグナルを補足。
2.  `shutdown_ftp_tx.send(())` でFTP Readerタスクに停止を通知。
3.  `tokio::time::timeout` を使用して、FTP Readerタスク (`ftp_handle`) の終了を待機。
4.  `tokio::time::timeout` を使用して、データ転送タスク (`forward_handle`) の終了を待機。
5.  `protocol.disconnect(server_addr_str).await` でサーバーに切断を通知。
6.  `protocol.stop().await` でクライアントのプロトコル関連タスク（メンテナンス、受信）を停止。

## 8. 未実装/改善点 (TODO)

*   **DB保存処理の実装**: `save_document_to_db` 関数内の具体的なデータベース保存ロジック。
*   **FTP接続情報の設定**: クライアントモードのFTP接続情報 (`FtpReaderConfig`) は現在ハードコードされているため、設定ファイルやコマンドライン引数から読み込めるようにする必要があります。
*   **Axum APIエンドポイントの実装**: `src/main.rs` の末尾にコメントアウトされている `status_handler` や `send_handler` などの具体的なAPI処理。
*   **クライアント送信エラーリカバリ**: クライアントのデータ転送ループ内で `protocol_clone_sender.send()` が失敗した場合のリカバリー処理（例: 再試行、再接続）。
*   **クライアントサーバー再接続**: クライアントのデータ転送ループ内でサーバーとの接続が切れた場合 (`is_connected()` が `false` を返した場合）の再接続ロジック。
*   **流量制御**: クライアントからサーバーへのデータ送信頻度が高い場合、ACKを待つか、`tokio::time::sleep` を入れるなどの流量制御が必要になる可能性があります (コメントに示唆あり)。

---

この仕様書が、アプリケーションの理解の一助となれば幸いです。
