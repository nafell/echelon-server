
はい、承知いたしました。`src/protocol.rs` のコードに基づいた詳細な仕様書を作成します。

## `NoiseResilientProtocol` 仕様書

### 1. 概要

このドキュメントは、`NoiseResilientProtocol` というUDPベースの信頼性のある通信プロトコル実装に関する仕様を記述します。このプロトコルは、パケットロスや遅延が発生する可能性のあるネットワーク環境において、順序保証、再送制御、輻輳制御（簡易的なもの）などの機能を提供することを目指しています。

主な機能は以下の通りです。

*   接続指向の通信（コネクション確立、切断）
*   データパケットのシーケンス番号による順序保証
*   ACKパケットによる到達確認
*   タイムアウトと再送による信頼性の向上
*   RTT推定とRTO計算による動的な再送タイミング調整
*   クライアント・サーバーモデル

### 2. 主要なデータ構造

#### 2.1. `PacketType` (enum)

パケットの種類を識別するための列挙型です。

*   `Connect`: 接続要求パケット
*   `ConnectAck`: 接続確認応答パケット
*   `Data`: データ転送パケット
*   `Ack`: 受信確認応答パケット
*   `Disconnect`: 切断要求パケット

#### 2.2. `PacketHeader` (struct)

各パケットの先頭に付与される16バイト固定長のヘッダ情報です。

*   `packet_type: PacketType` (1バイト): パケットの種類。
*   `seq_num: u32` (4バイト): パケットのシーケンス番号。
*   `ack_num: u32` (4バイト): 確認応答番号。最後に正常に受信したデータパケットのシーケンス番号。
*   `flags: u8` (1バイト): 将来の拡張用フラグ（現在は未使用）。
*   `checksum: u16` (2バイト): ヘッダとペイロードに対するチェックサム。
*   `payload_size: u16` (2バイト): ペイロードのサイズ。
*   予約領域 (2バイト): 常に0で埋められます。

##### 2.2.1. `PacketHeader::new(packet_type: PacketType, seq_num: u32, ack_num: u32, payload_size: u16) -> Self`

新しい `PacketHeader` インスタンスを生成します。チェックサムはここでは計算されません。

*   **出力データ**: 初期化された `PacketHeader` 構造体。

##### 2.2.2. `PacketHeader::to_bytes(&self) -> [u8; 16]`

`PacketHeader` 構造体をバイト配列（16バイト）にシリアライズします。

*   **出力データ**: ヘッダ情報を格納した16バイトの配列 `[u8; 16]`。

##### 2.2.3. `PacketHeader::from_bytes(bytes: &[u8]) -> Result<Self, Error>`

バイト配列から `PacketHeader` 構造体をデシリアライズします。

*   **出力データ**:
    *   `Ok(PacketHeader)`: 正常にデシリアライズされた `PacketHeader` 構造体。
    *   `Err(Error)`: デシリアライズに失敗した場合のエラー (`ErrorKind::InvalidData` など)。入力バイト長が16バイト未満の場合にエラーとなります。

##### 2.2.4. `PacketHeader::calculate_checksum(&self, payload: &[u8]) -> u16`

ヘッダ情報とペイロードデータに基づいて簡易的なチェックサムを計算します。

*   **出力データ**: 計算された16ビットのチェックサム値 `u16`。

#### 2.3. `Packet` (struct)

通信で送受信される実際のパケットを表す構造体です。

*   `header: PacketHeader`: パケットヘッダ。
*   `payload: Vec<u8>`: パケットのペイロードデータ。

##### 2.3.1. `Packet::new(packet_type: PacketType, seq_num: u32, ack_num: u32, payload: Vec<u8>) -> Self`

新しい `Packet` インスタンスを生成します。この際、ヘッダの `payload_size` と `checksum` が自動的に計算・設定されます。

*   **出力データ**: 初期化およびチェックサム計算済みの `Packet` 構造体。

##### 2.3.2. `Packet::to_bytes(&self) -> Vec<u8>`

`Packet` 構造体をバイトベクタ `Vec<u8>` にシリアライズします。ヘッダとペイロードが連結された形式になります。

*   **出力データ**: パケット全体（ヘッダ + ペイロード）を表すバイトベクタ `Vec<u8>`。

##### 2.3.3. `Packet::from_bytes(bytes: &[u8]) -> Result<Self, Error>`

バイト配列から `Packet` 構造体をデシリアライズします。ヘッダのデシリアライズ、ペイロードサイズの検証、チェックサムの検証を行います。

*   **出力データ**:
    *   `Ok(Packet)`: 正常にデシリアライズおよび検証された `Packet` 構造体。
    *   `Err(Error)`: 以下の場合にエラー (`ErrorKind::InvalidData` など) となります。
        *   入力バイト長がヘッダ長(16バイト)未満の場合。
        *   ヘッダに記述されたペイロードサイズと実際のデータ長が一致しない場合。
        *   チェックサムが一致しない場合。

#### 2.4. `PendingPacket` (struct)

送信後、まだACKを受信していないパケットの情報を管理するための構造体です。再送処理に使用されます。

*   `packet: Packet`: 送信したパケットのコピー。
*   `first_sent: Instant`: 最初にこのパケットを送信した時刻。
*   `last_sent: Instant`: 最後にこのパケットを送信した時刻（再送時に更新）。
*   `transmissions: u32`: このパケットを送信した回数。

#### 2.5. `ConnectionState` (enum)

接続の状態を表す列挙型です。

*   `Closed`: 接続が閉じている状態。
*   `Connecting`: 接続処理中の状態。
*   `Connected`: 接続が確立している状態。
*   `Disconnecting`: 切断処理中の状態。

#### 2.6. `ConnectionConfig` (struct)

接続ごとの設定値を保持する構造体です。

*   `initial_rtt: Duration`: RTT (Round Trip Time) の初期値。
*   `min_rto: Duration`: RTO (Retransmission Timeout) の最小値。
*   `max_rto: Duration`: RTO の最大値。
*   `max_retransmissions: u32`: データパケットの最大再送回数。
*   `receive_window_size: u32`: 受信ウィンドウサイズ（現在は明示的には使用されていませんが、将来的なフロー制御用）。
*   `max_packet_size: usize`: 送信可能な最大パケットサイズ（ヘッダ込み）。
*   `connection_timeout: Duration`: アイドル状態や接続試行がこの時間を超えるとタイムアウトとみなす期間。

##### 2.6.1. `ConnectionConfig::default() -> Self`

`ConnectionConfig` のデフォルト値を生成します。

*   **出力データ**: デフォルト値で初期化された `ConnectionConfig` 構造体。

#### 2.7. `Connection` (struct)

個々のリモートピアとの接続状態と関連情報を管理する構造体です。

*   `peer_addr: SocketAddr`: 接続相手のソケットアドレス。
*   `state: ConnectionState`: 現在の接続状態。
*   `config: ConnectionConfig`: この接続で使用する設定。
*   `next_seq_num: u32`: 次に送信するデータパケットのシーケンス番号。
*   `next_expected_seq: u32`: 次に受信を期待するデータパケットのシーケンス番号。
*   `rtt: Duration`: 推定された現在のRTT。
*   `rtt_var: Duration`: RTTの変動。
*   `rto: Duration`: 現在のRTO。
*   `pending_packets: HashMap<u32, PendingPacket>`: 送信済みでACK待ちのパケット (キー: シーケンス番号)。
*   `received_packets: HashMap<u32, Vec<u8>>`: 順序が前後して到着したデータパケットのペイロード (キー: シーケンス番号)。
*   `last_activity: Instant`: 最後に何らかのパケットを受信または送信した時刻。
*   `created_at: Instant`: この接続オブジェクトが生成された時刻。

##### 2.7.1. `Connection::new(peer_addr: SocketAddr, config: ConnectionConfig) -> Self`

新しい `Connection` インスタンスを生成します。

*   **出力データ**: 初期化された `Connection` 構造体。状態は `ConnectionState::Closed`。

##### 2.7.2. `async fn Connection::send_packet(&mut self, socket: &UdpSocket, packet_type: PacketType, payload: Vec<u8>) -> Result<u32, Error>`

指定されたタイプのパケットを生成し、UDPソケット経由で送信します。`Data` または `Connect` パケットの場合、再送管理のために `pending_packets` に登録します。

*   **処理内容**:
    1.  シーケンス番号をインクリメント (`next_seq_num`)。
    2.  `Packet::new` を呼び出してパケットを構築。
    3.  パケットをバイト列に変換 (`Packet::to_bytes`)。
    4.  `socket.send_to` で指定されたピアに送信。
    5.  `Data` または `Connect` パケットの場合、`PendingPacket` を生成し `pending_packets` に追加。
    6.  `last_activity` を更新。
*   **出力データ**:
    *   `Ok(u32)`: 送信したパケットのシーケンス番号。
    *   `Err(Error)`: ソケット送信エラーなどの `std::io::Error`。

##### 2.7.3. `async fn Connection::receive_packet(&mut self, socket: &UdpSocket, packet: Packet) -> Result<Option<Vec<u8>>, Error>`

受信したパケットを処理します。パケットタイプに応じて接続状態の遷移、ACKの送信、データ処理などを行います。

*   **処理内容**:
    *   `last_activity` を更新。
    *   `PacketType::Connect`:
        *   状態が `Closed` なら `Connecting` に遷移し、`next_expected_seq` を設定後、`ConnectAck` を送信。アプリケーションに接続要求があったことを通知するために空の `Vec<u8>` を返す。
    *   `PacketType::ConnectAck`:
        *   状態が `Connecting` なら `Connected` に遷移し、該当する `Connect` パケットを `pending_packets` から削除（`handle_ack` 経由）。アプリケーションに接続確立を通知するために空の `Vec<u8>` を返す。
    *   `PacketType::Data`:
        *   常に `Ack` パケットを送信元に返送。
        *   期待通りのシーケンス番号 (`next_expected_seq`) の場合:
            *   `next_expected_seq` をインクリメント。
            *   `handle_ack` を呼び出して相手のACK番号を処理。
            *   受信データを返し、`received_packets` に溜まっていた後続の連続パケットもあれば連結して返す。
        *   期待より大きいシーケンス番号の場合:
            *   パケットを `received_packets` に保存（ウィンドウサイズによる制御は未実装）。
            *   `handle_ack` を呼び出し。
        *   期待より小さいシーケンス番号の場合（古いパケット）:
            *   データは無視するが `handle_ack` は呼び出し。
    *   `PacketType::Ack`:
        *   `handle_ack` を呼び出して `pending_packets` を更新。
    *   `PacketType::Disconnect`:
        *   `Ack` パケットを送信元に返送。
        *   状態を `Closed` に遷移。アプリケーションに切断要求があったことを通知するために空の `Vec<u8>` を返す。
*   **出力データ**:
    *   `Ok(Some(Vec<u8>))`:
        *   `Connect`, `ConnectAck`, `Disconnect` を処理した場合: 空の `Vec<u8>` を返し、上位レイヤーにイベント発生を通知。
        *   `Data` パケットを正常に受信・処理した場合: アプリケーションペイロード `Vec<u8>`。順序通りに並べ替えられたデータが含まれる可能性がある。
    *   `Ok(None)`: `Ack` パケットの処理など、アプリケーションに渡すデータがない場合。
    *   `Err(Error)`: `send_packet` でのACK送信失敗などの `std::io::Error`。

##### 2.7.4. `fn Connection::handle_ack(&mut self, ack_num: u32)`

受信したACK番号 (`ack_num`) に基づいて、`pending_packets` から確認済みとみなされるパケットを削除し、RTTとRTOを更新します。

*   **処理内容**:
    1.  `pending_packets` をイテレートし、`seq_num <= ack_num` であるパケットを探す。
    2.  該当パケットについて、送信時刻と現在時刻からサンプルRTTを計算。
    3.  Jacobson's algorithm に基づいて `rtt` (平滑化RTT) と `rtt_var` (RTT変動) を更新。
    4.  `rto` (再送タイムアウト) を `rtt + 4 * rtt_var` として更新（設定された `min_rto` と `max_rto` の範囲内に収める）。
    5.  確認済みのパケットを `pending_packets` から削除。
*   **出力データ**: なし (内部状態を更新)。

##### 2.7.5. `async fn Connection::check_timeouts(&mut self, socket: &UdpSocket) -> Result<(), Error>`

`pending_packets` を確認し、RTOを超過したパケットを再送します。最大再送回数を超えた場合はパケットを破棄し、接続タイムアウトもチェックします。

*   **処理内容**:
    1.  `pending_packets` をイテレート。
    2.  各パケットについて、`last_sent` から `rto` が経過しているか確認。
    3.  タイムアウトしている場合:
        *   送信回数 (`transmissions`) が `config.max_retransmissions` 未満なら再送リストに追加。
        *   最大再送回数を超えていれば破棄リストに追加。
    4.  再送リストのパケットを再送 (`socket.send_to`)。`last_sent` と `transmissions` を更新し、RTOを指数バックオフ（2倍、最大 `config.max_rto`）。
    5.  破棄リストのパケットを `pending_packets` から削除。`Connect` パケット(seq=0)が破棄された場合は接続失敗とみなし、状態を `Closed` にする。
    6.  接続全体のタイムアウト (`last_activity` から `config.connection_timeout` 経過) をチェックし、タイムアウトしていれば状態を `Closed` にする。
*   **出力データ**:
    *   `Ok(())`: 処理が正常に完了。
    *   `Err(Error)`: パケット再送時のソケットエラーなどの `std::io::Error`。

### 3. `NoiseResilientProtocol` 構造体

プロトコル全体のエントリーポイントとなる構造体です。UDPソケットの管理、複数の接続の管理、送受信処理のインターフェースを提供します。

*   `socket: Arc<UdpSocket>`: プロトコルが使用するUDPソケット。複数の非同期タスクで共有されるため `Arc` でラップされています。
*   `connections: Arc<Mutex<HashMap<SocketAddr, Connection>>>`: アクティブな接続を管理するハッシュマップ。キーはリモートピアの `SocketAddr`。`Arc<Mutex>` で非同期タスク間の共有と排他アクセスを制御します。
*   `config: ConnectionConfig`: 新しい接続が確立される際のデフォルト設定。
*   `running: Arc<Mutex<bool>>`: プロトコルのメインループ（サーバータスクや受信タスクなど）が実行中かどうかを示すフラグ。`stop()` メソッドで `false` に設定されます。

#### 3.1. `async fn NoiseResilientProtocol::with_config(bind_addr: &str, config: ConnectionConfig) -> Result<Self, Error>`

指定されたアドレスにバインドするUDPソケットと設定で `NoiseResilientProtocol` インスタンスを生成します。

*   **処理内容**:
    1.  `UdpSocket::bind(bind_addr)` を呼び出してUDPソケットを作成。
    2.  各フィールドを初期化。
*   **出力データ**:
    *   `Ok(Self)`: 初期化された `NoiseResilientProtocol` インスタンス。
    *   `Err(Error)`: ソケットのバインド失敗などの `std::io::Error`。

#### 3.2. `async fn NoiseResilientProtocol::start_server(&self) -> Result<(), Error>`

サーバーモードで動作を開始します。新しい非同期タスクを起動し、クライアントからの接続要求やデータパケットを受信・処理します。

*   **処理内容**:
    1.  `Arc` でラップされた `socket`, `connections`, `running`, `config` をクローンして新しいタスクにムーブ。
    2.  `tokio::task::spawn` を使用して非同期タスクを起動。
    3.  タスク内部ループ:
        *   `running` フラグが `true` の間、`socket.recv_from` でパケットを受信待機。
        *   パケット受信成功時:
            *   `Packet::from_bytes` でパケットをデコード。
            *   デコード成功時:
                *   `connections` をロック。
                *   `Connect` パケットの場合:
                    *   送信元アドレスが未接続なら、新しい `Connection` を作成し、`Connection::receive_packet` で処理（`ConnectAck` 送信など）。成功すれば `connections` に追加。
                    *   既に接続がある場合（再送など）、既存の `Connection` で `receive_packet` を呼び出し。
                *   その他のパケットタイプの場合:
                    *   送信元アドレスに対応する `Connection` が存在し、かつ `Closed` 状態でなければ `Connection::receive_packet` で処理。
                    *   サーバー側で受信したデータは `tracing::debug` でログ出力され、破棄される（必要なら変更可能）。
            *   デコード失敗時は警告ログを出力。
        *   `recv_from` エラー時はエラーログを出力し、短時間スリープ。
    4.  ループ終了後（`running` が `false` になった場合）、タスク終了ログを出力。
*   **出力データ**:
    *   `Ok(())`: サーバータスクの起動に成功した場合。タスク自体はバックグラウンドで実行されます。
    *   `Err(Error)`: (現実装では `task::spawn` は `Result` を返さないため、この関数自体がエラーを返すことは稀です。将来的に変更される可能性はあります。)

#### 3.3. `async fn NoiseResilientProtocol::connect(&self, server_addr: &str) -> Result<(), Error>`

クライアントとして指定されたサーバーアドレスに接続を試みます。

*   **処理内容**:
    1.  `server_addr` (文字列) を `SocketAddr` にパース。
    2.  `connections` をロック。
    3.  既に指定アドレスへの接続が存在すれば `ErrorKind::AlreadyExists` エラー。
    4.  新しい `Connection` を作成 (状態は `Closed` から開始)。
    5.  `Connection::send_packet` を使用して `PacketType::Connect` パケットをサーバーに送信。
    6.  接続の状態を `ConnectionState::Connecting` に更新。
    7.  新しい接続を `connections` に追加。
*   **出力データ**:
    *   `Ok(())`: 接続要求パケットの送信に成功した場合。接続が確立したことを意味するわけではありません。接続状態は `start_receiver` や `is_connected` で確認します。
    *   `Err(Error)`:
        *   `ErrorKind::InvalidInput`: サーバーアドレスのパース失敗。
        *   `ErrorKind::AlreadyExists`: 既に接続（または接続試行中）の場合。
        *   その他、`send_packet` から返されるソケットエラーなど。

#### 3.4. `async fn NoiseResilientProtocol::is_connected(&self, server_addr: &str) -> Result<bool, Error>`

指定されたサーバーアドレスとの接続が確立しているか (`ConnectionState::Connected`) を確認します。

*   **処理内容**:
    1.  `server_addr` を `SocketAddr` にパース。
    2.  `connections` をロック。
    3.  指定アドレスに対応する `Connection` を取得。
    4.  存在すれば、その `Connection` の `state` が `ConnectionState::Connected` かどうかを返す。
    5.  存在しなければ `false` を返す。
*   **出力データ**:
    *   `Ok(bool)`:
        *   `true`: 接続が確立している。
        *   `false`: 接続が確立していない、または接続情報が存在しない。
    *   `Err(Error)`: `ErrorKind::InvalidInput` (サーバーアドレスのパース失敗)。

#### 3.5. `async fn NoiseResilientProtocol::send(&self, server_addr: &str, data: &[u8]) -> Result<(), Error>`

クライアントとして、指定されたサーバーアドレスにデータを送信します。データは `config.max_packet_size` に基づいて自動的に分割されます。

*   **処理内容**:
    1.  `server_addr` を `SocketAddr` にパース。
    2.  `connections` をロック（ミューテックス）。
    3.  指定アドレスに対応する `Connection` を取得 (可変参照)。
    4.  接続が存在しないか、状態が `Connected` でない場合は `ErrorKind::NotConnected` エラー。
    5.  `max_payload_size` を計算 (`config.max_packet_size - header_size`)。0以下なら `ErrorKind::InvalidInput` エラー。
    6.  入力 `data` を `max_payload_size` ごとにチャンク分割。
    7.  各チャンクについて `Connection::send_packet` を呼び出し、`PacketType::Data` として送信。
*   **出力データ**:
    *   `Ok(())`: 全てのデータチャンクの送信処理（`send_packet`呼び出し）が成功した場合。個々のパケットが相手に届いたことを保証するものではありません。
    *   `Err(Error)`:
        *   `ErrorKind::InvalidInput`: サーバーアドレスのパース失敗、または `max_packet_size` が小さすぎる場合。
        *   `ErrorKind::NotConnected`: 接続が存在しないか、確立していない場合。
        *   その他、`send_packet` から返されるソケットエラーなど。

#### 3.6. `async fn NoiseResilientProtocol::disconnect(&self, server_addr: &str) -> Result<(), Error>`

クライアントとして、指定されたサーバーアドレスとの接続を切断します。

*   **処理内容**:
    1.  `server_addr` を `SocketAddr` にパース。
    2.  `connections` をロック。
    3.  指定アドレスに対応する `Connection` を取得 (可変参照)。
    4.  接続が存在しない場合は `ErrorKind::NotConnected` エラー。
    5.  接続状態が `Connected` または `Connecting` の場合:
        *   `Connection::send_packet` を使用して `PacketType::Disconnect` パケットをサーバーに送信。
        *   接続の状態を `ConnectionState::Disconnecting` に更新。
        *   （接続情報はすぐには削除されず、ACK受信またはタイムアウトによりメンテナンス処理で削除されます）
*   **出力データ**:
    *   `Ok(())`: 切断要求パケットの送信処理が成功した場合。
    *   `Err(Error)`:
        *   `ErrorKind::InvalidInput`: サーバーアドレスのパース失敗。
        *   `ErrorKind::NotConnected`: 接続が存在しない場合。
        *   その他、`send_packet` から返されるソケットエラーなど。

#### 3.7. `async fn NoiseResilientProtocol::start_receiver<F>(&self, mut callback: F) -> Result<(), Error> where F: FnMut(SocketAddr, Vec<u8>) + Send + Sync + 'static`

データ受信処理のための非同期タスクを開始します。受信したデータは指定されたコールバック関数 `callback` を通じてアプリケーションに渡されます。主にクライアント側での使用を想定していますが、サーバー側でも特定の接続からのデータ受信に使えます。

*   **処理内容**:
    1.  `Arc` でラップされた `socket`, `connections`, `running`, `callback` をクローンして新しいタスクにムーブ。
    2.  `tokio::task::spawn` を使用して非同期タスクを起動。
    3.  タスク内部ループ:
        *   `running` フラグが `true` の間、`socket.recv_from` でパケットを受信待機。
        *   パケット受信成功時:
            *   `Packet::from_bytes` でパケットをデコード。
            *   デコード成功時:
                *   `connections` をロック。
                *   送信元アドレスに対応する `Connection` が存在し、かつ `Closed` 状態でなければ `Connection::receive_packet` で処理。
                *   `receive_packet` が `Ok(Some(data))` を返し、`data` が空でなければ:
                    *   `callback` をロックし、`callback(src_addr, data)` を呼び出し。
            *   デコード失敗時は警告ログを出力。
        *   `recv_from` エラー時はエラーログを出力し、短時間スリープ。
    4.  ループ終了後（`running` が `false` になった場合）、タスク終了ログを出力。
*   **出力データ**:
    *   `Ok(())`: 受信タスクの起動に成功した場合。タスク自体はバックグラウンドで実行されます。
    *   **コールバック関数 `callback` への出力**:
        *   `src_addr: SocketAddr`: データ送信元のソケットアドレス。
        *   `data: Vec<u8>`: 受信したアプリケーションペイロード。順序保証されたデータ。

#### 3.8. `async fn NoiseResilientProtocol::start_maintenance(&self) -> Result<(), Error>`

接続のメンテナンス処理（タイムアウト処理、不要になった接続のクリーンアップなど）を行う非同期タスクを開始します。

*   **処理内容**:
    1.  `Arc` でラップされた `connections`, `socket`, `running` をクローンして新しいタスクにムーブ。
    2.  `maintenance_interval` (デフォルト100ms) を設定。
    3.  `tokio::task::spawn` を使用して非同期タスクを起動。
    4.  タスク内部ループ:
        *   `running` フラグが `true` の間、定期的に (インターバル開始から次のインターバル開始までが `maintenance_interval` となるように `sleep` を挟む):
            *   `connections` をロック。
            *   全ての `Connection` について `Connection::check_timeouts` を呼び出し、パケット再送や接続タイムアウト処理を実行。エラー発生時はログ出力し、状況によっては接続を強制的に閉じる。
            *   以下の条件に合致する接続を削除対象リストに追加:
                *   `connection.state == ConnectionState::Closed`。
                *   接続試行中のまま一定時間経過 (`connection.created_at.elapsed() > connection.config.connection_timeout * 2`)。
                *   接続済みだが一定時間アイドル (`connection.last_activity.elapsed() > connection.config.connection_timeout`)。
            *   削除対象リストに含まれる接続を `connections` マップから実際に削除。
    5.  ループ終了後（`running` が `false` になった場合）、タスク終了ログを出力。
*   **出力データ**:
    *   `Ok(())`: メンテナンスタスクの起動に成功した場合。タスク自体はバックグラウンドで実行されます。

#### 3.9. `async fn NoiseResilientProtocol::stop(&self)`

プロトコルの全てのバックグラウンドタスク（サーバー、受信、メンテナンス）を安全に停止させるためのシグナルを送ります。

*   **処理内容**:
    1.  `running` フラグを (ロックを取得して) `false` に設定。
    2.  これにより、各タスクのメインループが次のイテレーションで終了します。
    3.  （現在の実装では、タスクの完了を明示的に待つ処理はありません。）
*   **出力データ**: なし。

### 4. 注意事項

*   このプロトコルは基本的な信頼性機能を提供しますが、高度な輻輳制御アルゴリズム（例: TCP Vegas, Renoなど）やフロー制御は完全には実装されていません。
*   エラー処理は `std::io::Error` を中心に行われますが、より詳細なカスタムエラータイプを定義することで改善の余地があります。
*   セキュリティ（暗号化、認証など）は考慮されていません。
*   パフォーマンスはネットワーク状況や負荷によって変動します。大量の同時接続や高スループット環境では、さらなる最適化が必要になる場合があります。
*   ログ出力には `tracing` クレートが使用されています。利用側で適切な subscriber の設定が必要です。
