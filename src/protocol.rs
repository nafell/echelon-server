use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::UdpSocket;
use tokio::time::{sleep, Duration, Instant};
use tokio::task;

// パケットタイプの定義
#[derive(Debug, Clone, Copy, PartialEq)]
enum PacketType {
    Connect = 0,
    ConnectAck = 1,
    Data = 2,
    Ack = 3,
    Disconnect = 4,
}

impl From<u8> for PacketType {
    fn from(value: u8) -> Self {
        match value {
            0 => PacketType::Connect,
            1 => PacketType::ConnectAck,
            2 => PacketType::Data,
            3 => PacketType::Ack,
            4 => PacketType::Disconnect,
            _ => PacketType::Data, // デフォルトはデータパケット
        }
    }
}

// パケットヘッダ (16バイト固定)
// - packet_type: u8 - パケットタイプ
// - seq_num: u32 - シーケンス番号
// - ack_num: u32 - 確認応答番号
// - flags: u8 - フラグ (ビットフィールド)
// - checksum: u16 - チェックサム
// - payload_size: u16 - ペイロードサイズ
struct PacketHeader {
    packet_type: PacketType,
    seq_num: u32,
    ack_num: u32,
    flags: u8,
    checksum: u16,
    payload_size: u16,
}

impl PacketHeader {
    fn new(packet_type: PacketType, seq_num: u32, ack_num: u32, payload_size: u16) -> Self {
        Self {
            packet_type,
            seq_num,
            ack_num,
            flags: 0,
            checksum: 0, // 後で計算
            payload_size,
        }
    }

    fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0] = self.packet_type as u8;
        
        // seq_num (4バイト)
        bytes[1..5].copy_from_slice(&self.seq_num.to_be_bytes());
        
        // ack_num (4バイト)
        bytes[5..9].copy_from_slice(&self.ack_num.to_be_bytes());
        
        bytes[9] = self.flags;
        
        // checksum (2バイト)
        bytes[10..12].copy_from_slice(&self.checksum.to_be_bytes());
        
        // payload_size (2バイト)
        bytes[12..14].copy_from_slice(&self.payload_size.to_be_bytes());
        
        // 予約領域 (2バイト)
        bytes[14..16].fill(0);
        
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 16 {
            return Err(Error::new(ErrorKind::InvalidData, "パケットヘッダが短すぎます"));
        }

        let packet_type = PacketType::from(bytes[0]);
        
        let seq_num = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        let ack_num = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        
        let flags = bytes[9];
        let checksum = u16::from_be_bytes([bytes[10], bytes[11]]);
        let payload_size = u16::from_be_bytes([bytes[12], bytes[13]]);

        Ok(Self {
            packet_type,
            seq_num,
            ack_num,
            flags,
            checksum,
            payload_size,
        })
    }

    // 単純なチェックサム計算
    fn calculate_checksum(&self, payload: &[u8]) -> u16 {
        let mut sum: u32 = self.packet_type as u32 + 
                          self.seq_num + 
                          self.ack_num + 
                          self.flags as u32 + 
                          self.payload_size as u32;
        
        // ペイロードの各バイトを合計
        for &byte in payload {
            sum += byte as u32;
        }
        
        // 16ビットに切り詰め
        (sum & 0xFFFF) as u16
    }
}

// パケット構造体
struct Packet {
    header: PacketHeader,
    payload: Vec<u8>,
}

impl Packet {
    fn new(packet_type: PacketType, seq_num: u32, ack_num: u32, payload: Vec<u8>) -> Self {
        let payload_size = payload.len() as u16;
        let mut header = PacketHeader::new(packet_type, seq_num, ack_num, payload_size);
        header.checksum = header.calculate_checksum(&payload);
        
        Self { header, payload }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16 + self.payload.len());
        bytes.extend_from_slice(&self.header.to_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 16 {
            return Err(Error::new(ErrorKind::InvalidData, "パケットが短すぎます"));
        }

        let header = PacketHeader::from_bytes(&bytes[0..16])?;
        
        // ペイロードサイズチェック
        if bytes.len() < 16 + header.payload_size as usize {
            return Err(Error::new(ErrorKind::InvalidData, "ペイロードサイズが不正です"));
        }
        
        let payload = bytes[16..16 + header.payload_size as usize].to_vec();
        
        // チェックサム検証
        let calculated_checksum = header.calculate_checksum(&payload);
        if calculated_checksum != header.checksum {
            return Err(Error::new(ErrorKind::InvalidData, "チェックサムエラー"));
        }
        
        Ok(Self { header, payload })
    }
}

// 送信中パケット情報
struct PendingPacket {
    packet: Packet,
    first_sent: Instant,
    last_sent: Instant,
    transmissions: u32,
}

// 接続状態
#[derive(Debug, Clone, Copy, PartialEq)]
enum ConnectionState {
    Closed,
    Connecting,
    Connected,
    Disconnecting,
}

// 接続設定 (pub に変更)
#[derive(Clone)]
pub struct ConnectionConfig {
    // 基本的なRTT (初期値)
    initial_rtt: Duration,
    // 最小再送タイムアウト
    min_rto: Duration,
    // 最大再送タイムアウト
    max_rto: Duration,
    // 最大再送回数
    max_retransmissions: u32,
    // 受信ウィンドウサイズ
    receive_window_size: u32,
    // 最大パケットサイズ
    max_packet_size: usize,
    // 接続タイムアウト
    connection_timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            initial_rtt: Duration::from_millis(100),
            min_rto: Duration::from_millis(50),
            max_rto: Duration::from_secs(10),
            max_retransmissions: 10,
            receive_window_size: 64,
            max_packet_size: 1400, // 一般的なMTUより小さいサイズ
            connection_timeout: Duration::from_secs(30),
        }
    }
}

// 接続管理
struct Connection {
    peer_addr: SocketAddr,
    state: ConnectionState,
    config: ConnectionConfig,
    
    // シーケンス番号関連
    next_seq_num: u32,
    next_expected_seq: u32,
    
    // 再送制御
    rtt: Duration,
    rtt_var: Duration,
    rto: Duration,
    
    // 送信中のパケット
    pending_packets: HashMap<u32, PendingPacket>,
    
    // 受信済みパケット
    received_packets: HashMap<u32, Vec<u8>>,
    
    // タイムスタンプ (tokio::time::Instant を使用)
    last_activity: Instant,
    created_at: Instant,
}

impl Connection {
    fn new(peer_addr: SocketAddr, config: ConnectionConfig) -> Self {
        let now = Instant::now();
        let initial_rtt = config.initial_rtt;
        Self {
            peer_addr,
            state: ConnectionState::Closed,
            config,
            next_seq_num: 0,
            next_expected_seq: 0,
            rtt: initial_rtt,
            rtt_var: initial_rtt / 2,
            rto: initial_rtt * 2, // RTO計算を確認・調整する必要があるかも
            pending_packets: HashMap::new(),
            received_packets: HashMap::new(),
            last_activity: now,
            created_at: now,
        }
    }
    
    // パケット送信処理 (async に変更, socket は Arc<UdpSocket> を想定)
    async fn send_packet(&mut self, socket: &UdpSocket, packet_type: PacketType, payload: Vec<u8>) -> Result<u32, Error> {
        let seq_num = self.next_seq_num;
        self.next_seq_num += 1;
        
        let packet = Packet::new(packet_type, seq_num, self.next_expected_seq, payload);
        let packet_data = packet.to_bytes();
        
        // socket.send_to を使用 (async)
        socket.send_to(&packet_data, self.peer_addr).await?;
        
        // データパケットの場合は再送用に保存
        if packet_type == PacketType::Data || packet_type == PacketType::Connect {
            let now = Instant::now();
            self.pending_packets.insert(seq_num, PendingPacket {
                packet,
                first_sent: now,
                last_sent: now,
                transmissions: 1,
            });
        }
        
        self.last_activity = Instant::now();
        Ok(seq_num)
    }
    
    // パケット受信処理 (async に変更, send_packet を呼ぶため)
    async fn receive_packet(&mut self, socket: &UdpSocket, packet: Packet) -> Result<Option<Vec<u8>>, Error> {
        self.last_activity = Instant::now();
        
        match packet.header.packet_type {
            PacketType::Connect => {
                if self.state == ConnectionState::Closed {
                    self.state = ConnectionState::Connecting;
                    self.next_expected_seq = packet.header.seq_num + 1;
                    tracing::info!("CONNECT from {}", self.peer_addr); // println! を tracing に変更 (推奨)
                    // 接続確認応答を送信
                    let _ = self.send_packet(socket, PacketType::ConnectAck, Vec::new()).await?;
                    return Ok(Some(Vec::new())); // 接続要求を通知
                }
            },
            PacketType::ConnectAck => {
                if self.state == ConnectionState::Connecting {
                    self.state = ConnectionState::Connected;
                    // 受信確認されたパケットを削除
                    self.handle_ack(packet.header.ack_num);
                    tracing::info!("CONNECT_ACK from {}", self.peer_addr);
                    return Ok(Some(Vec::new())); // 接続確立を通知
                }
            },
            PacketType::Data => {
                tracing::info!("DATA from {}", self.peer_addr);

                // 常にACKを返す (重複受信でもACKは返す)
                let ack_payload = Vec::new(); // ACKパケットにはペイロード不要
                let _ = self.send_packet(socket, PacketType::Ack, ack_payload).await?;

                // 順序どおりのパケットを処理
                if packet.header.seq_num == self.next_expected_seq {
                    self.next_expected_seq += 1;
                    
                    // 受信確認処理
                    self.handle_ack(packet.header.ack_num);
                    
                    // 連続したパケットを探す
                    let mut result = packet.payload;
                    
                    // 次のシーケンス番号から連続して受信済みのものを処理
                    let mut next_seq = self.next_expected_seq;
                    while let Some(data) = self.received_packets.remove(&next_seq) {
                        result.extend_from_slice(&data);
                        self.next_expected_seq = next_seq + 1;
                        next_seq += 1;
                    }
                    
                    return Ok(Some(result));
                } else if packet.header.seq_num > self.next_expected_seq {
                    // 将来のパケットは保存 (ウィンドウサイズチェックを追加すると良い)
                    if !self.received_packets.contains_key(&packet.header.seq_num) {
                         self.received_packets.insert(packet.header.seq_num, packet.payload);
                    }
                    // 受信確認処理
                    self.handle_ack(packet.header.ack_num);
                }
                // 古いパケットは無視（ただしACKは処理）
                else {
                    self.handle_ack(packet.header.ack_num);
                }
            },
            PacketType::Ack => {
                // 受信確認処理
                tracing::info!("ACK from {}", self.peer_addr);
                self.handle_ack(packet.header.ack_num);
            },
            PacketType::Disconnect => {
                tracing::info!("DISCONNECT from {}", self.peer_addr);
                // 即座にACKを返す
                let _ = self.send_packet(socket, PacketType::Ack, Vec::new()).await?;
                self.state = ConnectionState::Closed; // サーバー側は受信したら閉じる
                return Ok(Some(Vec::new())); // 切断要求を通知 (データは空)
            }
        }
        
        Ok(None)
    }
    
    // ACK処理 (同期のままで良い)
    fn handle_ack(&mut self, ack_num: u32) {
        // ack_num以下のすべてのパケットを確認済みとする
        let mut acked_seqs = Vec::new();
        
        for (&seq, pending) in self.pending_packets.iter() {
            if seq <= ack_num {
                // RTT更新（TCPのRTT計算をベースにした簡略版）
                let sample_rtt = pending.first_sent.elapsed();
                
                // Jacobson's algorithm for RTT estimation
                let delta = if sample_rtt > self.rtt {
                    sample_rtt - self.rtt
                } else {
                    self.rtt - sample_rtt
                };
                
                self.rtt_var = (3 * self.rtt_var + delta) / 4;
                self.rtt = (7 * self.rtt + sample_rtt) / 8;
                
                // RTO更新
                self.rto = self.rtt + 4 * self.rtt_var;
                if self.rto < self.config.min_rto {
                    self.rto = self.config.min_rto;
                } else if self.rto > self.config.max_rto {
                    self.rto = self.config.max_rto;
                }
                
                acked_seqs.push(seq);
            }
        }
        
        // 確認済みパケットを削除
        for seq in acked_seqs {
            self.pending_packets.remove(&seq);
        }
    }
    
    // タイムアウトしたパケットを再送 (async に変更, socket は Arc<UdpSocket> を想定)
    async fn check_timeouts(&mut self, socket: &UdpSocket) -> Result<(), Error> {
        let now = Instant::now();
        let mut to_retransmit = Vec::new();
        let mut to_remove = Vec::new();
        
        for (&seq, pending) in self.pending_packets.iter() {
            if now.duration_since(pending.last_sent) > self.rto {
                if pending.transmissions < self.config.max_retransmissions {
                    to_retransmit.push(seq);
                } else {
                    to_remove.push(seq);
                }
            }
        }
        
        // 再送処理
        for seq in to_retransmit {
            if let Some(pending) = self.pending_packets.get_mut(&seq) {
                let packet_data = pending.packet.to_bytes();
                // socket.send_to を使用 (async)
                socket.send_to(&packet_data, self.peer_addr).await?;
                
                pending.last_sent = now;
                pending.transmissions += 1;
                
                // 指数バックオフ（再送のたびにRTOを2倍にする）
                self.rto = std::cmp::min(self.rto * 2, self.config.max_rto);
            }
        }
        
        // 最大再送回数を超えたパケットを削除
        for seq in to_remove {
            self.pending_packets.remove(&seq);
            if self.state == ConnectionState::Connecting && seq == 0 {
                // 接続要求のタイムアウト
                self.state = ConnectionState::Closed;
            }
        }
        
        // 接続タイムアウトチェック
        if self.state != ConnectionState::Closed && 
           now.duration_since(self.last_activity) > self.config.connection_timeout {
            self.state = ConnectionState::Closed;
        }
        
        Ok(())
    }
}

// 簡易プロトコルの実装
pub struct NoiseResilientProtocol {
    // socket を Arc<UdpSocket> に変更 (複数タスクから共有・利用するため)
    socket: Arc<UdpSocket>,
    // connections の Mutex を tokio::sync::Mutex に変更
    connections: Arc<Mutex<HashMap<SocketAddr, Connection>>>,
    config: ConnectionConfig,
    // running フラグも Mutex で保護 (あるいは Watch channel なども検討可能)
    running: Arc<Mutex<bool>>,
}

impl NoiseResilientProtocol {
    // new は async にせず、with_config の async 版を用意
    // pub async fn new(bind_addr: &str) -> Result<Self, Error> {
    //     Self::with_config(bind_addr, ConnectionConfig::default()).await
    // }
    
    // with_config を async に変更 (UdpSocket::bind が async のため)
    pub async fn with_config(bind_addr: &str, config: ConnectionConfig) -> Result<Self, Error> {
        let socket = UdpSocket::bind(bind_addr).await?;
        // set_nonblocking は Tokio の UdpSocket では不要 (常にノンブロッキング)
        
        Ok(Self {
            // socket を Arc で包む
            socket: Arc::new(socket),
            connections: Arc::new(Mutex::new(HashMap::new())),
            config,
            running: Arc::new(Mutex::new(true)),
        })
    }
    
    // サーバーモードで起動 (async に変更, tokio::spawn を使用)
    pub async fn start_server(&self) -> Result<(), Error> {
        let socket = Arc::clone(&self.socket);
        let connections = Arc::clone(&self.connections);
        let running = Arc::clone(&self.running);
        let config = self.config.clone(); // config も渡す

        // tokio::spawn で非同期タスクを開始
        task::spawn(async move {
            let mut buffer = vec![0u8; 2048]; // MTUに合わせたサイズが良い
            
            // running フラグを確認しながらループ
            while *running.lock().await {
                // socket.recv_from を使用 (async)
                // select! を使って停止シグナルも待つとより良い
                match socket.recv_from(&mut buffer).await {
                    Ok((size, src_addr)) => {
                        match Packet::from_bytes(&buffer[..size]) {
                            Ok(packet) => {
                                let mut conns = connections.lock().await;
                                
                                // 新規接続の場合 (CONNECT パケット受信)
                                if packet.header.packet_type == PacketType::Connect {
                                     if !conns.contains_key(&src_addr) {
                                        tracing::info!("New connection attempt from {}", src_addr);
                                        let mut connection = Connection::new(src_addr, config.clone()); // Clone config for new connection
                                        // receive_packet 内で ConnectAck が送られるはず
                                        match connection.receive_packet(&socket, packet).await {
                                             Ok(Some(_)) => { // 接続要求通知 (データは空)
                                                tracing::info!("Connection established with {}", src_addr);
                                                conns.insert(src_addr, connection);
                                             }
                                             Ok(None) => {
                                                // receive_packet が None を返すのは通常のエラーケース以外では考えにくいが、念のためログ
                                                tracing::warn!("receive_packet returned None during connect for {}", src_addr);
                                             }
                                             Err(e) => {
                                                tracing::error!("Error processing CONNECT packet from {}: {}", src_addr, e);
                                             }
                                        }
                                    } else {
                                        // 既に接続があるのにCONNECTが来た場合 (再送など)
                                        // 既存のConnectionで処理させる (ConnectAck再送など)
                                        if let Some(connection) = conns.get_mut(&src_addr) {
                                            if let Err(e) = connection.receive_packet(&socket, packet).await {
                                                 tracing::error!("Error re-processing CONNECT packet from {}: {}", src_addr, e);
                                            }
                                        }
                                    }
                                }
                                // 既存の接続へのパケット
                                else if let Some(connection) = conns.get_mut(&src_addr) {
                                    // 状態が Closed でなければ処理
                                    if connection.state != ConnectionState::Closed {
                                        match connection.receive_packet(&socket, packet).await {
                                            Ok(Some(data)) => {
                                                // サーバー側では通常データ受信時の処理は不要かもしれない
                                                // 必要ならここに処理を追加
                                                if !data.is_empty() {
                                                    tracing::debug!("Server received data from {}: {} bytes (discarding)", src_addr, data.len());
                                                }
                                            }
                                            Ok(None) => { /* ACK受信など、データがない場合 */ }
                                            Err(e) => {
                                                 tracing::error!("Error processing packet from {}: {}", src_addr, e);
                                            }
                                        }
                                    } else {
                                         tracing::warn!("Received packet from already closed connection: {}", src_addr);
                                    }

                                    // タイムアウトチェック (エラーはログ出力)
                                    // check_timeouts はロックの外で行うべきかもしれないが、ここでは簡単化のため内側で実行
                                    // if let Err(e) = connection.check_timeouts(&socket).await {
                                    //     tracing::error!("Timeout check error for {}: {}", src_addr, e);
                                    // }

                                    // Note: 接続削除はメンテナンスで行うため、ここでは削除しない
                                } else {
                                     tracing::warn!("Received packet from unknown source without CONNECT: {}", src_addr);
                                     // 不明な送信元からのパケットは無視するか、エラー応答を返すか検討
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Failed to parse packet from {}: {}", src_addr, e);
                            }
                        }
                    },
                    // WouldBlock の代わりにタイムアウトやエラーを処理
                    Err(e) => {
                        // エラーの種類に応じて処理 (例: ICMPエラーなど)
                        tracing::error!("Socket recv_from error: {}", e);
                        // エラーによってはループを継続できない場合もある
                        // sleep を入れて CPU 使用率の上昇を防ぐ
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            tracing::info!("Server task shutting down.");
        });
        
        Ok(())
    }
    
    // クライアントとして接続 (async に変更)
    pub async fn connect(&self, server_addr: &str) -> Result<(), Error> {
        let addr: SocketAddr = server_addr.parse().map_err(|e| Error::new(ErrorKind::InvalidInput, format!("Invalid server address: {}", e)))?;
        let mut connections = self.connections.lock().await; // Acquire lock asynchronously
        
        if connections.contains_key(&addr) {
            return Err(Error::new(ErrorKind::AlreadyExists, "既に接続されています"));
        }
        
        // socket は Arc なので clone するだけ
        let socket = Arc::clone(&self.socket);
        let mut connection = Connection::new(addr, self.config.clone());
        
        // 接続要求を送信 (async)
        connection.send_packet(&socket, PacketType::Connect, Vec::new()).await?;
        connection.state = ConnectionState::Connecting;
        
        connections.insert(addr, connection);
        
        Ok(())
    }

    // isConnected (async に変更, Mutex lock のため)
    pub async fn is_connected(&self, server_addr: &str) -> Result<bool, Error> {
        let addr: SocketAddr = server_addr.parse().map_err(|e| Error::new(ErrorKind::InvalidInput, format!("Invalid server address: {}", e)))?;
        let connections = self.connections.lock().await; // Acquire lock asynchronously
        if let Some(connection) = connections.get(&addr) {
            Ok(connection.state == ConnectionState::Connected)
        } else {
            Ok(false)
        }
    }
    
    // クライアントとしてデータ送信 (async に変更)
    pub async fn send(&self, server_addr: &str, data: &[u8]) -> Result<(), Error> {
        let addr: SocketAddr = server_addr.parse().map_err(|e| Error::new(ErrorKind::InvalidInput, format!("Invalid server address: {}", e)))?;
        let mut connections = self.connections.lock().await; // Acquire lock asynchronously
        
        if let Some(connection) = connections.get_mut(&addr) {
            if connection.state != ConnectionState::Connected {
                return Err(Error::new(ErrorKind::NotConnected, "接続されていません"));
            }
            
            // データを適切なサイズに分割
            // ヘッダサイズ(16)を考慮
            let max_payload_size = self.config.max_packet_size.saturating_sub(16);
            if max_payload_size == 0 {
                 return Err(Error::new(ErrorKind::InvalidInput, "max_packet_size is too small"));
            }
            let chunks = data.chunks(max_payload_size);
            let socket = Arc::clone(&self.socket); // socket を clone
            
            for chunk in chunks {
                connection.send_packet(&socket, PacketType::Data, chunk.to_vec()).await?;
            }
            
            Ok(())
        } else {
            Err(Error::new(ErrorKind::NotConnected, "接続が見つかりません"))
        }
    }
    
    // クライアントとして切断 (async に変更)
    pub async fn disconnect(&self, server_addr: &str) -> Result<(), Error> {
        let addr: SocketAddr = server_addr.parse().map_err(|e| Error::new(ErrorKind::InvalidInput, format!("Invalid server address: {}", e)))?;
        let mut connections = self.connections.lock().await; // Acquire lock asynchronously
        
        if let Some(connection) = connections.get_mut(&addr) {
            if connection.state == ConnectionState::Connected || connection.state == ConnectionState::Connecting {
                let socket = Arc::clone(&self.socket); // socket を clone
                connection.send_packet(&socket, PacketType::Disconnect, Vec::new()).await?;
                connection.state = ConnectionState::Disconnecting;
                // Disconnect 送信後、即座に削除せず、ACKを待つかタイムアウトで削除 (メンテナンス処理で行う)
                 tracing::info!("DISCONNECT sent to {}", addr);
            }
            Ok(())
        } else {
            Err(Error::new(ErrorKind::NotConnected, "接続が見つかりません"))
        }
    }
    
    // 受信ループ開始（async に変更, tokio::spawn, FnMut + Send + 'static のまま）
    // 注意: callback 内で非同期処理を行う場合は、callback 側で tokio::spawn する必要がある
    pub async fn start_receiver<F>(&self, mut callback: F) -> Result<(), Error>
    where
        F: FnMut(SocketAddr, Vec<u8>) + Send + Sync + 'static, // Sync を追加 (Arc<Mutex<..>> 内で使うため)
    {
        let socket = Arc::clone(&self.socket);
        let connections = Arc::clone(&self.connections);
        let running = Arc::clone(&self.running);
        let callback = Arc::new(Mutex::new(callback)); // CallbackもArc<Mutex>で包む

        // tokio::spawn で非同期タスクを開始
        task::spawn(async move {
            let mut buffer = vec![0u8; 2048]; // MTUに合わせたサイズが良い

            while *running.lock().await {
                 // select! を使って停止シグナルも待つとより良い
                match socket.recv_from(&mut buffer).await {
                    Ok((size, src_addr)) => {
                         match Packet::from_bytes(&buffer[..size]) {
                             Ok(packet) => {
                                let mut conns_guard = connections.lock().await;
                                if let Some(connection) = conns_guard.get_mut(&src_addr) {
                                    // 状態が Closed でなければ処理
                                    if connection.state != ConnectionState::Closed {
                                         match connection.receive_packet(&socket, packet).await {
                                            Ok(Some(data)) => {
                                                if !data.is_empty() {
                                                    // コールバック呼び出し (ロックを取得して呼び出す)
                                                    let mut cb_guard = callback.lock().await;
                                                    (*cb_guard)(src_addr, data);
                                                    // コールバック実行中は connections のロックを解放したい場合、
                                                    // データを clone してロック解除後に cb を呼ぶ等の工夫が必要
                                                }
                                                // 接続確立時や切断完了時の通知は receive_packet 内で行うか、
                                                // state の変化を監視する別の仕組みが必要
                                            }
                                            Ok(None) => { /* ACK受信など */ }
                                            Err(e) => {
                                                tracing::error!("Error processing packet in receiver from {}: {}", src_addr, e);
                                            }
                                        }
                                    } else {
                                         // tracing::warn!("Receiver received packet from already closed connection: {}", src_addr);
                                    }
                                } else {
                                     // サーバータスクで処理されるはずなので、ここでは通常何もしない
                                     // tracing::warn!("Receiver received packet from unknown source: {}", src_addr);
                                }
                             }
                             Err(e) => {
                                 tracing::warn!("Receiver failed to parse packet from {}: {}", src_addr, e);
                             }
                         }
                    },
                    Err(e) => {
                        tracing::error!("Receiver socket recv_from error: {}", e);
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            tracing::info!("Receiver task shutting down.");
        });
        
        Ok(())
    }
    
    // 定期的なメンテナンス処理 (async に変更, tokio::spawn, tokio::time::sleep)
    pub async fn start_maintenance(&self) -> Result<(), Error> {
        let connections = Arc::clone(&self.connections);
        let socket = Arc::clone(&self.socket); // socket も渡す (check_timeouts で必要)
        let running = Arc::clone(&self.running);
        let maintenance_interval = Duration::from_millis(100); // インターバル

        // tokio::spawn で非同期タスクを開始
        task::spawn(async move {
            while *running.lock().await {
                // インターバルの開始時刻
                let start = Instant::now();
                
                { // Mutex のスコープを限定
                    let mut conns = connections.lock().await;
                    let mut to_remove = Vec::new();
                    
                    for (addr, connection) in conns.iter_mut() {
                        // タイムアウトチェック (async に変更)
                        if let Err(e) = connection.check_timeouts(&socket).await {
                            tracing::error!("Timeout check error for {}: {}", addr, e);
                            // エラーによっては接続を切断する必要があるかもしれない
                            // 例: 何度も再送に失敗した場合など
                            if connection.state != ConnectionState::Closed {
                                 tracing::warn!("Closing connection {} due to timeout check error: {}", addr, e);
                                connection.state = ConnectionState::Closed; // 強制的に閉じる
                            }
                        }
                        
                        // 切断済み接続、またはタイムアウトした接続を削除対象に
                        if connection.state == ConnectionState::Closed || 
                           (connection.state != ConnectionState::Connected && connection.created_at.elapsed() > connection.config.connection_timeout * 2) || // 接続試行タイムアウト
                           (connection.state == ConnectionState::Connected && connection.last_activity.elapsed() > connection.config.connection_timeout) // アイドルタイムアウト
                         {
                            if connection.state != ConnectionState::Closed {
                                tracing::info!("Connection timed out for {}", addr);
                                connection.state = ConnectionState::Closed; // 削除前に状態を Closed に
                            }
                            to_remove.push(*addr);
                        }
                    }
                    
                    // 切断済み接続を実際に削除
                    for addr in to_remove {
                        tracing::info!("Removing closed/timed-out connection: {}", addr);
                        conns.remove(&addr);
                    }
                } // Mutex ロック解放
                
                // 次の実行まで待機 (処理時間を考慮)
                let elapsed = start.elapsed();
                if elapsed < maintenance_interval {
                    sleep(maintenance_interval - elapsed).await;
                }
            }
             tracing::info!("Maintenance task shutting down.");
        });
        
        Ok(())
    }
    
    // プロトコルの停止 (async に変更, Mutex lock のため)
    pub async fn stop(&self) {
        let mut running = self.running.lock().await;
        *running = false;
        // TODO: 必要であれば、各タスクにシャットダウン通知を送り、完了を待つ処理を追加
        tracing::info!("Stop signal sent.");
    }
}
