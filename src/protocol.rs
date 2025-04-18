use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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

// 接続設定
#[derive(Clone)]
struct ConnectionConfig {
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
    
    // タイムスタンプ
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
            rto: initial_rtt * 2,
            pending_packets: HashMap::new(),
            received_packets: HashMap::new(),
            last_activity: now,
            created_at: now,
        }
    }
    
    // パケット送信処理
    fn send_packet(&mut self, socket: &UdpSocket, packet_type: PacketType, payload: Vec<u8>) -> Result<u32, Error> {
        let seq_num = self.next_seq_num;
        self.next_seq_num += 1;
        
        let packet = Packet::new(packet_type, seq_num, self.next_expected_seq, payload);
        let packet_data = packet.to_bytes();
        
        socket.send_to(&packet_data, self.peer_addr)?;
        
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
    
    // パケット受信処理
    fn receive_packet(&mut self, packet: Packet) -> Result<Option<Vec<u8>>, Error> {
        self.last_activity = Instant::now();
        
        match packet.header.packet_type {
            PacketType::Connect => {
                if self.state == ConnectionState::Closed {
                    self.state = ConnectionState::Connecting;
                    self.next_expected_seq = packet.header.seq_num + 1;
                    println!("CONNECT from {}", self.peer_addr);
                    return Ok(Some(Vec::new())); // 接続要求を通知
                }
            },
            PacketType::ConnectAck => {
                if self.state == ConnectionState::Connecting {
                    self.state = ConnectionState::Connected;
                    // 受信確認されたパケットを削除
                    self.handle_ack(packet.header.ack_num);
                    println!("CONNECT_ACK from {}", self.peer_addr);
                    return Ok(Some(Vec::new())); // 接続確立を通知
                }
            },
            PacketType::Data => {
                // 順序どおりのパケットを処理
                println!("DATA from {}", self.peer_addr);
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
                    // 将来のパケットは保存
                    self.received_packets.insert(packet.header.seq_num, packet.payload);
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
                println!("ACK from {}", self.peer_addr);
                self.handle_ack(packet.header.ack_num);
            },
            PacketType::Disconnect => {
                println!("DISCONNECT from {}", self.peer_addr);
                self.state = ConnectionState::Disconnecting;
                return Ok(Some(Vec::new())); // 切断要求を通知
            }
        }
        
        Ok(None)
    }
    
    // ACK処理
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
    
    // タイムアウトしたパケットを再送
    fn check_timeouts(&mut self, socket: &UdpSocket) -> Result<(), Error> {
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
                socket.send_to(&packet_data, self.peer_addr)?;
                
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
    socket: UdpSocket,
    connections: Arc<Mutex<HashMap<SocketAddr, Connection>>>,
    config: ConnectionConfig,
    running: Arc<Mutex<bool>>,
}

impl NoiseResilientProtocol {
    pub fn new(bind_addr: &str) -> Result<Self, Error> {
        let socket = UdpSocket::bind(bind_addr)?;
        socket.set_nonblocking(true)?;
        
        Ok(Self {
            socket,
            connections: Arc::new(Mutex::new(HashMap::new())),
            config: ConnectionConfig::default(),
            running: Arc::new(Mutex::new(true)),
        })
    }
    
    pub fn with_config(bind_addr: &str, config: ConnectionConfig) -> Result<Self, Error> {
        let socket = UdpSocket::bind(bind_addr)?;
        socket.set_nonblocking(true)?;
        
        Ok(Self {
            socket,
            connections: Arc::new(Mutex::new(HashMap::new())),
            config,
            running: Arc::new(Mutex::new(true)),
        })
    }
    
    // サーバーモードで起動
    pub fn start_server(&self) -> Result<(), Error> {
        let socket = self.socket.try_clone()?;
        let connections = Arc::clone(&self.connections);
        let running = Arc::clone(&self.running);
        
        thread::spawn(move || {
            let mut buffer = vec![0u8; 2048];
            
            while *running.lock().unwrap() {
                match socket.recv_from(&mut buffer) {
                    Ok((size, src_addr)) => {
                        if let Ok(packet) = Packet::from_bytes(&buffer[..size]) {
                            let mut conns = connections.lock().unwrap();
                            
                            // 新規接続の場合
                            if !conns.contains_key(&src_addr) && packet.header.packet_type == PacketType::Connect {
                                let mut connection = Connection::new(src_addr, ConnectionConfig::default());
                                connection.state = ConnectionState::Connecting;
                                connection.next_expected_seq = packet.header.seq_num + 1;
                                
                                // 接続確認応答
                                let _ = connection.send_packet(&socket, PacketType::ConnectAck, Vec::new());
                                conns.insert(src_addr, connection);
                            }
                            // 既存の接続
                            else if let Some(connection) = conns.get_mut(&src_addr) {
                                if let Ok(Some(_)) = connection.receive_packet(packet) {
                                    // データ受信時の処理（サーバー側での実装）
                                    
                                    // 接続確認応答
                                    if connection.state == ConnectionState::Connecting {
                                        let _ = connection.send_packet(&socket, PacketType::ConnectAck, Vec::new());
                                    }
                                    
                                    // 切断確認応答
                                    if connection.state == ConnectionState::Disconnecting {
                                        let _ = connection.send_packet(&socket, PacketType::Ack, Vec::new());
                                        connection.state = ConnectionState::Closed;
                                    }
                                }
                                
                                // タイムアウトチェック
                                let _ = connection.check_timeouts(&socket);
                                
                                // 切断された接続を削除
                                if connection.state == ConnectionState::Closed {
                                    // 実際の実装では定期的なクリーンアップで削除する方が良い
                                }
                            }
                        }
                    },
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        // ノンブロッキングなので、データがないときはここに来る
                        thread::sleep(Duration::from_millis(10));
                    },
                    Err(e) => {
                        eprintln!("受信エラー: {}", e);
                    }
                }
            }
        });
        
        Ok(())
    }
    
    // クライアントとして接続
    pub fn connect(&self, server_addr: &str) -> Result<(), Error> {
        let addr: SocketAddr = server_addr.parse().map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let mut connections = self.connections.lock().unwrap();
        
        if connections.contains_key(&addr) {
            return Err(Error::new(ErrorKind::AlreadyExists, "既に接続されています"));
        }
        
        let socket_clone = self.socket.try_clone()?;
        let mut connection = Connection::new(addr, self.config.clone());
        
        // 接続要求を送信
        connection.send_packet(&socket_clone, PacketType::Connect, Vec::new())?;
        connection.state = ConnectionState::Connecting;
        
        connections.insert(addr, connection);
        
        Ok(())
    }

    pub fn isConnected(&self, server_addr: &str) -> Result<bool, Error> {
        let addr: SocketAddr = server_addr.parse().map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let connections = self.connections.lock().unwrap();
        if let Some(connection) = connections.get(&addr) {
            Ok(connection.state == ConnectionState::Connected)
        } else {
            Ok(false)
        }
    }
    
    // クライアントとしてデータ送信
    pub fn send(&self, server_addr: &str, data: &[u8]) -> Result<(), Error> {
        let addr: SocketAddr = server_addr.parse().map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let mut connections = self.connections.lock().unwrap();
        
        if let Some(connection) = connections.get_mut(&addr) {
            if connection.state != ConnectionState::Connected {
                return Err(Error::new(ErrorKind::NotConnected, "接続されていません"));
            }
            
            // データを適切なサイズに分割
            let chunks = data.chunks(self.config.max_packet_size - 16);
            let socket_clone = self.socket.try_clone()?;
            
            for chunk in chunks {
                connection.send_packet(&socket_clone, PacketType::Data, chunk.to_vec())?;
            }
            
            Ok(())
        } else {
            Err(Error::new(ErrorKind::NotConnected, "接続が見つかりません"))
        }
    }
    
    // クライアントとして切断
    pub fn disconnect(&self, server_addr: &str) -> Result<(), Error> {
        let addr: SocketAddr = server_addr.parse().map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let mut connections = self.connections.lock().unwrap();
        
        if let Some(connection) = connections.get_mut(&addr) {
            if connection.state == ConnectionState::Connected {
                let socket_clone = self.socket.try_clone()?;
                connection.send_packet(&socket_clone, PacketType::Disconnect, Vec::new())?;
                connection.state = ConnectionState::Disconnecting;
            }
            Ok(())
        } else {
            Err(Error::new(ErrorKind::NotConnected, "接続が見つかりません"))
        }
    }
    
    // 受信ループ開始（S/C共通）
    pub fn start_receiver<F>(&self, mut callback: F) -> Result<(), Error>
    where
        F: FnMut(SocketAddr, Vec<u8>) + Send + 'static,
    {
        let socket = self.socket.try_clone()?;
        let connections = Arc::clone(&self.connections);
        let running = Arc::clone(&self.running);
        
        thread::spawn(move || {
            let mut buffer = vec![0u8; 2048];
            
            while *running.lock().unwrap() {
                match socket.recv_from(&mut buffer) {
                    Ok((size, src_addr)) => {
                        if let Ok(packet) = Packet::from_bytes(&buffer[..size]) {
                            let mut conns = connections.lock().unwrap();
                            
                            if let Some(connection) = conns.get_mut(&src_addr) {
                                if let Ok(Some(data)) = connection.receive_packet(packet) {
                                    // 空でないデータを受信したらコールバック
                                    if !data.is_empty() {
                                        callback(src_addr, data);
                                    }
                                    
                                    // 接続確立時
                                    if connection.state == ConnectionState::Connected {
                                        // ここでユーザーに通知
                                    }
                                    
                                    // 切断確認時
                                    if connection.state == ConnectionState::Closed {
                                        // ここでユーザーに通知
                                    }
                                }
                                
                                // タイムアウトチェック
                                let _ = connection.check_timeouts(&socket);
                            }
                        }
                    },
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        // ノンブロッキングなので、データがないときはここに来る
                        thread::sleep(Duration::from_millis(10));
                    },
                    Err(e) => {
                        eprintln!("受信エラー: {}", e);
                    }
                }
            }
        });
        
        Ok(())
    }
    
    // 定期的なメンテナンス処理
    pub fn start_maintenance(&self) -> Result<(), Error> {
        let connections = Arc::clone(&self.connections);
        let socket = self.socket.try_clone()?;
        let running = Arc::clone(&self.running);
        
        thread::spawn(move || {
            while *running.lock().unwrap() {
                {
                    let mut conns = connections.lock().unwrap();
                    let mut to_remove = Vec::new();
                    
                    for (addr, connection) in conns.iter_mut() {
                        // タイムアウトチェック
                        if let Err(e) = connection.check_timeouts(&socket) {
                            eprintln!("タイムアウトチェックエラー: {} for {}", e, addr);
                        }
                        
                        // 切断済み接続を削除
                        if connection.state == ConnectionState::Closed {
                            to_remove.push(*addr);
                        }
                    }
                    
                    // 切断済み接続を削除
                    for addr in to_remove {
                        conns.remove(&addr);
                    }
                }
                
                thread::sleep(Duration::from_millis(100));
            }
        });
        
        Ok(())
    }
    
    // プロトコルの停止
    pub fn stop(&self) {
        let mut running = self.running.lock().unwrap();
        *running = false;
    }
}

// 使用例
fn example_usage() {
    // サーバー側
    let server = NoiseResilientProtocol::new("127.0.0.1:12345").unwrap();
    server.start_server().unwrap();
    server.start_maintenance().unwrap();
    
    // クライアント側
    let client = NoiseResilientProtocol::new("127.0.0.1:0").unwrap();
    client.connect("127.0.0.1:12345").unwrap();
    
    // データ受信コールバック
    client.start_receiver(|addr, data| {
        println!("{}からデータを受信: {:?}", addr, data);
    }).unwrap();
    
    // データ送信
    thread::sleep(Duration::from_secs(1)); // 接続待ち
    client.send("127.0.0.1:12345", b"Hello, world!").unwrap();
    
    // 切断
    thread::sleep(Duration::from_secs(5));
    client.disconnect("127.0.0.1:12345").unwrap();
    
    // 停止
    thread::sleep(Duration::from_secs(1));
    client.stop();
    server.stop();
}

pub fn server() {
    let server = NoiseResilientProtocol::new("127.0.0.1:12345").unwrap();
    server.start_server().unwrap();
    server.start_maintenance().unwrap();

    server.start_receiver(|addr, data| {
        println!("Server received data from {}: {}", addr, String::from_utf8_lossy(&data));
    }).unwrap();
    println!("Server started, waiting for connections...");

    // クライアントからのメッセージ受信などを待機 (デモのためしばらく待つ)
    println!("Server waiting for client message or timeout...");
    thread::sleep(Duration::from_secs(30));
    println!("Server shutting down.");
    server.stop();
}

pub fn client() {
    let client = NoiseResilientProtocol::new("127.0.0.1:0").unwrap();
    let server_addr = "127.0.0.1:12345";

    // データ受信コールバックを設定
    client.start_receiver(|addr, data| {
        println!("Client received data from {}: {}", addr, String::from_utf8_lossy(&data));
    }).unwrap();
    println!("Client started, waiting for connections...");

    println!("Client connecting to {}...", server_addr);
    match client.connect(server_addr) {
        Ok(_) => println!("Client connection initiated to {}.", server_addr),
        Err(e) => {
            eprintln!("Client connect error: {}", e);
            client.stop();
            return;
        }
    }

    // 接続が確立(CONNECT_ACKを受信)するまで待つ 
    // while !client.isConnected(server_addr).unwrap() {
    //     println!("Waiting CONNECT_ACK from {}...", server_addr);
    //     thread::sleep(Duration::from_secs(1));
    // }

    // 接続待ち(デモ)
    println!("Waiting for ACK...");
    thread::sleep(Duration::from_secs(2));

    println!("Client sending message to {}...", server_addr);
    match client.send(server_addr, b"Hello from client!") {
        Ok(_) => println!("Client sent message."),
        Err(e) => eprintln!("Client send error: {}", e),
    }

    // 送信完了を待つ
    thread::sleep(Duration::from_secs(5));
    
    println!("Client shutting down.");
    client.stop();
}

