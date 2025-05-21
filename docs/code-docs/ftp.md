# FTP観測システム仕様書

## 1. システム概要
このシステムは、FTPサーバーから定期的にデータを取得し、摩耗データ（WearReading）を解析するためのシステムです。

## 2. 主要な構造体

### 2.1 FtpObservationConfig
```rust
pub struct FtpObservationConfig {
    pub host: String,              // FTPサーバーのホスト名
    pub username: String,          // FTPユーザー名
    pub password: String,          // FTPパスワード
    pub directory: String,         // 監視対象ディレクトリ
    pub facility_name: String,     // 施設名
    pub machine_type: String,      // 機械タイプ
    pub equipment_id: String,      // 設備ID
    pub equipment_version: String, // 設備バージョン
    pub oneshot: bool,            // ワンショット実行フラグ
}
```

### 2.2 FtpObservationClient
FTP観測を管理するクライアントクラスです。

## 3. 主要な関数とその仕様

### 3.1 list_files_by_date
**入力:**
- host: FTPサーバーのホスト名
- username: FTPユーザー名
- password: FTPパスワード
- directory: 対象ディレクトリ

**出力:**
- `Result<Vec<String>>`: ファイル名のリスト（昇順でソート済み）

**処理内容:**
1. FTPサーバーに接続
2. 指定ディレクトリに移動
3. ファイル一覧を取得
4. ファイル名を抽出してソート

### 3.2 download_file_from_ftp
**入力:**
- host: FTPサーバーのホスト名
- username: FTPユーザー名
- password: FTPパスワード
- file_path: ダウンロードするファイルのパス

**出力:**
- `Result<String>`: ファイルの内容（文字列）

**処理内容:**
1. FTPサーバーに接続
2. バイナリモードに設定
3. 指定ファイルをダウンロード
4. 内容を文字列として返却

### 3.3 parse_wear_reading_from_csv_line
**入力:**
- csv_data: CSVの1行のデータ
- facility_name: 施設名
- machine_type: 機械タイプ
- equipment_id: 設備ID
- equipment_version: 設備バージョン

**出力:**
- `Result<WearReading>`: 摩耗データの構造体

**処理内容:**
1. CSVデータをカンマで分割
2. 時間データをパース（日本時間を考慮）
3. 102個の数値データをパース
4. WearReading構造体を生成

### 3.4 observe_ftp
**入力:**
- host: FTPサーバーのホスト名
- username: FTPユーザー名
- password: FTPパスワード
- file_path: 監視対象ディレクトリ
- facility_name: 施設名
- machine_type: 機械タイプ
- equipment_id: 設備ID
- equipment_version: 設備バージョン
- oneshot: ワンショット実行フラグ

**出力:**
- `Result<Vec<WearReading>>`: 摩耗データのリスト

**処理内容:**
1. 前回処理したファイル名を読み込み
2. FTPサーバーからファイル一覧を取得
3. 前回処理したファイル以降の新しいファイルを処理
4. 各ファイルから摩耗データを抽出
5. 最後に処理したファイル名を保存

### 3.5 start_observation
**入力:**
- callback: 摩耗データを受け取るコールバック関数

**出力:**
- `Result<()>`: 処理結果

**処理内容:**
1. 5秒間隔でFTPサーバーを監視
2. 新しいデータが見つかった場合、コールバック関数を呼び出し
3. エラーが発生した場合はログに記録

## 4. 補助機能

### 4.1 カーソル管理
- `save_cursor_filename_to_file`: 最後に処理したファイル名を保存
- `load_cursor_filename_from_file`: 最後に処理したファイル名を読み込み

## 5. エラーハンドリング
- FTP接続エラー
- ファイルパースエラー
- データ形式エラー
- ファイル操作エラー

## 6. ログ出力
- トレースレベル: デバッグ情報
- 情報レベル: 処理開始・終了
- エラーレベル: エラー発生時

このシステムは、FTPサーバーから定期的にデータを取得し、CSVファイルを解析して摩耗データを抽出する機能を提供します。非同期処理を活用し、効率的なデータ取得と処理を実現しています。
