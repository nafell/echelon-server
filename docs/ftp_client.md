## `ftp_client.rs` モジュール仕様書

### 1. 概要

このモジュールは、指定されたFTPサーバーに接続し、特定のディレクトリ内のファイルを監視する機能を提供します。ファイルは更新日時順に処理され、各ファイルの内容を行単位で読み込みます。最後に読み込んだファイル名と行番号を状態として保存し、再起動時に中断した箇所から処理を再開できます。読み込んだ行データは、設定されたチャネルを通じて他のタスクに送信されます。

### 2. 主要な構造体

*   **`FtpReaderState`**
    *   説明: FTPリーダーの現在の状態を保持します。JSON形式でファイルに保存・読み込みされます。
    *   フィールド:
        *   `last_file: Option<String>`: 最後に処理したファイルの名前。まだ何も処理していない場合は `None`。
        *   `last_line_index: usize`: `last_file` 内で最後に処理した行の次の行インデックス（0ベース）。つまり、次に読み込むべき行のインデックス。
    *   出力データ形式: シリアライズされると以下のようなJSONになります。
        ```json
        {
          "last_file": "log_20240101.txt",
          "last_line_index": 150
        }
        ```
        または、初期状態では
        ```json
        {
          "last_file": null,
          "last_line_index": 0
        }
        ```

*   **`FtpFileEntry`**
    *   説明: FTPサーバー上の1つのファイルエントリを表します。ファイル名と最終更新日時を持ちます。
    *   フィールド:
        *   `name: String`: ファイル名。
        *   `modified: NaiveDateTime`: ファイルの最終更新日時（タイムゾーン情報なし）。
    *   出力データ形式: この構造体自体が直接外部に出力されることは少ないですが、`get_sorted_files` の結果として `Vec<FtpFileEntry>` が返されます。

*   **`FtpReaderConfig`**
    *   説明: `FtpReader` の動作に必要な設定情報を保持します。
    *   フィールド:
        *   `host: String`: 接続先FTPサーバーのホスト名またはIPアドレス。
        *   `user: String`: FTPログインユーザー名。
        *   `pass: String`: FTPログインパスワード。
        *   `remote_dir: String`: 監視対象のリモートディレクトリパス。
        *   `state_file: PathBuf`: `FtpReaderState` を保存/読み込みするファイルのパス。
        *   `line_sender: mpsc::Sender<String>`: 読み込んだ行データを送信するための非同期チャネル送信側。
        *   `shutdown_rx: watch::Receiver<()>`: シャットダウン通知を受け取るための監視チャネル受信側。

*   **`FtpReader`**
    *   説明: FTP接続、ファイルリスト取得、ファイル読み込み、状態管理など、主要なロジックを実行する構造体。
    *   フィールド:
        *   `config: FtpReaderConfig`: 設定情報。
        *   `state: FtpReaderState`: 現在の状態。
        *   `ftp_stream: Option<FtpStream>`: 現在のFTP接続ストリーム。接続されていない場合は `None`。

### 3. 主要な関数/メソッド

*   **`FtpReader::new(config: FtpReaderConfig) -> Result<Self>`**
    *   処理内容: `FtpReader` インスタンスを生成します。引数で受け取った `config` を保持し、`config.state_file` から `load_state` を呼び出して状態を復元します。状態ファイルが存在しない、または読み込めない場合は、デフォルトの `FtpReaderState` で初期化されます。
    *   入力: `FtpReaderConfig`
    *   出力: 初期化された `FtpReader` インスタンス (`Result::Ok`)、または状態ファイルの読み込みに失敗した場合のエラー (`Result::Err`)。

*   **`FtpReader::load_state(path: &Path) -> Result<FtpReaderState>`**
    *   処理内容: 指定された `path` から状態ファイル（JSON）を非同期に読み込み、`FtpReaderState` 構造体にデシリアライズします。ファイルが存在しない場合は `FtpReaderState::default()` を返します。
    *   入力: 状態ファイルのパス (`&Path`)。
    *   出力: 読み込まれた `FtpReaderState` (`Result::Ok`)、またはファイルの読み込みやJSONパースに失敗した場合のエラー (`Result::Err`)。

*   **`FtpReader::save_state(&self) -> Result<()>`**
    *   処理内容: 現在の `self.state` をJSON形式にシリアライズし、`self.config.state_file` で指定されたパスに非同期に書き込みます。
    *   入力: `&self`
    *   出力: 書き込み成功時は `Result::Ok(())`、シリアライズやファイル書き込みに失敗した場合はエラー (`Result::Err`)。

*   **`FtpReader::ensure_connection(&mut self) -> Result<()>`**
    *   処理内容: 現在のFTP接続 (`self.ftp_stream`) が有効か確認します (`PWD` コマンドを試行)。接続が存在しないか、無効になっている場合は、`self.config` の情報を使ってFTPサーバーに再接続し、ログイン、ディレクトリ変更 (`CWD`) を行います。成功すれば `self.ftp_stream` が更新されます。
    *   入力: `&mut self`
    *   出力: 接続が有効であるか、再接続に成功した場合は `Result::Ok(())`、接続やログイン、ディレクトリ変更に失敗した場合はエラー (`Result::Err`)。

*   **`FtpReader::get_stream_mut(&mut self) -> Result<&mut FtpStream>`**
    *   処理内容: 内部で保持している `FtpStream` への可変参照を返します。通常、`ensure_connection` の後に呼び出され、FTP操作を行うために使用されます。`self.ftp_stream` が `None` の場合（接続がない場合）はエラーを返します。
    *   入力: `&mut self`
    *   出力: `FtpStream` への可変参照 (`Result::Ok`)、またはストリームが存在しない場合のエラー (`Result::Err`)。

*   **`FtpReader::parse_windows_dir_line(line: &str) -> Option<FtpFileEntry>`**
    *   処理内容: Windows FTPサーバーの `LIST` コマンドが出力する形式の文字列1行を解析します。日付、時刻、ファイル名を抽出し、`NaiveDateTime` に変換して `FtpFileEntry` を生成します。ディレクトリを示す行 (`<DIR>`) や、日付時刻のパースに失敗した行、ファイル名が空の行の場合は `None` を返します。日付フォーマットは `"%m-%d-%y %I:%M %p"` (例: `01-15-24 10:30 AM`) を期待します。
    *   入力: `LIST` コマンドの出力1行 (`&str`)。
    *   出力: 解析成功時は `Some(FtpFileEntry)`、失敗時は `None`。
        *   `FtpFileEntry` データ例: `{ name: "data.log", modified: 2024-01-15T10:30:00 }`

*   **`FtpReader::get_sorted_files(&mut self) -> Result<Vec<FtpFileEntry>>`**
    *   処理内容: FTP接続を確認・確保 (`ensure_connection`) し、リモートディレクトリ (`self.config.remote_dir`) のファイルリストを取得 (`LIST` コマンド) します。取得した各行を `parse_windows_dir_line` で解析し、`FtpFileEntry` のベクターを作成します。最後に、このベクターを `modified` (更新日時) の昇順でソートします。
    *   入力: `&mut self`
    *   出力: ソート済みの `FtpFileEntry` のベクター (`Result::Ok`)、または接続エラー、リスト取得エラー、解析エラーが発生した場合のエラー (`Result::Err`)。
        *   出力データ例: `Ok([ { name: "log1.txt", modified: ... }, { name: "log2.txt", modified: ... } ])`

*   **`FtpReader::read_file_from_line(&mut self, filename: &str, start_line_index: usize) -> Result<(usize, Vec<String>)>`**
    *   処理内容: FTP接続を確認・確保 (`ensure_connection`) し、指定された `filename` のファイルを `RETR` コマンドで取得開始します。取得したデータストリームを `BufReader` でラップし、指定された `start_line_index` まで行を読み飛ばします。その後、ファイルの終端まで1行ずつ読み込み、改行コード (`\r\n` または `\n`) を除去した文字列を `Vec<String>` に格納します。読み飛ばしを含めたファイルの総行数と、`start_line_index` 以降に読み込んだ行データのベクターをタプルで返します。
    *   入力:
        *   `&mut self`
        *   `filename: &str`: 読み込むファイル名。
        *   `start_line_index: usize`: 読み込みを開始する行のインデックス (0ベース)。
    *   出力: `Result<(usize, Vec<String>)>`
        *   `Ok((total_lines, new_lines))`:
            *   `total_lines`: ファイル全体の総行数。
            *   `new_lines`: `start_line_index` から読み込んだ新しい行の内容のベクター。
        *   `Err(error)`: 接続エラー、`RETR` コマンド失敗、データ読み込みエラーなどが発生した場合。
        *   出力データ例: `Ok((500, ["line 151 data", "line 152 data", ...]))` (もし `start_line_index` が 150 で、ファイルが500行あり、151行目以降を読み込んだ場合)

*   **`FtpReader::run(mut self) -> Result<()>`**
    *   処理内容: `FtpReader` のメイン実行ループです。`tokio::select!` を使用して、シャットダウンシグナル (`self.config.shutdown_rx`) の受信と `process_files` メソッドの実行を監視します。
        *   シャットダウンシグナルを受信すると、FTP接続を終了 (`QUIT`) し、ループを抜けて `Ok(())` を返します。
        *   `process_files` を呼び出し、ファイル処理を行います。
            *   `process_files` が `Ok(true)` (何らかの処理が行われた) を返した場合、短い待機 (1秒) を入れます。
            *   `process_files` が `Ok(false)` (新しいデータやファイルがなかった) を返した場合、少し長い待機 (3秒) を入れます。
            *   `process_files` が `Err(e)` を返した場合、エラーをログ記録し、FTP接続をリセット (`self.ftp_stream = None`) して、長めの待機 (10秒) を入れてからループを継続します。
    *   入力: `self` (所有権を移動)
    *   出力: 正常にシャットダウンした場合は `Result::Ok(())`、ループ中に回復不能なエラー（例: チャネル送信失敗）が発生した場合はエラー (`Result::Err`)。

*   **`FtpReader::process_files(&mut self, file_list: &mut Vec<FtpFileEntry>, current_file_index: &mut Option<usize>) -> Result<bool>`**
    *   処理内容: ファイル処理の中核ロジック。
        1.  `get_sorted_files` を呼び出し、最新のファイルリストを取得します。
        2.  取得したリストが、引数で渡された `file_list` (前回のキャッシュ) と異なるか確認します。
        3.  リストが変更されていた場合:
            *   `file_list` を新しいリストで更新します。
            *   処理対象のファイルインデックス (`current_file_index`) をリセットします。
            *   `self.state.last_file` が新しいリスト内に存在すれば、そのインデックスを `current_file_index` に設定して再開します。存在しなければ、リストの先頭 (`0`) を `current_file_index` に設定し、`self.state` を初期化（最初のファイル、行インデックス0）します。
            *   リストが空の場合は `current_file_index` は `None` のままです。
            *   変更後の `self.state` を `save_state` で保存します。
            *   処理が行われたことを示すため、`processed` フラグを `true` にします。
        4.  現在の処理対象ファイル (`current_file`) と開始行 (`current_start_line`) を `current_file_index` と `self.state` から決定します。処理対象がない場合は `Ok(processed)` を返して終了します。
        5.  `read_file_from_line` を呼び出し、`current_file` を `current_start_line` から読み込みます。
        6.  新しい行 (`new_lines`) が読み込めた場合:
            *   各行を `self.config.line_sender` チャネルに送信します。送信に失敗した場合は即座にエラー (`Err`) を返します。
            *   送信成功ごとに `self.state.last_line_index` をインクリメントし、`save_state` で状態を保存します。（注: 現状の実装では行ごとではなく、ループ後に状態を保存している箇所もありますが、意図としては行ごとに進捗を保存することに近い）
            *   `self.state.last_file` を現在のファイル名で更新し、`save_state` を呼び出します。
            *   `processed` フラグを `true` にします。
        7.  新しい行がなかった (`new_lines` が空) 場合:
            *   `read_file_from_line` が返した総行数 (`total_lines_in_file`) と現在の `self.state.last_line_index` を比較し、ファイルの終端に達したか (`reached_eof`) を判断します。
            *   `reached_eof` が `true` で、かつ `file_list` に次のファイルが存在する場合:
                *   `current_file_index` をインクリメントします。
                *   `self.state` を次のファイルの先頭（ファイル名更新、行インデックス0）に設定します。
                *   `save_state` を呼び出します。
                *   `processed` フラグを `true` にします。
        8.  最終的に `processed` フラグの値を `Ok` で返します。
    *   入力:
        *   `&mut self`
        *   `file_list: &mut Vec<FtpFileEntry>`: 前回取得したファイルリスト（キャッシュ）。この関数内で更新される可能性があります。
        *   `current_file_index: &mut Option<usize>`: 現在処理中のファイルの `file_list` におけるインデックス。この関数内で更新される可能性があります。
    *   出力: `Result<bool>`
        *   `Ok(true)`: ファイルリストの変更、新しい行の処理、次のファイルへの移行のいずれかが行われた場合。
        *   `Ok(false)`: 上記のいずれも行われなかった場合（待機状態）。
        *   `Err(error)`: `get_sorted_files` や `read_file_from_line`、`line_sender.send`、`save_state` でエラーが発生した場合。

*   **`run_ftp_client_task(config: FtpReaderConfig)`**
    *   処理内容: `FtpReader` を非同期に実行するためのエントリーポイント関数。
        1.  `FtpReader::new` を呼び出して `FtpReader` インスタンスを初期化します。
        2.  初期化に成功した場合、`ftp_reader.run()` を呼び出してメインループを開始します。
        3.  `run()` が完了（正常終了またはエラー）するまで `await` します。
        4.  初期化または実行中にエラーが発生した場合は、エラー内容をログに出力します。
    *   入力: `FtpReaderConfig`
    *   出力: `()` (なし) - この関数自体は完了時に値を返しませんが、内部で `FtpReader::run` を実行します。

### 4. 実行フロー

1.  `run_ftp_client_task` が呼び出され、`FtpReaderConfig` を受け取ります。
2.  `FtpReader::new` で `FtpReader` が初期化されます。この際、状態ファイルが読み込まれます。
3.  `FtpReader::run` が呼び出され、メインループが開始します。
4.  ループ内で `process_files` が呼び出されます。
    a.  `get_sorted_files` でFTPサーバーから最新のファイルリスト（更新日時順）を取得します。
    b.  リストに変更があれば、処理対象ファイルインデックス (`current_file_index`) と状態 (`state`) が更新され、保存されます。
    c.  現在の状態に基づき、処理すべきファイルと開始行を決定します。
    d.  `read_file_from_line` でファイルから新しい行を読み込みます。
    e.  新しい行があれば、`line_sender` に送信し、状態 (`state`) を更新・保存します。
    f.  ファイルの終端に達し、次のファイルがあれば、状態を次のファイルに更新・保存します。
5.  `process_files` の結果に応じて待機時間が設定され、ループが繰り返されます。
6.  外部から `shutdown_rx` チャネルを通じてシャットダウン信号が送られると、`run` ループが終了し、FTP接続が閉じられ、`run_ftp_client_task` が完了します。
