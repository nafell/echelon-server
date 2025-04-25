import ftplib
import time
from io import StringIO
from datetime import datetime
import os
import threading

class FTPReader(threading.Thread):

    def __init__(self,
                 host,
                 user,
                 passwd,
                 remote_dir='/',
                 state_file='state.txt',
                 line_handler=print,
                 verbose=False):
        
        super().__init__(daemon=True)
        self.ftp_host = host
        self.ftp_user = user
        self.ftp_pass = passwd
        self.remote_dir = remote_dir
        self.state_file = state_file
        self.line_handler = line_handler
        self.verbose = verbose

        self.last_file, self.last_position = self.load_state()
        self.file_list = []
        self.file_index = 0
        self.stop_event = threading.Event()

    def dbg(self, out: str):
        if self.verbose:
            print(out)

    def seek(self, filename: str, line_number: int):
        if not self.file_list:
            self.dbg("⚠️ ファイルリストがまだ読み込まれていません。")
            return False

        filenames = [f[1] for f in self.file_list]
        if filename not in filenames:
            self.dbg(f"⚠️ ファイル {filename} が見つかりません。")
            return False

        self.file_index = filenames.index(filename)
        self.last_file = filename
        self.last_position = line_number - 1
        self.save_state(filename, line_number - 1)
        self.dbg(f"📍 {filename} の {line_number} 行目から再開するよう設定しました。")
        return True
    
    # 現在読んでいるファイルとその位置を保存
    def save_state(self, filename, position):
        with open(self.state_file, 'w') as f:
            f.write(f"{filename}\n{position}\n")

    # 保存された状態を読み込み
    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                filename = f.readline().strip()
                position = int(f.readline().strip())
                return filename, position
        return None, 0

    # FTPのdir出力を解析（Windows形式）
    def parse_windows_dir_line(self, line):
        parts = line.split()
        if len(parts) < 4:
            return None, None
        try:
            date_str = parts[0] + " " + parts[1]
            size_or_dir = parts[2]
            filename = " ".join(parts[3:])
            if size_or_dir.upper() == "<DIR>":
                return None, None
            dt = datetime.strptime(date_str, "%m-%d-%y %I:%M%p")
            return dt, filename
        except Exception as e:
            self.dbg(f"日付パース失敗: {e}")
            return None, None

    # ファイル一覧を日時でソート
    def get_all_files_sorted(self, ftp: ftplib.FTP):
        lines = []
        ftp.cwd(self.remote_dir)
        ftp.dir(lines.append)
        files = []
        for line in lines:
            dt, name = self.parse_windows_dir_line(line)
            if dt and name:
                files.append((dt, name))
        return sorted(files, key=lambda x: x[0])

    # 指定行からファイルを読む
    def read_file_from_line(self, ftp: ftplib.FTP, filename: str, start_line: int):
        file_content = ""
        def handle_binary(data):
            nonlocal file_content
            file_content = data.decode("utf-8", errors="ignore")
        try:
            ftp.retrbinary(f"RETR {filename}", callback=lambda data: handle_binary(data))
        except Exception as e:
            self.dbg(f"{filename} の取得に失敗しました: {e}")
            return []
        sio = StringIO(file_content)
        all_lines = sio.readlines()
        return all_lines[start_line:], len(all_lines)

    # メインの処理
    def run(self):
        with ftplib.FTP(self.ftp_host) as ftp:
            ftp.login(self.ftp_user, self.ftp_pass)
            self.dbg("FTP接続成功")

            while not self.stop_event.is_set():
                new_file_list = self.get_all_files_sorted(ftp)
                file_names = [f[1] for f in new_file_list]

                # 初回 or ファイルが追加された場合
                if not self.file_list or file_names != [f[1] for f in self.file_list]:
                    self.file_list = new_file_list
                    if self.last_file in file_names:
                        self.file_index = file_names.index(self.last_file)
                    else:
                        self.file_index = 0
                        self.last_position = 0

                if not self.file_list:
                    self.dbg("ファイルが見つかりません。3秒後に再試行します。")
                    time.sleep(3)
                    continue

                current_file = self.file_list[self.file_index][1]
                new_lines, total_lines = self.read_file_from_line(ftp, current_file, self.last_position)

                if new_lines:
                    for line in new_lines:
                        self.line_handler(line.strip())  # ← 行出力だけは差し替え関数
                        self.last_position += 1
                        self.save_state(current_file, self.last_position)
                        time.sleep(1)
                else:
                    if self.last_position >= total_lines:
                        # 現ファイルの最後まで読んでいた → 次のファイルへ
                        if self.file_index + 1 < len(self.file_list):
                            self.file_index += 1
                            self.last_file = self.file_list[self.file_index][1]
                            self.last_position = 0
                            self.save_state(self.last_file, self.last_position)
                        else:
                            # まだファイルが増えていないなら待機
                            time.sleep(3)
                    else:
                        # ファイルに追記されるのを待つ
                        time.sleep(1)

    # スレッドを停止する
    def stop(self):
        self.stop_event.set()


