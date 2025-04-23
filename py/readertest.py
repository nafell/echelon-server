from ftp_reader import FTPReader
import time

def log_line(line):
    print(f"[LINE] {line}")


def main():

    reader = FTPReader(
        host="localhost",
        user="FTP_test",
        passwd="qwerty",
        line_handler=log_line,
        verbose=True  # ← エラーとかの表示をする
    )

    reader.start()  # スレッド開始

    try:
        while True:
            time.sleep(30)
            reader.seek("test.txt", 3) # 30秒ごとに「test.txt」の3行目にカーソルを移動
    except KeyboardInterrupt:
        print("\n🛑 終了するゆん…")
        reader.stop()  # スレッド停止


if __name__ == "__main__":
    main()