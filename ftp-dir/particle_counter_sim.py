import time
import random
import time
from datetime import datetime
import os

# 0~3umの範囲に含まれる粒子の1列当たりの最大数
max_particles = 10

# 初期化
start_time = time.time()
row_counter = 0

def data():
    global start_time, row_counter
    row_counter += 1

    current_time = time.time()
    elapsed_seconds = current_time - start_time
    elapsed_minutes = int(elapsed_seconds / 60)
    elapsed_hours = int(elapsed_seconds / 3600)
    timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    delta_time = random.uniform(23.0, 27.0)



    values_7_to_108 = [1 for _ in range(102)]

    for i in range(3):
        values_7_to_108[i] = 1

    sum_7_to_108 = sum(values_7_to_108)
    sums_10_group = [sum(values_7_to_108[i:i+10]) for i in range(0, 102, 10)]
    sum_37_to_108 = sum(values_7_to_108[30:])
    final_zero = 0

    # データ構築
    row = [
        row_counter,
        round(elapsed_minutes, 5),
        round(elapsed_hours, 5),
        timestamp,
        round(delta_time, 5),
        sum_7_to_108
    ] + values_7_to_108 # + [0] + sums_10_group + [sum_37_to_108, final_zero]

    # CSVの1行分の文字列に変換（クォートなし、単純カンマ区切り）
    csv_line = ",".join(str(item) for item in row)
    return csv_line



# 設定値
lines_per_write = 100  # N：一度に書き込む行数
max_lines_per_file = 1  # M：1ファイル当たりの測定回数
interval_sec = 1  # 測定間隔（秒）
base_filename = "ftp-dir/measurements/Data"  # ベースファイル名

# 初期状態
file_index = 0

try:
    # while True:
    print(f"ファイルを開きました: {base_filename}{datetime.now().strftime("%Y%m%d%H%M%S")}.csv")
    current_file = open(f"{base_filename}{datetime.now().strftime("%Y%m%d%H%M%S")}.csv", "w", encoding="utf-8")
    # current_line_count = 0
    buffer = ""
    buffer += ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,100mlあたり,,,,,,,,,,,\n"
    buffer += "NO,min,h,計測日時,計測タイム(s),総個数,0μm,1μm,2μm,3μm,4μm,5μm,6μm,7μm,8μm,9μm,10μm,11μm,12μm,13μm,14μm,15μm,16μm,17μm,18μm,19μm,20μm,21μm,22μm,23μm,24μm,25μm,26μm,27μm,28μm,29μm,30μm,31μm,32μm,33μm,34μm,35μm,36μm,37μm,38μm,39μm,40μm,41μm,42μm,43μm,44μm,45μm,46μm,47μm,48μm,49μm,50μm,51μm,52μm,53μm,54μm,55μm,56μm,57μm,58μm,59μm,60μm,61μm,62μm,63μm,64μm,65μm,66μm,67μm,68μm,69μm,70μm,71μm,72μm,73μm,74μm,75μm,76μm,77μm,78μm,79μm,80μm,81μm,82μm,83μm,84μm,85μm,86μm,87μm,88μm,89μm,90μm,91μm,92μm,93μm,94μm,95μm,96μm,97μm,98μm,99μm,100μm,100μm<,,～10μm,10～20μm,20～30μm,30～40μm,40～50μm,50～60μm,60～70μm,70～80μm,80～90μm,90～100μm,３０＜,100μm<\n"

    for _ in range(max_lines_per_file):

        output_string = data()
        print(f"バッファに蓄積: {output_string}")
        buffer += output_string + "\n"
        # current_line_count += 1

        # if current_line_count >= lines_per_write:
        print("バッファからファイルに書き込みました")
        current_file.write(buffer)
        current_file.flush()
        buffer = ""
        # current_line_count = 0

        os.chmod(f"{base_filename}{datetime.now().strftime("%Y%m%d%H%M%S")}.csv", 0o644)

        # time.sleep(interval_sec)

    current_file.close()
    file_index += 1

except KeyboardInterrupt:
    current_file.flush()
    current_file.close()
    print("停止しました")
