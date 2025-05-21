このコードの仕様書を作成いたします。

# 摩耗測定システム仕様書

## 1. データ構造

### 1.1 WearReading構造体
摩耗測定データを格納する構造体です。

#### タグ情報（InfluxDB用）
- `time`: 測定時刻（UTC）
- `facility_name`: 施設名
- `machine_type`: 機械タイプ（"ボールベアリング"、"ギアトレイン"、"混合"のいずれか）
- `equipment_id`: 機器ID
- `equipment_version`: 機器バージョン

#### 測定データ
- `n_0um` から `n_101um` までの102個の粒子サイズ別カウント値（i32型）
  - 各フィールドは0μmから101μmまでの粒子数を記録

### 1.2 WearResult列挙型
摩耗状態を表す列挙型です。
- `Nominal = 1`: 正常
- `Warning = 2`: 警告
- `Critical = 3`: 危険

## 2. 主要関数

### 2.1 create_wear_reading
```rust
pub fn create_wear_reading(
    time: DateTime<Utc>, 
    facility_name: String, 
    machine_type: String, 
    equipment_id: String, 
    equipment_version: String, 
    data: Vec<i32>
) -> WearReading
```

#### 機能
- 摩耗測定データを構造体に変換します
- 入力された102個の粒子カウントデータを各サイズに対応するフィールドに割り当てます

#### 入力
- `time`: 測定時刻
- `facility_name`: 施設名
- `machine_type`: 機械タイプ
- `equipment_id`: 機器ID
- `equipment_version`: 機器バージョン
- `data`: 102個の粒子カウントデータ（Vec<i32>）

#### 出力
- `WearReading`構造体のインスタンス

### 2.2 wear_string
```rust
pub fn wear_string(wear_result: WearResult) -> String
```

#### 機能
- 摩耗状態を日本語の文字列に変換します

#### 入力
- `wear_result`: 摩耗状態（WearResult型）

#### 出力
- "正常"、"警告"、"危険"のいずれかの文字列

### 2.3 calc_wear
```rust
pub fn calc_wear(wear_reading: &WearReading) -> WearResult
```

#### 機能
- 機械タイプに応じて適切な摩耗計算関数を呼び出します
- "混合"タイプの場合は、ボールベアリングとギアトレインの両方の計算を行い、より深刻な結果を返します

#### 入力
- `wear_reading`: 摩耗測定データ（WearReading型）

#### 出力
- 摩耗状態（WearResult型）

### 2.4 calc_wear_bearing
```rust
pub fn calc_wear_bearing(wear_reading: &WearReading) -> WearResult
```

#### 機能
- ボールベアリングの摩耗状態を計算します
- 0-2μmの粒子数の合計に基づいて判定します

#### 判定基準
- 合計 > 5000: 危険
- 合計 > 3000: 警告
- それ以外: 正常

### 2.5 calc_wear_geartrain
```rust
pub fn calc_wear_geartrain(wear_reading: &WearReading) -> WearResult
```

#### 機能
- ギアトレインの摩耗状態を計算します
- 80-101μmの粒子数の合計に基づいて判定します

#### 判定基準
- 合計 > 0: 危険
- それ以外: 正常

## 3. データベース連携
- InfluxDBとの連携を想定して設計されています
- `WearReading`構造体は`InfluxDbWriteable`トレイトを実装しており、InfluxDBへの書き込みが可能です
- タグ情報（facility_name, machine_type, equipment_id, equipment_version）はInfluxDBのタグとして保存されます
