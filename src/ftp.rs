use async_ftp::FtpStream;
use tokio::io::AsyncReadExt;
use std::collections::HashMap;
use chrono::{Duration};
use std::fs::File;
use std::io::Write;
use crate::model::{WearReading, create_wear_reading};

pub async fn list_files_by_date(
    host: &str,
    username: &str,
    password: &str,
    directory: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // FTPサーバーに接続
    let mut ftp_stream = FtpStream::connect(host).await?;
    
    // ログイン
    ftp_stream.login(username, password).await?;
    
    // 指定ディレクトリに移動
    ftp_stream.cwd(directory).await?;
    
    // ファイル一覧を取得
    let files = ftp_stream.list(None).await?;
    
    // 接続を閉じる
    ftp_stream.quit().await?;
    
    let mut filenames: Vec<String> = Vec::new();
    for file in files {
        let fileinfo: Vec<&str> = file.split_whitespace().collect();
        
        let filename = fileinfo[8..].join(" ");
        filenames.push(filename);
    }
    filenames.sort_by(|a,b| a.cmp(&b));
    
    Ok(filenames)
}

async fn download_file_from_ftp(
    host: &str,
    username: &str,
    password: &str,
    file_path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    tracing::debug!("[FTP Observation] Downloading file: {}", file_path);
    // FTPサーバーに接続
    let mut ftp_stream = FtpStream::connect(host).await?;
    
    // ログイン
    ftp_stream.login(username, password).await?;
    
    // バイナリモードに設定
    ftp_stream.transfer_type(async_ftp::types::FileType::Binary).await?;
    
    // ファイルを取得
    let contents = ftp_stream.retr(file_path, |mut reader| async move {
        let mut contents = String::new();
        reader.read_to_string(&mut contents).await?;
        Ok::<String, Box<dyn std::error::Error>>(contents)
    }).await?;
    
    // 接続を閉じる
    ftp_stream.quit().await?;
    
    Ok(contents)
}

fn parse_wear_reading_from_csv_line(
    csv_data: &str,
    facility_name: String,
    machine_type: String,
    equipment_id: String,
    equipment_version: String,
) -> Result<WearReading, Box<dyn std::error::Error>> {
    let values: Vec<&str> = csv_data.split(',').collect();

    // 時間データをパース
    let time_str = values[3].trim();
    // let time = chrono::DateTime::parse_from_str(time_str, "%Y/%m/%d %H:%M:%S")?
    //     .with_timezone(&chrono::Utc) + Duration::hours(9);

    let time_str_with_timezone = format!("{} {}", time_str, "+0900");
    let time = chrono::DateTime::parse_from_str(time_str_with_timezone.as_str(), "%m/%d/%Y %H:%M:%S %z")?
        .with_timezone(&chrono::Utc);
    
    // 数値データをパース
    let mut data = Vec::with_capacity(102);
    for i in 0..102 {
        let value = values[i+6].trim().parse::<i32>()?;
        data.push(value);
    }
    
    // データの長さが102個であることを確認
    if data.len() != 102 {
        return Err("データの長さが不正です".into());
    }
    
    Ok(create_wear_reading(
        time,
        facility_name,
        machine_type,
        equipment_id,
        equipment_version,
        data,
    ))
}

async fn parse_wear_reading_from_csv_file(
    csv_data: &str,
    facility_name: String,
    machine_type: String,
    equipment_id: String,
        equipment_version: String,
    ) -> Result<Vec<WearReading>, Box<dyn std::error::Error>> {
    tracing::debug!("[FTP Observation] Parsing CSV file");
    let mut wear_readings: Vec<WearReading> = Vec::new();
    let mut lines = csv_data.lines();
    let _ = lines.next().ok_or("CSVデータが空です")?;

    for line in lines {
        let result = parse_wear_reading_from_csv_line(line, facility_name.clone(), machine_type.clone(), equipment_id.clone(), equipment_version.clone());
        match result {
            Ok(wear_reading) => {
                wear_readings.push(wear_reading);
            }
            Err(e) => {
                // tracing::error!("[FTP Observation] Error parsing CSV line: {}", e);
            }
        }
    }
    if wear_readings.len() == 0 {
        return Err("No wear readings found".into());
    }
    return Ok(wear_readings);
}

pub async fn observe_ftp(
    host: &str,
    username: &str,
    password: &str,
    file_path: &str,
    facility_name: String,
    machine_type: String,
    equipment_id: String,
    equipment_version: String,
    oneshot: bool,
) -> Result<Vec<WearReading>, Box<dyn std::error::Error>> {
    tracing::info!("Starting FTP Observation task");
    let mut last_file = load_cursor_filename_from_file()?;
    let files = list_files_by_date(host, username, password, file_path).await?;
    tracing::debug!("[FTP Observation] last_file: {:?}", last_file);
    let mut cursor_index = 0;
    for i in 0..files.len() {
        if files[i] == last_file {
            tracing::debug!("[FTP Observation] Found last file: {}", files[i]);
            cursor_index = i+1;
            break;
        }
    }

    if oneshot {
        cursor_index = files.len() - 1;
    }

    if cursor_index >= files.len() {
        tracing::info!("[FTP Observation] No new files found");
        return Ok(Vec::new());
    }

    tracing::debug!("[FTP Observation] cursor_index: {}", cursor_index);
    
    let mut wear_readings: Vec<WearReading> = Vec::new();
    for i in cursor_index..files.len() {
        let file = files[i].clone();
        let file_path = format!("{}{}", file_path, file);
        let contents = download_file_from_ftp(host, username, password, file_path.as_str()).await?;
        let wear_readings_from_file = parse_wear_reading_from_csv_file(contents.as_str(), facility_name.clone(), machine_type.clone(), equipment_id.clone(), equipment_version.clone()).await?;
        wear_readings.extend(wear_readings_from_file);
        last_file = file;
    }
    save_cursor_filename_to_file(last_file)?;
    Ok(wear_readings)
}

fn save_cursor_filename_to_file(
    cursor_filename: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create("csv_parsed_cursor.txt")?;
    file.write_all(cursor_filename.as_bytes())?;
    Ok(())
}

fn load_cursor_filename_from_file() -> Result<String, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string("csv_parsed_cursor.txt")?;
    Ok(content)
}