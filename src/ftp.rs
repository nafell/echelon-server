use async_ftp::FtpStream;
use tokio::io::AsyncReadExt;

async fn download_file_from_ftp(
    host: &str,
    username: &str,
    password: &str,
    file_path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
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