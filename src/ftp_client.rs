use anyhow::{anyhow, Context, Result};
use async_ftp::{DataStream, FtpStream};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str;
use std::time::Duration;
use futures::Future;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct FtpReaderState {
    last_file: Option<String>,
    last_line_index: usize, // 0-based index
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FtpFileEntry {
    name: String,
    modified: NaiveDateTime,
}

#[derive(Debug)]
pub struct FtpReaderConfig {
    pub host: String,
    pub user: String,
    pub pass: String,
    pub remote_dir: String,
    pub state_file: PathBuf,
    pub line_sender: mpsc::Sender<String>,
    pub shutdown_rx: watch::Receiver<()>,
}

pub struct FtpReader {
    config: FtpReaderConfig,
    state: FtpReaderState,
    ftp_stream: Option<FtpStream>,
}

impl FtpReader {
    pub async fn new(config: FtpReaderConfig) -> Result<Self> {
        let state = Self::load_state(&config.state_file).await.unwrap_or_default();
        info!("Loaded state: {:?}", state);
        Ok(Self {
            config,
            state,
            ftp_stream: None,
        })
    }

    async fn load_state(path: &Path) -> Result<FtpReaderState> {
        if !path.exists() {
            return Ok(FtpReaderState::default());
        }
        let content = fs::read_to_string(path).await.context("Failed to read state file")?;
        serde_json::from_str(&content).context("Failed to parse state file")
    }

    async fn save_state(&self) -> Result<()> {
        let content = serde_json::to_string_pretty(&self.state).context("Failed to serialize state")?;
        fs::write(&self.config.state_file, content).await.context("Failed to write state file")
    }

    async fn ensure_connection(&mut self) -> Result<()> {
        let mut needs_reconnect = false;
        if let Some(stream) = &mut self.ftp_stream {
            if stream.pwd().await.is_err() {
                warn!("FTP connection seems dead, will reconnect.");
                needs_reconnect = true;
            } else {
                debug!("FTP connection is still alive.");
                return Ok(());
            }
        } else {
            needs_reconnect = true;
        }

        if needs_reconnect {
            if self.ftp_stream.take().is_some() {
                debug!("Dropped old FTP stream.");
            }

            info!("Connecting to FTP server: {}", self.config.host);
            let mut stream = FtpStream::connect(&self.config.host).await.context("Failed to connect to FTP host")?;
            stream.login(&self.config.user, &self.config.pass).await.context("FTP login failed")?;
            info!("FTP login successful.");
            stream.cwd(&self.config.remote_dir).await.context("Failed to change remote directory")?;
            info!("Changed remote directory to: {}", self.config.remote_dir);
            self.ftp_stream = Some(stream);
        }
        Ok(())
    }

    fn get_stream_mut(&mut self) -> Result<&mut FtpStream> {
        self.ftp_stream.as_mut().ok_or_else(|| anyhow!("FTP stream is not available after ensure_connection"))
    }

    fn parse_windows_dir_line(line: &str) -> Option<FtpFileEntry> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 4 || parts[2].to_uppercase() == "<DIR>" {
            return None;
        }

        let date_str = parts[0];
        let time_str = parts[1];

        let am_pm_pos = time_str.find(|c: char| c == 'A' || c == 'P');
        let time_part = if let Some(pos) = am_pm_pos {
            let (time, ampm) = time_str.split_at(pos);
            format!("{} {}", time, ampm)
        } else {
            warn!("Could not parse time AM/PM: {}", time_str);
            return None;
        };

        let datetime_str = format!("{} {}", date_str, time_part);
        let format = "%m-%d-%y %I:%M %p";

        match NaiveDateTime::parse_from_str(&datetime_str, format) {
            Ok(modified) => {
                let filename_start_index = line.find(parts[3]).unwrap_or(line.len());
                let name = line[filename_start_index..].trim().to_string();
                if name.is_empty() {
                    None
                } else {
                    Some(FtpFileEntry { name, modified })
                }
            }
            Err(e) => {
                warn!("Failed to parse datetime string '{}': {}", datetime_str, e);
                None
            }
        }
    }

    async fn get_sorted_files(&mut self) -> Result<Vec<FtpFileEntry>> {
        self.ensure_connection().await?;
        let stream = self.get_stream_mut()?;

        let list_result = stream.list(None).await;

        match list_result {
            Ok(entries) => {
                let mut files: Vec<FtpFileEntry> = entries
                    .into_iter()
                    .filter_map(|entry_str| Self::parse_windows_dir_line(&entry_str))
                    .collect();

                files.sort_by_key(|f| f.modified);

                debug!("Found and sorted {} files in {}", files.len(), self.config.remote_dir);
                Ok(files)
            }
            Err(e) => {
                error!("Failed to list files: {}", e);
                self.ftp_stream = None;
                Err(e).context("Failed to list files")
            }
        }
    }

    async fn read_file_from_line(&mut self, filename: &str, start_line_index: usize) -> Result<(usize, Vec<String>)> {
        self.ensure_connection().await?;
        let stream = self.get_stream_mut()?;
        info!(
            "Reading file {} from line index {} using RETR with closure",
            filename, start_line_index
        );

        let process_stream = |reader: BufReader<DataStream>| async move {
            async fn read_lines_from_index(
                mut reader: BufReader<DataStream>,
                start_index: usize,
                fname: &str,
            ) -> Result<(usize, Vec<String>)> {
                let mut current_lines = Vec::new();
                let mut line_count = 0usize;

                for i in 0..start_index {
                    let mut line_buf = String::new();
                    match reader.read_line(&mut line_buf).await {
                        Ok(0) => {
                            warn!(
                                "Start line index {} is beyond the end of file {} ({} lines)",
                                start_index, fname, i
                            );
                            return Ok((i, Vec::new()));
                        }
                        Ok(_) => { line_count += 1; }
                        Err(e) => {
                            return Err(anyhow!(e)).context(format!("Error skipping lines in {}", fname));
                        }
                    }
                }

                loop {
                    let mut line_buf = String::new();
                    match reader.read_line(&mut line_buf).await {
                        Ok(0) => break,
                        Ok(_) => {
                            if line_buf.ends_with("\r\n") {
                                line_buf.pop(); line_buf.pop();
                            } else if line_buf.ends_with('\n') {
                                line_buf.pop();
                            }
                            current_lines.push(line_buf);
                            line_count += 1;
                        }
                        Err(e) => {
                            return Err(anyhow!(e)).context(format!("Error reading line from {}", fname));
                        }
                    }
                }
                Ok((line_count, current_lines))
            }

            read_lines_from_index(reader, start_line_index, filename).await
        };

        match stream.retr(filename, process_stream).await {
            Ok(result) => {
                info!(
                    "Successfully processed {} lines total, read {} new lines from {}",
                    result.0, result.1.len(), filename
                );
                Ok(result)
            }
            Err(e) => {
                self.ftp_stream = None;
                Err(e).context(format!("Failed RETR processing for file {}", filename))
            }
        }
    }

    pub async fn run(mut self) -> Result<()> {
        info!("Starting FTP Reader run loop...");
        let mut file_list: Vec<FtpFileEntry> = Vec::new();
        let mut current_file_index: Option<usize> = None;
        let mut shutdown_rx = self.config.shutdown_rx.clone();

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    info!("Shutdown signal received. Exiting FTP reader loop.");
                    if let Some(stream) = self.ftp_stream.as_mut() {
                        let _ = stream.quit().await;
                    }
                    return Ok(());
                }
                result = self.process_files(&mut file_list, &mut current_file_index) => {
                    match result {
                        Ok(processed) => {
                            if !processed {
                                debug!("No file updates or new lines. Waiting...");
                                sleep(Duration::from_secs(3)).await;
                            } else {
                                sleep(Duration::from_secs(1)).await;
                            }
                        }
                        Err(e) => {
                            error!("Error processing files: {:?}. Retrying after delay.", e);
                            self.ftp_stream = None;
                            current_file_index = None;
                            sleep(Duration::from_secs(10)).await;
                        }
                    }
                }
            }
        }
    }

    async fn process_files(&mut self, file_list: &mut Vec<FtpFileEntry>, current_file_index: &mut Option<usize>) -> Result<bool> {
        let mut processed = false;

        let new_file_list = self.get_sorted_files().await?;

        let list_changed = *file_list != new_file_list;

        if list_changed {
            info!("File list changed or initialized.");
            *file_list = new_file_list;
            *current_file_index = None;
            processed = true;

            if let Some(last_file_name) = &self.state.last_file {
                if let Some(idx) = file_list.iter().position(|f| &f.name == last_file_name) {
                    info!("Found last processed file '{}' at index {}. Resuming.", last_file_name, idx);
                    *current_file_index = Some(idx);
                } else {
                    info!("Last processed file '{}' not found in the new list. Starting from the beginning.", last_file_name);
                    self.state = FtpReaderState::default();
                    if !file_list.is_empty() {
                        *current_file_index = Some(0);
                        self.state.last_file = Some(file_list[0].name.clone());
                    }
                }
            } else if !file_list.is_empty() {
                *current_file_index = Some(0);
                self.state = FtpReaderState { last_file: Some(file_list[0].name.clone()), last_line_index: 0 };
            }
            self.save_state().await?;
        }

        let current_idx = match *current_file_index {
            Some(i) if i < file_list.len() => i,
            _ => {
                debug!("No current file to process.");
                return Ok(processed);
            }
        };

        let current_filename = file_list[current_idx].name.clone();
        let current_start_line = self.state.last_line_index;

        let (total_lines_in_file, new_lines) = self.read_file_from_line(
            &current_filename,
            current_start_line
        ).await?;

        if !new_lines.is_empty() {
            processed = true;
            debug!("Processing {} new lines from {}", new_lines.len(), current_filename);
            for line in new_lines {
                if let Err(e) = self.config.line_sender.send(line).await {
                    error!("Failed to send line to handler channel: {}. Stopping.", e);
                    return Err(anyhow!("Line handler channel closed"));
                }
                self.state.last_line_index += 1;
                self.save_state().await?;
            }
            info!("Finished processing new lines from {}. New position: {}", current_filename, self.state.last_line_index);
            self.state.last_file = Some(current_filename.clone());
            self.save_state().await?;

        } else {
            debug!("No new lines in {}. Current index: {}", current_filename, self.state.last_line_index);
            let next_file_exists = current_idx + 1 < file_list.len();
            let reached_eof = self.state.last_line_index >= total_lines_in_file;

            if reached_eof && next_file_exists {
                info!("Reached end of file {} ({} lines)", current_filename, total_lines_in_file);
                processed = true;
                let next_idx = current_idx + 1;
                let next_filename = file_list[next_idx].name.clone();
                info!("Moving to next file: {}", next_filename);
                *current_file_index = Some(next_idx);
                self.state = FtpReaderState {
                    last_file: Some(next_filename),
                    last_line_index: 0,
                };
                self.save_state().await?;
            } else if reached_eof && !next_file_exists {
                debug!("Reached end of the last file. Waiting for new files or content...");
            } else {
                debug!("No new lines found in {}, but not at EOF ({} lines processed). Waiting...", current_filename, total_lines_in_file);
            }
        }
        Ok(processed)
    }
}

pub async fn run_ftp_client_task(config: FtpReaderConfig) {
    info!("Initializing FTP Reader...");
    match FtpReader::new(config).await {
        Ok(ftp_reader) => {
            info!("FTP Reader initialized. Starting run loop...");
            if let Err(e) = ftp_reader.run().await {
                error!("FTP Reader task failed: {:?}", e);
            } else {
                info!("FTP Reader task finished gracefully.");
            }
        }
        Err(e) => {
            error!("Failed to initialize FTP Reader: {:?}", e);
        }
    }
}
