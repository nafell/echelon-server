use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use futures::future;

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use suppaftp::{
    list::File,
    types::{FileType, FormatControl, IpVersion, ProtectionLevel, TransferType},
    AsyncFtpStream,
    FtpError,
    Status,
};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{mpsc, watch, Notify}, // Use watch channel for stop signal
    time::sleep,
};
use tracing::{debug, error, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum FtpClientError {
    #[error("FTP operation failed: {0}")]
    Ftp(#[from] FtpError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("State file format error: {0}")]
    StateFormat(String),
    #[error("File not found in remote list: {0}")]
    FileNotFound(String),
    #[error("Date parsing error: {0}")]
    DateParse(#[from] chrono::ParseError),
    #[error("Invalid DIR line format: {0}")]
    DirParse(String),
    #[error("Connection not established")]
    NotConnected,
    #[error("Task channel send error")]
    ChannelSend,
    #[error("Invalid server address: {0}")]
    InvalidAddress(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)] // Added serde derive
struct ReaderState {
    last_file: String,
    last_position: usize, // 0-based line index
}

pub struct FtpReaderConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub remote_dir: String,
    pub state_file: PathBuf,
    pub verbose: bool,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub passive_mode: bool,
}

pub struct FtpReader<F>
where
    F: FnMut(String) -> Result<()> + Send + 'static,
{
    config: Arc<FtpReaderConfig>,
    state: Arc<tokio::sync::Mutex<ReaderState>>,
    line_handler: Arc<tokio::sync::Mutex<F>>,
    stop_tx: Option<watch::Sender<()>>,
    stop_notify: Arc<Notify>,
    ftp_stream: Arc<tokio::sync::Mutex<Option<AsyncFtpStream>>>,
    file_list_cache: Arc<tokio::sync::Mutex<VecDeque<(NaiveDateTime, String)>>>,
    current_file_index: Arc<tokio::sync::Mutex<usize>>,
}

impl<F> FtpReader<F>
where
    F: FnMut(String) -> Result<()> + Send + 'static,
{
    pub async fn new(config: FtpReaderConfig, line_handler: F) -> Result<Self> {
        let state = Self::load_state(&config.state_file)
            .await
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to load state file '{}': {}. Starting from scratch.",
                    config.state_file.display(),
                    e
                );
                ReaderState {
                    last_file: String::new(),
                    last_position: 0,
                }
            });

        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(tokio::sync::Mutex::new(state)),
            line_handler: Arc::new(tokio::sync::Mutex::new(line_handler)),
            stop_tx: None,
            stop_notify: Arc::new(Notify::new()),
            ftp_stream: Arc::new(tokio::sync::Mutex::new(None)),
            file_list_cache: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            current_file_index: Arc::new(tokio::sync::Mutex::new(0)),
        })
    }

    async fn load_state(state_file: &Path) -> Result<ReaderState, FtpClientError> {
        if !state_file.exists() {
            return Ok(ReaderState {
                last_file: String::new(),
                last_position: 0,
            });
        }
        let content = fs::read_to_string(state_file)
            .await
            .map_err(FtpClientError::Io)?;
        // Using serde for robustness
        serde_json::from_str(&content)
            .map_err(|e| FtpClientError::StateFormat(e.to_string()))

        /* // Simple text format parsing (alternative to serde)
        let mut lines = content.lines();
        let filename = lines.next().ok_or_else(|| FtpClientError::StateFormat("Missing filename".to_string()))?.trim().to_string();
        let position = lines.next().ok_or_else(|| FtpClientError::StateFormat("Missing position".to_string()))?
            .trim().parse::<usize>().map_err(|_| FtpClientError::StateFormat("Invalid position format".to_string()))?;
        Ok(ReaderState { last_file: filename, last_position: position })
        */
    }

    async fn save_state(
        state_file: &Path,
        state_data: &ReaderState,
    ) -> Result<(), FtpClientError> {
        let content = serde_json::to_string_pretty(state_data)
            .map_err(|e| FtpClientError::StateFormat(format!("Serialization error: {}", e)))?; // Use serde

        /* // Simple text format writing (alternative to serde)
        let content = format!("{}\n{}\n", state_data.last_file, state_data.last_position);
        */

        fs::write(state_file, content)
            .await
            .map_err(FtpClientError::Io)?;
        Ok(())
    }

    async fn connect_and_login(
        config: &FtpReaderConfig,
    ) -> Result<AsyncFtpStream, FtpClientError> {
        let addr = format!("{}:{}", config.host, config.port);
        debug!("Connecting to FTP server: {}", addr);

        let stream_result =
            tokio::time::timeout(config.connect_timeout, AsyncFtpStream::connect(addr)).await;

        let mut ftp_stream = match stream_result {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(FtpClientError::Ftp(e)),
            Err(_) => {
                return Err(FtpClientError::Ftp(FtpError::ConnectionTimeout(
                    "Connection attempt timed out".to_string(),
                )))
            }
        };

        debug!("Logging in as user '{}'", config.user);
        ftp_stream
            .login(&config.user, &config.pass)
            .await
            .map_err(FtpClientError::Ftp)?;

        // Set transfer type to ASCII (common for text files) or Binary
        ftp_stream
            .transfer_type(TransferType::Ascii) // or Binary
            .await
            .map_err(FtpClientError::Ftp)?;

        // Enable passive mode if configured
        if config.passive_mode {
             debug!("Enabling passive mode");
             ftp_stream.set_passive().await.map_err(FtpClientError::Ftp)?;
             // For non-standard ports or specific IP versions:
             // ftp_stream.set_passive_mode(IpVersion::IpV4).await.map_err(FtpClientError::Ftp)?;
        } else {
            debug!("Using active mode");
             // ftp_stream.set_active().await.map_err(FtpClientError::Ftp)?;
             // Active mode often requires firewall configuration and is less common.
        }

        debug!("Setting remote directory to '{}'", config.remote_dir);
        ftp_stream
            .cwd(&config.remote_dir)
            .await
            .map_err(FtpClientError::Ftp)?;

        info!("FTP connection successful to {}", config.host);
        Ok(ftp_stream)
    }

    fn parse_windows_dir_line(line: &str) -> Result<Option<(NaiveDateTime, String)>, FtpClientError> {
        // Example Windows DIR line: "07-18-24  11:00AM                 1234 file1.txt"
        //                          "07-18-24  11:01AM      <DIR>          directory"
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 4 {
            return Ok(None); // Not enough parts
        }

        // Try to parse date and time
        let date_str = parts[0];
        let time_str = parts[1];
        let dt_str = format!("{} {}", date_str, time_str);

        // Handle potential variations in date/time format if necessary
        let dt = NaiveDateTime::parse_from_str(&dt_str, "%m-%d-%y %I:%M%p")
            .or_else(|_| NaiveDateTime::parse_from_str(&dt_str, "%m-%d-%y %H:%M")) // Try 24-hour format if AM/PM fails
            .map_err(|e| FtpClientError::DirParse(format!("Failed to parse date '{}': {}", dt_str, e)))?;

        let size_or_dir = parts[2];
        if size_or_dir.to_uppercase() == "<DIR>" {
            return Ok(None); // Skip directories
        }

        // The rest is the filename, potentially containing spaces
        let filename = parts[3..].join(" ");

        Ok(Some((dt, filename)))
    }

    async fn get_all_files_sorted(
        ftp: &mut AsyncFtpStream,
        remote_dir: &str,
        verbose: bool,
    ) -> Result<VecDeque<(NaiveDateTime, String)>, FtpClientError> {
        debug!("Listing files in directory '{}'", remote_dir);
        // Ensure we are in the correct directory (connection might reset it)
        ftp.cwd(remote_dir).await.map_err(FtpClientError::Ftp)?;

        let lines = ftp.list(None).await.map_err(FtpClientError::Ftp)?;
        let mut files = Vec::new();

        for line in lines {
            match Self::parse_windows_dir_line(&line) {
                Ok(Some(file_info)) => files.push(file_info),
                Ok(None) => {} // Skip directories or unparseable lines silently unless verbose
                Err(e) => {
                    if verbose {
                        warn!("Failed to parse DIR line '{}': {}", line, e);
                    }
                }
            }
        }

        // Sort files by datetime
        files.sort_by_key(|k| k.0);
        debug!("Found and sorted {} files", files.len());
        Ok(files.into())
    }

    async fn read_file_from_position(
        ftp: &mut AsyncFtpStream,
        filename: &str,
        start_line_index: usize, // 0-based
        config: &FtpReaderConfig,
    ) -> Result<(Vec<String>, usize), FtpClientError> {
        debug!(
            "Reading file '{}' from line {}",
            filename,
            start_line_index + 1
        );

        let mut file_data = Vec::new();
        let retr_future = ftp.retr(filename, |chunk| {
            file_data.extend_from_slice(chunk);
            future::ready(Ok(()))
        });

        match tokio::time::timeout(config.read_timeout, retr_future).await {
            Ok(Ok(_)) => {} // Download successful
            Ok(Err(e)) => return Err(FtpClientError::Ftp(e)),
            Err(_) => return Err(FtpClientError::Ftp(FtpError::ConnectionTimeout(
                "File download timed out".to_string(),
            ))),
        };


        // Decode assuming UTF-8, ignoring errors for robustness
        let content = String::from_utf8_lossy(&file_data).to_string();
        let all_lines: Vec<String> = content.lines().map(String::from).collect();
        let total_lines = all_lines.len();

        if start_line_index >= total_lines {
            debug!(
                "Start line {} is beyond the end of file '{}' ({} lines)",
                start_line_index + 1, filename, total_lines
            );
            Ok((Vec::new(), total_lines)) // No new lines
        } else {
             let new_lines = all_lines[start_line_index..].to_vec();
             debug!("Read {} new lines from '{}'", new_lines.len(), filename);
             Ok((new_lines, total_lines))
        }
    }

     // Public method to seek to a specific file and line
    pub async fn seek(&self, filename: &str, line_number: usize) -> Result<(), FtpClientError> {
        info!("Seek requested for file '{}', line {}", filename, line_number);
        let mut file_list = self.file_list_cache.lock().await;
        let mut current_index = self.current_file_index.lock().await;
        let mut state = self.state.lock().await;

        match file_list.iter().position(|(_, name)| name == filename) {
            Some(index) => {
                *current_index = index;
                state.last_file = filename.to_string();
                // Ensure line_number is 1-based for user, convert to 0-based for internal state
                state.last_position = if line_number > 0 { line_number - 1 } else { 0 };

                let config = self.config.clone(); // Clone Arc for saving state
                let state_to_save = state.clone();
                Self::save_state(&config.state_file, &state_to_save).await?;
                info!("Seek successful. Will resume from line {} of file '{}'", line_number, filename);
                Ok(())
            }
            None => {
                 warn!("Seek failed: File '{}' not found in the current file list cache.", filename);
                 Err(FtpClientError::FileNotFound(filename.to_string()))
            }
        }
    }


    pub fn start(&mut self) -> Result<(), FtpClientError> {
        if self.stop_tx.is_some() {
            warn!("FTP reader task already started.");
            return Ok(());
        }

        let (stop_tx, mut stop_rx) = watch::channel(());
        self.stop_tx = Some(stop_tx);
        self.stop_notify.notify_waiters(); // Signal any initial waiters if needed

        let config = Arc::clone(&self.config);
        let state = Arc::clone(&self.state);
        let line_handler = Arc::clone(&self.line_handler);
        let stop_notify = Arc::clone(&self.stop_notify);
        let ftp_stream_arc = Arc::clone(&self.ftp_stream);
        let file_list_cache = Arc::clone(&self.file_list_cache);
        let current_file_index = Arc::clone(&self.current_file_index);

        tokio::spawn(async move {
            info!("FTP reader task started.");
            let mut consecutive_errors = 0;

            loop {
                let mut ftp_guard = ftp_stream_arc.lock().await;
                let mut ftp = match ftp_guard.take() {
                    Some(stream) => stream,
                    None => {
                        // Attempt to connect
                        match Self::connect_and_login(&config).await {
                            Ok(stream) => {
                                consecutive_errors = 0; // Reset errors on successful connect
                                stream
                            }
                            Err(e) => {
                                error!("Failed to connect/login to FTP: {}", e);
                                consecutive_errors += 1;
                                if consecutive_errors > 5 { // Avoid tight loop on persistent errors
                                    error!("Too many consecutive connection errors. Stopping task.");
                                    break; // Exit the loop
                                }
                                // Drop guard before sleeping
                                drop(ftp_guard);
                                tokio::select! {
                                    _ = sleep(Duration::from_secs(5 * consecutive_errors)) => continue, // Exponential backoff
                                     _ = stop_rx.changed() => {
                                        info!("Stop signal received during connection backoff.");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                };

                // --- Main processing logic ---
                let mut file_list = file_list_cache.lock().await;
                let mut current_index = current_file_index.lock().await;
                let mut current_state = state.lock().await;

                // Check for new files
                let new_file_list_result = Self::get_all_files_sorted(&mut ftp, &config.remote_dir, config.verbose).await;

                let updated_file_list = match new_file_list_result {
                     Ok(list) => {
                         consecutive_errors = 0; // Reset errors on success
                         list
                     },
                     Err(e) => {
                         error!("Failed to get file list: {}", e);
                         consecutive_errors += 1;
                         if consecutive_errors > 3 {
                            warn!("Too many errors getting file list. Resetting connection.");
                            let _ = ftp.quit().await; // Attempt graceful quit
                            // Let the next iteration reconnect by not putting ftp back
                         } else {
                             // Put ftp back if we are retrying shortly
                             *ftp_guard = Some(ftp);
                         }
                         // Drop locks before sleeping
                         drop(current_state);
                         drop(current_index);
                         drop(file_list);
                         drop(ftp_guard);

                         tokio::select! {
                             _ = sleep(Duration::from_secs(3 * consecutive_errors)) => continue,
                              _ = stop_rx.changed() => {
                                 info!("Stop signal received during file list error backoff.");
                                 // Attempt to quit before breaking
                                 if let Ok(mut ftp_inner) = ftp_stream_arc.try_lock() {
                                      if let Some(mut f) = ftp_inner.take() {
                                           let _ = f.quit().await;
                                      }
                                 }
                                 break;
                             }
                         }
                    }
                };


                let current_filenames: Vec<String> = file_list.iter().map(|(_, name)| name.clone()).collect();
                let new_filenames: Vec<String> = updated_file_list.iter().map(|(_, name)| name.clone()).collect();


                let needs_update = file_list.is_empty() || current_filenames != new_filenames;

                if needs_update {
                     debug!("File list changed or initialized.");
                     *file_list = updated_file_list;
                     // Re-evaluate current index based on last known file
                     if let Some(idx) = file_list.iter().position(|(_, name)| *name == current_state.last_file) {
                         *current_index = idx;
                         debug!("Resuming from known file '{}' at index {}", current_state.last_file, idx);
                     } else {
                          debug!("Last known file '{}' not found or initial run. Starting from the beginning.", current_state.last_file);
                          *current_index = 0;
                          current_state.last_position = 0;
                          if let Some((_, first_file)) = file_list.front() {
                              current_state.last_file = first_file.clone();
                          } else {
                              current_state.last_file = String::new(); // No files
                          }
                          // Save state immediately if starting fresh or file missing
                          let state_to_save = current_state.clone();
                           if let Err(e) = Self::save_state(&config.state_file, &state_to_save).await {
                               error!("Failed to save initial state: {}", e);
                           }
                     }
                }

                 if file_list.is_empty() {
                     debug!("No files found in remote directory. Waiting...");
                     // Put ftp back before sleeping
                     *ftp_guard = Some(ftp);
                     // Drop locks before sleeping/selecting
                     drop(current_state);
                     drop(current_index);
                     drop(file_list);
                     drop(ftp_guard);

                     tokio::select! {
                         _ = sleep(Duration::from_secs(5)) => continue,
                         _ = stop_rx.changed() => {
                             info!("Stop signal received while waiting for files.");
                             // Attempt to quit before breaking
                             if let Ok(mut ftp_inner) = ftp_stream_arc.try_lock() {
                                 if let Some(mut f) = ftp_inner.take() {
                                     let _ = f.quit().await;
                                 }
                             }
                             break;
                         }
                     }
                 }

                // Get the file to process based on current_index
                let (file_dt, file_name) = match file_list.get(*current_index) {
                    Some(f) => f.clone(), // Clone dt and name
                    None => {
                        warn!("Current index {} is out of bounds for file list (len {}). Resetting index.", *current_index, file_list.len());
                        *current_index = 0; // Reset to beginning
                        current_state.last_position = 0;
                        if let Some((_, first_file)) = file_list.front() {
                            current_state.last_file = first_file.clone();
                             let state_to_save = current_state.clone();
                             if let Err(e) = Self::save_state(&config.state_file, &state_to_save).await {
                                 error!("Failed to save state after index reset: {}", e);
                             }
                        } else {
                             current_state.last_file = String::new();
                        }
                         // Put ftp back before continuing
                         *ftp_guard = Some(ftp);
                         continue; // Re-evaluate in the next loop iteration
                    }
                };

                // Update current file name in state if we are processing it
                if current_state.last_file != file_name {
                    debug!("Moving to process file: {}", file_name);
                    current_state.last_file = file_name.clone();
                    current_state.last_position = 0; // Start from beginning of new file
                     // Save state for the new file
                     let state_to_save = current_state.clone();
                     if let Err(e) = Self::save_state(&config.state_file, &state_to_save).await {
                         error!("Failed to save state for new file '{}': {}", file_name, e);
                     }
                }


                // Read new lines from the current file
                match Self::read_file_from_position(&mut ftp, &file_name, current_state.last_position, &config).await {
                    Ok((new_lines, total_lines)) => {
                        consecutive_errors = 0; // Reset read errors
                        let mut handler = line_handler.lock().await;
                        for line in new_lines {
                            let current_line_index = current_state.last_position; // For state saving
                            match handler(line) {
                                Ok(_) => {
                                    current_state.last_position = current_line_index + 1; // Increment only on successful handling
                                    // Save state after processing each line (or batch if preferred)
                                    let state_to_save = current_state.clone();
                                    if let Err(e) = Self::save_state(&config.state_file, &state_to_save).await {
                                        error!("Failed to save state after processing line {} of '{}': {}", current_state.last_position, file_name, e);
                                        // Decide if we should stop or continue despite state saving error
                                    }
                                }
                                Err(e) => {
                                    error!("Line handler failed for line {} of file '{}': {}", current_state.last_position + 1, file_name, e);
                                    // Optional: Stop processing on handler error, or just log and continue
                                    // break; // Example: Stop processing this file on handler error
                                }
                            }
                            // Optional: Add a small delay if needed
                            // sleep(Duration::from_millis(10)).await;

                            // Check for stop signal frequently if processing many lines
                            if stop_rx.has_changed().unwrap_or(false) {
                                info!("Stop signal received while processing lines.");
                                // Put ftp back before breaking
                                *ftp_guard = Some(ftp);
                                break; // Break inner loop
                            }
                        }
                        drop(handler); // Release handler lock

                        // Check if we reached the end of the current file
                        if current_state.last_position >= total_lines {
                             debug!("Reached end of file '{}' ({} lines)", file_name, total_lines);
                             if *current_index + 1 < file_list.len() {
                                 debug!("Moving to next file index {}", *current_index + 1);
                                 *current_index += 1;
                                 // State (filename, position) will be updated at the start of the next loop iteration
                             } else {
                                 debug!("Reached end of the last file. Waiting for new files/updates.");
                                 // Put ftp back before sleeping/selecting
                                 *ftp_guard = Some(ftp);
                                 // Drop locks before sleeping/selecting
                                 drop(current_state);
                                 drop(current_index);
                                 drop(file_list);
                                 drop(ftp_guard);

                                 tokio::select! {
                                     _ = sleep(Duration::from_secs(3)) => {} // Wait before checking again
                                     _ = stop_rx.changed() => {
                                         info!("Stop signal received while waiting at end of list.");
                                         // Attempt to quit before breaking
                                         if let Ok(mut ftp_inner) = ftp_stream_arc.try_lock() {
                                              if let Some(mut f) = ftp_inner.take() {
                                                   let _ = f.quit().await;
                                              }
                                         }
                                         break; // Break outer loop
                                     }
                                 }
                                 continue; // Continue outer loop to check for new files
                             }
                        } else {
                             // File not finished, wait a bit before checking for appends
                             debug!("File '{}' not finished (at line {}/{}). Waiting for appends.", file_name, current_state.last_position, total_lines);
                             // Put ftp back before sleeping/selecting
                            *ftp_guard = Some(ftp);
                            // Drop locks before sleeping/selecting
                            drop(current_state);
                            drop(current_index);
                            drop(file_list);
                            drop(ftp_guard);

                            tokio::select! {
                                _ = sleep(Duration::from_secs(1)) => {}
                                _ = stop_rx.changed() => {
                                    info!("Stop signal received while waiting for appends.");
                                     // Attempt to quit before breaking
                                     if let Ok(mut ftp_inner) = ftp_stream_arc.try_lock() {
                                          if let Some(mut f) = ftp_inner.take() {
                                               let _ = f.quit().await;
                                          }
                                     }
                                    break; // Break outer loop
                                }
                            }
                            continue; // Continue outer loop
                        }

                    }
                    Err(e) => {
                        error!("Failed to read file '{}': {}", file_name, e);
                        consecutive_errors += 1;
                        if consecutive_errors > 3 {
                             warn!("Too many errors reading file '{}'. Resetting connection.", file_name);
                             let _ = ftp.quit().await; // Attempt graceful quit
                             // Let the next iteration reconnect
                        } else {
                             // Put ftp back for retry
                             *ftp_guard = Some(ftp);
                        }
                        // Drop locks before sleeping/selecting
                        drop(current_state);
                        drop(current_index);
                        drop(file_list);
                        drop(ftp_guard);

                        tokio::select! {
                            _ = sleep(Duration::from_secs(3 * consecutive_errors)) => continue,
                             _ = stop_rx.changed() => {
                                info!("Stop signal received during file read error backoff.");
                                 // Attempt to quit before breaking
                                 if let Ok(mut ftp_inner) = ftp_stream_arc.try_lock() {
                                      if let Some(mut f) = ftp_inner.take() {
                                           let _ = f.quit().await;
                                      }
                                 }
                                break;
                            }
                        }
                    }
                }

                // Put the potentially modified ftp stream back into the mutex guard
                *ftp_guard = Some(ftp);
                 drop(current_state);
                 drop(current_index);
                 drop(file_list);
                 drop(ftp_guard);


                // Check stop signal at the end of the loop iteration
                if stop_rx.has_changed().unwrap_or(false) {
                    info!("Stop signal received at end of loop.");
                    break; // Break outer loop
                }
            } // End main loop

            // --- Cleanup after loop exits ---
            info!("FTP reader task loop finished. Cleaning up.");
            // Attempt to quit the FTP connection if it exists
             if let Ok(mut guard) = ftp_stream_arc.try_lock() {
                 if let Some(mut ftp) = guard.take() {
                     match ftp.quit().await {
                         Ok(_) => info!("FTP connection quit successfully."),
                         Err(e) => warn!("Error quitting FTP connection: {}", e),
                     }
                 }
             } else {
                 warn!("Could not acquire FTP stream lock for cleanup.");
             }
            stop_notify.notify_one(); // Notify anyone waiting for stop completion
            info!("FTP reader task stopped.");
        }); // End tokio::spawn

        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            info!("Sending stop signal to FTP reader task...");
            // Sending on a watch channel doesn't really fail in a way we need to handle here.
            // The receiver side will detect the change.
            let _ = tx.send(()); // Signal the change

            // Wait for the task to actually finish cleanup
            let notified = tokio::time::timeout(Duration::from_secs(10), self.stop_notify.notified()).await;

            match notified {
                 Ok(_) => info!("FTP reader task cleanup confirmed."),
                 Err(_) => warn!("Timeout waiting for FTP reader task cleanup confirmation."),
            }

        } else {
            info!("FTP reader task already stopped or not started.");
        }
    }
}

// Optional: Example usage or test function (usually in tests/ or examples/)
#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tokio::sync::Mutex;

    // Define a simple line handler for testing
    fn test_line_handler(line: String) -> Result<()> {
        println!("[Test Handler] Received line: {}", line);
        // Simulate potential failure
        // if line.contains("error") {
        //     anyhow::bail!("Simulated handler error");
        // }
        Ok(())
    }

    // This test requires environment variables for FTP credentials and server details
    // FTP_HOST, FTP_PORT, FTP_USER, FTP_PASS, FTP_REMOTE_DIR
    // and potentially a dummy file on the server.
    #[tokio::test]
    #[ignore] // Ignore by default as it requires a live FTP server and credentials
    async fn test_ftp_reader_integration() {
        // Setup tracing for tests
        let _ = tracing_subscriber::fmt().try_init();

        let config = FtpReaderConfig {
            host: env::var("FTP_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("FTP_PORT")
                .map_or(Ok(21), |p| p.parse::<u16>())
                .unwrap_or(21),
            user: env::var("FTP_USER").unwrap_or_else(|_| "anonymous".to_string()),
            pass: env::var("FTP_PASS").unwrap_or_else(|_| "test@example.com".to_string()),
            remote_dir: env::var("FTP_REMOTE_DIR").unwrap_or_else(|_| "/".to_string()),
            state_file: PathBuf::from("/tmp/ftp_reader_test_state.json"), // Use temp file
            verbose: true,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            passive_mode: true, // Most servers require passive mode
        };

        // Clean up state file before test
        let _ = std::fs::remove_file(&config.state_file);

        let handler_log = Arc::new(Mutex::new(Vec::new()));
        let handler_log_clone = Arc::clone(&handler_log);

        let handler = move |line: String| -> Result<()> {
            println!("[Test Handler] Processing: {}", line);
            let mut log = handler_log_clone.blocking_lock(); // Use blocking lock in sync context
            log.push(line);
            Ok(())
        };


        let mut reader = FtpReader::new(config, handler)
            .await
            .expect("Failed to create FtpReader");

        reader.start().expect("Failed to start FTP reader task");

        // Let the reader run for a while to potentially pick up files/lines
        println!("Waiting for FTP reader to process...");
        sleep(Duration::from_secs(15)).await; // Adjust as needed

        // Example: Test seeking (if applicable)
        // reader.seek("your_test_file.log", 5).await.expect("Seek failed");
        // sleep(Duration::from_secs(5)).await; // Wait after seek

        println!("Stopping FTP reader...");
        reader.stop().await;
        println!("FTP reader stopped.");

        // Assertions based on expected lines received
        let log = handler_log.lock().await;
        println!("Total lines received: {}", log.len());
        // Add specific assertions here based on the files on your test FTP server
        // assert!(!log.is_empty(), "No lines were received by the handler");
        // assert!(log.iter().any(|l| l.contains("expected content")), "Expected content not found");

        // Clean up state file after test
        let _ = std::fs::remove_file("/tmp/ftp_reader_test_state.json");
    }
}
