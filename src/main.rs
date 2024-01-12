extern crate google_drive3 as drive3;
extern crate hyper;
extern crate hyper_rustls;
extern crate yup_oauth2 as oauth2;

use std::error::Error;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use std::{env, fs, thread};

use drive3::DriveHub;
use hyper::client::HttpConnector;
use hyper::Client;
use hyper_rustls::HttpsConnector;
use log::{debug, error, LevelFilter, SetLoggerError};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::threshold::ThresholdFilter;
use log4rs::Handle;
use oauth2::authenticator::Authenticator;
use oauth2::{ApplicationSecret, InstalledFlowAuthenticator, InstalledFlowReturnMethod};
use rusqlite::Connection;

use crate::drive::{Drive, FileWrapper};

mod dbcontext;
mod drive;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _handle = configure_logging().unwrap();
    let connection = get_db_connection();
    let hub = DriveHub::new(get_client(), get_authenticator().await);

    let drive = Drive::new(hub, connection);
    drive.init().await;

    loop {
        drive.store_fetched_files().await?;
        let existing_file_wrappers = drive.get_all_files(true).unwrap();
        debug!("Retrieved {} files", existing_file_wrappers.len());
        for file_wrapper in &existing_file_wrappers {
            handle_existing_file(&drive, file_wrapper).await
        }
        let local_files: Vec<FileWrapper> = drive.get_local_files().unwrap();
        for file_wrapper in &local_files {
            if existing_file_wrappers
                .iter()
                .any(|f| f.path.to_str().unwrap() == file_wrapper.path.to_str().unwrap())
            {
                debug!(
                    "Not handling {} as a local file as it's already been handled",
                    file_wrapper.path.display()
                )
            } else {
                if file_wrapper.directory {
                    debug!("Can't currently handle new directories");
                    continue;
                }
                debug!(
                    "Upload {} to Google Drive for the first time",
                    file_wrapper.path.display()
                );
                let result = drive.upload_file(file_wrapper).await;
                if result.is_err() {
                    error!(
                        "Error occurred whilst uploading {} to Google Drive for the first time. {}",
                        file_wrapper.path.display(),
                        result.unwrap_err()
                    )
                }
            }
        }
        thread::sleep(Duration::from_secs(30));
    }
}

async fn handle_existing_file(drive: &Drive, file_wrapper: &FileWrapper) {
    if file_wrapper.directory || file_wrapper.trashed {
        return;
    }
    if !file_wrapper.path.exists() {
        debug!(
            "Creating file {} for the first time",
            file_wrapper.path.display()
        );
        let created = drive.create_file(file_wrapper).await;
        if created.is_err() {
            error!("Unable to create file {}.", file_wrapper.path.display())
        }
    } else {
        let local_modified_time = file_wrapper
            .path
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let remote_modified_time = file_wrapper
            .last_accessed
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if remote_modified_time == 0 {
            debug!(
                "Remote modified time wasn't updated properly for file {} when it was created",
                file_wrapper.path.display()
            );
            let created = drive.create_file(file_wrapper).await;
            if created.is_err() {
                error!("Unable to create file {}.", file_wrapper.path.display())
            }
        } else if local_modified_time > remote_modified_time {
            debug!(
                "File {} has changed locally since last sync",
                file_wrapper.path.display()
            );
            let uploaded = drive.upload_file(file_wrapper).await;
            if uploaded.is_err() {
                error!("Unable to create file {}.", file_wrapper.path.display())
            }
        } else if local_modified_time < remote_modified_time {
            debug!(
                "File {} has changed on remote since last sync",
                file_wrapper.path.display()
            );
            let created = drive.create_file(file_wrapper).await;
            if created.is_err() {
                error!("Unable to create file {}.", file_wrapper.path.display())
            }
        } else {
            debug!("Nothing to do for file {}", file_wrapper.path.display());
        }
    }
}

fn configure_logging() -> Result<Handle, SetLoggerError> {
    let stdout = ConsoleAppender::builder().build();

    let file = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
        .build(get_base_log_path().join("rdrive.log"))
        .unwrap();

    let config = Config::builder()
        .appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(LevelFilter::Warn)))
                .build("stdout", Box::new(stdout)),
        )
        .appender(Appender::builder().build("file", Box::new(file)))
        .logger(
            Logger::builder()
                .appender("file")
                .build("rdrive", LevelFilter::Debug),
        )
        .build(Root::builder().appender("stdout").build(LevelFilter::Warn))
        .unwrap();

    log4rs::init_config(config)
}

fn get_client() -> Client<HttpsConnector<HttpConnector>> {
    Client::builder().build(
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("msg")
            .https_or_http()
            .enable_http1()
            .build(),
    )
}

async fn get_authenticator() -> Authenticator<drive3::hyper_rustls::HttpsConnector<HttpConnector>> {
    let secret: ApplicationSecret = yup_oauth2::read_application_secret("../secret.json")
        .await
        .expect("secret.json");
    let token_file = &get_base_data_path()
        .join("temp-key")
        .to_str()
        .unwrap()
        .to_owned();
    InstalledFlowAuthenticator::builder(secret, InstalledFlowReturnMethod::Interactive)
        .persist_tokens_to_disk(token_file)
        .build()
        .await
        .unwrap()
}

fn get_db_connection() -> Connection {
    let db_file = &get_base_data_path().join("rdrive.db");
    fs::create_dir_all(&db_file.parent().unwrap()).unwrap();
    return Connection::open(db_file).unwrap();
}

fn get_base_data_path() -> PathBuf {
    let data_path = match env::consts::OS {
        "windows" => PathBuf::from(env::var("LOCALAPPDATA").unwrap()),
        "linux" => PathBuf::from(
            env::var("XDG_DATA_HOME").unwrap_or(env::var("HOME").unwrap() + "/.local/share"),
        ),
        "macos" => PathBuf::from(env::var("HOME").unwrap() + "/Library"),
        _ => PathBuf::new(),
    };
    data_path.join("rdrive")
}

fn get_base_log_path() -> PathBuf {
    let log_path = match env::consts::OS {
        "windows" => PathBuf::from(env::var("LOCALAPPDATA").unwrap()),
        "linux" => PathBuf::from(
            env::var("XDG_DATA_HOME").unwrap_or(env::var("HOME").unwrap() + "/.local/share"),
        ),
        "macos" => PathBuf::from(env::var("HOME").unwrap())
            .join("Library")
            .join("Logs"),
        _ => PathBuf::new(),
    };
    log_path.join("rdrive")
}
