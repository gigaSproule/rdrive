extern crate google_drive3 as drive3;
extern crate hyper;
extern crate hyper_rustls;
extern crate yup_oauth2 as oauth2;

use std::{env, fs, thread};
use std::path::Path;
use std::time::{Duration, SystemTime};

use drive3::DriveHub;
use hyper::Client;
use hyper::net::HttpsConnector;
use hyper_rustls::TlsClient;
use log::{debug, error, LevelFilter, SetLoggerError};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Handle;
use oauth2::{ApplicationSecret, Authenticator, DefaultAuthenticatorDelegate, DiskTokenStorage};
use rusqlite::Connection;

use crate::drive::{Drive, FileWrapper};

mod drive;
mod dbcontext;

fn main() {
    let _handle = configure_logging().unwrap();
    let connection = get_db_connection();
    let hub = DriveHub::new(get_client(), get_authenticator());

    let drive = Drive::new(hub, connection);
    drive.init();

    loop {
        drive.store_fetched_files();
        let existing_file_wrappers = drive.get_all_files(true).unwrap();
        debug!("Retrieved {} files", existing_file_wrappers.len());
        for file_wrapper in &existing_file_wrappers {
            handle_existing_file(&drive, file_wrapper)
        }
        let local_files: Vec<FileWrapper> = drive.get_local_files().unwrap();
        for file_wrapper in &local_files {
            if existing_file_wrappers.iter().any(|f| f.path.to_str().unwrap() == file_wrapper.path.to_str().unwrap()) {
                debug!("Not handling {} as a local file as it's already been handled", file_wrapper.path.display())
            } else {
                debug!("Upload {} to Google Drive for the first time", file_wrapper.path.display());
                let result = drive.upload_file(file_wrapper);
                if result.is_err() {
                    error!("Error occurred whilst uploading {} to Google Drive for the first time. {}", file_wrapper.path.display(), result.unwrap_err())
                }
            }
        }
        thread::sleep(Duration::from_secs(30));
    }
}

fn handle_existing_file(drive: &Drive, file_wrapper: &FileWrapper) {
    if file_wrapper.directory || file_wrapper.trashed {
        return;
    }
    if !file_wrapper.path.exists() {
        debug!("Creating file {} for the first time", file_wrapper.path.display());
        drive.create_file(file_wrapper);
    } else {
        let local_modified_time = file_wrapper.path.metadata().unwrap().modified().unwrap().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let remote_modified_time = file_wrapper.last_accessed.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        if remote_modified_time == 0 {
            debug!("Remote modified time wasn't updated properly for file {} when it was created", file_wrapper.path.display());
            drive.create_file(file_wrapper);
        } else if local_modified_time > remote_modified_time {
            debug!("File {} has changed locally since last sync", file_wrapper.path.display());
            drive.upload_file(file_wrapper);
        } else if local_modified_time < remote_modified_time {
            debug!("File {} has changed on remote since last sync", file_wrapper.path.display());
            drive.create_file(file_wrapper);
        } else {
            debug!("Nothing to do for file {}", file_wrapper.path.display());
        }
    }
}

fn configure_logging() -> Result<Handle, SetLoggerError> {
    let stdout = ConsoleAppender::builder().build();

    let file = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
        .build("log/rdrive.log")
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("file", Box::new(file)))
        .logger(Logger::builder()
            .appender("file")
            .build("rdrive", LevelFilter::Debug))
        .build(Root::builder().appender("stdout").build(LevelFilter::Warn))
        .unwrap();

    log4rs::init_config(config)
}

// fn download_all_files(drive: &Drive<'_>, file_wrappers: Vec<FileWrapper>) {
//     let mut download_futures = vec![];
//     for file in file_wrappers {
//         debug!("Path: {}, Name: {}, Directory: {}", &file.path.display(), &file.name, &file.directory);
//         if !file.directory {
//             download_futures.push(drive.create_file(file.clone()));
//         }
//     }
//     for future in download_futures {
//         future.join().expect("Failed to join to future");
//     }
// }

fn get_client() -> Client {
    Client::with_connector(HttpsConnector::new(TlsClient::new()))
}

fn get_authenticator() -> Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client> {
    let secret: ApplicationSecret = yup_oauth2::read_application_secret(Path::new("secret.json")).expect("secret.json");
    let token_storage = DiskTokenStorage::new(&String::from("temp-key")).unwrap();
    Authenticator::new(
        &secret,
        DefaultAuthenticatorDelegate,
        get_client(),
        token_storage,
        Option::from(yup_oauth2::FlowType::InstalledInteractive),
    )
}

fn get_db_connection() -> Connection {
    let db_file = Path::new(&get_base_data_path())
        .join("rdrive")
        .join("rdrive.db");
    fs::create_dir_all(&db_file.parent().unwrap()).unwrap();
    return Connection::open(db_file).unwrap();
}

fn get_base_data_path() -> String {
    return match env::consts::OS {
        "windows" => env::var("LOCALAPPDATA").unwrap(),
        "linux" => env::var("XDG_DATA_HOME").unwrap_or(env::var("HOME").unwrap() + "/.local/share"),
        "macos" => env::var("HOME").unwrap() + "/Library",
        _ => String::new()
    };
}
