extern crate google_drive3 as drive3;
extern crate hyper;
extern crate hyper_rustls;
extern crate yup_oauth2 as oauth2;

use std::borrow::Borrow;
use std::path::Path;

use drive3::DriveHub;
use futures::{executor::block_on};
use hyper::Client;
use hyper::net::HttpsConnector;
use hyper_rustls::TlsClient;
use log::{debug, LevelFilter, SetLoggerError};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Handle;
use oauth2::{ApplicationSecret, Authenticator, DefaultAuthenticatorDelegate, DiskTokenStorage};

use crate::drive::{Drive, FileWrapper};

mod drive;

fn main() {
    let _handle = configure_logging().unwrap();
    let hub = DriveHub::new(get_client(), get_authenticator());
    let mut drive = Drive::new(hub);
    let file_wrappers = drive.get_all_files(true);
    debug!("Retrieved {} files", file_wrappers.len());
    block_on(download_all_files(&mut drive, file_wrappers));
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
            .appender("stdout")
            .build("rdrive", LevelFilter::Debug))
        .build(Root::builder().appender("stdout").build(LevelFilter::Warn))
        .unwrap();

    log4rs::init_config(config)
}

async fn download_all_files(drive: &mut Drive, file_wrappers: Vec<FileWrapper>) {
    let mut download_futures = vec![];
    for file in file_wrappers {
        debug!("Path: {}, Name: {}, Directory: {}", &file.path.display(), &file.file.name.borrow().as_ref().unwrap(), &file.directory);
        if !file.directory {
            download_futures.push(drive.create_file(file.clone()));
        }
    }
    for future in download_futures {
        future.join().expect("Failed to join to future");
    }
}

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
