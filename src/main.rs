extern crate google_drive3 as drive3;
extern crate hyper;
extern crate hyper_rustls;
extern crate yup_oauth2 as oauth2;

use std::borrow::Borrow;
use std::path::Path;

use drive3::DriveHub;
use hyper::Client;
use hyper::net::HttpsConnector;
use hyper_rustls::TlsClient;
use oauth2::{ApplicationSecret, Authenticator, DefaultAuthenticatorDelegate, DiskTokenStorage};

mod drive;

fn main() {
    let hub = DriveHub::new(get_client(), get_authenticator());
    let mut drive = drive::Drive::new(hub);
    let file_wrappers = drive.get_all_files(true);
    println!("Retrieved {} files", file_wrappers.len());
    for file in file_wrappers {
        println!("Path: {}, Name: {}, Directory: {}", &file.path, &file.file.name.borrow().as_ref().unwrap(), &file.directory);
        if file.directory {
            continue;
        }
        let created = drive.create_file(&file);
        if created.is_err() {
            println!("Failed to create file {}/{}. {}", &file.path, &file.file.name.borrow().as_ref().unwrap(), created.err().unwrap());
        }
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
