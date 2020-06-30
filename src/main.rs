extern crate google_drive3 as drive3;
extern crate hyper;
extern crate hyper_rustls;
extern crate yup_oauth2 as oauth2;

use std::borrow::Borrow;
use std::path::Path;

use drive3::{File, Scope};
use drive3::DriveHub;
use hyper::Client;
use hyper::net::HttpsConnector;
use hyper_rustls::TlsClient;
use oauth2::{ApplicationSecret, Authenticator, DefaultAuthenticatorDelegate, DiskTokenStorage};

fn main() {
    let secret: ApplicationSecret = yup_oauth2::read_application_secret(Path::new("secret.json"))
        .expect("secret.json");
    let token_storage = DiskTokenStorage::new(&String::from("temp-key")).unwrap();
    let auth = Authenticator::new(
        &secret,
        DefaultAuthenticatorDelegate,
        get_client(),
        token_storage,
        Option::from(yup_oauth2::FlowType::InstalledInteractive),
    );

    let hub = DriveHub::new(get_client(), auth);
    let files = get_files(hub, None);
    println!("Retrieved {} files", files.len());
    for file in files {
        println!("Name: {}, Mime-Type: {}", file.name.unwrap(), file.mime_type.unwrap())
    }
}

fn get_files(hub: DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>, page_token: Option<String>) -> Vec<File> {
    let fields = "nextPageToken, files(kind, name, description, kind, mimeType)";
    let mut file_list_call = hub.files().list().add_scope(Scope::Readonly).param("fields", fields);
    if page_token.is_some() {
        file_list_call = file_list_call.page_token(page_token.unwrap().as_str())
    }
    let hub_result = file_list_call.doit();
    return match hub_result {
        Ok(x) => {
            let mut files = x.1.files.unwrap();
            if x.1.next_page_token.is_some() {
                for file in get_files(hub, x.1.next_page_token) {
                    files.push(file);
                }
            }
            files
        }
        Err(e) => {
            println!("Error: {}", e);
            Vec::new()
        }
    };
}

fn get_client() -> Client {
    return Client::with_connector(HttpsConnector::new(TlsClient::new()));
}
