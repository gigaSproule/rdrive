use drive3::{File, Scope};
use drive3::DriveHub;
use hyper::Client;
use oauth2::{Authenticator, DefaultAuthenticatorDelegate, DiskTokenStorage};

pub struct Drive {
    hub: DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>
}

impl<'a> Drive {
    pub fn new(hub: DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>) -> Drive {
        Drive {
            hub
        }
    }

    pub fn get_all_files(&'a self, page_token: Option<String>) -> Vec<File> {
        let fields = "nextPageToken, files(kind, name, description, kind, mimeType)";
        let mut file_list_call = self.hub.files().list().add_scope(Scope::Readonly).param("fields", fields);
        if page_token.is_some() {
            file_list_call = file_list_call.page_token(page_token.unwrap().as_str())
        }
        let hub_result = file_list_call.doit();
        return match hub_result {
            Ok(x) => {
                let mut files = x.1.files.unwrap();
                if x.1.next_page_token.is_some() {
                    for file in self.get_all_files(x.1.next_page_token.to_owned()) {
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
}
