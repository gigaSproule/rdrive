use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::{Read, Write};

use drive3::{File, Scope};
use drive3::DriveHub;
use hyper::Client;
use oauth2::{Authenticator, DefaultAuthenticatorDelegate, DiskTokenStorage};

pub struct Drive {
    hub: DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>,
    root_dir: String,
    files: Vec<File>,
}

impl<'a> Drive {
    pub fn new(hub: DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>, root_dir: String) -> Drive {
        Drive {
            hub,
            root_dir,
            files: Vec::new(),
        }
    }

    pub fn get_all_files(&mut self, page_token: Option<String>) -> Vec<File> {
        let fields = "nextPageToken, files(id, kind, name, description, kind, mimeType, parents, ownedByMe, webContentLink)";
        let mut file_list_call = self.hub.files().list().add_scope(Scope::Full).param("fields", fields);
        if page_token.is_some() {
            file_list_call = file_list_call.page_token(page_token.unwrap().as_str())
        }
        let hub_result = file_list_call.doit();
        let fetched_files = match hub_result {
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
        self.files = fetched_files.clone();
        return fetched_files;
    }

    pub fn get_all_files_in_hierarchy(&mut self, owned_only: bool) -> Vec<FileWrapper> {
        let mut files_by_id = HashMap::new();
        let borrowed_files: &Vec<File> = self.files.borrow();
        for file in borrowed_files {
            files_by_id.insert(file.id.clone().unwrap(), file.clone());
        }
        let mut file_wrappers = Vec::new();
        for file in borrowed_files {
            if owned_only && !file.owned_by_me.unwrap() {
                continue;
            }
            let path = self.get_path(&file, &files_by_id);
            file_wrappers.push(FileWrapper {
                file: file.clone(),
                path,
                directory: file.mime_type.clone().unwrap() == DIRECTORY_MIME_TYPE,
            });
        }
        return file_wrappers;
    }

    fn get_path(&'a self, file: &File, files_by_id: &HashMap<String, File>) -> String {
        let parents = file.parents.as_ref();
        if parents.is_none() {
            return "/".parse().unwrap();
        }
        let parent_id = parents.unwrap().first();
        if parent_id.is_none() {
            return "/".parse().unwrap();
        }
        let mut parent = files_by_id.get(parent_id.unwrap());
        if parent.is_none() {
            return "/".parse().unwrap();
        }
        let parent_name = parent.unwrap().name.as_ref();
        if parent_name.is_none() {
            return "/".parse().unwrap();
        }
        let mut path = parent_name.unwrap().clone();
        while parent.is_some() {
            let parents = parent.unwrap().parents.as_ref();
            if parents.is_none() {
                parent = None;
            } else {
                let parent_id = parents.unwrap().first();
                if parent_id.is_none() {
                    parent = None;
                } else {
                    parent = files_by_id.get(parent_id.unwrap());
                    if parent.is_some() {
                        path = parent.unwrap().name.clone().unwrap() + "/" + &path;
                    }
                }
            }
        }
        let root: String = "/".to_string();
        return root + &path;
    }

    pub fn create_file(&'a self, file_wrapper: &FileWrapper) -> std::io::Result<()> {
        let mut path = self.root_dir.clone();
        path.push_str(&*file_wrapper.path);
        path.push_str("/");
        println!("Ensuring path {} exists", path);
        std::fs::create_dir_all(&path)?;
        path.push_str(file_wrapper.file.name.borrow().as_ref().unwrap());
        println!("Creating file {}", path);
        let mut created_file = std::fs::File::create(path)?;
        if !file_wrapper.file.mime_type.borrow().as_ref().unwrap().contains("google") {
            let response = self.hub.files().get(file_wrapper.file.id.borrow().as_ref().unwrap()).param("alt", "media").add_scope(Scope::Full).doit();
            let mut response_body = Vec::new();
            response.unwrap().0.read_to_end(&mut response_body)?;
            created_file.write_all(response_body.as_ref())?;
        }
        Ok(())
    }
}

const DIRECTORY_MIME_TYPE: &str = "application/vnd.google-apps.folder";

pub struct FileWrapper {
    pub file: File,
    pub path: String,
    pub directory: bool,
}
