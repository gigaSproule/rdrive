use std::{
    borrow::Borrow,
    collections::HashMap,
    io::{BufReader, Read, Write},
    path::Path,
};
use std::io::BufWriter;

use drive3::{File, Scope};
use drive3::DriveHub;
use hyper::Client;
use oauth2::{Authenticator, DefaultAuthenticatorDelegate, DiskTokenStorage};
use regex::Regex;
use serde::{Deserialize, Serialize};

pub struct Drive {
    hub: DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>,
    config: Config,
    files: Vec<File>,
}

impl<'a> Drive {
    pub fn new(hub: DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>) -> Drive {
        Drive {
            hub,
            config: Drive::get_config(),
            files: Vec::new(),
        }
    }

    fn init(&'a mut self) {
        self.files = self.fetch_files(None);
    }

    fn fetch_files(&'a self, page_token: Option<String>) -> Vec<File> {
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
                    for file in self.fetch_files(x.1.next_page_token.to_owned()) {
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
        return fetched_files;
    }

    pub fn get_all_files(&'a mut self, owned_only: bool) -> Vec<FileWrapper> {
        let mut files_by_id = HashMap::new();
        if self.files.is_empty() {
            self.init();
        }
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
            if self.config.ignore.iter().any(|regex| regex.is_match(&path)) {
                continue;
            }
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
        let path_string = self.config.root_dir.clone() + &file_wrapper.path + "/" + &file_wrapper.file.name.borrow().as_ref().unwrap();
        let path = Path::new(&path_string);
        if !path.exists() {
            std::fs::create_dir_all(&path.parent().unwrap())?;
            if !file_wrapper.file.mime_type.borrow().as_ref().unwrap().contains("google") {
                let response = self.hub.files().get(file_wrapper.file.id.borrow().as_ref().unwrap()).param("alt", "media").add_scope(Scope::Full).doit();
                let mut response_body = Vec::new();
                response.unwrap().0.read_to_end(&mut response_body)?;
                std::fs::File::create(path)?.write_all(response_body.as_ref())?;
            } else {
                std::fs::File::create(path)?;
            }
        }
        Ok(())
    }

    fn get_config() -> Config {
        let config_file = Drive::get_config_file();
        let stored_config: serde_json::Result<StoredConfig> = serde_json::from_reader(BufReader::new(&config_file));
        if stored_config.is_ok() {
            let config = stored_config.unwrap();
            return Config {
                ignore: config.ignore.iter().map(|regex| Regex::new(regex).unwrap()).collect(),
                root_dir: config.root_dir,
            };
        }
        let default_root_dir = std::env::var("HOME").unwrap() + "/rdrive";
        let default_stored_config = StoredConfig { ignore: Vec::new(), root_dir: default_root_dir.clone() };
        let write_result = serde_json::to_writer_pretty(BufWriter::new(&config_file), &default_stored_config);
        if write_result.is_err() {
            println!("{}", write_result.unwrap_err());
        }
        return Config {
            ignore: Vec::new(),
            root_dir: default_root_dir.clone(),
        };
    }

    fn get_config_file() -> std::fs::File {
        let file = Drive::get_base_config_path() + "/rdrive/config.json";
        let config_file = Path::new(file.as_str());
        if !config_file.exists() {
            let create_config_dir = std::fs::create_dir_all(config_file.parent().unwrap());
            if create_config_dir.is_err() {
                panic!("Failed to create config path {}. {}", config_file.display(), create_config_dir.unwrap_err());
            }
            return std::fs::File::create(config_file).unwrap();
        }
        return std::fs::OpenOptions::new().write(true).read(true).open(config_file).unwrap();
    }

    fn get_base_config_path() -> String {
        return match std::env::consts::OS {
            "windows" => std::env::var("LOCALAPPDATA").unwrap(),
            "linux" => std::env::var("XDG_CONFIG_HOME").unwrap_or(std::env::var("HOME").unwrap() + "/.config"),
            "mac" => std::env::var("HOME").unwrap() + "/Library/Preferences",
            _ => String::new()
        };
    }
}

const DIRECTORY_MIME_TYPE: &str = "application/vnd.google-apps.folder";

pub struct FileWrapper {
    pub file: File,
    pub path: String,
    pub directory: bool,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
struct StoredConfig {
    ignore: Vec<String>,
    root_dir: String,
}

struct Config {
    ignore: Vec<Regex>,
    root_dir: String,
}
