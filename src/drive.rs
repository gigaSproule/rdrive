use std::{borrow::Borrow, collections::HashMap, io::{BufReader, Read}, io, path::Path};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::thread::JoinHandle;

use chrono::{DateTime, FixedOffset, Utc};
use drive3::{File, Scope};
use drive3::DriveHub;
use glob::Pattern;
use hyper::Client;
use hyper::client::Response;
use log::{debug, error};
use oauth2::{Authenticator, DefaultAuthenticatorDelegate, DiskTokenStorage};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::dbcontext::DbContext;

pub struct Drive<'a> {
    hub: &'a DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>,
    context: DbContext<'a>,
    config: Config,
}

impl<'a> Drive<'a> {
    pub fn new(hub: &'a DriveHub<Client, Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>>, connection: &'a Connection) -> Drive<'a> {
        Drive {
            hub,
            context: DbContext::new(connection),
            config: Drive::get_config(),
        }
    }

    pub fn init(&mut self) {
        self.context.init();
        self.store_fetched_files();
    }

    fn fetch_files(&self, page_token: Option<String>) -> Vec<File> {
        let fields = "nextPageToken, files(id, kind, name, description, kind, mimeType, parents, ownedByMe, webContentLink, webViewLink, modifiedTime)";
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
                error!("Error: {}", e);
                Vec::new()
            }
        };
        return fetched_files;
    }

    pub fn store_fetched_files(&mut self) {
        let fetched_files = self.fetch_files(None);
        let mut files_by_id = HashMap::new();
        let borrowed_files: &Vec<File> = fetched_files.borrow();
        for file in borrowed_files {
            files_by_id.insert(file.id.clone().unwrap(), file.clone());
        }
        self.context.conn.execute_batch("BEGIN TRANSACTION;");
        for file in borrowed_files {
            let path = self.get_path(&file, &files_by_id);
            if self.should_be_ignored(&path) {
                continue;
            }
            let file_wrapper = FileWrapper {
                id: file.id.borrow().as_ref().unwrap().to_owned(),
                name: file.name.borrow().as_ref().unwrap().to_owned(),
                mime_type: file.mime_type.borrow().as_ref().unwrap().to_owned(),
                path,
                directory: file.mime_type.clone().unwrap() == DIRECTORY_MIME_TYPE,
                web_view_link: file.web_view_link.clone(),
                owned_by_me: file.owned_by_me.unwrap_or(true),
                last_modified: DateTime::parse_from_rfc3339(file.modified_time.clone().unwrap().as_str()).unwrap(),
                last_accessed: DateTime::from(Utc::now()),
            };
            self.context.create_file(&file_wrapper);
        }
        self.context.conn.execute_batch("COMMIT TRANSACTION;");
    }

    fn should_be_ignored(&mut self, path: &PathBuf) -> bool {
        if !self.config.include.is_empty() {
            return self.config.include.iter().any(|pattern| pattern.matches_path(path.as_path()));
        }
        return self.config.exclude.iter().any(|pattern| pattern.matches_path(path.as_path()));
    }

    fn get_path(&'a self, file: &File, files_by_id: &HashMap<String, File>) -> PathBuf {
        let parents = file.parents.as_ref();
        if parents.is_none() {
            return PathBuf::new();
        }
        let parent_id = parents.unwrap().first();
        if parent_id.is_none() {
            return PathBuf::new();
        }
        let mut parent = files_by_id.get(parent_id.unwrap());
        if parent.is_none() {
            return PathBuf::new();
        }
        let parent_name = parent.unwrap().name.as_ref();
        if parent_name.is_none() {
            return PathBuf::new();
        }
        let mut path = PathBuf::new();
        path.push(parent_name.unwrap());
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
                        let parent_name: String = parent.unwrap().name.clone().unwrap();
                        path = Path::new(&parent_name).join(&path.as_path());
                    }
                }
            }
        }
        let file_name: &String = file.name.borrow().as_ref().unwrap();
        return path.join(file_name);
    }

    pub fn get_all_files(&mut self, owned_only: bool) -> Vec<FileWrapper> {
        let all_files: Vec<FileWrapper> = self.context.get_all_files().unwrap();
        if !owned_only {
            return all_files;
        }
        let mut filtered_files = Vec::new();
        for file in all_files {
            if file.owned_by_me {
                filtered_files.push(file);
            }
        }
        return filtered_files;
    }

    pub fn create_file(&'a self, file_wrapper: FileWrapper) -> JoinHandle<()> {
        let mut path = self.config.root_dir.clone();
        path.push(&file_wrapper.path.as_path());
        if !path.exists() {
            let create_dirs_result = std::fs::create_dir_all(&path.parent().unwrap());
            if create_dirs_result.is_err() {
                error!("Failed to create directory {} with error {}", &path.parent().unwrap().display(), create_dirs_result.unwrap_err());
                return std::thread::spawn(|| {});
            }
            if !file_wrapper.mime_type.contains("google") {
                let response = self.hub.files().get(file_wrapper.id.as_ref()).param("alt", "media").add_scope(Scope::Full).doit();
                if response.is_ok() {
                    let unwrapped_response = response.unwrap();
                    return std::thread::spawn(move || <Drive>::write_to_file(&mut path, unwrapped_response));
                }
            } else {
                return std::thread::spawn(move || <Drive>::write_to_google_file(&file_wrapper, path));
            };
        }
        return std::thread::spawn(|| {});
    }

    fn write_to_file(path: &PathBuf, mut unwrapped_response: (Response, File)) {
        debug!("Creating file {}", path.display());
        let mut file = std::fs::File::create(path.as_path()).unwrap();
        let mut response = unwrapped_response.0;
        let mut buf = [0; 128];
        let mut written = 0;
        loop {
            let len = match response.read(&mut buf) {
                Ok(0) => break,  // EOF.
                Ok(len) => len,
                Err(ref err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return,
            };
            file.write_all(&buf[..len]);
            written += len;
        }
        match file.sync_all() {
            Ok(_) => debug!("Created file {}", path.display()),
            Err(error) => error!("Failed to sync file {} with error {}", path.display(), error)
        }
    }

    fn write_to_google_file(file_wrapper: &FileWrapper, path: PathBuf) {
        debug!("Creating Google file {}", path.display());
        let mut file_content: String = "#!/usr/bin/env bash\nxdg-open ".to_string();
        file_content.push_str(&file_wrapper.web_view_link.borrow().as_ref().unwrap());
        let mut file = std::fs::File::create(path.as_path()).unwrap();
        let write_result = file.write_all(file_content.as_bytes());
        if write_result.is_err() {
            error!("Failed to write data to Google file {} with error {}", path.display(), write_result.unwrap_err());
            return;
        }
        match file.sync_all() {
            Ok(_) => debug!("Created Google file {}", path.display()),
            Err(error) => error!("Failed to sync Google file {} with error {}", path.display(), error)
        }
    }

    fn get_config() -> Config {
        let config_file = Drive::get_config_file();
        let stored_config: serde_json::Result<StoredConfig> = serde_json::from_reader(BufReader::new(&config_file));
        if stored_config.is_ok() {
            let config = stored_config.unwrap();
            return Config {
                exclude: config.exclude.iter().map(|pattern| Pattern::new(pattern).unwrap()).collect(),
                include: config.include.iter().map(|pattern| Pattern::new(pattern).unwrap()).collect(),
                root_dir: config.root_dir,
            };
        }
        let default_root_dir = Path::new(&<Drive>::get_home_dir()).join("rdrive");
        let default_stored_config = StoredConfig { exclude: Vec::new(), include: Vec::new(), root_dir: default_root_dir.clone() };
        let write_result = serde_json::to_writer_pretty(BufWriter::new(&config_file), &default_stored_config);
        if write_result.is_err() {
            error!("{}", write_result.unwrap_err());
        }
        return Config {
            exclude: Vec::new(),
            include: Vec::new(),
            root_dir: default_root_dir.clone(),
        };
    }

    fn get_home_dir() -> String {
        return match std::env::consts::OS {
            "windows" => std::env::var("USERPROFILE").unwrap(),
            _ => std::env::var("HOME").unwrap()
        };
    }

    fn get_config_file() -> std::fs::File {
        let config_file = Path::new(&Drive::get_base_config_path())
            .join("rdrive")
            .join("config.json");
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
            "macos" => std::env::var("HOME").unwrap() + "/Library/Preferences",
            _ => String::new()
        };
    }
}

const DIRECTORY_MIME_TYPE: &str = "application/vnd.google-apps.folder";

#[derive(Clone)]
pub struct FileWrapper {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    pub path: PathBuf,
    pub directory: bool,
    pub web_view_link: Option<String>,
    pub owned_by_me: bool,
    pub last_modified: DateTime<FixedOffset>,
    pub last_accessed: DateTime<FixedOffset>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
struct StoredConfig {
    exclude: Vec<String>,
    include: Vec<String>,
    root_dir: PathBuf,
}

struct Config {
    exclude: Vec<Pattern>,
    include: Vec<Pattern>,
    root_dir: PathBuf,
}
