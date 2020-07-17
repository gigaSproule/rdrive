use std::{borrow::Borrow, collections::HashMap, env, fs, io::{BufReader, Read}, io, path::Path, thread};
use std::fs::create_dir_all;
use std::future::Future;
use std::io::{BufWriter, Error, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::time::SystemTime;
use thread::JoinHandle;

use chrono::{DateTime, FixedOffset};
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

    pub async fn init(&mut self) {
        self.context.init();
        self.store_fetched_files().await;
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

    pub async fn store_fetched_files(&mut self) -> Result<(), rusqlite::Error> {
        let fetched_files = self.fetch_files(None);
        let mut files_by_id = HashMap::new();
        let borrowed_files: &Vec<File> = fetched_files.borrow();
        for file in borrowed_files {
            files_by_id.insert(file.id.clone().unwrap(), file.clone());
        }
        self.context.conn.execute_batch("BEGIN TRANSACTION;")?;
        for file in borrowed_files {
            let mut path = self.config.root_dir.clone();
            path.push(self.get_path(&file, &files_by_id));
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
                last_accessed: SystemTime::UNIX_EPOCH,
            };
            self.context.store_file(&file_wrapper)?;
        }
        self.context.conn.execute_batch("COMMIT TRANSACTION;")?;
        Ok(())
    }

    fn should_be_ignored(&self, path: &PathBuf) -> bool {
        if !self.config.include.is_empty() {
            return self.config.include.iter().any(|pattern| pattern.matches_path(path.as_path()));
        }
        return self.config.exclude.iter().any(|pattern| pattern.matches_path(path.as_path()));
    }

    fn get_path(&'a self, file: &File, files_by_id: &HashMap<String, File>) -> PathBuf {
        let parents = file.parents.as_ref();
        if parents.is_none() {
            let file_name: &String = file.name.borrow().as_ref().unwrap();
            return PathBuf::from(file_name);
        }
        let parent_id = parents.unwrap().first();
        if parent_id.is_none() {
            let file_name: &String = file.name.borrow().as_ref().unwrap();
            return PathBuf::from(file_name);
        }
        let mut parent = files_by_id.get(parent_id.unwrap());
        if parent.is_none() {
            let file_name: &String = file.name.borrow().as_ref().unwrap();
            return PathBuf::from(file_name);
        }
        let parent_name = parent.unwrap().name.as_ref();
        if parent_name.is_none() {
            let file_name: &String = file.name.borrow().as_ref().unwrap();
            return PathBuf::from(file_name);
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

    pub async fn get_all_files(&mut self, owned_only: bool) -> Result<Vec<FileWrapper>, Error> {
        let all_files: Vec<FileWrapper> = self.context.get_all_files().await.unwrap();
        if !owned_only {
            return Ok(all_files);
        }
        let mut filtered_files = Vec::new();
        for file in all_files {
            if file.owned_by_me {
                filtered_files.push(file);
            }
        }
        return Ok(filtered_files);
    }

    /// Creates a directory, but no idea if this is even required
    pub fn create_directory(&'a self, file_wrapper: FileWrapper) -> JoinHandle<()> {
        let path = file_wrapper.path.clone();
        if !path.exists() {
            return thread::spawn(move || {
                debug!("Creating directory {}", path.display());
                create_dir_all(&path).unwrap()
            });
        }
        return thread::spawn(|| {});
    }

    pub async fn create_file(&'a self, file_wrapper: FileWrapper) -> Result<(), Error> {
        let path = file_wrapper.path.clone();
        if !path.exists() {
            let create_dirs_result = create_dir_all(&path.parent().unwrap());
            if create_dirs_result.is_err() {
                error!("Failed to create directory {} with error {}", &path.parent().unwrap().display(), create_dirs_result.unwrap_err());
            }
            if !file_wrapper.mime_type.contains("google") {
                let response = self.hub.files().get(file_wrapper.id.as_ref()).param("alt", "media").add_scope(Scope::Full).doit();
                if response.is_ok() {
                    let unwrapped_response = response.unwrap();
                    <Drive>::write_to_file(&path, unwrapped_response).await?;
                }
            } else {
                <Drive>::write_to_google_file(&file_wrapper, &path).await?;
            };
            let update_result = self.context.update_last_accessed(file_wrapper.id, path.metadata().unwrap().modified().unwrap()).await;
            match update_result {
                Ok(_) => debug!("Update was successful"),
                Err(error) => error!("Something went wrong during update {}", error)
            }
        }
        Ok(())
    }

    async fn write_to_file(path: &PathBuf, unwrapped_response: (Response, File)) -> Result<(), Error> {
        debug!("Creating file {}", path.display());
        let mut file = fs::File::create(path.as_path())?;
        let mut response = unwrapped_response.0;
        let mut buf = [0; 128];
        loop {
            let len = match response.read(&mut buf) {
                Ok(0) => break,  // EOF.
                Ok(len) => len,
                Err(ref err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            };
            file.write_all(&buf[..len]);
        }
        match file.sync_all() {
            Ok(_) => {
                debug!("Created file {}", path.display());
                Ok(())
            }
            Err(error) => {
                error!("Failed to sync file {} with error {}", path.display(), error);
                Err(error)
            }
        }
    }

    async fn write_to_google_file(file_wrapper: &FileWrapper, path: &PathBuf) -> Result<(), Error> {
        debug!("Creating Google file {}", path.display());
        let mut file_content: String = "#!/usr/bin/env bash\nxdg-open ".to_string();
        file_content.push_str(&file_wrapper.web_view_link.borrow().as_ref().unwrap());
        let mut file = fs::File::create(path.as_path())?;
        let write_result = file.write_all(file_content.as_bytes());
        if write_result.is_err() {
            let error = write_result.unwrap_err();
            error!("Failed to write data to Google file {} with error {}", path.display(), &error);
            return Err(error);
        }
        match file.sync_all() {
            Ok(_) => {
                debug!("Created Google file {}", path.display());
                Ok(())
            }
            Err(error) => {
                error!("Failed to sync Google file {} with error {}", path.display(), error);
                Err(error)
            }
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
        return match env::consts::OS {
            "windows" => env::var("USERPROFILE").unwrap(),
            _ => env::var("HOME").unwrap()
        };
    }

    fn get_config_file() -> fs::File {
        let config_file = Path::new(&Drive::get_base_config_path())
            .join("rdrive")
            .join("config.json");
        if !config_file.exists() {
            let create_config_dir = create_dir_all(config_file.parent().unwrap());
            if create_config_dir.is_err() {
                panic!("Failed to create config path {}. {}", config_file.display(), create_config_dir.unwrap_err());
            }
            return fs::File::create(config_file).unwrap();
        }
        return fs::OpenOptions::new().write(true).read(true).open(config_file).unwrap();
    }

    fn get_base_config_path() -> String {
        return match env::consts::OS {
            "windows" => env::var("LOCALAPPDATA").unwrap(),
            "linux" => env::var("XDG_CONFIG_HOME").unwrap_or(env::var("HOME").unwrap() + "/.config"),
            "macos" => env::var("HOME").unwrap() + "/Library/Preferences",
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
    pub last_accessed: SystemTime,
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
