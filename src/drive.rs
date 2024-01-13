use std::fs::{create_dir_all, read_dir};
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::SystemTime;
use std::{borrow::Borrow, collections::HashMap, env, fs, path::Path};

use async_recursion::async_recursion;
use chrono::{DateTime, FixedOffset, Local};
use drive3::api::{File, Scope};
use drive3::DriveHub;
use glob::Pattern;
use hyper::{body::Body, client::HttpConnector, Response};
use hyper_rustls::HttpsConnector;
use log::{debug, error};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::dbcontext::DbContext;

pub struct Drive {
    hub: DriveHub<HttpsConnector<HttpConnector>>,
    context: DbContext,
    config: Config,
}

impl Drive {
    pub fn new(hub: DriveHub<HttpsConnector<HttpConnector>>, connection: Connection) -> Drive {
        Drive {
            hub,
            context: DbContext::new(connection),
            config: Drive::get_config(),
        }
    }

    pub async fn init(&self) {
        self.context.init().unwrap();
        self.store_fetched_files().await.unwrap();
    }

    #[async_recursion(?Send)]
    async fn fetch_files(&self, page_token: Option<String>) -> Vec<File> {
        let fields = "nextPageToken, files(id, kind, name, description, kind, mimeType, parents, ownedByMe, webContentLink, webViewLink, modifiedTime, trashed)";
        let mut file_list_call = self
            .hub
            .files()
            .list()
            .add_scope(Scope::Full)
            .param("fields", fields);
        if page_token.is_some() {
            file_list_call = file_list_call.page_token(page_token.unwrap().as_str())
        }
        let hub_result = file_list_call.doit().await;
        match hub_result {
            Ok(x) => {
                let mut files = x.1.files.unwrap();
                if x.1.next_page_token.is_some() {
                    for file in self.fetch_files(x.1.next_page_token.to_owned()).await {
                        files.push(file);
                    }
                }
                files
            }
            Err(e) => {
                error!("Error: {}", e);
                Vec::new()
            }
        }
    }

    pub async fn store_fetched_files(&self) -> Result<(), rusqlite::Error> {
        let fetched_files = self.fetch_files(None).await;
        let mut files_by_id = HashMap::new();
        let borrowed_files: &Vec<File> = fetched_files.borrow();
        for file in borrowed_files {
            files_by_id.insert(file.id.clone().unwrap(), file.clone());
        }
        let stored_files_result = self.context.transaction(|| -> Result<(), rusqlite::Error> {
            for file in borrowed_files {
                let mut path = self.config.root_dir.clone();
                path.push(self.get_path(file, &files_by_id));
                if self.should_be_ignored(&path) {
                    continue;
                }
                let file_wrapper = Drive::convert_to_file_wrapper(file, &path);
                self.context.store_file(&file_wrapper)?;
            }
            Ok(())
        });
        if stored_files_result.is_err() {
            error!(
                "Failed to store files {}",
                fetched_files
                    .into_iter()
                    .map(|x| x.name.unwrap_or("".to_string()))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
        }
        Ok(())
    }

    fn should_be_ignored(&self, path: &Path) -> bool {
        if !self.config.include.is_empty() {
            return self
                .config
                .include
                .iter()
                .any(|pattern| pattern.matches_path(path));
        }
        return self
            .config
            .exclude
            .iter()
            .any(|pattern| pattern.matches_path(path));
    }

    fn get_path(&self, file: &File, files_by_id: &HashMap<String, File>) -> PathBuf {
        let parents = file.parents.as_ref();
        if parents.is_none() {
            let file_name = Drive::clean_file_name(&file.name);
            return PathBuf::from(file_name);
        }
        let parent_id = parents.unwrap().first();
        if parent_id.is_none() {
            let file_name = Drive::clean_file_name(&file.name);
            return PathBuf::from(file_name);
        }
        let mut parent = files_by_id.get(parent_id.unwrap());
        if parent.is_none() {
            let file_name = Drive::clean_file_name(&file.name);
            return PathBuf::from(file_name);
        }
        let parent_name = parent.unwrap().clone().name;
        if parent_name.is_none() {
            let file_name = Drive::clean_file_name(&file.name);
            return PathBuf::from(file_name);
        }
        let mut path = PathBuf::new();
        path.push(Drive::clean_file_name(&parent_name));
        while parent.is_some() {
            if let Some(parents) = parent.unwrap().parents.as_ref() {
                if let Some(parent_id) = parents.first() {
                    parent = files_by_id.get(parent_id);
                    if let Some(parent_file) = parent {
                        let parent_name = Drive::clean_file_name(&parent_file.name);
                        path = Path::new(&parent_name).join(path.as_path());
                    }
                } else {
                    parent = None;
                }
            } else {
                parent = None;
            }
        }
        path.join(Drive::clean_file_name(&file.name))
    }

    fn clean_file_name(file_name: &Option<String>) -> String {
        match env::consts::OS {
            "windows" => file_name.as_ref().unwrap().replace('\\', "_"),
            "linux" => file_name.as_ref().unwrap().replace('/', "_"),
            "macos" => file_name.as_ref().unwrap().replace('/', "_"),
            _ => file_name.clone().unwrap(),
        }
    }

    pub fn get_all_files(&self, owned_only: bool) -> Result<Vec<FileWrapper>, std::io::Error> {
        let all_files: Vec<FileWrapper> = self.context.get_all_files().unwrap();
        if !owned_only {
            return Ok(all_files);
        }
        let mut filtered_files = Vec::new();
        for file in all_files {
            if file.owned_by_me {
                filtered_files.push(file);
            }
        }
        Ok(filtered_files)
    }

    pub async fn create_file(
        &self,
        file_wrapper: &FileWrapper,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = file_wrapper.path.clone();
        let create_dirs_result = create_dir_all(path.parent().unwrap());
        if create_dirs_result.is_err() {
            error!(
                "Failed to create directory {} with error {}",
                &path.parent().unwrap().display(),
                create_dirs_result.unwrap_err()
            );
        }
        if !file_wrapper.mime_type.contains("google") {
            let response = self
                .hub
                .files()
                .get(file_wrapper.id.as_ref())
                .param("alt", "media")
                .add_scope(Scope::Full)
                .doit()
                .await;
            if response.is_ok() {
                let unwrapped_response = response.unwrap();
                <Drive>::write_to_file(&path, unwrapped_response).await?;
            }
        } else {
            <Drive>::write_to_google_file(file_wrapper, &path)?;
        };
        let metadata = path.metadata();
        if metadata.is_err() {
            error!("Somehow the file {} doesn't exist", path.display());
        }
        let update_result = self
            .context
            .update_last_accessed(&file_wrapper.id, &metadata.unwrap().modified().unwrap());
        match update_result {
            Ok(_) => debug!(
                "Updated last accessed for {} successfully",
                file_wrapper.path.display()
            ),
            Err(error) => error!(
                "Something went wrong during update for {}: {}",
                file_wrapper.path.display(),
                error
            ),
        }
        Ok(())
    }

    async fn write_to_file(
        path: &Path,
        unwrapped_response: (Response<Body>, File),
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("Creating file {}", path.display());
        let mut file = fs::File::create(path)?;
        let response = unwrapped_response.0;
        let bytes = hyper::body::to_bytes(response.into_body()).await?;
        file.write_all(&bytes)?;
        match file.sync_all() {
            Ok(_) => {
                debug!("Created file {}", path.display());
                Ok(())
            }
            Err(error) => {
                error!(
                    "Failed to sync file {} with error {}",
                    path.display(),
                    error
                );
                Err(Box::new(error))
            }
        }
    }

    fn write_to_google_file(file_wrapper: &FileWrapper, path: &Path) -> Result<(), std::io::Error> {
        debug!("Creating Google file {}", path.display());
        let mut file_content: String = "#!/usr/bin/env bash\nxdg-open ".to_string();
        file_content.push_str(file_wrapper.web_view_link.borrow().as_ref().unwrap());
        let mut file = fs::File::create(path)?;
        let write_result = file.write_all(file_content.as_bytes());
        if let Err(error) = write_result {
            error!(
                "Failed to write data to Google file {} with error {}",
                path.display(),
                &error
            );
            return Err(error);
        }
        match file.sync_all() {
            Ok(_) => {
                debug!("Created Google file {}", path.display());
                Ok(())
            }
            Err(error) => {
                error!(
                    "Failed to sync Google file {} with error {}",
                    path.display(),
                    error
                );
                Err(error)
            }
        }
    }

    pub fn get_local_files(&self) -> Result<Vec<FileWrapper>, std::io::Error> {
        self.read_local_dir(&self.config.root_dir)
    }

    fn read_local_dir(&self, dir: &PathBuf) -> Result<Vec<FileWrapper>, std::io::Error> {
        debug!("Traversing {}", dir.display());
        Ok(read_dir(dir)?
            .flat_map(|res| {
                res.into_iter().flat_map(|e| {
                    let metadata = e.metadata().unwrap();
                    let last_modified = <DateTime<Local>>::from(metadata.modified().unwrap());
                    let mime_type = if e.file_type().unwrap().is_dir() {
                        DIRECTORY_MIME_TYPE.to_string()
                    } else {
                        mime_guess::from_path(e.path().as_path())
                            .first()
                            .unwrap_or(mime::TEXT_PLAIN)
                            .essence_str()
                            .to_string()
                    };
                    let mut files = if e.file_type().unwrap().is_dir() {
                        self.read_local_dir(&e.path()).unwrap_or(vec![])
                    } else {
                        vec![]
                    };
                    files.extend(vec![FileWrapper {
                        id: String::new(),
                        name: e.file_name().into_string().unwrap(),
                        mime_type,
                        path: e.path(),
                        directory: e.file_type().unwrap().is_dir(),
                        web_view_link: None,
                        owned_by_me: true,
                        last_modified: <DateTime<FixedOffset>>::from(last_modified),
                        last_accessed: metadata.modified().unwrap(),
                        trashed: false,
                    }]);
                    files
                })
            })
            .collect::<Vec<FileWrapper>>())
    }

    pub async fn upload_file(
        &self,
        file_wrapper: &FileWrapper,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let fields = "id, kind, name, description, kind, mimeType, parents, ownedByMe, webContentLink, webViewLink, modifiedTime, trashed";
        let response = self
            .hub
            .files()
            .create(self.convert_to_file(file_wrapper))
            .add_scope(Scope::Full)
            .param("fields", fields)
            .upload(
                fs::File::open(&file_wrapper.path).unwrap(),
                file_wrapper.mime_type.parse().unwrap(),
            )
            .await?;
        let mut response_file_wrapper =
            Drive::convert_to_file_wrapper(&response.1, &file_wrapper.path);
        response_file_wrapper.last_accessed = file_wrapper.last_accessed;
        self.context.store_file(&response_file_wrapper)?;
        debug!(
            "Uploaded and stored {} correctly",
            file_wrapper.path.display()
        );
        Ok(())
    }

    fn convert_to_file_wrapper(file: &File, path: &Path) -> FileWrapper {
        FileWrapper {
            id: file.id.clone().unwrap(),
            name: file.name.clone().unwrap(),
            mime_type: file.mime_type.clone().unwrap(),
            path: path.to_path_buf(),
            directory: file.mime_type.as_ref().unwrap() == DIRECTORY_MIME_TYPE,
            web_view_link: file.web_view_link.clone(),
            owned_by_me: file.owned_by_me.unwrap_or(true),
            last_modified: file.modified_time.unwrap().into(),
            last_accessed: SystemTime::UNIX_EPOCH,
            trashed: file.trashed.unwrap_or(false),
        }
    }

    fn convert_to_file(&self, file_wrapper: &FileWrapper) -> File {
        let mime_type = if file_wrapper.directory {
            Some(DIRECTORY_MIME_TYPE.to_string())
        } else {
            Some(file_wrapper.clone().mime_type)
        };
        let path_parent = file_wrapper.path.parent();
        let parents = if let Some(path) = path_parent {
            if path == self.config.root_dir {
                None
            } else {
                Some(vec![path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()])
            }
        } else {
            None
        };
        File {
            mime_type,
            parents,
            name: Some(file_wrapper.name.clone()),
            ..Default::default()
        }
    }

    fn get_config() -> Config {
        let config_file = Drive::get_config_file();
        let stored_config: serde_json::Result<StoredConfig> =
            serde_json::from_reader(BufReader::new(&config_file));
        if let Ok(config) = stored_config {
            return Config {
                exclude: config
                    .exclude
                    .iter()
                    .map(|pattern| Pattern::new(pattern).unwrap())
                    .collect(),
                include: config
                    .include
                    .iter()
                    .map(|pattern| Pattern::new(pattern).unwrap())
                    .collect(),
                root_dir: config.root_dir,
            };
        }
        let default_root_dir = Path::new(&<Drive>::get_home_dir()).join("rdrive");
        let default_stored_config = StoredConfig {
            exclude: Vec::new(),
            include: Vec::new(),
            root_dir: default_root_dir.clone(),
        };
        let write_result =
            serde_json::to_writer_pretty(BufWriter::new(&config_file), &default_stored_config);
        if write_result.is_err() {
            error!("{}", write_result.unwrap_err());
        }
        Config {
            exclude: Vec::new(),
            include: Vec::new(),
            root_dir: default_root_dir.clone(),
        }
    }

    fn get_home_dir() -> String {
        match env::consts::OS {
            "windows" => env::var("USERPROFILE").unwrap(),
            _ => env::var("HOME").unwrap(),
        }
    }

    fn get_config_file() -> fs::File {
        let config_file = Path::new(&Drive::get_base_config_path())
            .join("rdrive")
            .join("config.json");
        if !config_file.exists() {
            let create_config_dir = create_dir_all(config_file.parent().unwrap());
            if create_config_dir.is_err() {
                panic!(
                    "Failed to create config path {}. {}",
                    config_file.display(),
                    create_config_dir.unwrap_err()
                );
            }
            return fs::File::create(config_file).unwrap();
        }
        return fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(config_file)
            .unwrap();
    }

    fn get_base_config_path() -> String {
        match env::consts::OS {
            "windows" => env::var("LOCALAPPDATA").unwrap(),
            "linux" => {
                env::var("XDG_CONFIG_HOME").unwrap_or(env::var("HOME").unwrap() + "/.config")
            }
            "macos" => env::var("HOME").unwrap() + "/Library/Preferences",
            _ => String::new(),
        }
    }
}

const DIRECTORY_MIME_TYPE: &str = "application/vnd.google-apps.folder";

#[derive(Clone, Eq, PartialEq, Debug)]
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
    pub trashed: bool,
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
