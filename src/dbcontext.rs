use std::path::PathBuf;
use std::time::SystemTime;

use chrono::{DateTime, Local};
use log::{debug, error};
use rusqlite::{Connection, Error, NO_PARAMS, Row, Statement};

use crate::drive::FileWrapper;

pub struct DbContext {
    pub conn: Connection
}

impl DbContext {
    pub fn new(conn: Connection) -> Self {
        return DbContext {
            conn
        };
    }

    pub fn init(&self) -> Result<(), Error> {
        self.conn.execute("CREATE TABLE IF NOT EXISTS file (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                path TEXT NOT NULL,
                directory INTEGER NOT NULL,
                web_view_link TEXT,
                owned_by_me INTEGER NOT NULL,
                last_modified TEXT NOT NULL,
                last_accessed TEXT NOT NULL,
                trashed INTEGER NOT NULL
            )", NO_PARAMS)?;
        Ok(())
    }

    pub fn store_file(&self, file_wrapper: &FileWrapper) -> Result<i64, Error> {
        let last_accessed: SystemTime = file_wrapper.last_accessed;
        let last_accessed_converted: DateTime<Local> = DateTime::from(last_accessed);
        let stored_file = self.get_file(&file_wrapper.id);
        if stored_file.is_some() && stored_file.as_ref().unwrap().last_modified == file_wrapper.last_modified {
            return Ok(-1);
        }
        let mut statement: Statement = if stored_file.is_some() {
            self.conn.prepare("UPDATE file SET name = :name, mime_type = :mime_type, path = :path, directory = :directory, web_view_link = :web_view_link, owned_by_me = :owned_by_me, last_modified = :last_modified, last_accessed = :last_accessed, trashed = :trashed WHERE id = :id")?
        } else {
            self.conn.prepare("INSERT INTO file (id, name, mime_type, path, directory, web_view_link, owned_by_me, last_modified, last_accessed, trashed) VALUES (:id, :name, :mime_type, :path, :directory, :web_view_link, :owned_by_me, :last_modified, :last_accessed, :trashed)")?
        };
        statement.execute_named(
            &[
                (":id", &file_wrapper.id),
                (":name", &file_wrapper.name),
                (":mime_type", &file_wrapper.mime_type),
                (":path", &file_wrapper.path.to_str().unwrap()),
                (":directory", &file_wrapper.directory),
                (":web_view_link", &file_wrapper.web_view_link),
                (":owned_by_me", &file_wrapper.owned_by_me),
                (":last_modified", &file_wrapper.last_modified.to_rfc3339()),
                (":last_accessed", &last_accessed_converted.to_rfc3339()),
                (":trashed", &file_wrapper.trashed)
            ]
        )?;
        return Ok(self.conn.last_insert_rowid());
    }

    pub fn get_file(&self, id: &String) -> Option<FileWrapper> {
        let mut statement = self.conn.prepare("SELECT * FROM file where id = :id LIMIT 1").unwrap();
        let mut rows = statement.query_named(&[(":id", &id)]).unwrap();
        let result = rows.next().unwrap();
        match result {
            Some(row) => Some(DbContext::convert_to_file_wrapper(row)),
            _ => None
        }
    }

    pub fn get_all_files(&self) -> Result<Vec<FileWrapper>, Error> {
        let mut statement = self.conn.prepare("SELECT * FROM file")?;
        let mut rows = statement.query(NO_PARAMS)?;
        let mut files = Vec::new();
        while let Some(row) = rows.next()? {
            files.push(
                DbContext::convert_to_file_wrapper(&row)
            );
        }
        return Ok(files);
    }

    fn convert_to_file_wrapper(row: &Row) -> FileWrapper {
        let path: String = row.get(3).unwrap();
        let last_changed: String = row.get(7).unwrap();
        let last_accessed: String = row.get(8).unwrap();
        FileWrapper {
            id: row.get(0).unwrap(),
            name: row.get(1).unwrap(),
            mime_type: row.get(2).unwrap(),
            path: PathBuf::from(path),
            directory: row.get(4).unwrap(),
            web_view_link: row.get(5).unwrap(),
            owned_by_me: row.get(6).unwrap(),
            last_modified: DateTime::parse_from_rfc3339(&last_changed).unwrap(),
            last_accessed: SystemTime::from(DateTime::parse_from_rfc3339(&last_accessed).unwrap()),
            trashed: row.get(9).unwrap(),
        }
    }

    pub fn update_last_accessed(&self, id: &String, last_accessed: &SystemTime) -> Result<(), Error> {
        let last_accessed_converted: DateTime<Local> = DateTime::from(last_accessed.clone());
        debug!("Updating last_accessed to {}", &last_accessed_converted.to_rfc3339());
        debug!("Autocommit: {}", self.conn.is_autocommit());
        let mut statement = self.conn.prepare("UPDATE file SET last_accessed = :last_accessed WHERE id = :id")?;
        let update_result = statement.execute_named(
            &[
                (":last_accessed", &last_accessed_converted.to_rfc3339()),
                (":id", &id)
            ]
        );
        match update_result {
            Ok(num_rows) => {
                debug!("Update affected {} rows", num_rows);
                Ok(())
            }
            Err(error) => {
                error!("Error occured when updating the last_accessed - {}", error);
                Err(error)
            }
        }
    }
}
