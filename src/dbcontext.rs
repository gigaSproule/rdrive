use std::path::PathBuf;
use std::time::SystemTime;

use chrono::{DateTime, Local};
use rusqlite::{Connection, Error, NO_PARAMS, Row};

use crate::drive::FileWrapper;

pub struct DbContext<'a> {
    pub conn: &'a Connection
}

impl<'a> DbContext<'a> {
    pub fn new(conn: &'a Connection) -> Self {
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
                last_accessed TEXT NOT NULL
            )", NO_PARAMS)?;
        Ok(())
    }

    pub fn store_file(&self, file_wrapper: &FileWrapper) -> Result<i64, Error> {
        let last_accessed: SystemTime = file_wrapper.last_accessed;
        let last_accessed_converted: DateTime<Local> = DateTime::from(last_accessed);
        let mut statement = self.conn.prepare("INSERT INTO file (id, name, mime_type, path, directory, web_view_link, owned_by_me, last_modified, last_accessed) VALUES (:id, :name, :mime_type, :path, :directory, :web_view_link, :owned_by_me, :last_modified, :last_accessed)")?;
        statement
            .execute_named(
                &[
                    (":id", &file_wrapper.id),
                    (":name", &file_wrapper.name),
                    (":mime_type", &file_wrapper.mime_type),
                    (":path", &file_wrapper.path.to_str().unwrap()),
                    (":directory", &file_wrapper.directory),
                    (":web_view_link", &file_wrapper.web_view_link),
                    (":owned_by_me", &file_wrapper.owned_by_me),
                    (":last_modified", &file_wrapper.last_modified.to_rfc3339()),
                    (":last_accessed", &last_accessed_converted.to_rfc3339())
                ]
            )?;
        return Ok(self.conn.last_insert_rowid());
    }

    pub async fn get_all_files(&self) -> Result<Vec<FileWrapper>, Error> {
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
        }
    }

    pub async fn update_last_accessed(&self, id: String, last_accessed: SystemTime) -> Result<(), Error> {
        let last_accessed_converted: DateTime<Local> = DateTime::from(last_accessed);
        let mut statement = self.conn.prepare("UPDATE file SET last_accessed = :last_accessed WHERE id = :id")?;
        statement.execute_named(
            &[
                (":last_accessed", &last_accessed_converted.to_rfc3339()),
                (":id", &id)
            ]
        )?;
        return Ok(());
    }
}
