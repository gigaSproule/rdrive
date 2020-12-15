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

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use chrono::offset::Utc;
    use Connection;

    use serial_test::serial;

    use super::*;

    #[test]
    #[serial]
    fn init_should_create_table() {
        let connection = get_connection();
        let dbcontext = DbContext::new(connection);
        let result = dbcontext.init();
        assert_eq!(result, Ok(()));
        let new_connection = get_connection();
        let table = new_connection.query_row("SELECT sql FROM sqlite_master WHERE type='table' AND name='file'", NO_PARAMS, |row| -> rusqlite::Result<String> {
            row.get(0)
        });
        assert_eq!(table, Ok("CREATE TABLE file (\n                id TEXT PRIMARY KEY,\n                name TEXT NOT NULL,\n                mime_type TEXT NOT NULL,\n                path TEXT NOT NULL,\n                directory INTEGER NOT NULL,\n                web_view_link TEXT,\n                owned_by_me INTEGER NOT NULL,\n                last_modified TEXT NOT NULL,\n                last_accessed TEXT NOT NULL,\n                trashed INTEGER NOT NULL\n            )".to_string()));
        delete_db();
    }

    #[test]
    #[serial]
    fn should_store_new_file() {
        let connection = get_connection();
        let dbcontext = DbContext::new(connection);
        dbcontext.init();
        let expected_file_wrapper = FileWrapper {
            id: "id".to_string(),
            name: "name".to_string(),
            mime_type: "mime_type".to_string(),
            path: PathBuf::from("dbcontext.rs"),
            directory: false,
            web_view_link: Some("web_view_link".to_string()),
            owned_by_me: true,
            last_modified: DateTime::from(Utc::now()),
            last_accessed: SystemTime::from(Utc::now()),
            trashed: false,
        };
        let result = dbcontext.store_file(&expected_file_wrapper);
        assert_eq!(result, Ok(1));

        let new_connection = get_connection();
        let actual_file_wrapper = new_connection.query_row("SELECT * FROM file", NO_PARAMS, |row: &Row| -> rusqlite::Result<FileWrapper> {
            let path: String = row.get(3).unwrap();
            let last_changed: String = row.get(7).unwrap();
            let last_accessed: String = row.get(8).unwrap();
            Ok(FileWrapper {
                id: row.get(0).unwrap(),
                name: row.get(1).unwrap(),
                mime_type: row.get(2).unwrap(),
                path: path.parse().unwrap(),
                directory: row.get(4).unwrap(),
                web_view_link: row.get(5).unwrap(),
                owned_by_me: row.get(6).unwrap(),
                last_modified: DateTime::parse_from_rfc3339(&last_changed).unwrap(),
                last_accessed: SystemTime::from(DateTime::parse_from_rfc3339(&last_accessed).unwrap()),
                trashed: row.get(9).unwrap(),
            })
        });
        assert_eq!(actual_file_wrapper.unwrap(), expected_file_wrapper);
        delete_db();
    }

    const DB_PATH: &'static str = "test.db";

    fn get_connection() -> Connection {
        Connection::open("test.db").unwrap()
    }

    fn delete_db() {
        remove_file(DB_PATH);
    }
}
