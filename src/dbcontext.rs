use std::path::PathBuf;
use std::time::SystemTime;

use chrono::{DateTime, Local};
use log::{debug, error};
use rusqlite::{named_params, Connection, Error, Row, Statement};

use crate::drive::FileWrapper;

pub struct DbContext {
    conn: Connection,
}

impl DbContext {
    pub fn new(conn: Connection) -> Self {
        DbContext { conn }
    }

    pub fn init(&self) -> Result<(), Error> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS file (
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
            )",
            [],
        )?;
        Ok(())
    }

    pub fn store_file(&self, file_wrapper: &FileWrapper) -> Result<(), Error> {
        let last_accessed: SystemTime = file_wrapper.last_accessed;
        let last_accessed_converted: DateTime<Local> = DateTime::from(last_accessed);
        let stored_file = self.get_file(&file_wrapper.id);
        if stored_file.is_some()
            && stored_file.as_ref().unwrap().last_modified == file_wrapper.last_modified
        {
            return Ok(());
        }
        let mut statement: Statement = if stored_file.is_some() {
            self.conn.prepare("UPDATE file SET name = :name, mime_type = :mime_type, path = :path, directory = :directory, web_view_link = :web_view_link, owned_by_me = :owned_by_me, last_modified = :last_modified, last_accessed = :last_accessed, trashed = :trashed WHERE id = :id")?
        } else {
            self.conn.prepare("INSERT INTO file (id, name, mime_type, path, directory, web_view_link, owned_by_me, last_modified, last_accessed, trashed) VALUES (:id, :name, :mime_type, :path, :directory, :web_view_link, :owned_by_me, :last_modified, :last_accessed, :trashed)")?
        };
        statement.execute(named_params! {
            ":id": &file_wrapper.id,
            ":name": &file_wrapper.name,
            ":mime_type": &file_wrapper.mime_type,
            ":path": &file_wrapper.path.to_str().unwrap(),
            ":directory": &file_wrapper.directory,
            ":web_view_link": &file_wrapper.web_view_link,
            ":owned_by_me": &file_wrapper.owned_by_me,
            ":last_modified": &file_wrapper.last_modified.to_rfc3339(),
            ":last_accessed": &last_accessed_converted.to_rfc3339(),
            ":trashed": &file_wrapper.trashed
        })?;
        Ok(())
    }

    pub fn get_file(&self, id: &String) -> Option<FileWrapper> {
        let mut statement = self
            .conn
            .prepare("SELECT * FROM file where id = :id LIMIT 1")
            .unwrap();
        let mut rows = statement.query(&[(":id", &id)]).unwrap();
        let result = rows.next().unwrap();
        result.map(DbContext::convert_to_file_wrapper)
    }

    pub fn get_all_files(&self) -> Result<Vec<FileWrapper>, Error> {
        let mut statement = self.conn.prepare("SELECT * FROM file")?;
        let mut rows = statement.query([])?;
        let mut files = Vec::new();
        while let Some(row) = rows.next()? {
            files.push(DbContext::convert_to_file_wrapper(row));
        }
        Ok(files)
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

    pub fn update_last_accessed(
        &self,
        id: &String,
        last_accessed: &SystemTime,
    ) -> Result<(), Error> {
        let last_accessed_converted: DateTime<Local> = DateTime::from(*last_accessed);
        debug!(
            "Updating last_accessed to {}",
            &last_accessed_converted.to_rfc3339()
        );
        debug!("Autocommit: {}", self.conn.is_autocommit());
        let mut statement = self
            .conn
            .prepare("UPDATE file SET last_accessed = :last_accessed WHERE id = :id")?;
        let update_result = statement.execute(&[
            (":last_accessed", &last_accessed_converted.to_rfc3339()),
            (":id", id),
        ]);
        match update_result {
            Ok(num_rows) => {
                debug!("Update affected {} rows", num_rows);
                Ok(())
            }
            Err(error) => {
                error!("Error occurred when updating the last_accessed - {}", error);
                Err(error)
            }
        }
    }

    pub fn transaction(&self, func: impl Fn() -> Result<(), Error>) -> Result<(), Error> {
        self.conn.execute_batch("BEGIN TRANSACTION;")?;
        let func_result = func();
        if func_result.is_err() {
            self.conn.execute_batch("ROLLBACK TRANSACTION;")?;
        } else {
            self.conn.execute_batch("COMMIT TRANSACTION;")?;
        }
        func_result
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::fs::remove_file;

    use chrono::offset::Utc;
    use chrono::{Duration, Timelike};
    use rusqlite::ffi::ErrorCode;
    use rusqlite::Result;
    use serial_test::serial;

    use super::*;

    #[test]
    #[serial]
    fn init_should_create_table() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let result = dbcontext.init();
        assert!(result.is_ok());

        let table = connection.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='file'",
            [],
            |row| -> Result<String> { row.get(0) },
        );
        assert_eq!(table, Ok("CREATE TABLE file (\n                id TEXT PRIMARY KEY,\n                name TEXT NOT NULL,\n                mime_type TEXT NOT NULL,\n                path TEXT NOT NULL,\n                directory INTEGER NOT NULL,\n                web_view_link TEXT,\n                owned_by_me INTEGER NOT NULL,\n                last_modified TEXT NOT NULL,\n                last_accessed TEXT NOT NULL,\n                trashed INTEGER NOT NULL\n            )".to_string()));
    }

    #[test]
    #[serial]
    fn store_file_should_store_new_file() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
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
        assert!(result.is_ok());

        let actual_file_wrapper = connection.query_row(
            "SELECT * FROM file",
            [],
            |row: &Row| -> Result<FileWrapper> {
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
                    last_accessed: SystemTime::from(
                        DateTime::parse_from_rfc3339(&last_accessed).unwrap(),
                    ),
                    trashed: row.get(9).unwrap(),
                })
            },
        );
        assert_eq!(actual_file_wrapper.unwrap(), expected_file_wrapper);
    }

    #[test]
    #[serial]
    fn store_file_should_update_stored_file_details() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
        let original_file_wrapper = FileWrapper {
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
        let updated_file_wrapper = FileWrapper {
            id: original_file_wrapper.id.clone(),
            name: "updated name".to_string(),
            mime_type: "updated mime_type".to_string(),
            path: PathBuf::from("src"),
            directory: true,
            web_view_link: Some("updated web_view_link".to_string()),
            owned_by_me: false,
            last_modified: DateTime::from(Utc::now().with_minute(Utc::now().minute() + 1).unwrap()),
            last_accessed: SystemTime::from(
                Utc::now().with_minute(Utc::now().minute() + 1).unwrap(),
            ),
            trashed: true,
        };
        insert_file_wrapper(&connection, &original_file_wrapper);
        let result = dbcontext.store_file(&updated_file_wrapper);
        assert!(result.is_ok());

        let actual_file_wrapper = connection.query_row(
            "SELECT * FROM file",
            [],
            |row: &Row| -> Result<FileWrapper> {
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
                    last_accessed: SystemTime::from(
                        DateTime::parse_from_rfc3339(&last_accessed).unwrap(),
                    ),
                    trashed: row.get(9).unwrap(),
                })
            },
        );
        assert_eq!(actual_file_wrapper.unwrap(), updated_file_wrapper);
    }

    #[test]
    #[serial]
    fn store_file_should_not_update_stored_file_details_if_last_modified_is_the_same() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
        let original_file_wrapper = FileWrapper {
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
        let updated_file_wrapper = FileWrapper {
            id: original_file_wrapper.id.clone(),
            name: "updated name".to_string(),
            mime_type: "updated mime_type".to_string(),
            path: PathBuf::from("src"),
            directory: true,
            web_view_link: Some("updated web_view_link".to_string()),
            owned_by_me: false,
            last_modified: original_file_wrapper.last_modified,
            last_accessed: SystemTime::from(Utc::now() + Duration::minutes(1)),
            trashed: true,
        };
        insert_file_wrapper(&connection, &original_file_wrapper);
        let result = dbcontext.store_file(&updated_file_wrapper);
        assert!(result.is_ok());

        let actual_file_wrapper = connection.query_row(
            "SELECT * FROM file",
            [],
            |row: &Row| -> Result<FileWrapper> {
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
                    last_accessed: SystemTime::from(
                        DateTime::parse_from_rfc3339(&last_accessed).unwrap(),
                    ),
                    trashed: row.get(9).unwrap(),
                })
            },
        );
        assert_eq!(actual_file_wrapper.unwrap(), original_file_wrapper);
    }

    #[test]
    #[serial]
    fn get_file_should_return_none_if_no_stored_file() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
        let result = dbcontext.get_file(&"id".to_string());
        assert_eq!(result, None);
    }

    #[test]
    #[serial]
    fn get_file_should_get_stored_file() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
        let stored_file_wrapper = FileWrapper {
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
        insert_file_wrapper(&connection, &stored_file_wrapper);
        let result = dbcontext.get_file(&stored_file_wrapper.id);
        assert_eq!(result.unwrap(), stored_file_wrapper);
    }

    #[test]
    #[serial]
    fn get_all_files_should_return_empty_vec_if_no_stored_file() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
        let result = dbcontext.get_all_files();
        assert_eq!(result, Ok(vec![]));
    }

    #[test]
    #[serial]
    fn get_all_files_should_get_stored_files() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
        let stored_file_wrapper_1 = FileWrapper {
            id: "id1".to_string(),
            name: "name1".to_string(),
            mime_type: "mime_type1".to_string(),
            path: PathBuf::from("dbcontext1.rs"),
            directory: false,
            web_view_link: Some("web_view_link1".to_string()),
            owned_by_me: true,
            last_modified: DateTime::from(Utc::now()),
            last_accessed: SystemTime::from(Utc::now()),
            trashed: false,
        };
        let stored_file_wrapper_2 = FileWrapper {
            id: "id2".to_string(),
            name: "name2".to_string(),
            mime_type: "mime_type2".to_string(),
            path: PathBuf::from("dbcontext2.rs"),
            directory: true,
            web_view_link: Some("web_view_link2".to_string()),
            owned_by_me: false,
            last_modified: DateTime::from(Utc::now().with_minute(Utc::now().minute() + 1).unwrap()),
            last_accessed: SystemTime::from(
                Utc::now().with_minute(Utc::now().minute() + 1).unwrap(),
            ),
            trashed: true,
        };
        insert_file_wrapper(&connection, &stored_file_wrapper_1);
        insert_file_wrapper(&connection, &stored_file_wrapper_2);
        let result = dbcontext.get_file(&stored_file_wrapper_2.id);
        assert_eq!(result.unwrap(), stored_file_wrapper_2);
    }

    #[test]
    #[serial]
    fn update_last_accessed_should_update_last_accessed() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
        let file_wrapper = FileWrapper {
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
        insert_file_wrapper(&connection, &file_wrapper);
        let time = SystemTime::now();
        let result = dbcontext.update_last_accessed(&file_wrapper.id, &time);
        assert!(result.is_ok());
        let stored_last_accessed = connection.query_row(
            "SELECT last_accessed FROM file",
            [],
            |row: &Row| -> Result<SystemTime> {
                let last_accessed: String = row.get(0).unwrap();
                Ok(SystemTime::from(
                    DateTime::parse_from_rfc3339(&last_accessed).unwrap(),
                ))
            },
        );
        assert_eq!(stored_last_accessed.unwrap(), time);
    }

    #[test]
    #[serial]
    fn transaction_should_rollback_transaction_on_error() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
        let result = dbcontext.transaction(|| -> Result<(), Error> {
            let file_wrapper = FileWrapper {
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
            dbcontext.store_file(&file_wrapper)?;
            Err(Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: ErrorCode::OutOfMemory,
                    extended_code: 1,
                },
                Some("Something went wrong".to_string()),
            ))
        });
        let expected_error = Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::OutOfMemory,
                extended_code: 1,
            },
            Some("Something went wrong".to_string()),
        );
        assert_eq!(result.unwrap_err(), expected_error);

        let count: Result<i32> =
            connection.query_row("SELECT COUNT(*) FROM file", [], |row| row.get(0));
        assert_eq!(count.unwrap(), 0);
    }

    #[test]
    #[serial]
    fn transaction_should_commit_transaction_on_success() {
        delete_db();
        let dbcontext_connection = get_connection();
        let dbcontext = DbContext::new(dbcontext_connection);
        let connection = get_connection();
        let init_result = dbcontext.init();
        assert!(init_result.is_ok());
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
        let result = dbcontext.transaction(|| -> Result<(), Error> {
            dbcontext.store_file(&expected_file_wrapper)?;
            Ok(())
        });
        assert!(result.is_ok());

        let actual_file_wrapper = connection.query_row(
            "SELECT * FROM file",
            [],
            |row: &Row| -> Result<FileWrapper> {
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
                    last_accessed: SystemTime::from(
                        DateTime::parse_from_rfc3339(&last_accessed).unwrap(),
                    ),
                    trashed: row.get(9).unwrap(),
                })
            },
        );
        assert_eq!(actual_file_wrapper.unwrap(), expected_file_wrapper);
    }

    fn insert_file_wrapper(connection: &Connection, file_wrapper: &FileWrapper) {
        let last_accessed_converted: DateTime<Local> = DateTime::from(file_wrapper.last_accessed);
        let result = connection.execute("INSERT INTO file (id, name, mime_type, path, directory, web_view_link, owned_by_me, last_modified, last_accessed, trashed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            &file_wrapper.id,
            &file_wrapper.name,
            &file_wrapper.mime_type,
            &file_wrapper.path.to_str().unwrap().to_string(),
            &(file_wrapper.directory as i32).to_string(),
            file_wrapper.web_view_link.borrow().as_ref().unwrap(),
            &(file_wrapper.owned_by_me as i32).to_string(),
            &file_wrapper.last_modified.to_rfc3339(),
            &last_accessed_converted.to_rfc3339(),
            &(file_wrapper.trashed as i32).to_string()
        ]);
        assert!(result.is_ok());
    }

    const DB_PATH: &str = "test.db";

    fn get_connection() -> Connection {
        Connection::open("test.db").unwrap()
    }

    fn delete_db() {
        let removed = remove_file(DB_PATH);
        if removed.is_err() {
            println!(
                "Failed to remove {}. If any tests failed, this could be why.",
                DB_PATH
            );
        }
    }
}
