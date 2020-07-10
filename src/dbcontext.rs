use std::path::PathBuf;

use chrono::DateTime;
use rusqlite::{Connection, Error, NO_PARAMS, Row, Statement};

use crate::drive::FileWrapper;

pub struct DbContext<'a> {
    pub conn: &'a Connection,
    pub create_file_statement: Option<Statement<'a>>,
    pub get_all_files_statement: Option<Statement<'a>>,
}

impl<'a> DbContext<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        return DbContext {
            conn,
            create_file_statement: None,
            get_all_files_statement: None,
        };
    }

    pub fn init(&mut self) {
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
            )", NO_PARAMS).unwrap();
    }

    pub fn create_file(&mut self, file_wrapper: &FileWrapper) -> Result<i64, Error> {
        if let None = &self.create_file_statement {
            let stmt = self.conn.prepare("INSERT INTO file (id, name, mime_type, path, directory, web_view_link, owned_by_me, last_modified, last_accessed) VALUES (:id, :name, :mime_type, :path, :directory, :web_view_link, :owned_by_me, :last_modified, :last_accessed)")?;
            self.create_file_statement = Some(stmt);
        };
        self.create_file_statement.as_mut().unwrap().execute_named(
            &[
                (":id", &file_wrapper.id),
                (":name", &file_wrapper.name),
                (":mime_type", &file_wrapper.mime_type),
                (":path", &file_wrapper.path.to_str()),
                (":directory", &file_wrapper.directory),
                (":web_view_link", &file_wrapper.web_view_link),
                (":owned_by_me", &file_wrapper.owned_by_me),
                (":last_modified", &file_wrapper.last_modified.to_rfc3339()),
                (":last_accessed", &file_wrapper.last_accessed.to_rfc3339())
            ]
        )?;
        return Ok(self.conn.last_insert_rowid());
    }

    pub fn get_all_files(&mut self) -> Result<Vec<FileWrapper>, Error> {
        if let None = &self.get_all_files_statement {
            let stmt = self.conn.prepare("SELECT * FROM file")?;
            self.get_all_files_statement = Some(stmt);
        };
        let mut rows = self.get_all_files_statement.as_mut().unwrap().query(NO_PARAMS)?;
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
            last_accessed: DateTime::parse_from_rfc3339(&last_accessed).unwrap(),
        }
    }
}
