use rusqlite::{Connection, Error, NO_PARAMS, Statement};

use crate::drive::FileWrapper;

pub struct DbContext<'a> {
    pub conn: &'a Connection,
    pub create_file_statement: Option<Statement<'a>>,
}

impl<'a> DbContext<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        return DbContext {
            conn,
            create_file_statement: None,
        };
    }

    pub fn init(&mut self) {
        self.conn.execute("CREATE TABLE IF NOT EXISTS file (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                path TEXT NOT NULL,
                directory INTEGER NOT NULL,
                web_view_link TEXT
            )", NO_PARAMS).unwrap();
    }

    pub fn create_file(&mut self, file_wrapper: &FileWrapper) -> Result<i64, Error> {
        if let None = &self.create_file_statement {
            let stmt = self.conn.prepare("INSERT INTO file (id, name, mime_type, path, directory) VALUES (:id, :name, :mime_type, :path, :directory)")?;
            self.create_file_statement = Some(stmt);
        };
        self.create_file_statement.as_mut().unwrap().execute_named(
            &[
                (":id", &file_wrapper.id),
                (":name", &file_wrapper.name),
                (":mime_type", &file_wrapper.mime_type),
                (":path", &file_wrapper.path.to_str()),
                (":directory", &file_wrapper.directory)
            ]
        )?;
        return Ok(self.conn.last_insert_rowid());
    }
}
