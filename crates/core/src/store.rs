//! The SQLite adapter: the database worker and every store. SQL lives only here.

pub mod conversation;
pub mod db;
pub mod grants;
pub mod roots;
pub mod state;

#[cfg(test)]
mod tests;
