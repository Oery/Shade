//! The SQLite adapter: the database worker and every store.
//!
//! SQL lives only here. This is the one crate that depends on `rusqlite`, and
//! [`db::Db`] runs queries only for the stores in it.

pub mod backup;
pub mod conversation;
pub mod db;
pub mod grants;
pub mod hooks;
pub mod inbox;
pub mod operations;
pub mod roots;
pub mod schedules;
pub mod state;
pub mod usage;

#[cfg(test)]
mod tests;

#[cfg(test)]
type BoxError = Box<dyn std::error::Error + Send + Sync>;
