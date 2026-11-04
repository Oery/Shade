use infinity_agent_core::message::InfinityMessage;
use rusqlite::params;
use shade_proto::api::Page;

use super::{Chats, owner};
use crate::db::DbError;

pub struct StoredMessage {
    pub ordinal: i64,
    pub turn_id: Option<String>,
    pub message: InfinityMessage,
}

impl Chats {
    /// Read the full persisted history, regardless of model compaction.
    /// The cursor advances over hidden rows as well as visible messages.
    pub async fn history(&self, root: String, after: i64, limit: usize) -> Result<Page<StoredMessage>, DbError> {
        self.db
            .call(move |connection| {
                owner(connection, &root)?;
                let sql = "SELECT m.ord, m.body,
                       (SELECT t.input_id FROM messages u JOIN chat_turns t ON t.input_id = u.message_id
                        WHERE u.thread_id = m.thread_id AND u.ord <= m.ord AND json_extract(u.body, '$.kind') = 'user'
                        ORDER BY u.ord DESC LIMIT 1)
                       FROM messages m WHERE m.thread_id = ?1 AND m.ord > ?2 ORDER BY m.ord LIMIT ?3";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map(params![root, after, (limit + 1) as i64], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?))
                })?;
                let mut items = Vec::new();
                let mut last = after;
                let mut more = false;

                for row in rows {
                    let (ordinal, body, turn_id) = row?;

                    if items.len() == limit {
                        more = true;
                        break;
                    }

                    items.push(StoredMessage { ordinal, turn_id, message: serde_json::from_str(&body)? });
                    last = ordinal;
                }

                Ok(Page { items, next_cursor: more.then(|| last.to_string()) })
            })
            .await
    }
}
