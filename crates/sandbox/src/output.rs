//! Bounded capture of one output stream: its start, its end, and its length.
//!
//! The stream is read to the end, so a chatty command never blocks on a full
//! pipe, but memory stays at twice the limit however much it writes.

use std::collections::VecDeque;

/// Bytes kept from the start, and again from the end, of each stream.
pub const KEEP: usize = 16 * 1024;

pub struct Output {
    limit: usize,
    head: Vec<u8>,
    tail: VecDeque<u8>,
    total: usize,
}

impl Output {
    pub fn new(limit: usize) -> Self {
        Self { limit, head: Vec::new(), tail: VecDeque::new(), total: 0 }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.total += bytes.len();

        let room = self.limit.saturating_sub(self.head.len()).min(bytes.len());
        let (head, rest) = bytes.split_at(room);
        self.head.extend_from_slice(head);
        self.tail.extend(rest);

        let excess = self.tail.len().saturating_sub(self.limit);
        self.tail.drain(..excess);
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// The kept bytes as text. Invalid UTF-8 becomes U+FFFD, but a character
    /// cut in half by the omission is dropped whole and counted as omitted.
    pub fn text(&self) -> String {
        let mut tail = Vec::with_capacity(self.tail.len());
        tail.extend(&self.tail);

        if self.total == self.head.len() + tail.len() {
            let mut all = Vec::with_capacity(self.total);
            all.extend_from_slice(&self.head);
            all.extend_from_slice(&tail);

            return String::from_utf8_lossy(&all).into_owned();
        }

        let head = complete_prefix(&self.head);
        let tail = complete_suffix(&tail);
        let omitted = self.total - head.len() - tail.len();

        format!(
            "{}\n[… {omitted} bytes omitted …]\n{}",
            String::from_utf8_lossy(head),
            String::from_utf8_lossy(tail)
        )
    }
}

/// `bytes` without a last character that the cut left incomplete.
fn complete_prefix(bytes: &[u8]) -> &[u8] {
    // A character is at most four bytes, so its lead byte is among the last three
    // unless the character is complete.
    for back in 1..=bytes.len().min(3) {
        let at = bytes.len() - back;

        if is_continuation(bytes[at]) {
            continue;
        }

        if width(bytes[at]) > back {
            return &bytes[..at];
        }

        return bytes;
    }

    bytes
}

/// `bytes` without the continuation bytes of a character cut at its start.
fn complete_suffix(bytes: &[u8]) -> &[u8] {
    let cut = bytes.iter().take(3).take_while(|byte| is_continuation(**byte)).count();

    &bytes[cut..]
}

fn is_continuation(byte: u8) -> bool {
    byte & 0b1100_0000 == 0b1000_0000
}

/// The length of the character a lead byte starts. Invalid lead bytes count as one.
fn width(lead: u8) -> usize {
    match lead.leading_ones() {
        2 => 2,
        3 => 3,
        4 => 4,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::Output;

    #[test]
    fn truncation_keeps_whole_characters() {
        // `€` is three bytes: e2 82 ac. Every case keeps four bytes at each end.
        let cases: [(&[&str], &str); 6] = [
            (&["abcdefgh"], "abcdefgh"),
            // Head and tail meet inside a character: nothing was omitted, so it survives.
            (&["ab€cd"], "ab€cd"),
            (&["ab€", "zzzzz", "wxyz"], "ab\n[… 8 bytes omitted …]\nwxyz"),
            (&["abcd", "zzzzz", "€yz"], "abcd\n[… 8 bytes omitted …]\nyz"),
            // Many small writes still keep only the ends.
            (&["ab", "cd", "ef", "gh", "ij", "kl"], "abcd\n[… 4 bytes omitted …]\nijkl"),
            // A four-byte character ending exactly at the head limit is complete.
            (&["😀", "zzzzz", "wxyz"], "😀\n[… 5 bytes omitted …]\nwxyz"),
        ];

        for (chunks, expected) in cases {
            let mut output = Output::new(4);

            for chunk in chunks {
                output.push(chunk.as_bytes());
            }

            assert_eq!(output.text(), expected, "{chunks:?}");
        }
    }
}
