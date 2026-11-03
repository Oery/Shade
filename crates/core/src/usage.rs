//! The `shade usage` report: what each conversation has cost in tokens.

use chrono::DateTime;
use infinity_provider_protocol::Usage;
use serde::Deserialize;
use shade_store::usage::{ConversationUsage, OriginUsage};

use crate::Error;
use crate::audit::safe;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prices {
    pub input_per_million: f64,
    pub cached_input_per_million: f64,
    pub output_per_million: f64,
}

impl Prices {
    pub fn validate(self) -> Result<(), Error> {
        let rates = [self.input_per_million, self.cached_input_per_million, self.output_per_million];

        if rates.iter().any(|rate| !rate.is_finite() || *rate < 0.0 || *rate > 1_000_000.0) {
            return Err(Error::Prices);
        }

        Ok(())
    }

    pub fn estimate(self, usage: &Usage) -> f64 {
        let cached = usage.cached_input_tokens.min(usage.input_tokens);
        let uncached = usage.input_tokens - cached;

        (uncached as f64 * self.input_per_million
            + cached as f64 * self.cached_input_per_million
            + usage.output_tokens as f64 * self.output_per_million)
            / 1_000_000.0
    }
}

/// Conversations shown, most recently active first.
pub const SHOWN: i64 = 20;

const NOTE: &str = "
input counts every token sent, cached ones included. context is the latest
request's input: roughly what the next message resends. cached reads 0 while
rig 0.31 does not report it on streaming responses.
USD is an estimate at each request's configured prices, not a bill. A dash
means at least one request had no price. Failed/unreported requests and
observations lost to pressure or a crash are not counted.
";

pub fn render(conversations: &[ConversationUsage], origins: &[OriginUsage]) -> String {
    if conversations.is_empty() {
        return "No model requests recorded yet.\n".to_owned();
    }

    let mut text = format!(
        "{:<32} {:>8} {:>12} {:>10} {:>10} {:>10}  {}\n",
        "conversation", "requests", "input", "cached", "output", "context", "last request (UTC)"
    );

    for conversation in conversations {
        text.push_str(&row(conversation));
    }

    text.push_str("\nAll-history totals by origin/source (up to 100 groups):\n");

    for total in origins {
        text.push_str(&format!(
            "{}:{} requests={} input={} cached={} output={} USD~{}\n",
            safe(&total.origin),
            safe(&total.source),
            total.requests,
            total.input_tokens,
            total.cached_input_tokens,
            total.output_tokens,
            money(total.estimated_usd)
        ));
    }

    text.push_str(NOTE);

    text
}

fn row(conversation: &ConversationUsage) -> String {
    let name = match &conversation.name {
        Some(name) => name.to_owned(),
        None => format!("unnamed {}", conversation.root.get(..8).unwrap_or(&conversation.root)),
    };
    let name = safe(&name);

    let last = match DateTime::from_timestamp(conversation.last_at, 0) {
        Some(time) => time.format("%Y-%m-%d %H:%M").to_string(),
        None => String::new(),
    };

    format!(
        "{name:<32} {:>8} {:>12} {:>10} {:>10} {:>10}  {last}\n  root={} {}:{} status={} USD~{}\n",
        conversation.requests,
        conversation.input_tokens,
        conversation.cached_input_tokens,
        conversation.output_tokens,
        conversation.last_input_tokens,
        safe(&conversation.root),
        safe(&conversation.origin),
        safe(&conversation.source),
        safe(&conversation.status),
        money(conversation.estimated_usd),
    )
}

pub fn money(value: Option<f64>) -> String {
    match value {
        Some(value) => format!("{value:.6}"),
        None => "-".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prices_count_cached_input_once_and_reject_invalid_rates() {
        let prices = Prices { input_per_million: 2.0, cached_input_per_million: 0.5, output_per_million: 10.0 };
        let usage = Usage { input_tokens: 1_000_000, cached_input_tokens: 200_000, output_tokens: 100_000, ..Usage::default() };

        assert!((prices.estimate(&usage) - 2.7).abs() < 0.000001);

        for rate in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1_000_001.0] {
            assert!(Prices { input_per_million: rate, ..prices }.validate().is_err());
            assert!(Prices { cached_input_per_million: rate, ..prices }.validate().is_err());
            assert!(Prices { output_per_million: rate, ..prices }.validate().is_err());
        }

        assert!(Prices { input_per_million: 0.0, ..prices }.validate().is_ok());
    }
}
