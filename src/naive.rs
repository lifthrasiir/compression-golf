//! # Naive Codec (Baseline)
//!
//! **Strategy:** Plain JSON serialization using serde_json.
//!
//! **Result:** ~2.16 MB (baseline)
//!
//! This is the simplest possible implementation - just serialize the entire
//! event list to JSON. It serves as the baseline for comparison.
//!
//! **Why it's inefficient:**
//! - JSON is text-based with verbose field names repeated for every event
//! - No compression of repeated values (event types, repo names)
//! - Numbers stored as ASCII digits instead of binary
//! - Timestamps stored as full ISO 8601 strings (24 bytes each)
//!
//! **Lesson:** Sometimes the simplest solution is a good starting point,
//! but there's 10x+ improvement available with domain-specific encoding.

use bytes::Bytes;
use std::error::Error;

use crate::codec::EventCodec;
use crate::{EventKey, EventValue};

pub struct NaiveCodec;

impl EventCodec for NaiveCodec {
    fn encode(events: &[(EventKey, EventValue)]) -> Result<Bytes, Box<dyn Error>> {
        let json = serde_json::to_vec(events)?;
        Ok(Bytes::from(json))
    }

    fn decode(bytes: &[u8]) -> Result<Vec<(EventKey, EventValue)>, Box<dyn Error>> {
        let events = serde_json::from_slice(bytes)?;
        Ok(events)
    }
}
