//! # Naive Codec (Baseline)
//!
//! **Strategy:** Plain JSON serialization using serde_json.
//!
//! This is the simplest possible implementation - just serialize the entire
//! event list to JSON. It serves as the baseline for comparison.

use bytes::Bytes;
use std::error::Error;

use crate::codec::EventCodec;
use crate::{EventKey, EventValue};

pub struct NaiveCodec;

impl NaiveCodec {
    pub fn new() -> Self {
        Self
    }
}

impl EventCodec for NaiveCodec {
    fn name(&self) -> &str {
        "Naive"
    }

    fn encode(&self, events: &[(EventKey, EventValue)]) -> Result<Bytes, Box<dyn Error>> {
        let json = serde_json::to_vec(events)?;
        Ok(Bytes::from(json))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Vec<(EventKey, EventValue)>, Box<dyn Error>> {
        let events = serde_json::from_slice(bytes)?;
        Ok(events)
    }
}
