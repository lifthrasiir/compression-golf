use bytes::Bytes;
use std::error::Error;

use crate::{EventKey, EventValue};

pub trait EventCodec {
    fn name(&self) -> &str;
    fn encode(&self, events: &[(EventKey, EventValue)]) -> Result<Bytes, Box<dyn Error>>;
    fn decode(&self, bytes: &[u8]) -> Result<Vec<(EventKey, EventValue)>, Box<dyn Error>>;
}
