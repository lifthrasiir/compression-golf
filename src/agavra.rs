//! # Agavra Codec (Current Best)
//!
//! **Strategy:** Combine delta encoding, prefix compression, and type enumeration.
//!
//! ## How it works:
//!
//! This codec combines the best ideas from multiple approaches:
//!
//! ### 1. Type Enumeration
//! Event types are mapped to single-byte indices (0-13 for 14 types).
//! The dictionary is stored once at the start, then each event uses 1 byte.
//!
//! ### 2. Delta Encoding for Numbers
//! Instead of storing absolute values, store the difference from previous:
//! ```text
//! IDs:        2489651045, 2489651051, 2489651053
//! Deltas:     2489651045, +6,         +2          (much smaller!)
//! ```
//! Uses signed varints so negative deltas are also compact.
//!
//! ### 3. Hybrid Dictionary for Repo Names
//! Repo names (e.g., "user/repo") are split into two prefix-encoded dictionaries:
//! - Username dictionary: prefix-encoded list of unique usernames (41% prefix sharing)
//! - Repo name dictionary: prefix-encoded list of unique repo names (40% prefix sharing)
//! Each event stores `user_idx` + `repo_idx` as delta-encoded varints.
//! URLs are derived from repo names (`https://api.github.com/repos/{name}`).
//!
//! ### 4. Timestamp Delta Encoding
//! All timestamps are within a few hours. Delta-encoded, most are 0-3 seconds
//! apart → 1 byte each instead of 24 bytes for ISO 8601 strings.
//!
//! ## Data layout:
//!
//! ```text
//! [type_dict][user_dict][repo_dict][event_count][event1][event2]...
//!
//! Each event:
//! [type_idx: 1 byte]
//! [id_delta: signed varint]
//! [repo_id_delta: signed varint]
//! [user_idx_delta: signed varint]
//! [repo_idx_delta: signed varint]
//! [timestamp_delta: signed varint]
//! ```
//!

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;
use std::error::Error;

use crate::codec::EventCodec;
use crate::{EventKey, EventValue, Repo};

// ============================================================================
// Varint encoding
// ============================================================================

fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn decode_varint(bytes: &[u8], pos: &mut usize) -> u64 {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = bytes[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn encode_signed_varint(value: i64, buf: &mut Vec<u8>) {
    let encoded = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(encoded, buf);
}

fn decode_signed_varint(bytes: &[u8], pos: &mut usize) -> i64 {
    let encoded = decode_varint(bytes, pos);
    ((encoded >> 1) as i64) ^ (-((encoded & 1) as i64))
}

// ============================================================================
// Timestamp utilities
// ============================================================================

fn parse_timestamp(ts: &str) -> u64 {
    DateTime::parse_from_rfc3339(ts)
        .map(|dt| dt.timestamp() as u64)
        .unwrap_or(0)
}

fn format_timestamp(ts: u64) -> String {
    Utc.timestamp_opt(ts as i64, 0)
        .single()
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

// ============================================================================
// Prefix-encoded string dictionary
// ============================================================================

fn common_prefix_len(a: &str, b: &str) -> usize {
    a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
}

struct StringDict {
    strings: Vec<String>,
    str_to_idx: HashMap<String, u32>,
}

impl StringDict {
    fn build(items: impl Iterator<Item = String>) -> Self {
        let mut unique: Vec<String> = items.collect();
        unique.sort();
        unique.dedup();

        let mut str_to_idx = HashMap::new();
        for (i, s) in unique.iter().enumerate() {
            str_to_idx.insert(s.clone(), i as u32);
        }

        Self {
            strings: unique,
            str_to_idx,
        }
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint(self.strings.len() as u64, buf);
        let mut prev = String::new();
        for s in &self.strings {
            let prefix_len = common_prefix_len(s, &prev);
            let suffix = &s[prefix_len..];
            encode_varint(prefix_len as u64, buf);
            encode_varint(suffix.len() as u64, buf);
            buf.extend_from_slice(suffix.as_bytes());
            prev = s.clone();
        }
    }

    fn decode(bytes: &[u8], pos: &mut usize) -> Self {
        let count = decode_varint(bytes, pos) as usize;
        let mut strings = Vec::with_capacity(count);
        let mut str_to_idx = HashMap::new();
        let mut prev = String::new();

        for i in 0..count {
            let prefix_len = decode_varint(bytes, pos) as usize;
            let suffix_len = decode_varint(bytes, pos) as usize;
            let suffix = std::str::from_utf8(&bytes[*pos..*pos + suffix_len]).unwrap();
            *pos += suffix_len;
            let s = format!("{}{}", &prev[..prefix_len], suffix);
            str_to_idx.insert(s.clone(), i as u32);
            prev = s.clone();
            strings.push(s);
        }

        Self {
            strings,
            str_to_idx,
        }
    }

    fn get_index(&self, s: &str) -> u32 {
        self.str_to_idx[s]
    }

    fn get_string(&self, index: u32) -> &str {
        &self.strings[index as usize]
    }
}

fn split_repo_name(full_name: &str) -> (&str, &str) {
    full_name.split_once('/').unwrap_or((full_name, ""))
}

// ============================================================================
// Type enumeration (maps event types to single-byte indices)
// ============================================================================

struct TypeEnum {
    type_to_idx: HashMap<String, u8>,
    types: Vec<String>,
}

impl TypeEnum {
    fn build(events: &[(EventKey, EventValue)]) -> Self {
        let mut freq: HashMap<&str, usize> = HashMap::new();
        for (key, _) in events {
            *freq.entry(&key.event_type).or_insert(0) += 1;
        }

        let mut types_with_freq: Vec<_> = freq.into_iter().collect();
        types_with_freq.sort_by(|a, b| b.1.cmp(&a.1));

        let mut type_to_idx: HashMap<String, u8> = HashMap::new();
        let mut types: Vec<String> = Vec::new();
        for (i, (t, _)) in types_with_freq.into_iter().enumerate() {
            type_to_idx.insert(t.to_string(), i as u8);
            types.push(t.to_string());
        }

        Self { type_to_idx, types }
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint(self.types.len() as u64, buf);
        for t in &self.types {
            encode_varint(t.len() as u64, buf);
            buf.extend_from_slice(t.as_bytes());
        }
    }

    fn decode(bytes: &[u8], pos: &mut usize) -> Self {
        let type_count = decode_varint(bytes, pos) as usize;
        let mut types: Vec<String> = Vec::with_capacity(type_count);
        let mut type_to_idx: HashMap<String, u8> = HashMap::new();

        for i in 0..type_count {
            let len = decode_varint(bytes, pos) as usize;
            let t = std::str::from_utf8(&bytes[*pos..*pos + len])
                .unwrap()
                .to_string();
            *pos += len;
            type_to_idx.insert(t.clone(), i as u8);
            types.push(t);
        }

        Self { type_to_idx, types }
    }

    fn get_index(&self, event_type: &str) -> u8 {
        self.type_to_idx[event_type]
    }

    fn get_type(&self, index: u8) -> &str {
        &self.types[index as usize]
    }
}

// ============================================================================
// The codec implementation
// ============================================================================

pub struct AgavraCodec;

impl AgavraCodec {
    pub fn new() -> Self {
        Self
    }
}

impl EventCodec for AgavraCodec {
    fn name(&self) -> &str {
        "agavra"
    }

    fn encode(&self, events: &[(EventKey, EventValue)]) -> Result<Bytes, Box<dyn Error>> {
        let type_enum = TypeEnum::build(events);
        let user_dict = StringDict::build(events.iter().map(|(_, v)| {
            let (user, _) = split_repo_name(&v.repo.name);
            user.to_string()
        }));
        let repo_dict = StringDict::build(events.iter().map(|(_, v)| {
            let (_, repo) = split_repo_name(&v.repo.name);
            repo.to_string()
        }));

        let mut sorted: Vec<_> = events.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        let mut buf = Vec::new();

        type_enum.encode(&mut buf);
        user_dict.encode(&mut buf);
        repo_dict.encode(&mut buf);
        encode_varint(sorted.len() as u64, &mut buf);

        let mut prev_id: u64 = 0;
        let mut prev_repo_id: u64 = 0;
        let mut prev_user_idx: u32 = 0;
        let mut prev_repo_idx: u32 = 0;
        let mut prev_ts: u64 = 0;

        for (key, value) in &sorted {
            buf.push(type_enum.get_index(&key.event_type));

            let id: u64 = key.id.parse().unwrap_or(0);
            let delta_id = id as i64 - prev_id as i64;
            encode_signed_varint(delta_id, &mut buf);
            prev_id = id;

            let delta_repo_id = value.repo.id as i64 - prev_repo_id as i64;
            encode_signed_varint(delta_repo_id, &mut buf);
            prev_repo_id = value.repo.id;

            let (user, repo) = split_repo_name(&value.repo.name);
            let user_idx = user_dict.get_index(user);
            let delta_user_idx = user_idx as i64 - prev_user_idx as i64;
            encode_signed_varint(delta_user_idx, &mut buf);
            prev_user_idx = user_idx;

            let repo_idx = repo_dict.get_index(repo);
            let delta_repo_idx = repo_idx as i64 - prev_repo_idx as i64;
            encode_signed_varint(delta_repo_idx, &mut buf);
            prev_repo_idx = repo_idx;

            let ts = parse_timestamp(&value.created_at);
            let delta_ts = ts as i64 - prev_ts as i64;
            encode_signed_varint(delta_ts, &mut buf);
            prev_ts = ts;
        }

        Ok(Bytes::from(buf))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Vec<(EventKey, EventValue)>, Box<dyn Error>> {
        let mut pos = 0;

        let type_enum = TypeEnum::decode(bytes, &mut pos);
        let user_dict = StringDict::decode(bytes, &mut pos);
        let repo_dict = StringDict::decode(bytes, &mut pos);
        let count = decode_varint(bytes, &mut pos) as usize;

        let mut events = Vec::with_capacity(count);
        let mut prev_id: u64 = 0;
        let mut prev_repo_id: u64 = 0;
        let mut prev_user_idx: u32 = 0;
        let mut prev_repo_idx: u32 = 0;
        let mut prev_ts: u64 = 0;

        for _ in 0..count {
            let type_idx = bytes[pos];
            pos += 1;
            let event_type = type_enum.get_type(type_idx).to_string();

            let delta_id = decode_signed_varint(bytes, &mut pos);
            let id = (prev_id as i64 + delta_id) as u64;
            prev_id = id;

            let delta_repo_id = decode_signed_varint(bytes, &mut pos);
            let repo_id = (prev_repo_id as i64 + delta_repo_id) as u64;
            prev_repo_id = repo_id;

            let delta_user_idx = decode_signed_varint(bytes, &mut pos);
            let user_idx = (prev_user_idx as i64 + delta_user_idx) as u32;
            prev_user_idx = user_idx;

            let delta_repo_idx = decode_signed_varint(bytes, &mut pos);
            let repo_idx = (prev_repo_idx as i64 + delta_repo_idx) as u32;
            prev_repo_idx = repo_idx;

            let user = user_dict.get_string(user_idx);
            let repo = repo_dict.get_string(repo_idx);
            let repo_name = format!("{}/{}", user, repo);
            let repo_url = format!("https://api.github.com/repos/{}", repo_name);

            let delta_ts = decode_signed_varint(bytes, &mut pos);
            let ts = (prev_ts as i64 + delta_ts) as u64;
            prev_ts = ts;
            let created_at = format_timestamp(ts);

            events.push((
                EventKey {
                    event_type,
                    id: id.to_string(),
                },
                EventValue {
                    repo: Repo {
                        id: repo_id,
                        name: repo_name,
                        url: repo_url,
                    },
                    created_at,
                },
            ));
        }

        Ok(events)
    }
}
