//! Record envelope for transports that don't model `shareable` / tags.
//!
//! skeg-server's RESP3 surface stores `(vector_id, f32_bytes)` for
//! vectors and `(key, value_bytes)` for KV records. There is no slot
//! for `shareable` or `tags`. To preserve hansa's filter semantics
//! across the network, the hansa-side writer wraps the record's
//! payload in a JSON envelope and stores it under
//! `hansa:rec:<vector_id>`. The reader fetches the envelope, decodes
//! it, applies the filter client-side, and returns the inner payload
//! up the `skeg-rigging` stack.
//!
//! This convention lives **outside** skeg so the engine stays
//! engine-neutral. Other adapters (e.g. an in-process file-based
//! `skeg-rigging-skeg`) can pick a different metadata strategy without
//! touching this crate.

use serde::{Deserialize, Serialize};

/// JSON envelope wrapping a record's payload, shareable flag, and tags.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordEnvelope {
    /// Whether peers in a hansa may see this record.
    pub shareable: bool,
    /// Tag strings attached to the record.
    pub tags: Vec<String>,
    /// Raw payload bytes. Held as a Vec<u8>; serde encodes as JSON
    /// array of numbers for now. Switch to base64 if/when bandwidth
    /// matters.
    pub payload: Vec<u8>,
}

impl RecordEnvelope {
    /// Convenience constructor.
    pub fn new(shareable: bool, tags: Vec<String>, payload: Vec<u8>) -> Self {
        Self {
            shareable,
            tags,
            payload,
        }
    }

    /// Encode to JSON bytes, ready to be the KV value.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("RecordEnvelope is always serialisable")
    }

    /// Decode from raw KV bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(buf)
    }
}

/// KV key prefix the bridge uses to store envelopes. Composed with a
/// record's vector id: `hansa:rec:<id>`.
pub const ENVELOPE_KEY_PREFIX: &str = "hansa:rec:";

/// Compute the KV key for a given record id under this convention.
pub fn envelope_key_for(record_id: u64) -> String {
    format!("{ENVELOPE_KEY_PREFIX}{record_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let env = RecordEnvelope {
            shareable: true,
            tags: vec!["topic".into(), "crypto".into()],
            payload: b"hello world".to_vec(),
        };
        let bytes = env.encode();
        let back = RecordEnvelope::decode(&bytes).unwrap();
        assert_eq!(env, back);
    }

    #[test]
    fn envelope_key_is_predictable() {
        assert_eq!(envelope_key_for(42), "hansa:rec:42");
        assert_eq!(envelope_key_for(0), "hansa:rec:0");
    }

    #[test]
    fn rejects_garbage() {
        let err = RecordEnvelope::decode(b"not json").unwrap_err();
        let _ = err; // just checking the type compiles
    }
}
