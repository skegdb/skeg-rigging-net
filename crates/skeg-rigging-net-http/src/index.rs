//! Index JSON shape: what `/sagas` returns.

use serde::{Deserialize, Serialize};

/// One row of the saga index. Wire format for `GET /sagas`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SagaIndexEntry {
    /// Lower-case 32-char hex form of the tenant id.
    pub tenant_id_hex: String,
    /// Saga file size in bytes.
    pub bytes: u64,
    /// Last modification timestamp in Unix seconds.
    pub last_modified: i64,
}
