//! Synchronous HTTP client for the saga side-channel.

use std::io::Read;
use std::path::Path;
use std::time::Duration;

use skeg_rigging::TenantId;
use skeg_rigging_net::NetError;

use crate::index::SagaIndexEntry;

/// Synchronous saga-distribution client.
pub struct SagaClient {
    base_url: String,
    agent: ureq::Agent,
}

impl SagaClient {
    /// Construct a client targeting `base_url` (e.g. `"http://host:9000"`).
    /// Trailing slashes are normalised away.
    pub fn new(base_url: impl Into<String>) -> Self {
        let mut url = base_url.into();
        while url.ends_with('/') {
            url.pop();
        }
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(2))
            .timeout_read(Duration::from_secs(10))
            .timeout_write(Duration::from_secs(10))
            .build();
        Self {
            base_url: url,
            agent,
        }
    }

    /// List the sagas the server is currently exposing.
    pub fn list(&self) -> Result<Vec<SagaIndexEntry>, NetError> {
        let url = format!("{}/sagas", self.base_url);
        let resp = self.agent.get(&url).call().map_err(map_ureq)?;
        let entries: Vec<SagaIndexEntry> = resp.into_json().map_err(NetError::Io)?;
        Ok(entries)
    }

    /// Fetch one saga by tenant id. Returns the raw `SagaV1` bytes.
    pub fn fetch(&self, tenant_id: TenantId) -> Result<Vec<u8>, NetError> {
        let hex = format_tenant_hex(tenant_id);
        let url = format!("{}/sagas/{hex}.saga", self.base_url);
        let resp = self.agent.get(&url).call().map_err(map_ureq)?;
        let mut buf = Vec::new();
        resp.into_reader().read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Fetch the raw members.snap bytes for a hansa from the remote
    /// peer. The bytes are a JSON array; the caller parses with their
    /// own `MemberRecord` type (lives in hansa).
    pub fn fetch_members_raw(&self, hansa_id_hex: &str) -> Result<Vec<u8>, NetError> {
        if hansa_id_hex.len() != 64 || !hansa_id_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(NetError::Protocol(format!(
                "invalid hansa id hex: {hansa_id_hex:?}"
            )));
        }
        let url = format!("{}/hansa/{hansa_id_hex}/members", self.base_url);
        let resp = self.agent.get(&url).call().map_err(map_ureq)?;
        let mut buf = Vec::new();
        resp.into_reader().read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Last-Modified timestamp without downloading the body.
    pub fn head(&self, tenant_id: TenantId) -> Result<Option<i64>, NetError> {
        let hex = format_tenant_hex(tenant_id);
        let url = format!("{}/sagas/{hex}.saga", self.base_url);
        let resp = match self.agent.head(&url).call() {
            Ok(r) => r,
            Err(ureq::Error::Status(404, _)) => return Ok(None),
            Err(e) => return Err(map_ureq(e)),
        };
        let lm = resp.header("Last-Modified").and_then(|s| s.parse().ok());
        Ok(lm)
    }
}

/// Convenience: fetch a saga and write it to `dest_path` atomically.
/// Returns the byte length written.
pub fn fetch_to_path(
    client: &SagaClient,
    tenant_id: TenantId,
    dest_path: &Path,
) -> Result<usize, NetError> {
    let bytes = client.fetch(tenant_id)?;
    let parent = dest_path
        .parent()
        .ok_or_else(|| NetError::Protocol("dest_path has no parent".into()))?;
    std::fs::create_dir_all(parent)?;
    let tmp = dest_path.with_extension(format!(
        "{}.tmp.{}",
        dest_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("saga"),
        std::process::id()
    ));
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, dest_path)?;
    Ok(bytes.len())
}

fn format_tenant_hex(t: TenantId) -> String {
    let mut s = String::with_capacity(32);
    for b in t.0 {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn map_ureq(e: ureq::Error) -> NetError {
    match e {
        ureq::Error::Status(code, resp) => {
            NetError::Remote(format!("HTTP {code}: {}", resp.status_text()))
        }
        ureq::Error::Transport(t) => NetError::Io(std::io::Error::other(t.to_string())),
    }
}
