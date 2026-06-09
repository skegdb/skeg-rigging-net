//! Async HTTP client for the saga side-channel (F.52).
//!
//! Mirrors [`crate::SagaClient`] but issues requests via `reqwest`
//! under a Tokio runtime. The sync client stays the default; this
//! type is opt-in via the `async` feature.
//!
//! The server side (`SagaServer`) is unchanged — saga distribution
//! is a low-rate side channel where the bottleneck is the client's
//! ability to interleave many peer fetches concurrently, not the
//! server's request handling.

use std::path::Path;
use std::time::Duration;

use skeg_rigging::TenantId;
use skeg_rigging_net::NetError;

use crate::index::SagaIndexEntry;

/// Async saga-distribution client.
///
/// Cheap to clone — internally an `Arc`-shared `reqwest::Client`.
/// Share one across many tenants targeting the same peer endpoint.
#[derive(Clone)]
pub struct AsyncSagaClient {
    base_url: String,
    client: reqwest::Client,
}

impl AsyncSagaClient {
    /// Construct a client targeting `base_url` (e.g.
    /// `"http://host:9000"`). Trailing slashes are normalised away.
    pub fn new(base_url: impl Into<String>) -> Result<Self, NetError> {
        let mut url = base_url.into();
        while url.ends_with('/') {
            url.pop();
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(15))
            // The reference `SagaServer` (tiny_http) doesn't multiplex
            // multiple requests over a keep-alive connection: its
            // `recv_timeout` loop accepts new sockets, not subsequent
            // requests on the same socket. Disable the client-side
            // idle-conn pool so each request opens a fresh socket
            // tiny_http can accept. The overhead is negligible — saga
            // fetches are low-rate and large-bodied.
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| NetError::Io(std::io::Error::other(e.to_string())))?;
        Ok(Self {
            base_url: url,
            client,
        })
    }

    /// List the sagas the server is currently exposing.
    pub async fn list(&self) -> Result<Vec<SagaIndexEntry>, NetError> {
        let url = format!("{}/sagas", self.base_url);
        let resp = self.client.get(&url).send().await.map_err(map_reqwest)?;
        let resp = error_for_status(resp)?;
        let entries: Vec<SagaIndexEntry> = resp.json().await.map_err(map_reqwest)?;
        Ok(entries)
    }

    /// Fetch one saga by tenant id. Returns the raw `SagaV1` bytes.
    pub async fn fetch(&self, tenant_id: TenantId) -> Result<Vec<u8>, NetError> {
        let hex = format_tenant_hex(tenant_id);
        let url = format!("{}/sagas/{hex}.saga", self.base_url);
        let resp = self.client.get(&url).send().await.map_err(map_reqwest)?;
        let resp = error_for_status(resp)?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        Ok(bytes.to_vec())
    }

    /// Fetch the raw `members.snap` bytes for a hansa. JSON array of
    /// `MemberRecord` — caller parses with their own type.
    pub async fn fetch_members_raw(&self, hansa_id_hex: &str) -> Result<Vec<u8>, NetError> {
        if hansa_id_hex.len() != 64 || !hansa_id_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(NetError::Protocol(format!(
                "invalid hansa id hex: {hansa_id_hex:?}"
            )));
        }
        let url = format!("{}/hansa/{hansa_id_hex}/members", self.base_url);
        let resp = self.client.get(&url).send().await.map_err(map_reqwest)?;
        let resp = error_for_status(resp)?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        Ok(bytes.to_vec())
    }

    /// `Last-Modified` timestamp without downloading the body.
    /// Returns `None` if the saga is absent (404).
    pub async fn head(&self, tenant_id: TenantId) -> Result<Option<i64>, NetError> {
        let hex = format_tenant_hex(tenant_id);
        let url = format!("{}/sagas/{hex}.saga", self.base_url);
        let resp = self.client.head(&url).send().await.map_err(map_reqwest)?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = error_for_status(resp)?;
        let lm = resp
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        Ok(lm)
    }
}

/// Async sibling of [`crate::fetch_to_path`]. Atomic write via
/// temp+rename. Returns the byte length written.
pub async fn fetch_to_path_async(
    client: &AsyncSagaClient,
    tenant_id: TenantId,
    dest_path: &Path,
) -> Result<usize, NetError> {
    let bytes = client.fetch(tenant_id).await?;
    let parent = dest_path
        .parent()
        .ok_or_else(|| NetError::Protocol("dest_path has no parent".into()))?;
    let parent = parent.to_owned();
    let dest = dest_path.to_owned();
    // File ops stay sync — tokio::fs would pull a larger surface for
    // no real win here (saga writes are KB-scale, infrequent).
    let len = bytes.len();
    tokio::task::spawn_blocking(move || -> Result<(), NetError> {
        std::fs::create_dir_all(&parent)?;
        let tmp = dest.with_extension(format!(
            "{}.tmp.{}",
            dest.extension().and_then(|e| e.to_str()).unwrap_or("saga"),
            std::process::id()
        ));
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &dest)?;
        Ok(())
    })
    .await
    .map_err(|e| NetError::Io(std::io::Error::other(e.to_string())))??;
    Ok(len)
}

fn format_tenant_hex(t: TenantId) -> String {
    let mut s = String::with_capacity(32);
    for b in t.0 {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn error_for_status(resp: reqwest::Response) -> Result<reqwest::Response, NetError> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp)
    } else {
        Err(NetError::Remote(format!(
            "HTTP {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        )))
    }
}

fn map_reqwest(e: reqwest::Error) -> NetError {
    if let Some(status) = e.status() {
        NetError::Remote(format!(
            "HTTP {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        ))
    } else {
        NetError::Io(std::io::Error::other(e.to_string()))
    }
}
