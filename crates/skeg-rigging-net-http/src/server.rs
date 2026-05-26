//! Synchronous HTTP server for serving a hansa `saga_dir`.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use skeg_rigging_net::NetError;
use tiny_http::{Header, Method, Response, Server, StatusCode};

use crate::index::SagaIndexEntry;

/// Synchronous HTTP server serving sagas from `saga_dir`, optionally
/// also serving the matching `members.snap` from `members_root`.
///
/// File layout the server expects:
///
/// ```text
/// <saga_dir>/<tenant_id_hex>.saga           ← served by GET /sagas/<id>.saga
/// <members_root>/<hansa_id_hex>/members.snap ← served by GET /hansa/<id>/members
/// ```
///
/// `members_root` is optional - when not set, `/hansa/.../members`
/// returns 404 and the server is saga-only.
pub struct SagaServer {
    inner: Arc<Server>,
    saga_dir: PathBuf,
    members_root: Option<PathBuf>,
    addr: SocketAddr,
}

impl SagaServer {
    /// Bind to `addr` and prepare to serve files from `saga_dir`. The
    /// directory is read on every request; new sagas appear without
    /// restart.
    pub fn bind(addr: impl std::net::ToSocketAddrs, saga_dir: PathBuf) -> Result<Self, NetError> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        let server = Server::from_listener(listener, None)
            .map_err(|e| NetError::Protocol(format!("tiny_http: {e}")))?;
        Ok(Self {
            inner: Arc::new(server),
            saga_dir,
            members_root: None,
            addr: local_addr,
        })
    }

    /// Attach a `members_root` so the server also exposes member
    /// snapshots (used by `HybridRegistry` to discover remote peers).
    pub fn with_members_root(mut self, members_root: PathBuf) -> Self {
        self.members_root = Some(members_root);
        self
    }

    /// Address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Block the current thread serving requests until `stop` is set.
    /// Polls every 100 ms; long enough not to burn CPU, short enough
    /// that test teardown is bearable.
    pub fn serve_until(self, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::Relaxed) {
            match self.inner.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(Some(req)) => {
                    if let Err(e) = handle(req, &self.saga_dir, self.members_root.as_deref()) {
                        eprintln!("saga server: handler error: {e}");
                    }
                }
                Ok(None) => {} // timeout, loop
                Err(e) => {
                    eprintln!("saga server: recv error: {e}");
                    break;
                }
            }
        }
    }
}

fn handle(
    req: tiny_http::Request,
    saga_dir: &Path,
    members_root: Option<&Path>,
) -> Result<(), NetError> {
    let url = req.url().to_string();
    let method = req.method().clone();
    match (method, url.as_str()) {
        (Method::Get, "/sagas") => respond_index(req, saga_dir),
        (Method::Get, path) if path.starts_with("/sagas/") && path.ends_with(".saga") => {
            respond_file(req, saga_dir, path, false)
        }
        (Method::Head, path) if path.starts_with("/sagas/") && path.ends_with(".saga") => {
            respond_file(req, saga_dir, path, true)
        }
        (Method::Get, path) if path.starts_with("/hansa/") && path.ends_with("/members") => {
            respond_members(req, members_root, path)
        }
        _ => Ok(req.respond(Response::empty(StatusCode(404)))?),
    }
}

fn respond_members(
    req: tiny_http::Request,
    members_root: Option<&Path>,
    url_path: &str,
) -> Result<(), NetError> {
    let Some(root) = members_root else {
        return Ok(req.respond(Response::empty(StatusCode(404)))?);
    };
    // URL shape: /hansa/<hex>/members
    let trimmed = url_path
        .strip_prefix("/hansa/")
        .and_then(|p| p.strip_suffix("/members"));
    let Some(hex) = trimmed else {
        return Ok(req.respond(Response::empty(StatusCode(400)))?);
    };
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(req.respond(Response::empty(StatusCode(400)))?);
    }
    // We serve the snapshot file verbatim - JSON array of MemberRecord
    // as written by FileRegistry::compact. If only the log exists (no
    // snapshot yet), return an empty array.
    let snap_path = root.join(hex).join("members.snap");
    let body: Vec<u8> = if snap_path.exists() {
        std::fs::read(&snap_path)?
    } else {
        b"[]".to_vec()
    };
    let resp = Response::from_data(body).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).expect("header"),
    );
    req.respond(resp)?;
    Ok(())
}

fn respond_index(req: tiny_http::Request, saga_dir: &Path) -> Result<(), NetError> {
    let mut entries: Vec<SagaIndexEntry> = Vec::new();
    if saga_dir.exists() {
        for entry in std::fs::read_dir(saga_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else { continue };
            if !name_str.ends_with(".saga") {
                continue;
            }
            let hex = &name_str[..name_str.len() - 5];
            if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            let meta = entry.metadata()?;
            entries.push(SagaIndexEntry {
                tenant_id_hex: hex.to_string(),
                bytes: meta.len(),
                last_modified: mtime_secs(&meta),
            });
        }
    }
    entries.sort_by(|a, b| a.tenant_id_hex.cmp(&b.tenant_id_hex));
    let body = serde_json::to_vec(&entries)
        .map_err(|e| NetError::Protocol(format!("json: {e}")))?;
    let resp = Response::from_data(body).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .expect("header"),
    );
    req.respond(resp)?;
    Ok(())
}

fn respond_file(
    req: tiny_http::Request,
    saga_dir: &Path,
    url_path: &str,
    head_only: bool,
) -> Result<(), NetError> {
    let file_name = &url_path["/sagas/".len()..];
    // Strict path validation: no '..' or '/'.
    if file_name.contains('/') || file_name.contains("..") {
        return Ok(req.respond(Response::empty(StatusCode(400)))?);
    }
    let path = saga_dir.join(file_name);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(req.respond(Response::empty(StatusCode(404)))?);
        }
        Err(e) => return Err(NetError::Io(e)),
    };
    let meta = std::fs::metadata(&path)?;
    let last_modified = mtime_secs(&meta);
    let content_length = bytes.len();
    let etag = format!("\"{}-{last_modified}\"", content_length);

    let make_headers = || -> Vec<Header> {
        vec![
            Header::from_bytes(&b"Content-Type"[..], &b"application/octet-stream"[..])
                .expect("header"),
            Header::from_bytes(
                &b"Last-Modified"[..],
                last_modified.to_string().as_bytes(),
            )
            .expect("header"),
            Header::from_bytes(&b"ETag"[..], etag.as_bytes()).expect("header"),
        ]
    };

    if head_only {
        let mut resp = Response::empty(StatusCode(200)).with_data(std::io::empty(), Some(0));
        for h in make_headers() {
            resp = resp.with_header(h);
        }
        // Manually advertise Content-Length for HEAD.
        resp = resp.with_header(
            Header::from_bytes(&b"Content-Length"[..], content_length.to_string().as_bytes())
                .expect("header"),
        );
        req.respond(resp)?;
    } else {
        let mut resp = Response::from_data(bytes);
        for h in make_headers() {
            resp = resp.with_header(h);
        }
        req.respond(resp)?;
    }
    Ok(())
}

fn mtime_secs(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
