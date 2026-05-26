//! Server + client end-to-end test on loopback.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use skeg_rigging::TenantId;
use skeg_rigging_net_http::{SagaClient, SagaServer, fetch_to_path};

fn write_saga(dir: &std::path::Path, tenant: TenantId, content: &[u8]) {
    let mut hex = String::with_capacity(32);
    for b in tenant.0 {
        hex.push_str(&format!("{b:02x}"));
    }
    std::fs::write(dir.join(format!("{hex}.saga")), content).unwrap();
}

fn spawn_server(saga_dir: std::path::PathBuf) -> (u16, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let server = SagaServer::bind("127.0.0.1:0", saga_dir).unwrap();
    let port = server.local_addr().port();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let handle = std::thread::spawn(move || server.serve_until(stop_clone));
    // Give the server a moment to begin recv_timeout.
    std::thread::sleep(Duration::from_millis(50));
    (port, stop, handle)
}

#[test]
fn fetch_roundtrip_returns_bytes_byte_equal() {
    let dir = tempfile::tempdir().unwrap();
    let tenant = TenantId::from_bytes([0x42; 16]);
    let payload = b"this is the saga blob, doesn't have to be real hull bytes for the test";
    write_saga(dir.path(), tenant, payload);

    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    let client = SagaClient::new(format!("http://127.0.0.1:{port}"));
    let bytes = client.fetch(tenant).expect("fetch");
    assert_eq!(bytes, payload);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}

#[test]
fn list_returns_every_saga_alphabetically() {
    let dir = tempfile::tempdir().unwrap();
    for seed in [0x11u8, 0x55, 0xaa, 0x33] {
        write_saga(
            dir.path(),
            TenantId::from_bytes([seed; 16]),
            format!("payload-{seed}").as_bytes(),
        );
    }
    // Some non-saga files should be ignored.
    std::fs::write(dir.path().join("README"), b"ignore me").unwrap();
    std::fs::write(dir.path().join("garbage.saga"), b"bad hex name").unwrap();

    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    let client = SagaClient::new(format!("http://127.0.0.1:{port}"));
    let entries = client.list().expect("list");
    assert_eq!(entries.len(), 4);
    let hexes: Vec<&str> = entries.iter().map(|e| e.tenant_id_hex.as_str()).collect();
    let mut sorted = hexes.clone();
    sorted.sort();
    assert_eq!(hexes, sorted, "entries must be sorted by tenant id hex");

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}

#[test]
fn fetch_missing_returns_404_as_error() {
    let dir = tempfile::tempdir().unwrap();
    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    let client = SagaClient::new(format!("http://127.0.0.1:{port}"));
    let r = client.fetch(TenantId::from_bytes([0x99; 16]));
    assert!(r.is_err(), "expected error for missing tenant");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}

#[test]
fn fetch_to_path_writes_bytes_to_disk() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    let tenant = TenantId::from_bytes([0xee; 16]);
    let payload = b"sample blob";
    write_saga(src_dir.path(), tenant, payload);

    let (port, stop, handle) = spawn_server(src_dir.path().to_path_buf());
    let client = SagaClient::new(format!("http://127.0.0.1:{port}"));
    let dest = dst_dir.path().join("local.saga");
    let n = fetch_to_path(&client, tenant, &dest).expect("fetch_to_path");
    assert_eq!(n, payload.len());
    let on_disk = std::fs::read(&dest).unwrap();
    assert_eq!(on_disk, payload);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}

#[test]
fn path_traversal_attempt_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    // The client API doesn't let us craft a bad URL, but we can hit
    // the raw endpoint via ureq.
    let url = format!("http://127.0.0.1:{port}/sagas/..%2Fsecret.saga");
    let result = ureq::get(&url).call();
    match result {
        Err(ureq::Error::Status(code, _)) => assert!(code == 400 || code == 404),
        Ok(resp) => panic!("expected error, got {}", resp.status()),
        Err(e) => panic!("transport error: {e}"),
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}
