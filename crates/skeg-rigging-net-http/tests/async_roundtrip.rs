//! F.52 - server (sync tiny_http) + async client roundtrip.

#![cfg(feature = "async")]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use skeg_rigging::TenantId;
use skeg_rigging_net_http::{AsyncSagaClient, SagaServer, fetch_to_path_async};

fn write_saga(dir: &std::path::Path, tenant: TenantId, content: &[u8]) {
    let mut hex = String::with_capacity(32);
    for b in tenant.0 {
        hex.push_str(&format!("{b:02x}"));
    }
    std::fs::write(dir.join(format!("{hex}.saga")), content).unwrap();
}

fn spawn_server(
    saga_dir: std::path::PathBuf,
) -> (u16, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let server = SagaServer::bind("127.0.0.1:0", saga_dir).unwrap();
    let port = server.local_addr().port();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let handle = std::thread::spawn(move || server.serve_until(stop_clone));
    std::thread::sleep(Duration::from_millis(50));
    (port, stop, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_fetch_roundtrip_returns_bytes_byte_equal() {
    let dir = tempfile::tempdir().unwrap();
    let tenant = TenantId::from_bytes([0x42; 16]);
    let payload = b"async saga blob, just bytes on the wire";
    write_saga(dir.path(), tenant, payload);

    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    let client = AsyncSagaClient::new(format!("http://127.0.0.1:{port}")).unwrap();
    let bytes = client.fetch(tenant).await.expect("fetch");
    assert_eq!(bytes, payload);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_list_returns_every_saga() {
    let dir = tempfile::tempdir().unwrap();
    for seed in [0x11u8, 0x55, 0xaa, 0x33] {
        write_saga(
            dir.path(),
            TenantId::from_bytes([seed; 16]),
            format!("payload-{seed}").as_bytes(),
        );
    }
    // Non-saga files must be ignored.
    std::fs::write(dir.path().join("README"), b"ignore me").unwrap();

    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    let client = AsyncSagaClient::new(format!("http://127.0.0.1:{port}")).unwrap();
    let entries = client.list().await.expect("list");
    assert_eq!(entries.len(), 4);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_head_missing_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    let client = AsyncSagaClient::new(format!("http://127.0.0.1:{port}")).unwrap();
    let res = client.head(TenantId::from_bytes([0; 16])).await.unwrap();
    assert_eq!(res, None);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_concurrent_fetches() {
    // Spin 8 concurrent fetches against the same server. Reqwest's
    // connection pool should multiplex over HTTP/1.1 keep-alive.
    let dir = tempfile::tempdir().unwrap();
    for seed in 0u8..8 {
        write_saga(
            dir.path(),
            TenantId::from_bytes([seed; 16]),
            format!("payload-{seed}").as_bytes(),
        );
    }
    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    let client = AsyncSagaClient::new(format!("http://127.0.0.1:{port}")).unwrap();

    let mut tasks = vec![];
    for seed in 0u8..8 {
        let c = client.clone();
        tasks.push(tokio::spawn(async move {
            c.fetch(TenantId::from_bytes([seed; 16])).await
        }));
    }
    for (i, t) in tasks.into_iter().enumerate() {
        let bytes = t.await.unwrap().unwrap();
        assert_eq!(bytes, format!("payload-{i}").as_bytes());
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_to_path_async_writes_file() {
    let dir = tempfile::tempdir().unwrap();
    let tenant = TenantId::from_bytes([0xcc; 16]);
    let payload = b"saga written via fetch_to_path_async";
    write_saga(dir.path(), tenant, payload);

    let (port, stop, handle) = spawn_server(dir.path().to_path_buf());
    let client = AsyncSagaClient::new(format!("http://127.0.0.1:{port}")).unwrap();

    let out_dir = tempfile::tempdir().unwrap();
    let dest = out_dir.path().join("downloaded.saga");
    let n = fetch_to_path_async(&client, tenant, &dest).await.unwrap();
    assert_eq!(n, payload.len());
    let written = std::fs::read(&dest).unwrap();
    assert_eq!(written, payload);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
}
