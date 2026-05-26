#![deny(unsafe_code)]
#![warn(missing_docs)]

//! HTTP side-channel for hansa saga distribution.
//!
//! RESP3 (in the sibling crate) moves *query* traffic between peers.
//! It does not move *sagas*: skeg-server has no command for them, and
//! the sagas live under hansa's own directory, not the engine's. Cross-
//! machine federation therefore needs a complementary transport for
//! sagas only. HTTP is the right shape - large blobs, low rate, no
//! sub-millisecond latency required.
//!
//! Two halves ship in this crate:
//!
//! - [`SagaServer`]: tiny synchronous HTTP server backed by
//!   `tiny_http`. Reads from a hansa `saga_dir` and serves the files
//!   verbatim. No auth in v0.1; protect with reverse proxy or
//!   firewall if needed (a Bearer-token path lands in F.41).
//! - [`SagaClient`]: synchronous client built on `ureq`. Lists
//!   available sagas and fetches one by tenant id.
//!
//! The on-the-wire shape is intentionally trivial:
//!
//! | endpoint                                | method | body                                                  |
//! | --------------------------------------- | ------ | ----------------------------------------------------- |
//! | `/sagas`                                | GET    | JSON array of `SagaIndexEntry`                        |
//! | `/sagas/<tenant_id_hex>.saga`           | GET    | binary `SagaV1` blob (`application/octet-stream`)     |
//! | `/sagas/<tenant_id_hex>.saga`           | HEAD   | metadata only (`Content-Length`, `Last-Modified`)     |
//!
//! `tenant_id_hex` is the lowercase 32-char hex form of the tenant id.

mod client;
mod index;
mod server;

pub use client::{SagaClient, fetch_to_path};
pub use index::SagaIndexEntry;
pub use server::SagaServer;
