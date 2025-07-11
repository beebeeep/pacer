//! ## pacer
//!
//! Simple and fast distributed rate-limiter with HTTP interface.
//!
//! ### Usage
//! Send empty POST request to `/limit/{max_rps}/{bucket}`, response code will be either `200 OK`
//! or `429 Too Many Requests`. Buckets for each RPS limit are created on-demand.

pub mod hyper_compat;
