use bytes::Buf;
use clap::Parser;
use config::{Config, File};
use glommio::{
    CpuSet, LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement, PoolPlacement, spawn_local,
    timer,
};
use http_body_util::BodyExt;
use hyper::{
    Method, Request as HttpRequest, Response, StatusCode, body::Incoming, service::Service,
};
use hyper_compat::{ResponseBody, start_http_server};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use snafu::{ErrorCompat, ResultExt, Whatever, whatever};
use std::{
    collections::HashMap,
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub mod hyper_compat;

enum RequestURL {
    Limit(BucketKey),
    Replicate,
    Invalid,
}

impl From<&str> for RequestURL {
    fn from(path: &str) -> Self {
        let mut parts = path.split('/');
        match (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) {
            (Some(""), Some("limit"), Some(limit), Some(bucket), None) => {
                let limit = match limit.parse() {
                    Ok(v) => v,
                    Err(_) => return Self::Invalid,
                };
                let bucket = String::from(bucket);
                Self::Limit(BucketKey { limit, bucket })
            }
            (Some(""), Some("replicate"), None, None, None) => Self::Replicate,
            _ => Self::Invalid,
        }
    }
}

#[derive(Clone, Parser)]
#[command(version, about = "pacer: rate limiter", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[derive(Deserialize, Clone)]
struct AppConfig {
    limiter_addr: String,
    replicator_addr: String,
    threads: usize,
    replicas: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
struct BucketKey {
    limit: u64,
    bucket: String,
}

#[derive(Deserialize, Serialize)]
struct Counter {
    local: u64,
    remote: u64,
}

#[derive(Deserialize, Serialize)]
struct ReplicationData {
    buckets: Vec<BucketKey>,
    counters: Vec<u64>,
}

#[derive(Clone)]
struct Limiter {
    buckets: Arc<Mutex<HashMap<BucketKey, Counter>>>,
}

#[derive(Clone)]
struct Replicator {
    cfg: AppConfig,
    buckets: Arc<Mutex<HashMap<BucketKey, Counter>>>,
}

impl Limiter {
    fn bump(&self, bucket: BucketKey) -> bool {
        let mut buckets = self.buckets.lock().unwrap();
        match buckets.get_mut(&bucket) {
            Some(c) => {
                if c.remote + c.local < bucket.limit {
                    c.local += 1;
                    true
                } else {
                    false
                }
            }
            None => {
                buckets.insert(
                    bucket,
                    Counter {
                        remote: 0,
                        local: 0,
                    },
                );
                true
            }
        }
    }
}

impl Service<HttpRequest<Incoming>> for Limiter {
    type Response = Response<ResponseBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;
    fn call(&self, req: HttpRequest<Incoming>) -> Self::Future {
        let res = match (req.method(), RequestURL::from(req.uri().path())) {
            (&Method::POST, RequestURL::Limit(k)) => {
                let resp = Response::builder();
                let status = if self.bump(k) {
                    StatusCode::OK
                } else {
                    StatusCode::TOO_MANY_REQUESTS
                };

                Ok(resp.status(status).body(ResponseBody::from("")).unwrap())
            }
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(ResponseBody::from("incorrect url"))
                .unwrap()),
        };
        Box::pin(async { res })
    }
}

impl Replicator {
    async fn replicate(&self, req: HttpRequest<Incoming>) -> Result<(), Whatever> {
        let body = req
            .collect()
            .await
            .whatever_context("error reading body")?
            .to_bytes();
        let des = flexbuffers::Reader::get_root(body.as_ref())
            .whatever_context("failed to deserialize body")?;
        let data =
            ReplicationData::deserialize(des).whatever_context("failed to deserialize body")?;
        if data.buckets.len() != data.counters.len() {
            whatever!("buckets and counters count mismatch");
        }
        let mut buckets = self.buckets.lock().unwrap();
        for i in 0..data.buckets.len() {
            let key = BucketKey {
                limit: data.buckets[i].limit,
                bucket: data.buckets[i].bucket.clone(),
            };
            let c = data.counters[i];
            buckets
                .entry(key)
                .and_modify(|e| e.remote += c)
                .or_insert(Counter {
                    local: 0,
                    remote: c,
                });
        }

        Ok(())
    }
}

impl Service<HttpRequest<Incoming>> for Replicator {
    type Response = Response<ResponseBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;
    fn call(&self, req: HttpRequest<Incoming>) -> Self::Future {
        match (req.method(), RequestURL::from(req.uri().path())) {
            (&Method::POST, RequestURL::Replicate) => {
                let r = self.clone();
                Box::pin(async move {
                    let b = Response::builder();
                    let r = match r.replicate(req).await {
                        Ok(_) => b
                            .status(StatusCode::OK)
                            .body(ResponseBody::from(""))
                            .unwrap(),
                        Err(e) => b
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(ResponseBody::from(e.to_string().as_str()))
                            .unwrap(),
                    };
                    Ok(r)
                })
            }
            _ => Box::pin(async {
                Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(ResponseBody::from("incorrect url"))
                    .unwrap())
            }),
        }
    }
}

async fn run_leaker(buckets: Arc<Mutex<HashMap<BucketKey, Counter>>>) {
    loop {
        let s = Instant::now();
        {
            let mut buckets = buckets.lock().unwrap();
            buckets.iter_mut().for_each(|(k, v)| {
                v.local = v.local.saturating_sub(k.limit); // TODO: can limit bursting by going through buckets more often
            });
        }
        info!(duration :? = s.elapsed(); "leak");
        timer::sleep(Duration::from_secs(1)).await;
    }
}

async fn run_dispatcher(cfg: &AppConfig, buckets: Arc<Mutex<HashMap<BucketKey, Counter>>>) {
    let leaker = spawn_local(run_leaker(buckets.clone())).detach();
    let replicator = Replicator {
        cfg: cfg.clone(),
        buckets,
    };
    start_http_server(replicator, cfg.replicator_addr.parse().unwrap())
        .await
        .unwrap();
    let _ = leaker.await.unwrap();
}

fn main() {
    env_logger::init();

    let args = Args::parse();
    let config = Config::builder()
        .add_source(File::with_name(&args.config).required(true))
        .build()
        .unwrap();
    let cfg: AppConfig = config.try_deserialize().unwrap();

    let cpus = CpuSet::online().unwrap();
    let buckets = Arc::new(Mutex::new(HashMap::new()));
    let limiter = Limiter {
        buckets: buckets.clone(),
    };

    // spawn leaker on cpu 0 and network threads on the rest of cpus up to requested limit
    let leaker_cfg = cfg.clone();
    let leaker = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(async move || {
            run_dispatcher(&leaker_cfg, buckets).await;
        })
        .expect("spawning leaker");

    let limiter_cpus = cpus.filter(|l| l.cpu > 0 && l.cpu <= cfg.threads);
    assert!(limiter_cpus.len() > 0, "not enough CPUs");
    let limiter_addr = cfg.limiter_addr;
    let network_threads = LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
        limiter_cpus.len(),
        Some(limiter_cpus),
    ))
    .on_all_shards(|| async move {
        let id = glommio::executor().id();
        info!(id; "starting server");
        start_http_server(limiter, limiter_addr.parse().unwrap())
            .await
            .unwrap();
    })
    .unwrap();
    for l in network_threads.join_all() {
        if let Err(e) = l {
            eprintln!("{e:?}");
        }
    }
    leaker.join().expect("joining leaker");

    println!("Hello, world!");
}
