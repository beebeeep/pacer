use clap::Parser;
use config::{Config, File};
use flexbuffers::FlexbufferSerializer;
use glommio::{
    CpuSet, LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement, PoolPlacement,
    net::TcpStream, spawn_local, timer,
};
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request as HttpRequest, Response, StatusCode,
    body::{self, Incoming},
    client::conn::http1,
    service::Service,
};
use hyper_compat::{HyperStream, ResponseBody, start_http_server};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Whatever, whatever};
use std::{
    collections::HashMap,
    convert::Infallible,
    pin::Pin,
    sync::{Arc, RwLock},
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
    id: u32,
    limiter_addr: String,
    replicator_addr: String,
    threads: usize,
    replicas: Vec<String>,
    require_quorum: bool,
    replication_period_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
struct BucketKey {
    limit: u64,
    bucket: String,
}

type Buckets = HashMap<BucketKey, Counter>;
struct LimiterState {
    buckets: Buckets,
    has_quorum: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct Counter {
    local_hits: u64,
    remote_hits: u64, // sum of local_hits from all peers
}

#[derive(Clone, Deserialize, Serialize)]
struct ReplicationData {
    node_id: u32,
    buckets: Vec<BucketKey>,
    hits: Vec<u64>,
}

#[derive(Clone)]
struct Limiter {
    require_quorum: bool,
    state: Arc<RwLock<LimiterState>>,
}

#[derive(Clone)]
struct Replicator {
    peers_buckets: Arc<RwLock<HashMap<u32, Buckets>>>,
    state: Arc<RwLock<LimiterState>>,
}

impl Limiter {
    fn hit(&self, bucket: BucketKey) -> bool {
        let mut state = self.state.write().unwrap();
        if self.require_quorum && !state.has_quorum {
            return false;
        }
        match state.buckets.get_mut(&bucket) {
            Some(c) => {
                if c.local_hits + c.remote_hits < bucket.limit {
                    c.local_hits += 1;
                    true
                } else {
                    false
                }
            }
            None => {
                state.buckets.insert(
                    bucket,
                    Counter {
                        local_hits: 0,
                        remote_hits: 0,
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
                let resp = Response::builder().header("Connection", "close");
                let status = if self.hit(k) {
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
    fn update_buckets(&self, peer_id: u32, new_peer_buckets: HashMap<BucketKey, Counter>) {
        let mut peers_buckets = self.peers_buckets.write().unwrap();
        let mut state = self.state.write().unwrap();

        peers_buckets.insert(peer_id, new_peer_buckets);

        for (bucket, counter) in state.buckets.iter_mut() {
            // recalculate remote_hits for each bucket
            // by summing respective local_hits for that bucket from each peer
            counter.remote_hits = 0;
            for (_, buckets) in peers_buckets.iter() {
                if let Some(peer_counter) = buckets.get(bucket) {
                    counter.remote_hits += peer_counter.local_hits;
                }
            }
        }
    }

    async fn replicate(&self, req: HttpRequest<Incoming>) -> Result<(), Whatever> {
        let start = Instant::now();
        let body = req
            .collect()
            .await
            .whatever_context("error reading body")?
            .to_bytes();
        let des = flexbuffers::Reader::get_root(body.as_ref())
            .whatever_context("failed to deserialize body")?;
        let data =
            ReplicationData::deserialize(des).whatever_context("failed to deserialize body")?;
        if data.buckets.len() != data.hits.len() {
            whatever!("buckets and counters count mismatch");
        }

        // update peer info
        let mut new_peer_buckets = HashMap::new();
        for i in 0..data.buckets.len() {
            new_peer_buckets.insert(
                data.buckets[i].clone(),
                Counter {
                    local_hits: data.hits[i],
                    remote_hits: 0,
                },
            );
        }

        self.update_buckets(data.node_id, new_peer_buckets);
        debug!(peer = data.node_id, duration:? = start.elapsed();  "processed replicated data");

        Ok(())
    }
}

impl Service<HttpRequest<Incoming>> for Replicator {
    type Response = Response<ResponseBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;
    fn call(&self, req: HttpRequest<Incoming>) -> Self::Future {
        let r = self.clone();
        Box::pin(async move {
            match (req.method(), RequestURL::from(req.uri().path())) {
                (&Method::POST, RequestURL::Replicate) => {
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
                }
                _ => Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(ResponseBody::from("incorrect url"))
                    .unwrap()),
            }
        })
    }
}

async fn run_leaker(state: Arc<RwLock<LimiterState>>) {
    loop {
        let s = Instant::now();
        {
            let mut state = state.write().unwrap();
            state.buckets.iter_mut().for_each(|(_bucket, counter)| {
                counter.local_hits = 0; // TODO: can limit bursting by going through buckets more often 
            });
        }
        debug!(duration :? = s.elapsed(); "leak");
        timer::sleep(Duration::from_secs(1)).await;
    }
}

async fn run_distributor(cfg: AppConfig, state: Arc<RwLock<LimiterState>>) {
    loop {
        let start = Instant::now();
        let mut data = ReplicationData {
            node_id: cfg.id,
            buckets: Vec::new(),
            hits: Vec::new(),
        };

        // construct replication data under lock
        {
            let state = state.read().unwrap();
            for (k, v) in state.buckets.iter() {
                debug!(bucket:? = k, localhits = v.local_hits, remote_hits = v.remote_hits ; "bucket info");
                data.buckets.push(k.clone());
                data.hits.push(v.local_hits);
            }
        }

        // distribute it
        let mut handles = Vec::with_capacity(cfg.replicas.len());
        for replica in cfg.replicas.iter() {
            let replica = replica.clone();
            handles.push(spawn_local(send_replication_data(replica, data.clone())).detach());
        }
        let mut fails = 0;
        for h in handles {
            if let Err(e) = h.await.unwrap() {
                fails += 1;
                warn!(err:? = e; "failed to replicate data");
            }
        }

        // if we failed to replicate our data to quorum of peers, transition into inactive mode
        // this will prevent partitioning RPS counter in case of network partition
        {
            let mut state = state.write().unwrap();
            state.has_quorum = fails < cfg.replicas.len() / 2 + 1;
        }

        debug!(buckets = data.buckets.len(), duration:? = start.elapsed(); "sent replicatin data");

        timer::sleep(Duration::from_millis(cfg.replication_period_ms)).await;
    }
}

async fn send_replication_data(replica: String, data: ReplicationData) -> Result<(), Whatever> {
    let mut ser = FlexbufferSerializer::new();
    data.serialize(&mut ser)
        .whatever_context("serializing replication data")?;
    let data = ser.take_buffer();
    let stream = TcpStream::connect_timeout(replica, Duration::from_millis(300))
        .await
        .whatever_context("connecting")?;
    stream
        .set_read_timeout(Some(Duration::from_millis(300)))
        .whatever_context("setting timeout")?;
    let stream = HyperStream(stream);

    let (mut sender, conn) = http1::handshake(stream)
        .await
        .whatever_context("handshake")?;
    spawn_local(async move {
        if let Err(e) = conn.await {
            warn!(err:? = e;"replica connection error");
        }
    })
    .detach();

    let req = HttpRequest::builder()
        .method("POST")
        .uri("/replicate")
        .body(Full::<body::Bytes>::from(data))
        .whatever_context("building request")?;

    let resp = sender
        .send_request(req)
        .await
        .whatever_context("sending request")?;
    if resp.status() != StatusCode::OK {
        whatever!("bad response code {:?}", resp.status());
    }
    Ok(())
}

async fn run_dispatcher(cfg: &AppConfig, buckets: Arc<RwLock<LimiterState>>) {
    let leaker = spawn_local(run_leaker(buckets.clone())).detach();
    let distributor = spawn_local(run_distributor(cfg.clone(), buckets.clone())).detach();

    let replicator = Replicator {
        state: buckets,
        peers_buckets: Arc::new(RwLock::new(HashMap::new())),
    };
    start_http_server(replicator, cfg.replicator_addr.parse().unwrap())
        .await
        .unwrap();

    let _ = leaker.await.unwrap();
    let _ = distributor.await.unwrap();
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
    let buckets = Arc::new(RwLock::new(LimiterState {
        buckets: HashMap::new(),
        has_quorum: true,
    }));
    let limiter = Limiter {
        require_quorum: cfg.require_quorum,
        state: buckets.clone(),
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
