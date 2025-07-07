use clap::Parser;
use config::{Config, File};
use glommio::{
    CpuSet, LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement, PoolPlacement, spawn_local,
    timer,
};
use hyper::{Method, Request, Response, StatusCode, body::Incoming, service::Service};
use hyper_compat::{ResponseBody, start_http_server};
use log::{debug, info};
use serde::Deserialize;
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub mod hyper_compat;

#[derive(Clone, Parser)]
#[command(version, about = "pacer: rate limiter", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[derive(Deserialize)]
struct AppConfig {
    bind_addr: String,
    threads: usize,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct BucketKey {
    limit: u64,
    bucket: Arc<str>,
}

#[derive(Clone)]
struct Limiter {
    buckets: Arc<Mutex<HashMap<BucketKey, u64>>>,
}

impl Limiter {
    fn bump(&self, bucket: BucketKey) -> bool {
        let mut buckets = self.buckets.lock().unwrap();
        match buckets.get_mut(&bucket) {
            Some(counter) => {
                if *counter == 0 {
                    false
                } else {
                    *counter -= 1;
                    true
                }
            }
            None => {
                let limit = bucket.limit;
                buckets.insert(bucket, limit);
                true
            }
        }
    }
}

impl Service<Request<Incoming>> for Limiter {
    type Response = Response<ResponseBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;
    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let res = match (req.method(), req.uri().path()) {
            (&Method::POST, path) => {
                let resp = Response::builder();
                match parse_request_path(path) {
                    Some(k) => match self.bump(k) {
                        true => Ok(resp
                            .status(StatusCode::OK)
                            .body(ResponseBody::from(""))
                            .unwrap()),
                        false => Ok(resp
                            .status(StatusCode::TOO_MANY_REQUESTS)
                            .body(ResponseBody::from(""))
                            .unwrap()),
                    },
                    _ => Ok(resp
                        .status(StatusCode::NOT_FOUND)
                        .body(ResponseBody::from("incorrect url"))
                        .unwrap()),
                }
            }
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(ResponseBody::from("incorrect url"))
                .unwrap()),
        };
        Box::pin(async { res })
    }
}

fn parse_request_path(path: &str) -> Option<BucketKey> {
    let mut parts = path.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(""), Some(limit), Some(bucket), None) => {
            let limit = limit.parse().ok()?;
            let bucket = Arc::from(bucket);
            Some(BucketKey { limit, bucket })
        }
        _ => None,
    }
}

async fn run_leaker(buckets: Arc<Mutex<HashMap<BucketKey, u64>>>) {
    loop {
        let s = Instant::now();
        {
            let mut buckets = buckets.lock().unwrap();
            buckets.iter_mut().for_each(|(key, v)| {
                debug!(limit:? = key.limit, bucket:? = key.bucket, rps = key.limit - *v; "bucket rps");
                *v = key.limit; // TODO: can limit bursting by going through buckets more often
            });
        }
        info!(duration :? = s.elapsed(); "leak");
        timer::sleep(Duration::from_secs(1)).await;
    }
}

async fn run_dispatcher(buckets: Arc<Mutex<HashMap<BucketKey, u64>>>) {
    let leaker = spawn_local(run_leaker(buckets)).detach();
    let _ = leaker.await;
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

    info!(binding_addr:% = cfg.bind_addr; "starting");
    // spawn leaker on cpu 0 and network threads on the rest of cpus up to requested limit
    let leaker = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(async move || {
            run_dispatcher(buckets).await;
        })
        .expect("spawning leaker");

    let net_cpus = cpus.filter(|l| l.cpu > 0 && l.cpu <= cfg.threads);
    assert!(net_cpus.len() > 0, "not enough CPUs");
    let network_threads =
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(net_cpus.len(), Some(net_cpus)))
            .on_all_shards(|| async move {
                let id = glommio::executor().id();
                let addr = cfg.bind_addr.parse::<SocketAddr>().unwrap();
                info!(id; "starting server");
                start_http_server(limiter, addr).await.unwrap();
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
