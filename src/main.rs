use clap::Parser;
use config::{Config, File};
use log::{debug, info, warn};
use pacer::{HyperStream, ResponseBody};
use serde::Deserialize;
use std::{
    collections::HashMap,
    convert::Infallible,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use glommio::{
    CpuSet, LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement, PoolPlacement, enclose,
    net::TcpListener, spawn_local, timer,
};
use hyper::{Method, Request, Response, StatusCode, body::Incoming, service::Service};

#[derive(Clone, Parser)]
#[command(version, about = "pacer: rate limiter", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[derive(Deserialize)]
struct AppConfig {
    bind_addr: String,
    classes: HashMap<String, u64>,
    threads: usize,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct BucketKey {
    class: Arc<str>,
    bucket: Arc<str>,
}

#[derive(Clone)]
struct Limiter {
    limits: HashMap<Arc<str>, u64>,
    buckets: Arc<Mutex<HashMap<BucketKey, u64>>>,
}

impl Limiter {
    fn bump(&self, bucket: BucketKey) -> Option<bool> {
        let mut buckets = self.buckets.lock().unwrap();
        match buckets.get_mut(&bucket) {
            Some(counter) => {
                if *counter == 0 {
                    Some(false)
                } else {
                    *counter -= 1;
                    Some(true)
                }
            }
            None => {
                if let Some(limit) = self.limits.get(&bucket.class) {
                    buckets.insert(bucket, *limit);
                    Some(true)
                } else {
                    None
                }
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
                        Some(true) => Ok(resp
                            .status(StatusCode::OK)
                            .body(ResponseBody::from(""))
                            .unwrap()),
                        Some(false) => Ok(resp
                            .status(StatusCode::TOO_MANY_REQUESTS)
                            .body(ResponseBody::from(""))
                            .unwrap()),
                        None => Ok(resp
                            .status(StatusCode::NOT_FOUND)
                            .body(ResponseBody::from("bucket class not found"))
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
        (Some(""), Some(class), Some(bucket), None) => {
            let class = Arc::from(class);
            let bucket = Arc::from(bucket);
            Some(BucketKey { class, bucket })
        }
        _ => None,
    }
}

async fn run_leaker(limits: HashMap<Arc<str>, u64>, buckets: Arc<Mutex<HashMap<BucketKey, u64>>>) {
    loop {
        let s = Instant::now();
        {
            let mut buckets = buckets.lock().unwrap();
            buckets.iter_mut().for_each(|(key, v)| {
                let limit = limits.get(&key.class).unwrap();
                debug!(class:? = key.class, bucket:? = key.bucket, rps = limit - *v; "bucket rps");
                *v = *limits.get(&key.class).unwrap(); // TODO: can limit bursting by going through buckets more often
            });
        }
        info!(duration :? = s.elapsed(); "leak");
        timer::sleep(Duration::from_secs(1)).await;
    }
}

async fn serve(limiter: Limiter, addr: impl Into<SocketAddr>) -> io::Result<()> {
    let listener = TcpListener::bind(addr.into())?;
    loop {
        match listener.accept().await {
            Err(e) => return Err(e.into()),
            Ok(stream) => {
                let addr = stream.local_addr().unwrap();
                let io = HyperStream(stream);
                spawn_local(enclose! {(limiter)async move {
                    if let Err(e) = hyper::server::conn::http1::Builder::new().serve_connection(io, limiter.clone()).await {
                            if !e.is_incomplete_message() {
                                warn!(addr:? = addr, err:? = e; "stream failed");
                            }
                    }
                }})
                .detach();
            }
        }
    }
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
    let mut limits = HashMap::new();
    for (class, rps) in cfg.classes {
        limits.insert(Arc::from(class.as_str()), rps);
    }
    let limiter = Limiter {
        buckets: buckets.clone(),
        limits: limits.clone(),
    };

    info!(binding_addr:% = cfg.bind_addr, limits:? ; "starting");
    // spawn leaker on cpu 0 and network threads on the rest of cpus up to requested limit
    let leaker = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(async move || {
            run_leaker(limits, buckets).await;
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
                serve(limiter, addr).await.unwrap();
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
