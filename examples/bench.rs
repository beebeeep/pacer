use atomic_counter::{AtomicCounter, RelaxedCounter};
use bytes::Bytes;
use glommio::{CpuSet, LocalExecutorPoolBuilder, net::TcpStream, spawn_local};
use hdrhistogram::Histogram;
use http_body_util::Empty;
use hyper::{Request, StatusCode};
use pacer::hyper_compat::HyperStream;
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

fn main() {
    let cpus = CpuSet::online().unwrap().filter(|l| l.cpu > 3);
    let req_counter = Arc::new(atomic_counter::RelaxedCounter::new(0));
    let cnt = req_counter.clone();
    let _w =
        LocalExecutorPoolBuilder::new(glommio::PoolPlacement::MaxSpread(cpus.len(), Some(cpus)))
            .on_all_shards(|| async move {
                let mut tasks = Vec::new();
                for _ in 0..5 {
                    let cnt = cnt.clone();
                    tasks.push(spawn_local(async move { start_test(cnt).await }).detach());
                }
                for t in tasks {
                    t.await.unwrap();
                }
            })
            .unwrap();
    loop {
        let c = req_counter.get();
        let t = Instant::now();
        thread::sleep(Duration::from_secs(1));
        eprintln!(
            "allowed {} RPS",
            (req_counter.get() - c) as f64 / t.elapsed().as_secs_f64()
        );
    }
}

async fn start_test(req_counter: Arc<RelaxedCounter>) {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 1000000, 2).unwrap();
    let mut show_hist = Instant::now();
    let mut count = 0;
    loop {
        let s = Instant::now();
        let stream = TcpStream::connect("localhost:8080").await.unwrap();
        let stream = HyperStream(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(stream).await.unwrap();

        spawn_local(async move {
            if let Err(e) = conn.await {
                eprintln!("connection failed: {e:?}");
            }
        })
        .detach();

        let req = Request::builder()
            .method("POST")
            .uri("/limit/20000/foo")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        if res.status() == StatusCode::OK {
            req_counter.inc();
        }
        count += 1;
        hist.record(s.elapsed().as_micros() as u64).unwrap();
        if show_hist.elapsed() > Duration::from_secs(5) {
            eprintln!(
                "{count} requests, RTT: p50 {}us, p90 {}us, p99 {}us",
                hist.value_at_quantile(0.5),
                hist.value_at_quantile(0.9),
                hist.value_at_quantile(0.99)
            );
            show_hist = Instant::now();
        }
    }
}
