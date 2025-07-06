use bytes::Bytes;
use glommio::{CpuSet, LocalExecutorPoolBuilder, net::TcpStream, spawn_local};
use hdrhistogram::Histogram;
use http_body_util::Empty;
use hyper::Request;
use std::time::{Duration, Instant};

fn main() {
    let cpus = CpuSet::online().unwrap().filter(|l| l.cpu > 3);
    let w =
        LocalExecutorPoolBuilder::new(glommio::PoolPlacement::MaxSpread(cpus.len(), Some(cpus)))
            .on_all_shards(|| async move {
                let mut tasks = Vec::new();
                for _ in 0..5 {
                    tasks.push(spawn_local(async { start_test().await }).detach());
                }
                for t in tasks {
                    t.await.unwrap();
                }
            })
            .unwrap();
    w.join_all();
}

async fn start_test() {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 1000000, 2).unwrap();
    let mut show_hist = Instant::now();
    let mut count = 0;
    loop {
        let s = Instant::now();
        let stream = TcpStream::connect("localhost:8080").await.unwrap();
        let stream = pacer::HyperStream(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(stream).await.unwrap();

        spawn_local(async move {
            if let Err(e) = conn.await {
                eprintln!("connection failed: {e:?}");
            }
        })
        .detach();

        let req = Request::builder()
            .method("POST")
            .uri("/default/foo")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let _res = sender.send_request(req).await.unwrap();
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
        // eprintln!("resp {} took {:?}", res.status(), s.elapsed());
    }
}
