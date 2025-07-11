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
    let req_counter_ok = Arc::new(atomic_counter::RelaxedCounter::new(0));
    let req_counter_rej = Arc::new(atomic_counter::RelaxedCounter::new(0));
    let ok_cnt = req_counter_ok.clone();
    let rej_cnt = req_counter_rej.clone();

    // loadtest(ok_cnt, rej_cnt);
    small_test(ok_cnt, rej_cnt);

    loop {
        let c_ok = req_counter_ok.get();
        let c_rej = req_counter_rej.get();
        let t = Instant::now();
        thread::sleep(Duration::from_secs(1));
        eprintln!(
            "allowed {} RPS, rejected {} RPS",
            (req_counter_ok.get() - c_ok) as f64 / t.elapsed().as_secs_f64(),
            (req_counter_rej.get() - c_rej) as f64 / t.elapsed().as_secs_f64(),
        );
    }
}

#[allow(dead_code)]
fn loadtest(ok_cnt: Arc<RelaxedCounter>, rej_cnt: Arc<RelaxedCounter>) {
    let cpus = CpuSet::online().unwrap().filter(|l| l.cpu > 3);
    let _w =
        LocalExecutorPoolBuilder::new(glommio::PoolPlacement::MaxSpread(cpus.len(), Some(cpus)))
            .on_all_shards(|| async move {
                let mut tasks = Vec::new();
                for _ in 0..5 {
                    let ok_cnt = ok_cnt.clone();
                    let rej_cnt = rej_cnt.clone();
                    tasks.push(spawn_local(start_test(ok_cnt, rej_cnt, 20000)).detach());
                }
                for t in tasks {
                    t.await.unwrap();
                }
            })
            .unwrap();
}

#[allow(dead_code)]
fn small_test(ok_cnt: Arc<RelaxedCounter>, rej_cnt: Arc<RelaxedCounter>) {
    let _w = LocalExecutorPoolBuilder::new(glommio::PoolPlacement::Unbound(2))
        .on_all_shards(|| async move {
            start_test(ok_cnt, rej_cnt, 100).await;
        })
        .unwrap();
}

async fn start_test(
    req_counter_ok: Arc<RelaxedCounter>,
    req_counter_rej: Arc<RelaxedCounter>,
    limit: u64,
) {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 1000000, 2).unwrap();
    let mut show_hist = Instant::now();
    let mut count = 0usize;
    // let endpoints = vec!["localhost:8080", "localhost:8090", "localhost:8070"];
    let endpoints = vec!["127.0.0.1:8000"];
    loop {
        let s = Instant::now();
        let stream = TcpStream::connect(endpoints[count.rem_euclid(endpoints.len())])
            .await
            .unwrap();
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
            .uri(format!("/limit/{limit}/foo"))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        if res.status() == StatusCode::OK {
            req_counter_ok.inc();
        } else if res.status() == StatusCode::TOO_MANY_REQUESTS {
            req_counter_rej.inc();
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
