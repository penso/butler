#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Results travel back: the enqueuer sends arguments to a worker, and the
//! worker's return value comes back through the same backend. Runs against
//! memory, file, and Redis when reachable. One test, because
//! `butler::configure` is process-global.

use std::time::Duration;

use butler::{Error, FileQueue, JobHandle, MemoryQueue, Queue, Worker};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Sum {
    total: i64,
    slept_ms: u64,
}

/// A job's own error type: plain `thiserror`, nothing butler-specific.
#[derive(Debug, thiserror::Error)]
enum SumError {
    #[error("{0} + {1} overflows")]
    Overflow(i64, i64),
}

/// Async: runs as a task on the worker's tokio runtime, so it can await tokio.
#[butler::job]
async fn add(a: i64, b: i64) -> Result<Sum, SumError> {
    tokio::time::sleep(Duration::from_millis(20)).await;
    Ok(Sum {
        total: a.checked_add(b).ok_or(SumError::Overflow(a, b))?,
        slept_ms: 20,
    })
}

/// Sync and CPU-bound: runs on the blocking pool, not an async worker thread.
/// It can't fail, so its error type is `Infallible`.
#[butler::job]
fn fib(n: u64) -> Result<u64, std::convert::Infallible> {
    let (mut a, mut b) = (0u64, 1u64);
    for _ in 0..n {
        (a, b) = (b, a.wrapping_add(b));
    }
    Ok(a)
}

#[butler::job]
async fn nothing() {}

/// A plain message works as an error too.
#[butler::job]
async fn refuse() -> Result<u32, String> {
    Err("refused on purpose".to_owned())
}

const POLL: Duration = Duration::from_millis(5);

async fn round_trip(queue: Queue) {
    let name = queue.describe();
    butler::configure(queue.clone());

    // Enqueued while no worker runs, so this one can still be cancelled.
    let cancelled = add(0, 0).await.unwrap();
    assert!(cancelled.cancel().await.unwrap());

    let sum: JobHandle<Sum> = add(2, 3).await.unwrap();
    let fibonacci: JobHandle<u64> = fib(50).await.unwrap();
    let unit: JobHandle<()> = nothing().await.unwrap();
    let refused: JobHandle<u32> = refuse().await.unwrap();
    let overflowed: JobHandle<Sum> = add(i64::MAX, 1).await.unwrap();
    assert_eq!(sum.result().await.unwrap(), None, "{name}: not run yet");

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = Worker::new(queue.clone())
        .max_retries(0)
        .poll_interval(Duration::from_millis(20));
    let running = tokio::spawn(worker.run_async(async {
        let _ = stopped.await;
    }));

    let results = async {
        (
            sum.wait_result(POLL).await,
            fibonacci.wait_result(POLL).await,
            unit.wait_result(POLL).await,
            refused.wait_result(POLL).await,
            cancelled.wait_result(POLL).await,
            overflowed.wait_result(POLL).await,
        )
    };
    let (sum_out, fib_out, unit_out, refused_out, cancelled_out, overflow_out) =
        tokio::time::timeout(Duration::from_secs(10), results)
            .await
            .unwrap_or_else(|_| panic!("{name}: results did not arrive"));
    stop.send(()).unwrap();
    running.await.unwrap();

    assert_eq!(
        sum_out.unwrap(),
        Sum {
            total: 5,
            slept_ms: 20
        },
        "{name}"
    );
    assert_eq!(fib_out.unwrap(), 12_586_269_025, "{name}");
    unit_out.unwrap();
    assert!(
        matches!(&refused_out, Err(Error::JobFailed { error, .. }) if error == "refused on purpose"),
        "{name}: {refused_out:?}"
    );
    assert!(
        matches!(cancelled_out, Err(Error::JobCancelled { .. })),
        "{name}"
    );
    // The job's own thiserror message comes back as the failure.
    assert!(
        matches!(&overflow_out, Err(Error::JobFailed { error, .. }) if error == "9223372036854775807 + 1 overflows"),
        "{name}: {overflow_out:?}"
    );

    // Anyone holding just the id can read the result too.
    let by_id = queue.handle(sum.id()).with_output::<Sum>();
    assert_eq!(by_id.result().await.unwrap().unwrap().total, 5, "{name}");
}

#[tokio::test(flavor = "multi_thread")]
async fn results_come_back_from_every_backend() {
    round_trip(MemoryQueue::new().into()).await;

    let dir = std::env::temp_dir().join(format!("butler-results-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    round_trip(FileQueue::new(&dir).unwrap().into()).await;

    #[cfg(feature = "redis")]
    {
        let url = std::env::var("BUTLER_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
        let prefix = format!("butler-results-{}", std::process::id());
        match butler::RedisQueue::connect(&url, &prefix) {
            Ok(q) => round_trip(q.into()).await,
            Err(e) => eprintln!("skipping redis: {e}"),
        }
    }
}
