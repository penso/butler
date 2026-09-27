#![cfg(feature = "redis")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! How Redis keeps finished jobs: done and cancelled ones expire on their own
//! after `keep_finished`, and dead ones from before retention are counted
//! from when cleaning up first sees them. Needs a Redis server:
//! `$BUTLER_TEST_REDIS_URL`, or `redis://127.0.0.1:6379/`. Skipped when none
//! is reachable.

use std::time::{Duration, SystemTime};

use butler::{JobState, Keep, Queue, RedisQueue, Retention};
use serde_json::json;

const HOUR: Duration = Duration::from_secs(3600);

fn url() -> String {
    std::env::var("BUTLER_TEST_REDIS_URL").unwrap_or("redis://127.0.0.1:6379/".into())
}

/// A queue under its own prefix, and a raw connection to look at its keys.
fn connect(test: &str, retention: Retention) -> Option<(Queue, String, redis::Connection)> {
    let prefix = format!("butler-retention-{test}-{}", std::process::id());
    match RedisQueue::connect(&url(), &prefix) {
        Ok(queue) => {
            let conn = redis::Client::open(url())
                .unwrap()
                .get_connection()
                .unwrap();
            Some((queue.retention(retention).into(), prefix, conn))
        }
        Err(e) => {
            eprintln!("skipping: no redis at {}: {e}", url());
            None
        }
    }
}

fn ttl(conn: &mut redis::Connection, prefix: &str, id: &str) -> i64 {
    redis::cmd("TTL")
        .arg(format!("{prefix}:job:{id}"))
        .query(conn)
        .unwrap()
}

#[test]
fn done_and_cancelled_jobs_expire_after_keep_finished() {
    let retention = Retention {
        finished: Keep::For(2 * HOUR),
        dead: Keep::Forever,
    };
    let Some((queue, prefix, mut conn)) = connect("ttl", retention) else {
        return;
    };
    queue.heartbeat("w", HOUR).unwrap();
    let done = queue.push("a", "default", vec![]).unwrap();
    let job = queue
        .claim("w", &["default"], Duration::ZERO)
        .unwrap()
        .unwrap();
    queue.complete("w", job, json!(1)).unwrap();
    let cancelled = queue.push("b", "default", vec![]).unwrap();
    assert!(queue.cancel(&cancelled).unwrap());
    let dead = queue.push("c", "default", vec![]).unwrap();
    let job = queue
        .claim("w", &["default"], Duration::ZERO)
        .unwrap()
        .unwrap();
    queue.fail("w", job, "boom".into(), 0).unwrap();

    for id in [&done, &cancelled] {
        let left = ttl(&mut conn, &prefix, id);
        assert!((2 * 3600 - 60..=2 * 3600).contains(&left), "{id}: {left}");
    }
    // Dead jobs are kept until cleaned up: forever, here.
    assert_eq!(ttl(&mut conn, &prefix, &dead), -1);
    let far = SystemTime::now() + 1000 * 24 * HOUR;
    assert_eq!(queue.clean_finished(far, 100).unwrap(), 0);
    assert_eq!(queue.state(&dead), Some(JobState::Dead));
}

#[test]
fn keeping_finished_jobs_forever_sets_no_ttl() {
    let forever = Retention {
        finished: Keep::Forever,
        dead: Keep::Forever,
    };
    let Some((queue, prefix, mut conn)) = connect("forever", forever) else {
        return;
    };
    queue.heartbeat("w", HOUR).unwrap();
    let done = queue.push("a", "default", vec![]).unwrap();
    let job = queue
        .claim("w", &["default"], Duration::ZERO)
        .unwrap()
        .unwrap();
    queue.complete("w", job, json!(1)).unwrap();
    let cancelled = queue.push("b", "default", vec![]).unwrap();
    assert!(queue.cancel(&cancelled).unwrap());
    assert_eq!(ttl(&mut conn, &prefix, &done), -1);
    assert_eq!(ttl(&mut conn, &prefix, &cancelled), -1);
}

#[test]
fn dead_jobs_from_before_retention_count_from_the_first_cleanup() {
    let retention = Retention {
        finished: Keep::For(HOUR),
        dead: Keep::For(HOUR),
    };
    let Some((queue, prefix, mut conn)) = connect("legacy", retention) else {
        return;
    };
    queue.heartbeat("w", HOUR).unwrap();
    let old = queue.push("a", "default", vec![]).unwrap();
    let job = queue
        .claim("w", &["default"], Duration::ZERO)
        .unwrap()
        .unwrap();
    queue.fail("w", job, "boom".into(), 0).unwrap();
    // As an older version left it: no finish time.
    let _: () = redis::cmd("HDEL")
        .arg(format!("{prefix}:job:{old}"))
        .arg("finished_at")
        .query(&mut conn)
        .unwrap();
    let new = queue.push("b", "default", vec![]).unwrap();
    let job = queue
        .claim("w", &["default"], Duration::ZERO)
        .unwrap()
        .unwrap();
    queue.fail("w", job, "boom".into(), 0).unwrap();

    // The first look only records when it saw the old job.
    let later = SystemTime::now() + 2 * HOUR;
    assert_eq!(queue.clean_finished(later, 100).unwrap(), 0);
    assert_eq!(queue.state(&old), Some(JobState::Dead));
    let seen: Option<u64> = redis::cmd("HGET")
        .arg(format!("{prefix}:job:{old}"))
        .arg("finished_at")
        .query(&mut conn)
        .unwrap();
    assert!(seen.is_some());
    // Then both go once they are old enough, oldest first.
    assert_eq!(queue.clean_finished(later, 1).unwrap(), 1);
    assert_eq!(queue.state(&old), None);
    assert_eq!(queue.state(&new), Some(JobState::Dead));
    assert_eq!(queue.clean_finished(later, 1).unwrap(), 1);
    assert_eq!(queue.state(&new), None);
    let dead: i64 = redis::cmd("LLEN")
        .arg(format!("{prefix}:dead"))
        .query(&mut conn)
        .unwrap();
    assert_eq!(dead, 0);
}
