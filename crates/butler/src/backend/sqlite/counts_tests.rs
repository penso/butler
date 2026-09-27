//! `butler_job_counts` against a count of the jobs themselves, after every
//! kind of write, and the migration that adds it to an existing database.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::BTreeMap, path::PathBuf};

use super::*;

/// What `stats()` counted before `butler_job_counts`: every job, grouped.
const SCAN: &str = "SELECT queue, state, COUNT(*) FROM butler_jobs GROUP BY queue, state";

fn temp_db(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "butler-sqlite-counts-{name}-{}.db",
        std::process::id()
    ));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    path
}

fn rows(conn: &Connection, sql: &str) -> BTreeMap<(String, String), i64> {
    conn.prepare(sql)
        .unwrap()
        .query_map([], |row| Ok(((row.get(0)?, row.get(1)?), row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// The kept counts match a fresh count of the rows, through another
/// connection, and `stats()` reports what the old scan did.
#[track_caller]
fn assert_exact(queue: &SqliteQueue, step: &str) {
    let conn = Connection::open(queue.path()).unwrap();
    assert_eq!(
        rows(&conn, COUNTS),
        rows(&conn, SCAN),
        "counts after {step}"
    );
    let negative: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM butler_job_counts WHERE n < 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(negative, 0, "no negative count after {step}");
    // Heartbeats' remaining time moves between the two reads.
    let mut kept = queue.stats().unwrap();
    let mut scanned = stats_with(&conn, SCAN).unwrap();
    kept.workers.clear();
    scanned.workers.clear();
    assert_eq!(kept, scanned, "stats after {step}");
}

fn job(name: &str, queue: &str) -> NewJob {
    NewJob::new(name, queue, vec![])
}

fn claim(queue: &SqliteQueue, worker: &str, queues: &[&str]) -> JobRecord {
    queue
        .claim(worker, queues, Duration::ZERO)
        .unwrap()
        .unwrap()
}

#[test]
fn counts_stay_exact_through_every_write() {
    let path = temp_db("exact");
    let queue = SqliteQueue::open(&path).unwrap();
    assert_exact(&queue, "open");

    queue.push(job("a", "default")).unwrap();
    queue.push(job("b", "mailers")).unwrap();
    assert_exact(&queue, "push");

    queue
        .push_many((0..5).map(|_| job("c", "bulk")).collect())
        .unwrap();
    assert_exact(&queue, "push_many");

    let soon = SystemTime::now() + Duration::from_secs(60);
    let later = SystemTime::now() + Duration::from_secs(3600);
    queue.push(job("s", "default").run_at(soon)).unwrap();
    let runs_now = queue.push(job("s", "mailers").run_at(later)).unwrap();
    queue.push(job("s", "bulk").run_at(later)).unwrap();
    assert_exact(&queue, "schedule");
    assert_eq!(queue.promote(soon).unwrap().moved, 1);
    assert_exact(&queue, "promote");
    assert!(queue.run_now(&runs_now).unwrap());
    assert_exact(&queue, "run_now");

    let done = claim(&queue, "w", &["default"]);
    assert_exact(&queue, "claim");
    queue.complete("w", &done).unwrap();
    assert_exact(&queue, "complete");

    let retried = claim(&queue, "w", &["mailers"]);
    queue.fail("w", &retried, JobState::Pending).unwrap();
    assert_exact(&queue, "fail to a retry");
    let mut later_retry = claim(&queue, "w", &["mailers"]);
    later_retry.run_at_ms = Some(millis(later));
    queue.fail("w", &later_retry, JobState::Scheduled).unwrap();
    assert_exact(&queue, "fail to a scheduled retry");
    let dead = claim(&queue, "w", &["default"]);
    queue.fail("w", &dead, JobState::Dead).unwrap();
    assert_exact(&queue, "fail to dead");

    assert!(queue.retry(&dead.id).unwrap());
    assert_exact(&queue, "retry of a dead job");
    let dead = claim(&queue, "w", &["default"]);
    queue.fail("w", &dead, JobState::Dead).unwrap();
    assert!(queue.discard(&dead.id).unwrap());
    assert_exact(&queue, "discard");

    let cancelled = queue.push(job("x", "default")).unwrap();
    assert!(queue.cancel(&cancelled).unwrap());
    assert!(!queue.cancel(&cancelled).unwrap());
    assert_exact(&queue, "cancel");

    // A checkpoint writes the job, not its state; an interruption requeues it.
    let mut continued = claim(&queue, "w", &["bulk"]);
    continued.progress = Some(serde_json::json!({"step": 1}));
    queue.checkpoint("w", &continued).unwrap();
    assert_exact(&queue, "checkpoint");
    queue.fail("w", &continued, JobState::Pending).unwrap();
    assert_exact(&queue, "interrupt");

    // A worker that stopped beating: recovery requeues what it held.
    queue.heartbeat("gone", Duration::ZERO).unwrap();
    claim(&queue, "gone", &["bulk"]);
    claim(&queue, "gone", &["bulk"]);
    queue.heartbeat("alive", Duration::from_secs(60)).unwrap();
    claim(&queue, "alive", &["bulk"]);
    assert_eq!(queue.recover().unwrap(), 2);
    assert_exact(&queue, "recover");

    assert!(queue.pause_queue("bulk").unwrap());
    assert!(queue.resume_queue("bulk").unwrap());
    assert_exact(&queue, "pause and resume");

    // A unique job's second push stores nothing.
    let unique = || {
        let mut job = job("u", "keys");
        job.unique = Some(crate::UniqueKey {
            key: "u:[]".into(),
            until: crate::Unique::UntilFinished,
        });
        job
    };
    let first = queue.push(unique()).unwrap();
    assert_eq!(queue.push(unique()).unwrap(), first);
    assert_eq!(
        queue.push_many(vec![unique(), job("v", "keys")]).unwrap()[0],
        first
    );
    assert_exact(&queue, "unique pushes");

    // A full concurrency key: the claim skips its job and takes the next.
    let limited = || {
        let mut job = job("c", "limited");
        job.concurrency = Some(crate::ConcurrencyKey {
            key: "c:[1]".into(),
            limit: 1,
        });
        job
    };
    queue.push(limited()).unwrap();
    queue.push(limited()).unwrap();
    queue.push(job("free", "limited")).unwrap();
    let running = claim(&queue, "w", &["limited"]);
    assert_eq!(claim(&queue, "w", &["limited"]).name, "free");
    assert!(
        queue
            .claim("w", &["limited"], Duration::ZERO)
            .unwrap()
            .is_none()
    );
    assert_exact(&queue, "concurrency-limited claims");
    queue.complete("w", &running).unwrap();
    let limits = [GlobalLimit {
        queue: "limited",
        max: 1,
    }];
    queue
        .claim_within_limits("w", &["limited"], &limits, Duration::ZERO)
        .unwrap()
        .unwrap();
    assert_exact(&queue, "claim under a global limit");

    // A recurring tick, and the same tick again from another worker.
    let schedule = crate::Recurring::from_parts(
        "report".into(),
        "reports".into(),
        vec![],
        crate::Cron::parse("0 * * * *").unwrap(),
    )
    .unwrap()
    .record(SystemTime::now());
    queue.register_recurring(&[schedule]).unwrap();
    let key = queue.recurring().unwrap()[0].key.clone();
    let tick = SystemTime::now();
    assert!(
        queue
            .push_recurring(&key, tick, job("report", "reports"))
            .unwrap()
            .is_some()
    );
    assert!(
        queue
            .push_recurring(&key, tick, job("report", "reports"))
            .unwrap()
            .is_none()
    );
    assert_exact(&queue, "recurring ticks");

    // Another connection, standing in for another process.
    let other = SqliteQueue::open(&path).unwrap();
    other.push(job("o", "default")).unwrap();
    let theirs = claim(&other, "w2", &["default"]);
    other.complete("w2", &theirs).unwrap();
    assert_exact(&queue, "writes from another connection");

    // Writes the backend doesn't make: deleting finished jobs, as a
    // retention cleanup would, and moving a job to another queue.
    let conn = Connection::open(&path).unwrap();
    let deleted = conn
        .execute(
            "DELETE FROM butler_jobs WHERE state IN ('done', 'dead', 'cancelled')",
            [],
        )
        .unwrap();
    assert!(deleted > 0);
    assert_exact(&queue, "a raw delete of finished jobs");
    conn.execute(
        "UPDATE butler_jobs SET queue = 'moved' WHERE state = 'pending' AND queue = 'bulk'",
        [],
    )
    .unwrap();
    assert_exact(&queue, "a raw queue change");
    conn.execute("DELETE FROM butler_jobs", []).unwrap();
    assert_exact(&queue, "deleting every job");
    assert!(queue.stats().unwrap().queues.is_empty());
    drop(conn);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn an_older_database_gains_counts_that_match_its_jobs() {
    let path = temp_db("migrate");
    {
        // The schema as it was before `butler_job_counts`, with jobs in
        // every state across a few queues, and one in a state this version
        // doesn't know.
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute_batch(SCHEDULED_INDEX).unwrap();
        let states = [
            "pending",
            "scheduled",
            "processing",
            "done",
            "dead",
            "cancelled",
        ];
        let mut insert = conn
            .prepare(
                "INSERT INTO butler_jobs (id, queue, state, worker, seq, data)
                 VALUES (?1, ?2, ?3, 'old-worker', ?4, ?5)",
            )
            .unwrap();
        let mut seq = 0;
        for (q, queue) in ["default", "mailers", "reports"].iter().enumerate() {
            for (s, state) in states.iter().enumerate() {
                // A different number of each, so a mix-up shows.
                for _ in 0..=(q * 7 + s) {
                    seq += 1;
                    let id = format!("{seq}-1-0");
                    let data = format!(
                        r#"{{"id":"{id}","name":"old","queue":"{queue}","args":[],
                        "attempts":0,"enqueued_at_ms":1,"last_error":null}}"#
                    );
                    insert
                        .execute(params![id, queue, state, seq, data])
                        .unwrap();
                }
            }
        }
        insert
            .execute(params!["0-1-0", "odd", "paused", 0, "{}"])
            .unwrap();
        let tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'butler_job_counts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0);
    }

    let queue = SqliteQueue::open(&path).unwrap();
    // A second open finds the counts there and fills nothing twice.
    drop(SqliteQueue::open(&path).unwrap());
    assert_exact(&queue, "the migration");
    let stats = queue.stats().unwrap();
    assert_eq!(
        stats
            .queues
            .iter()
            .map(|q| q.name.as_str())
            .collect::<Vec<_>>(),
        ["default", "mailers", "odd", "reports"]
    );
    assert_eq!(stats.processing, 3 + 10 + 17);

    // The triggers came with it.
    queue.push(job("new", "default")).unwrap();
    let claimed = claim(&queue, "w", &["default"]);
    queue.complete("w", &claimed).unwrap();
    assert_exact(&queue, "writes after the migration");
    let _ = std::fs::remove_file(&path);
}

/// Times `stats()` against the scan it replaces, on large databases, and
/// the triggers' cost on the write path. Run in release:
///
/// ```sh
/// cargo test --locked --release -p butler-jobs --lib -- --ignored --nocapture sqlite_counts_bench
/// ```
#[test]
#[ignore = "a benchmark: minutes, and gigabytes of disk"]
fn sqlite_counts_bench() {
    for jobs in [1_000_000, 5_000_000] {
        bench_stats(jobs);
    }
    // Alternating, so load on the machine weighs on both alike.
    let mut on = Vec::new();
    let mut off = Vec::new();
    for _ in 0..5 {
        on.push(bench_writes(true));
        off.push(bench_writes(false));
    }
    for (label, runs) in [("on ", on), ("off", off)] {
        let rate = |metric: fn(&[f64; 3]) -> f64| {
            let mut rates: Vec<f64> = runs.iter().map(metric).collect();
            rates.sort_by(f64::total_cmp);
            rates[rates.len() / 2]
        };
        println!(
            "triggers {label}: push {:.0}/s, push_many {:.0}/s, claim+complete {:.0}/s (median of 5)",
            rate(|r| r[0]),
            rate(|r| r[1]),
            rate(|r| r[2]),
        );
    }
}

fn median(mut times: Vec<Duration>) -> Duration {
    times.sort();
    times[times.len() / 2]
}

fn bench_stats(jobs: i64) {
    let path = temp_db(&format!("bench-{jobs}"));
    let queue = SqliteQueue::open(&path).unwrap();
    let started = Instant::now();
    queue
        .with_conn(|conn| {
            // Mostly finished jobs, as a busy queue keeps them, over four
            // queues, with records about the size of a small job's; a
            // worker's handful running.
            conn.execute(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?1)
                 INSERT INTO butler_jobs (id, queue, state, seq, data, finished_seq)
                 SELECT i || '-1-0',
                        'queue' || (i % 4),
                        CASE WHEN i % 100 < 90 THEN 'done'
                             WHEN i % 100 < 94 THEN 'pending'
                             WHEN i % 100 < 96 THEN 'scheduled'
                             WHEN i % 100 < 98 THEN 'dead'
                             WHEN i % 100000 = 99 THEN 'processing'
                             ELSE 'cancelled' END,
                        i,
                        '{\"id\":\"' || i || '-1-0\",\"name\":\"send_email\",\"queue\":\"queue' || (i % 4)
                          || '\",\"args\":[42,\"someone@example.com\"],\"attempts\":0,'
                          || '\"enqueued_at_ms\":1700000000000,\"last_error\":null,\"result\":null}',
                        i
                 FROM n",
                params![jobs],
            )
        })
        .unwrap();
    let fill = started.elapsed();
    let conn = Connection::open(&path).unwrap();
    let time = |f: &dyn Fn()| {
        f(); // warm the page cache
        median(
            (0..7)
                .map(|_| {
                    let started = Instant::now();
                    f();
                    started.elapsed()
                })
                .collect(),
        )
    };
    let kept = time(&|| drop(queue.stats().unwrap()));
    let scanned = time(&|| drop(stats_with(&conn, SCAN).unwrap()));
    assert_eq!(rows(&conn, COUNTS), rows(&conn, SCAN));
    let size = std::fs::metadata(&path).unwrap().len() >> 20;
    drop(queue);

    // The same database as the previous version left it: the first open
    // counts every job.
    conn.execute_batch(
        "DROP TABLE butler_job_counts;
         DROP TRIGGER butler_jobs_count_insert;
         DROP TRIGGER butler_jobs_count_update;
         DROP TRIGGER butler_jobs_count_delete;",
    )
    .unwrap();
    let started = Instant::now();
    drop(SqliteQueue::open(&path).unwrap());
    let migrated = started.elapsed();
    assert_eq!(rows(&conn, COUNTS), rows(&conn, SCAN));
    println!(
        "{jobs} jobs ({size} MiB, filled in {fill:.1?} with triggers): \
         stats() {kept:?} from counts, {scanned:?} scanning (median of 7); \
         migration {migrated:.1?}"
    );
    drop(conn);
    let _ = std::fs::remove_file(&path);
}

/// Jobs per second: `[push, push_many, claim and complete]`.
fn bench_writes(triggers: bool) -> [f64; 3] {
    let path = temp_db(&format!("bench-writes-{triggers}"));
    let queue = SqliteQueue::open(&path).unwrap();
    if !triggers {
        queue
            .with_conn(|conn| {
                conn.execute_batch(
                    "DROP TRIGGER butler_jobs_count_insert;
                     DROP TRIGGER butler_jobs_count_update;
                     DROP TRIGGER butler_jobs_count_delete;",
                )
            })
            .unwrap();
    }
    let rate = |n: u32, took: Duration| f64::from(n) / took.as_secs_f64();

    let n = 20_000;
    let started = Instant::now();
    for _ in 0..n {
        queue.push(job("send_email", "default")).unwrap();
    }
    let push = rate(n, started.elapsed());

    let batches = 50;
    let started = Instant::now();
    for _ in 0..batches {
        queue
            .push_many((0..1_000).map(|_| job("send_email", "bulk")).collect())
            .unwrap();
    }
    let push_many = rate(batches * 1_000, started.elapsed());

    let started = Instant::now();
    for _ in 0..n {
        let job = claim(&queue, "w", &["default"]);
        queue.complete("w", &job).unwrap();
    }
    let claim_complete = rate(n, started.elapsed());
    drop(queue);
    let _ = std::fs::remove_file(&path);
    [push, push_many, claim_complete]
}
