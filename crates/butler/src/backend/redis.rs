//! A Redis-backed queue, using the same layout idea as Sidekiq Pro's
//! `super_fetch`:
//!
//! ```text
//! <prefix>:queue:<queue>         LIST  job ids; LPUSH to enqueue, taken from the right (FIFO)
//! <prefix>:processing:<worker>   LIST  ids that worker claimed
//! <prefix>:worker:<worker>       STRING  heartbeat; expires unless the worker refreshes it
//! <prefix>:workers               SET   worker ids that may hold jobs
//! <prefix>:dead                  LIST  ids that exhausted their retries
//! <prefix>:job:<id>              HASH  { state, queue, data (job JSON) }
//! ```
//!
//! A claim is an `LMOVE queue:<q> processing:<worker>` for each queue the
//! worker serves, in its priority order, which Redis runs atomically, so only
//! one worker gets each job, and the job never exists only in a worker's
//! memory.
//!
//! Waiting is push-based. Every push, retry and recovery also `PUBLISH`es to
//! `<prefix>:wake`; a listener thread per worker process turns those messages
//! into a wake-up, so an idle claim re-checks its queues the moment a job
//! lands on any of them. Pub/sub only carries the signal, never the job: a
//! missed message costs at most the claim's `wait`, after which it re-checks
//! anyway. If a worker dies, its heartbeat key expires and `recover` moves the
//! ids in its processing list back to the front of their own queues, one atomic
//! script call per job.
//!
//! Cancelling is `LREM queue:<q>`, also atomic: either a worker's claim or the
//! cancel gets the id, never both.
//!
//! We use lists instead of `PUBLISH`/`SUBSCRIBE` because pub/sub delivers each
//! message to every subscriber, and messages sent while no worker is connected
//! are lost.

use std::{
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
    thread,
    time::{Duration, Instant},
};

use redis::{Client, Connection, RedisResult};
use serde_json::Value;

use super::{Backend, NewJob};
use crate::{Error, JobId, JobRecord, JobState, Result, Signal, signal::JobWatch};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long finished (done or cancelled) jobs stay queryable before Redis expires them.
const DONE_TTL_SECS: u64 = 24 * 60 * 60;

/// Stores a job's data (KEYS[2]) only while the job's id is still in the
/// worker's processing list (KEYS[1]).
const CHECKPOINT_IF_HELD: &str = r"
if redis.call('LPOS', KEYS[1], ARGV[1]) then
  redis.call('HSET', KEYS[2], 'data', ARGV[2])
end
return 0
";

/// Moves the oldest id out of a processing list (KEYS[1]) to the claim end of
/// its own queue, and marks it pending, as one atomic step: two workers
/// recovering at once can't move the same job, or send one to the wrong queue.
/// Returns the id, or nil once the list is empty. ARGV[1] is the key prefix.
const RECOVER_ONE: &str = r"
local id = redis.call('RPOP', KEYS[1])
if not id then return false end
local job = ARGV[1] .. 'job:' .. id
local queue = redis.call('HGET', job, 'queue') or 'default'
redis.call('RPUSH', ARGV[1] .. 'queue:' .. queue, id)
redis.call('HSET', job, 'state', 'pending')
redis.call('PUBLISH', ARGV[1] .. 'wake', queue)
return id
";

pub struct RedisQueue {
    client: Client,
    /// Idle connections. A blocking claim holds one for up to its wait, so
    /// calls check out their own instead of sharing a single connection.
    idle: Mutex<Vec<Connection>>,
    prefix: String,
    display: String,
    /// Notified from pub/sub messages; see [`Signals`].
    signals: Arc<Signals>,
    /// The pub/sub listener thread, started by the first waiting claim or
    /// result wait.
    subscriber: OnceLock<()>,
}

/// The two things a worker process listens for: a job was pushed (idle claims
/// re-check their queues), and a job finished (`JobHandle::wait` re-checks).
#[derive(Default)]
struct Signals {
    pushed: Signal,
    finished: JobWatch,
}

/// How often the listener checks whether its queue was dropped, and the pause
/// before resubscribing after an error.
const LISTEN_TICK: Duration = Duration::from_secs(1);

/// Listens for wake-ups and bumps the matching signal for every message,
/// until the `RedisQueue` that owns `signals` is dropped. Reconnects after
/// errors; while disconnected, waiters still re-check at their fallback
/// interval.
fn subscribe_until_dropped(client: &Client, prefix: &str, signals: &Weak<Signals>) {
    while signals.strong_count() > 0 {
        if let Err(err) = listen(client, prefix, signals) {
            tracing::warn!(error = %err, "redis pub/sub wake-ups interrupted; reconnecting");
            thread::sleep(LISTEN_TICK);
        }
    }
}

fn listen(client: &Client, prefix: &str, signals: &Weak<Signals>) -> RedisResult<()> {
    let (wake, done) = (format!("{prefix}:wake"), format!("{prefix}:done"));
    let mut conn = client.get_connection_with_timeout(CONNECT_TIMEOUT)?;
    let mut pubsub = conn.as_pubsub();
    pubsub.subscribe(&wake)?;
    pubsub.subscribe(&done)?;
    pubsub.set_read_timeout(Some(LISTEN_TICK))?;
    // Anything that happened while we weren't subscribed sent no message we saw.
    let Some(all) = signals.upgrade() else {
        return Ok(());
    };
    all.pushed.notify();
    all.finished.notify_all();
    drop(all);
    loop {
        match pubsub.get_message() {
            Ok(message) => {
                let Some(signals) = signals.upgrade() else {
                    return Ok(());
                };
                if message.get_channel_name() == done {
                    // The payload is the finished job's id: wake only its waiters.
                    signals
                        .finished
                        .notify(&String::from_utf8_lossy(message.get_payload_bytes()));
                } else {
                    signals.pushed.notify();
                }
            }
            Err(err) if err.is_timeout() => {
                if signals.strong_count() == 0 {
                    return Ok(());
                }
            }
            Err(err) => return Err(err),
        }
    }
}

impl RedisQueue {
    /// Connects right away, so a bad URL or unreachable server fails at startup.
    pub fn connect(url: &str, prefix: &str) -> Result<Self> {
        let client = Client::open(url)?;
        let conn = client.get_connection_with_timeout(CONNECT_TIMEOUT)?;
        Ok(Self {
            client,
            idle: Mutex::new(vec![conn]),
            prefix: prefix.to_string(),
            display: format!("redis:{} (prefix {prefix})", redact(url)),
            signals: Arc::default(),
            subscriber: OnceLock::new(),
        })
    }

    fn key(&self, name: &str) -> String {
        format!("{}:{name}", self.prefix)
    }

    fn job_key(&self, id: &str) -> String {
        format!("{}:job:{id}", self.prefix)
    }

    fn queue_key(&self, queue: &str) -> String {
        format!("{}:queue:{queue}", self.prefix)
    }

    fn processing_key(&self, worker: &str) -> String {
        format!("{}:processing:{worker}", self.prefix)
    }

    fn worker_key(&self, worker: &str) -> String {
        format!("{}:worker:{worker}", self.prefix)
    }

    /// Runs `f` on an idle connection, or a new one. The connection goes back
    /// to the pool afterwards unless it failed at the I/O level. The lock is
    /// only held to take or return a connection, never during a command.
    fn with_conn<T>(&self, f: impl FnOnce(&mut Connection) -> RedisResult<T>) -> Result<T> {
        let idle = self
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let mut conn = match idle {
            Some(conn) => conn,
            None => self.client.get_connection_with_timeout(CONNECT_TIMEOUT)?,
        };
        let result = f(&mut conn);
        let broken = matches!(&result, Err(e)
            if e.is_io_error() || e.is_connection_dropped() || e.is_unrecoverable_error());
        if !broken {
            self.idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(conn);
        }
        Ok(result?)
    }

    /// Takes the oldest job of the first non-empty queue, in order, into
    /// `worker`'s processing list. Never blocks.
    fn sweep(&self, worker: &str, queues: &[&str]) -> Result<Option<JobRecord>> {
        let processing = self.processing_key(worker);
        loop {
            let mut id: Option<String> = None;
            for queue in queues {
                id = self.with_conn(|con| {
                    redis::cmd("LMOVE")
                        .arg(self.queue_key(queue))
                        .arg(&processing)
                        .arg("RIGHT")
                        .arg("LEFT")
                        .query(con)
                })?;
                if id.is_some() {
                    break;
                }
            }
            let Some(id) = id else { return Ok(None) };

            // Not atomic with the move above. If this worker dies in between,
            // the id is already in its processing list, so `recover` requeues
            // it; the stale `pending` state is only visible until then.
            let data: Option<String> = self.with_conn(|con| {
                redis::pipe()
                    .cmd("HSET")
                    .arg(self.job_key(&id))
                    .arg("state")
                    .arg(JobState::Processing.as_str())
                    .ignore()
                    .cmd("HGET")
                    .arg(self.job_key(&id))
                    .arg("data")
                    .query::<(Option<String>,)>(con)
                    .map(|(data,)| data)
            })?;
            match data {
                Some(data) => return Ok(Some(serde_json::from_str(&data)?)),
                // An id without job data can't run: drop it and try the next one.
                None => self.with_conn(|con| {
                    redis::pipe()
                        .cmd("LREM")
                        .arg(&processing)
                        .arg(1)
                        .arg(&id)
                        .ignore()
                        .cmd("DEL")
                        .arg(self.job_key(&id))
                        .ignore()
                        .exec(con)
                })?,
            }
        }
    }

    /// Starts, once per queue, the thread that turns pub/sub messages into
    /// [`Signals`]. Only workers and result waiters need it, so enqueue-only
    /// processes never open the extra connection.
    fn listen_for_signals(&self) {
        self.subscriber.get_or_init(|| {
            let client = self.client.clone();
            let prefix = self.prefix.clone();
            let signals = Arc::downgrade(&self.signals);
            let spawned = thread::Builder::new()
                .name("butler-redis-wake".into())
                .spawn(move || subscribe_until_dropped(&client, &prefix, &signals));
            if let Err(err) = spawned {
                tracing::error!(error = %err, "no pub/sub wake-ups; waiters fall back to polling");
            }
        });
    }
}

impl Backend for RedisQueue {
    fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId> {
        let job = JobRecord::new(name, queue, args);
        let data = serde_json::to_string(&job)?;
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("HSET")
                .arg(self.job_key(&job.id))
                .arg("state")
                .arg(JobState::Pending.as_str())
                .arg("queue")
                .arg(queue)
                .arg("data")
                .arg(&data)
                .ignore()
                .cmd("LPUSH")
                .arg(self.queue_key(queue))
                .arg(&job.id)
                .ignore()
                .cmd("PUBLISH")
                .arg(self.key("wake"))
                .arg(queue)
                .ignore()
                .exec(con)
        })?;
        Ok(job.id)
    }

    /// One pipelined transaction for every job, and one wake-up per queue.
    fn push_many(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        let mut pipe = redis::pipe();
        pipe.atomic();
        let mut ids = Vec::with_capacity(jobs.len());
        let mut queues: Vec<String> = Vec::new();
        for new in jobs {
            let job = JobRecord::new(&new.name, &new.queue, new.args);
            pipe.cmd("HSET")
                .arg(self.job_key(&job.id))
                .arg("state")
                .arg(JobState::Pending.as_str())
                .arg("queue")
                .arg(&new.queue)
                .arg("data")
                .arg(serde_json::to_string(&job)?)
                .ignore()
                .cmd("LPUSH")
                .arg(self.queue_key(&new.queue))
                .arg(&job.id)
                .ignore();
            if !queues.contains(&new.queue) {
                queues.push(new.queue);
            }
            ids.push(job.id);
        }
        for queue in &queues {
            pipe.cmd("PUBLISH")
                .arg(self.key("wake"))
                .arg(queue)
                .ignore();
        }
        self.with_conn(|con| pipe.exec(con))?;
        Ok(ids)
    }

    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<JobRecord>> {
        if queues.is_empty() {
            return Ok(None);
        }
        if !wait.is_zero() {
            self.listen_for_signals();
        }
        let deadline = Instant::now() + wait;
        loop {
            // Read before sweeping: a push that lands during the sweep changes
            // it, so the wait below returns at once instead of missing the job.
            let seen = self.signals.pushed.generation();
            if let Some(job) = self.sweep(worker, queues)? {
                return Ok(Some(job));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            self.signals.pushed.wait_past(seen, deadline - now);
        }
    }

    fn complete(&self, worker: &str, job: &JobRecord) -> Result<()> {
        let data = serde_json::to_string(job)?;
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("LREM")
                .arg(self.processing_key(worker))
                .arg(1)
                .arg(&job.id)
                .ignore()
                .cmd("HSET")
                .arg(self.job_key(&job.id))
                .arg("state")
                .arg(JobState::Done.as_str())
                .arg("data")
                .arg(&data)
                .ignore()
                .cmd("EXPIRE")
                .arg(self.job_key(&job.id))
                .arg(DONE_TTL_SECS)
                .ignore()
                .cmd("PUBLISH")
                .arg(self.key("done"))
                .arg(&job.id)
                .ignore()
                .exec(con)
        })
    }

    fn fail(&self, worker: &str, job: &JobRecord, next: JobState) -> Result<()> {
        let state = next;
        let data = serde_json::to_string(job)?;
        let target = if state == JobState::Dead {
            self.key("dead")
        } else {
            self.queue_key(&job.queue)
        };
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("LREM")
                .arg(self.processing_key(worker))
                .arg(1)
                .arg(&job.id)
                .ignore()
                .cmd("HSET")
                .arg(self.job_key(&job.id))
                .arg("state")
                .arg(state.as_str())
                .arg("data")
                .arg(&data)
                .ignore()
                .cmd("LPUSH")
                .arg(&target)
                .arg(&job.id)
                .ignore()
                // A retry wakes idle claims; a dead job wakes result waiters.
                .cmd("PUBLISH")
                .arg(self.key(if state == JobState::Dead {
                    "done"
                } else {
                    "wake"
                }))
                .arg(&job.id)
                .ignore()
                .exec(con)
        })
    }

    fn get(&self, id: &str) -> Result<Option<(JobState, JobRecord)>> {
        let (state, data): (Option<String>, Option<String>) = self.with_conn(|con| {
            redis::cmd("HMGET")
                .arg(self.job_key(id))
                .arg("state")
                .arg("data")
                .query(con)
        })?;
        let (Some(state), Some(data)) = (state, data) else {
            return Ok(None);
        };
        let state = JobState::parse(&state).ok_or_else(|| Error::UnknownState {
            id: id.to_string(),
            state,
        })?;
        Ok(Some((state, serde_json::from_str(&data)?)))
    }

    /// Saves only if `worker` still holds the job, checked and written in one
    /// script: after a recovery, a slow former owner can't overwrite the
    /// progress of the job's new run.
    fn checkpoint(&self, worker: &str, job: &JobRecord) -> Result<()> {
        let data = serde_json::to_string(job)?;
        self.with_conn(|con| {
            redis::cmd("EVAL")
                .arg(CHECKPOINT_IF_HELD)
                .arg(2)
                .arg(self.processing_key(worker))
                .arg(self.job_key(&job.id))
                .arg(&job.id)
                .arg(&data)
                .exec(con)
        })
    }

    fn cancel(&self, id: &str) -> Result<bool> {
        let queue: Option<String> = self.with_conn(|con| {
            redis::cmd("HGET")
                .arg(self.job_key(id))
                .arg("queue")
                .query(con)
        })?;
        let Some(queue) = queue else {
            return Ok(false);
        };
        let removed: usize = self.with_conn(|con| {
            redis::cmd("LREM")
                .arg(self.queue_key(&queue))
                .arg(1)
                .arg(id)
                .query(con)
        })?;
        if removed == 0 {
            return Ok(false);
        }
        // The id is out of the pending list, so no worker can claim it now.
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("HSET")
                .arg(self.job_key(id))
                .arg("state")
                .arg(JobState::Cancelled.as_str())
                .ignore()
                .cmd("EXPIRE")
                .arg(self.job_key(id))
                .arg(DONE_TTL_SECS)
                .ignore()
                .cmd("PUBLISH")
                .arg(self.key("done"))
                .arg(id)
                .ignore()
                .exec(con)
        })?;
        Ok(true)
    }

    fn heartbeat(&self, worker: &str, ttl: Duration) -> Result<()> {
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("SET")
                .arg(self.worker_key(worker))
                .arg(1)
                .arg("PX")
                .arg(ttl_ms)
                .ignore()
                .cmd("SADD")
                .arg(self.key("workers"))
                .arg(worker)
                .ignore()
                .exec(con)
        })
    }

    fn retire(&self, worker: &str) -> Result<()> {
        // Leaves the `workers` entry: `recover` drops it once it has checked
        // that nothing is left in the processing list.
        self.with_conn(|con| redis::cmd("DEL").arg(self.worker_key(worker)).exec(con))
    }

    fn recover(&self) -> Result<usize> {
        let workers: Vec<String> =
            self.with_conn(|con| redis::cmd("SMEMBERS").arg(self.key("workers")).query(con))?;
        let mut recovered = 0;
        for worker in workers {
            let alive: bool = self.with_conn(|con| {
                redis::cmd("EXISTS")
                    .arg(self.worker_key(&worker))
                    .query(con)
            })?;
            if alive {
                continue;
            }
            while let Some(_id) = self.with_conn(|con| {
                redis::cmd("EVAL")
                    .arg(RECOVER_ONE)
                    .arg(1)
                    .arg(self.processing_key(&worker))
                    .arg(format!("{}:", self.prefix))
                    .query::<Option<String>>(con)
            })? {
                recovered += 1;
            }
            self.with_conn(|con| {
                redis::cmd("SREM")
                    .arg(self.key("workers"))
                    .arg(&worker)
                    .exec(con)
            })?;
        }
        Ok(recovered)
    }

    fn watch_finished(&self, id: &str) -> Option<Arc<Signal>> {
        self.listen_for_signals();
        Some(self.signals.finished.watch(id))
    }

    fn describe(&self) -> String {
        self.display.clone()
    }
}

/// Hides the credentials in `redis://user:pass@host/`.
fn redact(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(scheme_end), Some(at)) if at > scheme_end => {
            format!("{}***{}", &url[..scheme_end + 3], &url[at..])
        }
        _ => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn redacts_credentials() {
        assert_eq!(
            super::redact("redis://:secret@h:6379/0"),
            "redis://***@h:6379/0"
        );
        assert_eq!(super::redact("redis://127.0.0.1/"), "redis://127.0.0.1/");
    }
}
