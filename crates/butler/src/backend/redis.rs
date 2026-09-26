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
//! memory. When every queue is empty, the claim blocks with `BLMOVE` on the
//! first one, so jobs on it start at once and jobs on the others within the
//! wait. If a worker dies, its heartbeat key expires and `recover` moves the
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
    sync::{Mutex, PoisonError},
    time::Duration,
};

use redis::{Client, Connection, RedisResult};
use serde_json::Value;

use super::{Backend, record_failure};
use crate::{Error, Job, JobId, JobState, Result};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long finished (done or cancelled) jobs stay queryable before Redis expires them.
const DONE_TTL_SECS: u64 = 24 * 60 * 60;

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
return id
";

/// `BLMOVE` treats 0 as "block forever", so a claim always waits at least this.
const MIN_BLOCK: Duration = Duration::from_millis(10);

pub struct RedisQueue {
    client: Client,
    /// Idle connections. A blocking claim holds one for up to its wait, so
    /// calls check out their own instead of sharing a single connection.
    idle: Mutex<Vec<Connection>>,
    prefix: String,
    display: String,
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
}

impl Backend for RedisQueue {
    fn push(&self, name: &str, queue: &str, args: Vec<Value>) -> Result<JobId> {
        let job = Job::new(name, queue, args);
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
                .exec(con)
        })?;
        Ok(job.id)
    }

    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<Job>> {
        let processing = self.processing_key(worker);
        let Some(&first) = queues.first() else {
            return Ok(None);
        };
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
            if id.is_none() && !wait.is_zero() {
                // Everything is empty: wait on the highest-priority queue.
                id = self.with_conn(|con| {
                    redis::cmd("BLMOVE")
                        .arg(self.queue_key(first))
                        .arg(&processing)
                        .arg("RIGHT")
                        .arg("LEFT")
                        .arg(wait.max(MIN_BLOCK).as_secs_f64())
                        .query(con)
                })?;
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

    fn complete(&self, worker: &str, job: &Job) -> Result<()> {
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
                .exec(con)
        })
    }

    fn fail(
        &self,
        worker: &str,
        mut job: Job,
        error: String,
        max_retries: u32,
    ) -> Result<JobState> {
        let state = record_failure(&mut job, error, max_retries);
        let data = serde_json::to_string(&job)?;
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
                .exec(con)
        })?;
        Ok(state)
    }

    fn get(&self, id: &str) -> Result<Option<(JobState, Job)>> {
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
