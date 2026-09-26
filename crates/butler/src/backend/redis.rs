//! A Redis-backed queue, using the same list layout idea as Sidekiq:
//!
//! ```text
//! <prefix>:pending      LIST  job ids; LPUSH to enqueue, taken from the right (FIFO)
//! <prefix>:processing   LIST  ids claimed by a worker
//! <prefix>:dead         LIST  ids that exhausted their retries
//! <prefix>:job:<id>     HASH  { state, data (job JSON) }
//! ```
//!
//! A claim is one `LMOVE pending processing`, which Redis runs atomically, so
//! only one worker gets each job. We use lists instead of `PUBLISH`/`SUBSCRIBE`
//! because pub/sub delivers each message to every subscriber, and messages
//! sent while no worker is connected are lost.

use std::{sync::Mutex, time::Duration};

use redis::{Client, Connection, RedisResult};
use serde_json::Value;

use super::{Backend, record_failure};
use crate::{Error, Job, JobId, JobState, Result};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long finished jobs stay queryable before Redis expires them.
const DONE_TTL_SECS: u64 = 24 * 60 * 60;

pub struct RedisQueue {
    client: Client,
    /// One shared connection, reopened after I/O errors. Every Redis call goes
    /// through this lock, which is fine for an MVP; use a pool to scale.
    conn: Mutex<Option<Connection>>,
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
            conn: Mutex::new(Some(conn)),
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

    fn with_conn<T>(&self, f: impl FnOnce(&mut Connection) -> RedisResult<T>) -> Result<T> {
        let mut guard = match self.conn.lock() {
            Ok(guard) => guard,
            // A panic inside `f` may have left the connection mid-reply, so
            // it can't be trusted: drop it and reconnect below.
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = None;
                self.conn.clear_poison();
                guard
            }
        };
        let conn = match guard.take() {
            Some(conn) => conn,
            None => self.client.get_connection_with_timeout(CONNECT_TIMEOUT)?,
        };
        let conn = guard.insert(conn);
        let result = f(conn);
        if let Err(e) = &result
            && (e.is_io_error() || e.is_connection_dropped() || e.is_unrecoverable_error())
        {
            *guard = None;
        }
        Ok(result?)
    }
}

impl Backend for RedisQueue {
    fn push(&self, name: &str, args: Vec<Value>) -> Result<JobId> {
        let job = Job::new(name, args);
        let data = serde_json::to_string(&job)?;
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("HSET")
                .arg(self.job_key(&job.id))
                .arg("state")
                .arg(JobState::Pending.as_str())
                .arg("data")
                .arg(&data)
                .ignore()
                .cmd("LPUSH")
                .arg(self.key("pending"))
                .arg(&job.id)
                .ignore()
                .exec(con)
        })?;
        Ok(job.id)
    }

    fn claim(&self) -> Result<Option<Job>> {
        loop {
            let id: Option<String> = self.with_conn(|con| {
                redis::cmd("LMOVE")
                    .arg(self.key("pending"))
                    .arg(self.key("processing"))
                    .arg("RIGHT")
                    .arg("LEFT")
                    .query(con)
            })?;
            let Some(id) = id else { return Ok(None) };

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
                        .arg(self.key("processing"))
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

    fn complete(&self, job: &Job) -> Result<()> {
        let data = serde_json::to_string(job)?;
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("LREM")
                .arg(self.key("processing"))
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

    fn fail(&self, mut job: Job, error: String, max_retries: u32) -> Result<JobState> {
        let state = record_failure(&mut job, error, max_retries);
        let data = serde_json::to_string(&job)?;
        let target = if state == JobState::Dead {
            "dead"
        } else {
            "pending"
        };
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("LREM")
                .arg(self.key("processing"))
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
                .arg(self.key(target))
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
