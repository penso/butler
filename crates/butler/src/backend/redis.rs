//! A Redis-backed queue, using the same layout idea as Sidekiq Pro's
//! `super_fetch`:
//!
//! ```text
//! <prefix>:queue:<queue>         LIST  job ids; LPUSH to enqueue, taken from the right (FIFO)
//! <prefix>:scheduled             ZSET  ids waiting for their run time (score: ms since the epoch)
//! <prefix>:processing:<worker>   LIST  ids that worker claimed
//! <prefix>:worker:<worker>       STRING  heartbeat; expires unless the worker refreshes it
//! <prefix>:workers               SET   worker ids that may hold jobs
//! <prefix>:dead                  LIST  ids that exhausted their retries
//! <prefix>:job:<id>              HASH  { state, queue, data (job JSON), worker, ckey, climit, ukey, umode }
//! <prefix>:running:<key>         SET   ids running with that concurrency key (by its hash)
//! <prefix>:blocked:<key>         LIST  ids skipped because their concurrency key was full
//! <prefix>:blocked               HASH  per queue, how many of its ids wait in blocked lists
//! <prefix>:blocked_keys          SET   concurrency keys that ever had skipped jobs
//! <prefix>:unique:<key>          STRING  the id of the job holding that unique key (by its hash)
//! <prefix>:paused                SET   queues workers don't claim from
//! <prefix>:slots:<queue>         SET   ids running in one of the queue's global-limit slots
//! <prefix>:recurring             SET   keys of the recurring schedules workers registered
//! <prefix>:recurring:<key>       HASH  { data (schedule JSON), created_at, seen_at, last_tick, last_job }
//! <prefix>:recurring:tick:<key>:<ms>  STRING  the job enqueued for that tick; expires after a day
//! ```
//!
//! A claim is one script per queue the worker serves, in its priority order:
//! it pops the job's id from `queue:<q>`, pushes it on `processing:<worker>`
//! and marks it processing, all at once, so only one worker gets each job,
//! and the job never exists only in a worker's memory. It also writes the
//! worker's id into the job hash's `worker` field, which completing, failing
//! and recovering the job remove: a checkpoint saves progress only while that
//! field names the saving worker, one hash read however many jobs the worker
//! holds. Jobs claimed by an older version have no `worker` field; their
//! checkpoints search the processing list with `LPOS`, as that version did.
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
//! Cancelling is one script too (`ZREM scheduled`, then `LREM queue:<q>`,
//! then the job's blocked list): either a worker's claim or the cancel gets
//! the id, never both.
//!
//! A scheduled job's id waits in the `scheduled` sorted set. Promotion is one
//! script that moves every due id onto the back of its own queue, so a job
//! moves exactly once however many workers promote. Cancelling a scheduled
//! job is part of the cancel script, so it can't slip between the set and
//! the queue.
//!
//! A claim is one script per queue: it pops the oldest id, and if the job's
//! concurrency key already has `climit` ids in `running:<key>`, parks it on
//! `blocked:<key>` and pops the next one, so a full key never holds back the
//! jobs behind it. A claimed keyed job joins `running:<key>`. Completing,
//! failing or recovering it leaves that set and moves the oldest parked job
//! of the key back to the claim end of its queue, in the same script. Parked
//! jobs stay `pending`: they are counted, listed and cancellable.
//!
//! A unique job's push is a script that stores it only if `unique:<key>` is
//! free, or names a job that no longer holds it (done, dead, cancelled, or
//! started for `until_started`), and returns the holder otherwise. The lock
//! is cleared at claim (`until_started`), or when the job is done, dead or
//! cancelled.
//!
//! Under a global queue limit, the claim script first checks
//! `SCARD slots:<q>`: at the limit it claims nothing, otherwise it adds the
//! claimed id to the set. Completing or failing a job `SREM`s it, and so does
//! recovery, which frees a crashed worker's slots.
//!
//! A recurring tick is one script: `SET recurring:tick:<key>:<ms> NX`, and
//! only if that succeeded, the job's push. However many workers run it for a
//! tick, one job is enqueued.
//!
//! We use lists instead of `PUBLISH`/`SUBSCRIBE` because pub/sub delivers each
//! message to every subscriber, and messages sent while no worker is connected
//! are lost.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
    thread,
    time::{Duration, Instant, SystemTime},
};

use redis::{Client, Connection, RedisResult};

use super::{GlobalLimit, Monitor, NewJob, Promoted, Store, TICK_RETENTION, Watch};
use crate::{
    Error, JobId, JobRecord, JobState, RecurringRecord, RedisConfig, Result, Signal,
    job::{from_millis, millis},
    monitor::{
        JobMetric, ListFilter, METRICS_RETENTION_MINUTES, MetricBucket, QueueStats, Stats,
        WorkerStats, current_minute,
    },
    signal::JobWatch,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How many recent done and cancelled job ids are kept for listing.
const RECENT_CAP: isize = 1_000;

/// Records one finished attempt: per-minute bucket fields (KEYS[1], a hash
/// that expires), and the lifetime counters (KEYS[2], KEYS[3]). ARGV: field
/// prefix, failed (0/1), duration in ms, expiry in seconds.
const RECORD_METRIC: &str = r"
local key, base, duration = KEYS[1], ARGV[1], tonumber(ARGV[3])
redis.call('HINCRBY', key, base .. 'processed', 1)
redis.call('HINCRBY', key, base .. 'failed', tonumber(ARGV[2]))
redis.call('HINCRBY', key, base .. 'total_ms', duration)
local max = tonumber(redis.call('HGET', key, base .. 'max_ms') or '0')
if duration > max then redis.call('HSET', key, base .. 'max_ms', duration) end
redis.call('EXPIRE', key, tonumber(ARGV[4]))
redis.call('INCR', KEYS[2])
if tonumber(ARGV[2]) > 0 then redis.call('INCR', KEYS[3]) end
return 0
";

/// Separates queue, job and counter name in metric hash fields.
const FIELD_SEP: char = '\u{1f}';

/// How long finished (done or cancelled) jobs stay queryable before Redis expires them.
const DONE_TTL_SECS: u64 = 24 * 60 * 60;

/// Stores a job's data (KEYS[2]) only while worker ARGV[3] holds it. The
/// claim writes the holder into the job's `worker` field and completing,
/// failing or recovering the job removes it, so the check is one hash read.
/// A processing job without the field was claimed by a version that didn't
/// write it: then the worker's processing list (KEYS[1]) is searched, as
/// those versions did. ARGV[1] is the job's id, ARGV[2] its data.
const CHECKPOINT_IF_HELD: &str = r"
local f = redis.call('HMGET', KEYS[2], 'state', 'worker')
if f[1] ~= 'processing' then return 0 end
if f[2] then
  if f[2] ~= ARGV[3] then return 0 end
elseif not redis.call('LPOS', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('HSET', KEYS[2], 'data', ARGV[2])
return 1
";

/// Frees what job `id` holds as it leaves processing (Lua, used by the
/// scripts below): its concurrency key, moving the oldest job parked on that
/// key back to the claim end of its queue, and, when `finish` is true, its
/// unique key if it still holds it.
const RELEASE_FN: &str = r"
local function release(p, id, finish)
  local f = redis.call('HMGET', p .. 'job:' .. id, 'ckey', 'ukey')
  if f[1] then
    redis.call('SREM', p .. 'running:' .. f[1], id)
    local parked = redis.call('RPOP', p .. 'blocked:' .. f[1])
    if parked then
      local queue = redis.call('HGET', p .. 'job:' .. parked, 'queue')
      if queue then
        redis.call('RPUSH', p .. 'queue:' .. queue, parked)
        redis.call('HINCRBY', p .. 'blocked', queue, -1)
        redis.call('PUBLISH', p .. 'wake', queue)
      end
    end
  end
  if finish and f[2] and redis.call('GET', p .. 'unique:' .. f[2]) == id then
    redis.call('DEL', p .. 'unique:' .. f[2])
  end
end
";

/// Frees what job ARGV[2] holds; ARGV[1] is the key prefix, ARGV[3] `1`
/// when the job finished (done or dead), `0` for a retry.
const RELEASE: &str = r"
release(ARGV[1], ARGV[2], ARGV[3] == '1')
return 0
";

/// Moves the oldest id out of a processing list (KEYS[1]) to the claim end of
/// its own queue, and marks it pending, as one atomic step: two workers
/// recovering at once can't move the same job, or send one to the wrong queue.
/// Frees its concurrency key. Returns the id, or nil once the list is empty.
/// ARGV[1] is the key prefix.
const RECOVER_ONE: &str = r"
local id = redis.call('RPOP', KEYS[1])
if not id then return false end
local job = ARGV[1] .. 'job:' .. id
local queue = redis.call('HGET', job, 'queue') or 'default'
redis.call('RPUSH', ARGV[1] .. 'queue:' .. queue, id)
redis.call('HSET', job, 'state', 'pending')
redis.call('HDEL', job, 'worker')
release(ARGV[1], id, false)
redis.call('SREM', ARGV[1] .. 'slots:' .. queue, id)
redis.call('PUBLISH', ARGV[1] .. 'wake', queue)
return id
";

/// Claims the oldest job of a queue (KEYS[1]) that may start into a
/// processing list (KEYS[2]), marked processing and held by worker ARGV[4].
/// ARGV[1] is the key prefix, ARGV[2] the queue's name. With a global limit
/// ARGV[3] (`''` for none),
/// claims nothing while the queue's slot set (KEYS[3]) holds that many ids,
/// and adds the claimed id to it. Parks jobs whose concurrency key is full
/// on that key's blocked list, and drops ids without job data. Returns
/// `{1, id, data}`, `{0, '', ''}` when nothing may start, or `{2, '', ''}`
/// after skipping many jobs, to be called again.
const CLAIM: &str = r"
local p = ARGV[1]
local limit = tonumber(ARGV[3])
if limit and redis.call('SCARD', KEYS[3]) >= limit then return {0, '', ''} end
for _ = 1, 100 do
  local id = redis.call('RPOP', KEYS[1])
  if not id then return {0, '', ''} end
  local job = p .. 'job:' .. id
  local f = redis.call('HMGET', job, 'data', 'ckey', 'climit', 'ukey', 'umode')
  if not f[1] then
    redis.call('DEL', job)
  elseif f[2] and redis.call('SCARD', p .. 'running:' .. f[2]) >= tonumber(f[3]) then
    redis.call('LPUSH', p .. 'blocked:' .. f[2], id)
    redis.call('HINCRBY', p .. 'blocked', ARGV[2], 1)
    redis.call('SADD', p .. 'blocked_keys', f[2])
  else
    redis.call('LPUSH', KEYS[2], id)
    redis.call('HSET', job, 'state', 'processing', 'worker', ARGV[4])
    if f[2] then redis.call('SADD', p .. 'running:' .. f[2], id) end
    if limit then redis.call('SADD', KEYS[3], id) end
    if f[5] == 'until_started' and redis.call('GET', p .. 'unique:' .. f[4]) == id then
      redis.call('DEL', p .. 'unique:' .. f[4])
    end
    return {1, id, f[1]}
  end
end
return {2, '', ''}
";

/// Removes pending, parked or scheduled job ARGV[2] so no worker runs it,
/// and marks it cancelled, as one step. ARGV[1] is the key prefix, ARGV[3]
/// how long it stays, ARGV[4] how many recent cancellations are listed.
/// Returns 1, or 0 if it wasn't waiting.
const CANCEL: &str = r"
local p, id = ARGV[1], ARGV[2]
local job = p .. 'job:' .. id
local f = redis.call('HMGET', job, 'queue', 'ckey', 'ukey')
if not f[1] then return 0 end
local removed = redis.call('ZREM', p .. 'scheduled', id)
if removed == 0 then removed = redis.call('LREM', p .. 'queue:' .. f[1], 1, id) end
if removed == 0 and f[2] then
  removed = redis.call('LREM', p .. 'blocked:' .. f[2], 1, id)
  if removed > 0 then redis.call('HINCRBY', p .. 'blocked', f[1], -1) end
end
if removed == 0 then return 0 end
redis.call('HSET', job, 'state', 'cancelled')
redis.call('EXPIRE', job, tonumber(ARGV[3]))
redis.call('LPUSH', p .. 'recent:cancelled', id)
redis.call('LTRIM', p .. 'recent:cancelled', 0, tonumber(ARGV[4]) - 1)
if f[3] and redis.call('GET', p .. 'unique:' .. f[3]) == id then
  redis.call('DEL', p .. 'unique:' .. f[3])
end
redis.call('PUBLISH', p .. 'done', id)
return 1
";

/// Stores a unique job unless a job holds its key; returns the holder's id,
/// or the new job's. ARGV: prefix, id, queue, data, run_at (`''` for none),
/// concurrency key hash and limit (`''` for none), unique key hash, mode.
const PUSH_UNIQUE: &str = r"
local p = ARGV[1]
local lock = p .. 'unique:' .. ARGV[8]
local held = redis.call('GET', lock)
if held then
  local f = redis.call('HMGET', p .. 'job:' .. held, 'state', 'umode')
  if f[1] == 'pending' or f[1] == 'scheduled'
     or (f[1] == 'processing' and f[2] == 'until_finished') then
    return held
  end
end
redis.call('SET', lock, ARGV[2])
local job = p .. 'job:' .. ARGV[2]
local state = 'pending'
if ARGV[5] ~= '' then state = 'scheduled' end
redis.call('HSET', job, 'state', state, 'queue', ARGV[3], 'data', ARGV[4], 'ukey', ARGV[8], 'umode', ARGV[9])
if ARGV[6] ~= '' then redis.call('HSET', job, 'ckey', ARGV[6], 'climit', ARGV[7]) end
redis.call('SADD', p .. 'queues', ARGV[3])
if state == 'scheduled' then
  redis.call('ZADD', p .. 'scheduled', ARGV[5], ARGV[2])
else
  redis.call('LPUSH', p .. 'queue:' .. ARGV[3], ARGV[2])
  redis.call('PUBLISH', p .. 'wake', ARGV[3])
end
return ARGV[2]
";

/// `script` with the Lua functions it calls in front.
fn with_release(script: &str) -> String {
    format!("{RELEASE_FN}{script}")
}

/// Moves up to ARGV[3] ids due by ARGV[1] (ms) from the scheduled set
/// (KEYS[1]) onto the back of their own queues, as pending, soonest first.
/// ARGV[2] is the key prefix. Returns how many ids it took from the set, how
/// many of those moved (an id without job data is dropped), and the next run
/// time left, if any.
const PROMOTE_DUE: &str = r"
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3]))
local moved, woken = 0, {}
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  local job = ARGV[2] .. 'job:' .. id
  local queue = redis.call('HGET', job, 'queue')
  if queue then
    redis.call('LPUSH', ARGV[2] .. 'queue:' .. queue, id)
    redis.call('HSET', job, 'state', 'pending')
    moved = moved + 1
    if not woken[queue] then
      woken[queue] = true
      redis.call('PUBLISH', ARGV[2] .. 'wake', queue)
    end
  end
end
local next = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return {#ids, moved, next[2] or false}
";

/// How many due ids one promotion script call moves, so a large backlog
/// doesn't hold Redis for long.
const PROMOTE_BATCH: usize = 1_000;

/// Moves scheduled id ARGV[2] out of the scheduled set (KEYS[1]) onto the
/// back of its own queue, ahead of its run time. ARGV[1] is the key prefix.
/// Returns 1, or 0 if it wasn't scheduled.
const RUN_NOW: &str = r"
if redis.call('ZREM', KEYS[1], ARGV[2]) == 0 then return 0 end
local job = ARGV[1] .. 'job:' .. ARGV[2]
local queue = redis.call('HGET', job, 'queue') or 'default'
redis.call('LPUSH', ARGV[1] .. 'queue:' .. queue, ARGV[2])
redis.call('HSET', job, 'state', 'pending')
redis.call('PUBLISH', ARGV[1] .. 'wake', queue)
return 1
";

/// Enqueues a recurring tick's job unless the tick was taken. KEYS: the tick
/// (1), the schedule's hash (2), the job's hash (3), its queue (4), the set
/// of queues (5). ARGV: job id, queue, job data, tick (ms), tick expiry (ms),
/// wake channel. Returns 1 if it enqueued the job, 0 if the tick was taken.
const PUSH_RECURRING: &str = r"
if not redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[5]) then return 0 end
redis.call('HSET', KEYS[3], 'state', 'pending', 'queue', ARGV[2], 'data', ARGV[3])
redis.call('LPUSH', KEYS[4], ARGV[1])
redis.call('SADD', KEYS[5], ARGV[2])
if redis.call('EXISTS', KEYS[2]) == 1 then
  local last = tonumber(redis.call('HGET', KEYS[2], 'last_tick') or '-1')
  if tonumber(ARGV[4]) > last then
    redis.call('HSET', KEYS[2], 'last_tick', ARGV[4], 'last_job', ARGV[1])
  end
end
redis.call('PUBLISH', ARGV[6], ARGV[2])
return 1
";

pub struct RedisQueue {
    client: Client,
    /// Idle connections. Concurrent calls check out their own instead of
    /// sharing a single connection; at most `max_idle` are kept afterwards,
    /// so a burst of calls doesn't leave its connections open for good.
    idle: Mutex<Vec<Connection>>,
    max_idle: usize,
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
            max_idle: RedisConfig::DEFAULT_MAX_IDLE_CONNECTIONS,
            prefix: prefix.to_string(),
            display: format!("redis:{} (prefix {prefix})", redact(url)),
            signals: Arc::default(),
            subscriber: OnceLock::new(),
        })
    }

    /// Keeps at most `max` idle connections for reuse ([`RedisConfig::DEFAULT_MAX_IDLE_CONNECTIONS`] by default). A
    /// call that finds none idle opens one, and closes it afterwards if
    /// `max` are already idle; `0` closes every connection after its call.
    /// It doesn't limit how many calls run at once: size it to the calls a
    /// process makes concurrently, about its worker's `concurrency` plus
    /// `claimers`, to avoid reconnecting under steady load.
    #[must_use]
    pub fn max_idle_connections(mut self, max: usize) -> Self {
        self.max_idle = max;
        self.idle
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .truncate(max);
        self
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

    fn recurring_key(&self, key: &str) -> String {
        format!("{}:recurring:{key}", self.prefix)
    }

    /// The fields of `recurring:<key>` that [`read_recurring`] reads, in order.
    fn recurring_fields(pipe: &mut redis::Pipeline, key: String) {
        pipe.cmd("HMGET")
            .arg(key)
            .arg("data")
            .arg("created_at")
            .arg("seen_at")
            .arg("last_tick")
            .arg("last_job");
    }

    fn metrics_key(&self, minute: u64) -> String {
        format!("{}:metrics:{minute}", self.prefix)
    }

    /// The records of `ids`, in order, skipping ids whose job has expired.
    fn records(&self, ids: &[String]) -> Result<Vec<JobRecord>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for id in ids {
            pipe.cmd("HGET").arg(self.job_key(id)).arg("data");
        }
        let data: Vec<Option<String>> = self.with_conn(|con| pipe.query(con))?;
        data.into_iter()
            .flatten()
            .map(|data| Ok(serde_json::from_str(&data)?))
            .collect()
    }

    fn slots_key(&self, queue: &str) -> String {
        format!("{}:slots:{queue}", self.prefix)
    }

    fn processing_key(&self, worker: &str) -> String {
        format!("{}:processing:{worker}", self.prefix)
    }

    fn worker_key(&self, worker: &str) -> String {
        format!("{}:worker:{worker}", self.prefix)
    }

    /// Runs `f` on an idle connection, or a new one. The connection goes back
    /// to the pool afterwards unless it failed at the I/O level or the pool
    /// is full. The lock is only held to take or return a connection, never
    /// during a command.
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
            let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
            if idle.len() < self.max_idle {
                idle.push(conn);
            }
        }
        Ok(result?)
    }

    /// Takes the oldest job that may start of the first queue that has one,
    /// in order, into `worker`'s processing list, skipping queues at their
    /// global limit: one script per queue, which moves it and marks it
    /// processing together. Never blocks.
    fn sweep(
        &self,
        worker: &str,
        queues: &[&str],
        limits: &[GlobalLimit<'_>],
    ) -> Result<Option<JobRecord>> {
        let processing = self.processing_key(worker);
        for queue in queues {
            loop {
                let (status, _id, data): (u8, String, String) = self.with_conn(|con| {
                    redis::cmd("EVAL")
                        .arg(CLAIM)
                        .arg(3)
                        .arg(self.queue_key(queue))
                        .arg(&processing)
                        .arg(self.slots_key(queue))
                        .arg(format!("{}:", self.prefix))
                        .arg(*queue)
                        .arg(
                            GlobalLimit::of(limits, queue)
                                .map(|max| max.to_string())
                                .unwrap_or_default(),
                        )
                        .arg(worker)
                        .query(con)
                })?;
                match status {
                    1 => return Ok(Some(serde_json::from_str(&data)?)),
                    // It parked many jobs of full keys: look further.
                    2 => continue,
                    _ => break,
                }
            }
        }
        Ok(None)
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

impl RedisQueue {
    /// Stores a unique job, unless a job holds its key: see [`PUSH_UNIQUE`].
    fn push_unique(&self, job: &JobRecord, unique: &crate::UniqueKey) -> Result<JobId> {
        let data = serde_json::to_string(job)?;
        let (ckey, climit) = match &job.concurrency {
            Some(concurrency) => (concurrency.hash(), concurrency.limit.to_string()),
            None => (String::new(), String::new()),
        };
        self.with_conn(|con| {
            redis::cmd("EVAL")
                .arg(PUSH_UNIQUE)
                .arg(0)
                .arg(format!("{}:", self.prefix))
                .arg(&job.id)
                .arg(&job.queue)
                .arg(&data)
                .arg(job.run_at_ms.map(|at| at.to_string()).unwrap_or_default())
                .arg(ckey)
                .arg(climit)
                .arg(unique.hash())
                .arg(unique.until.as_str())
                .query(con)
        })
    }

    fn push_pending(&self, job: JobRecord) -> Result<JobId> {
        let queue = &job.queue;
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
                .arg(key_fields(&job))
                .ignore()
                .cmd("LPUSH")
                .arg(self.queue_key(queue))
                .arg(&job.id)
                .ignore()
                .cmd("SADD")
                .arg(self.key("queues"))
                .arg(queue)
                .ignore()
                .cmd("PUBLISH")
                .arg(self.key("wake"))
                .arg(queue)
                .ignore()
                .exec(con)
        })?;
        Ok(job.id)
    }

    fn push_scheduled(&self, job: JobRecord, run_at: u64) -> Result<JobId> {
        let queue = &job.queue;
        let data = serde_json::to_string(&job)?;
        // No wake-up: there is nothing to claim until it is promoted.
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("HSET")
                .arg(self.job_key(&job.id))
                .arg("state")
                .arg(JobState::Scheduled.as_str())
                .arg("queue")
                .arg(queue)
                .arg("data")
                .arg(&data)
                .arg(key_fields(&job))
                .ignore()
                .cmd("ZADD")
                .arg(self.key("scheduled"))
                .arg(run_at)
                .arg(&job.id)
                .ignore()
                .cmd("SADD")
                .arg(self.key("queues"))
                .arg(queue)
                .ignore()
                .exec(con)
        })?;
        Ok(job.id)
    }
}

impl Store for RedisQueue {
    fn push(&self, job: NewJob) -> Result<JobId> {
        let job = job.into_record();
        if let Some(unique) = &job.unique {
            return self.push_unique(&job, unique);
        }
        match job.run_at_ms {
            Some(run_at) => self.push_scheduled(job, run_at),
            None => self.push_pending(job),
        }
    }

    /// One pipelined transaction for every job, and one wake-up per queue.
    /// Unique jobs are pushed one by one afterwards, each checked and stored
    /// in one script.
    fn push_many(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        let mut pipe = redis::pipe();
        pipe.atomic();
        let mut ids = Vec::with_capacity(jobs.len());
        let mut unique = Vec::new();
        // Queues that got a pending job, which wakes claims, and whether it did.
        let mut queues: Vec<(String, bool)> = Vec::new();
        for new in jobs {
            let job = new.into_record();
            if job.unique.is_some() {
                unique.push((ids.len(), job));
                ids.push(String::new());
                continue;
            }
            let state = match job.run_at_ms {
                Some(_) => JobState::Scheduled,
                None => JobState::Pending,
            };
            pipe.cmd("HSET")
                .arg(self.job_key(&job.id))
                .arg("state")
                .arg(state.as_str())
                .arg("queue")
                .arg(&job.queue)
                .arg("data")
                .arg(serde_json::to_string(&job)?)
                .arg(key_fields(&job))
                .ignore();
            match job.run_at_ms {
                Some(run_at) => pipe
                    .cmd("ZADD")
                    .arg(self.key("scheduled"))
                    .arg(run_at)
                    .arg(&job.id)
                    .ignore(),
                None => pipe
                    .cmd("LPUSH")
                    .arg(self.queue_key(&job.queue))
                    .arg(&job.id)
                    .ignore(),
            };
            let pending = job.run_at_ms.is_none();
            match queues.iter_mut().find(|(queue, _)| *queue == job.queue) {
                Some((_, wakes)) => *wakes |= pending,
                None => queues.push((job.queue.clone(), pending)),
            }
            ids.push(job.id);
        }
        for (queue, wakes) in &queues {
            pipe.cmd("SADD").arg(self.key("queues")).arg(queue).ignore();
            if *wakes {
                pipe.cmd("PUBLISH")
                    .arg(self.key("wake"))
                    .arg(queue)
                    .ignore();
            }
        }
        self.with_conn(|con| pipe.exec(con))?;
        for (index, job) in unique {
            if let Some(key) = &job.unique {
                ids[index] = self.push_unique(&job, key)?;
            }
        }
        Ok(ids)
    }

    fn promote(&self, now: SystemTime) -> Result<Promoted> {
        let mut promoted = Promoted::default();
        loop {
            let (taken, moved, next): (usize, usize, Option<String>) = self.with_conn(|con| {
                redis::cmd("EVAL")
                    .arg(PROMOTE_DUE)
                    .arg(1)
                    .arg(self.key("scheduled"))
                    .arg(millis(now))
                    .arg(format!("{}:", self.prefix))
                    .arg(PROMOTE_BATCH)
                    .query(con)
            })?;
            promoted.moved += moved;
            // Scores come back as text; run times are whole milliseconds.
            promoted.next = next
                .and_then(|score| score.parse::<f64>().ok())
                .map(|ms| from_millis(ms as u64));
            if taken < PROMOTE_BATCH {
                return Ok(promoted);
            }
        }
    }

    fn claim(&self, worker: &str, queues: &[&str], wait: Duration) -> Result<Option<JobRecord>> {
        self.claim_within_limits(worker, queues, &[], wait)
    }

    fn claim_within_limits(
        &self,
        worker: &str,
        queues: &[&str],
        limits: &[GlobalLimit<'_>],
        wait: Duration,
    ) -> Result<Option<JobRecord>> {
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
            if let Some(job) = self.sweep(worker, queues, limits)? {
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
                .cmd("SREM")
                .arg(self.slots_key(&job.queue))
                .arg(&job.id)
                .ignore()
                .cmd("HSET")
                .arg(self.job_key(&job.id))
                .arg("state")
                .arg(JobState::Done.as_str())
                .arg("data")
                .arg(&data)
                .ignore()
                .cmd("HDEL")
                .arg(self.job_key(&job.id))
                .arg("worker")
                .ignore()
                .cmd("EXPIRE")
                .arg(self.job_key(&job.id))
                .arg(DONE_TTL_SECS)
                .ignore()
                .cmd("EVAL")
                .arg(with_release(RELEASE))
                .arg(0)
                .arg(format!("{}:", self.prefix))
                .arg(&job.id)
                .arg(1)
                .ignore()
                .cmd("LPUSH")
                .arg(self.key("recent:done"))
                .arg(&job.id)
                .ignore()
                .cmd("LTRIM")
                .arg(self.key("recent:done"))
                .arg(0)
                .arg(RECENT_CAP - 1)
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
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("LREM")
            .arg(self.processing_key(worker))
            .arg(1)
            .arg(&job.id)
            .ignore()
            .cmd("SREM")
            .arg(self.slots_key(&job.queue))
            .arg(&job.id)
            .ignore()
            .cmd("HSET")
            .arg(self.job_key(&job.id))
            .arg("state")
            .arg(state.as_str())
            .arg("data")
            .arg(&data)
            .ignore()
            .cmd("HDEL")
            .arg(self.job_key(&job.id))
            .arg("worker")
            .ignore()
            .cmd("EVAL")
            .arg(with_release(RELEASE))
            .arg(0)
            .arg(format!("{}:", self.prefix))
            .arg(&job.id)
            .arg(u8::from(state == JobState::Dead))
            .ignore();
        match state {
            // A retry that waits: nothing to wake until it is promoted.
            JobState::Scheduled => pipe
                .cmd("ZADD")
                .arg(self.key("scheduled"))
                .arg(job.run_at_ms.unwrap_or_default())
                .arg(&job.id)
                .ignore(),
            // A dead job wakes result waiters.
            JobState::Dead => pipe
                .cmd("LPUSH")
                .arg(self.key("dead"))
                .arg(&job.id)
                .ignore()
                .cmd("PUBLISH")
                .arg(self.key("done"))
                .arg(&job.id)
                .ignore(),
            // A retry wakes idle claims.
            _ => pipe
                .cmd("LPUSH")
                .arg(self.queue_key(&job.queue))
                .arg(&job.id)
                .ignore()
                .cmd("PUBLISH")
                .arg(self.key("wake"))
                .arg(&job.id)
                .ignore(),
        };
        self.with_conn(|con| pipe.exec(con))
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
    /// progress of the job's new run. The check reads the job's hash, so its
    /// cost doesn't grow with the number of jobs the worker holds.
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
                .arg(worker)
                .exec(con)
        })
    }

    /// One script: the scheduled set, the queue, then the key's blocked
    /// list, so a job moving between them can't slip past.
    fn cancel(&self, id: &str) -> Result<bool> {
        let cancelled: u8 = self.with_conn(|con| {
            redis::cmd("EVAL")
                .arg(CANCEL)
                .arg(0)
                .arg(format!("{}:", self.prefix))
                .arg(id)
                .arg(DONE_TTL_SECS)
                .arg(RECENT_CAP)
                .query(con)
        })?;
        Ok(cancelled > 0)
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
                    .arg(with_release(RECOVER_ONE))
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

    fn paused_queues(&self) -> Result<Vec<String>> {
        let mut paused: Vec<String> =
            self.with_conn(|con| redis::cmd("SMEMBERS").arg(self.key("paused")).query(con))?;
        paused.sort();
        Ok(paused)
    }

    /// One transaction: `HSETNX` keeps an existing schedule's creation time,
    /// and its last run is left alone.
    fn register_recurring(&self, schedules: &[RecurringRecord]) -> Result<Vec<RecurringRecord>> {
        if schedules.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        pipe.atomic();
        for schedule in schedules {
            let key = self.recurring_key(&schedule.key);
            pipe.cmd("HSETNX")
                .arg(&key)
                .arg("created_at")
                .arg(schedule.created_at_ms)
                .ignore()
                .cmd("HSET")
                .arg(&key)
                .arg("data")
                .arg(serde_json::to_string(schedule)?)
                .arg("seen_at")
                .arg(schedule.seen_at_ms)
                .ignore()
                .cmd("SADD")
                .arg(self.key("recurring"))
                .arg(&schedule.key)
                .ignore();
            Self::recurring_fields(&mut pipe, key);
        }
        let rows: Vec<RecurringFields> = self.with_conn(|con| pipe.query(con))?;
        // Written in the same transaction, so the data is there.
        rows.into_iter()
            .zip(schedules)
            .map(|(row, schedule)| Ok(read_recurring(row)?.unwrap_or_else(|| schedule.clone())))
            .collect()
    }

    fn push_recurring(&self, key: &str, tick: SystemTime, job: NewJob) -> Result<Option<JobId>> {
        let job = job.into_record();
        let data = serde_json::to_string(&job)?;
        let tick = millis(tick);
        let pushed: u8 = self.with_conn(|con| {
            redis::cmd("EVAL")
                .arg(PUSH_RECURRING)
                .arg(5)
                .arg(format!("{}:recurring:tick:{key}:{tick}", self.prefix))
                .arg(self.recurring_key(key))
                .arg(self.job_key(&job.id))
                .arg(self.queue_key(&job.queue))
                .arg(self.key("queues"))
                .arg(&job.id)
                .arg(&job.queue)
                .arg(&data)
                .arg(tick)
                .arg(u64::try_from(TICK_RETENTION.as_millis()).unwrap_or(u64::MAX))
                .arg(self.key("wake"))
                .query(con)
        })?;
        Ok((pushed > 0).then_some(job.id))
    }

    fn describe(&self) -> String {
        self.display.clone()
    }
}

impl Monitor for RedisQueue {
    fn stats(&self) -> Result<Stats> {
        let queues: Vec<String> =
            self.with_conn(|con| redis::cmd("SMEMBERS").arg(self.key("queues")).query(con))?;
        let workers: Vec<String> =
            self.with_conn(|con| redis::cmd("SMEMBERS").arg(self.key("workers")).query(con))?;
        let mut pipe = redis::pipe();
        for queue in &queues {
            pipe.cmd("LLEN").arg(self.queue_key(queue));
            // Jobs parked because their concurrency key was full.
            pipe.cmd("HGET").arg(self.key("blocked")).arg(queue);
        }
        for worker in &workers {
            pipe.cmd("LLEN").arg(self.processing_key(worker));
            pipe.cmd("PTTL").arg(self.worker_key(worker));
        }
        pipe.cmd("LLEN").arg(self.key("dead"));
        pipe.cmd("LLEN").arg(self.key("recent:done"));
        pipe.cmd("LLEN").arg(self.key("recent:cancelled"));
        pipe.cmd("GET").arg(self.key("counter:processed"));
        pipe.cmd("GET").arg(self.key("counter:failed"));
        pipe.cmd("ZCARD").arg(self.key("scheduled"));
        let values: Vec<Option<i64>> = self.with_conn(|con| pipe.query(con))?;
        let mut values = values.into_iter().map(|value| value.unwrap_or(0));
        let mut next = || values.next().unwrap_or(0);

        let mut stats = Stats::default();
        let mut queue_stats: Vec<QueueStats> = queues
            .into_iter()
            .map(|name| QueueStats {
                name,
                pending: count(next()) + count(next()),
            })
            .collect();
        queue_stats.sort_by(|a, b| a.name.cmp(&b.name));
        stats.queues = queue_stats;
        let mut worker_stats: Vec<WorkerStats> = workers
            .into_iter()
            .map(|id| WorkerStats {
                id,
                running: count(next()),
                // PTTL: -2 once the key expired, -1 without expiry.
                expires_in_ms: next(),
            })
            .collect();
        worker_stats.sort_by(|a, b| a.id.cmp(&b.id));
        stats.processing = worker_stats.iter().map(|worker| worker.running).sum();
        stats.workers = worker_stats;
        stats.dead = count(next());
        stats.done = count(next());
        stats.cancelled = count(next());
        stats.processed_total = count(next());
        stats.failed_total = count(next());
        stats.scheduled = count(next());
        Ok(stats)
    }

    fn list(&self, filter: &ListFilter) -> Result<Vec<JobRecord>> {
        // `LRANGE` and `ZRANGE` both read members in order, by index.
        let ordered = match filter.state {
            JobState::Dead => Some(("LRANGE", self.key("dead"))),
            JobState::Done => Some(("LRANGE", self.key("recent:done"))),
            JobState::Cancelled => Some(("LRANGE", self.key("recent:cancelled"))),
            JobState::Scheduled => Some(("ZRANGE", self.key("scheduled"))),
            JobState::Pending | JobState::Processing => None,
        };
        let Some((range, list_key)) = ordered else {
            // Pending and processing: gather ids (oldest first) from every list.
            let lists: Vec<String> = if filter.state == JobState::Pending {
                match &filter.queue {
                    Some(queue) => vec![self.queue_key(queue)],
                    None => self
                        .with_conn(|con| {
                            redis::cmd("SMEMBERS")
                                .arg(self.key("queues"))
                                .query::<Vec<String>>(con)
                        })?
                        .iter()
                        .map(|queue| self.queue_key(queue))
                        .collect(),
                }
            } else {
                self.with_conn(|con| {
                    redis::cmd("SMEMBERS")
                        .arg(self.key("workers"))
                        .query::<Vec<String>>(con)
                })?
                .iter()
                .map(|worker| self.processing_key(worker))
                .collect()
            };
            let mut lists = lists;
            if filter.state == JobState::Pending {
                let parked: Vec<String> = self.with_conn(|con| {
                    redis::cmd("SMEMBERS")
                        .arg(self.key("blocked_keys"))
                        .query(con)
                })?;
                lists.extend(
                    parked
                        .iter()
                        .map(|key| format!("{}:blocked:{key}", self.prefix)),
                );
            }
            let mut ids = Vec::new();
            for list in lists {
                let mut some: Vec<String> = self
                    .with_conn(|con| redis::cmd("LRANGE").arg(&list).arg(0).arg(-1).query(con))?;
                ids.append(&mut some);
            }
            // Ids start with the enqueue time.
            ids.sort();
            let records = self.records(&ids)?;
            return Ok(records
                .into_iter()
                .filter(|job| {
                    filter
                        .queue
                        .as_deref()
                        .is_none_or(|queue| queue == job.queue)
                })
                .skip(filter.offset)
                .take(filter.limit)
                .collect());
        };
        // Finished jobs: the lists hold the most recent first; scheduled ones
        // are by run time. With a queue filter, read windows until the page
        // is full or the list ends.
        let window = (filter.limit.max(1) * 4) as isize;
        let mut skipped = 0;
        let mut page = Vec::new();
        let mut start: isize = if filter.queue.is_some() {
            0
        } else {
            filter.offset as isize
        };
        loop {
            let ids: Vec<String> = self.with_conn(|con| {
                redis::cmd(range)
                    .arg(&list_key)
                    .arg(start)
                    .arg(start + window - 1)
                    .query(con)
            })?;
            let exhausted = (ids.len() as isize) < window;
            for job in self.records(&ids)? {
                if filter
                    .queue
                    .as_deref()
                    .is_some_and(|queue| queue != job.queue)
                {
                    continue;
                }
                if filter.queue.is_some() && skipped < filter.offset {
                    skipped += 1;
                    continue;
                }
                page.push(job);
                if page.len() == filter.limit {
                    return Ok(page);
                }
            }
            if exhausted {
                return Ok(page);
            }
            start += window;
        }
    }

    fn retry(&self, id: &str) -> Result<bool> {
        let removed: i64 = self.with_conn(|con| {
            redis::cmd("LREM")
                .arg(self.key("dead"))
                .arg(1)
                .arg(id)
                .query(con)
        })?;
        if removed == 0 {
            return Ok(false);
        }
        // Out of the dead list, so this retry is the only one moving it.
        let Some(mut job) = self.records(&[id.to_owned()])?.pop() else {
            return Ok(false);
        };
        job.attempts = 0;
        let data = serde_json::to_string(&job)?;
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("HSET")
                .arg(self.job_key(id))
                .arg("state")
                .arg(JobState::Pending.as_str())
                .arg("data")
                .arg(&data)
                .ignore()
                .cmd("LPUSH")
                .arg(self.queue_key(&job.queue))
                .arg(id)
                .ignore()
                .cmd("PUBLISH")
                .arg(self.key("wake"))
                .arg(&job.queue)
                .ignore()
                .exec(con)
        })?;
        Ok(true)
    }

    fn run_now(&self, id: &str) -> Result<bool> {
        let moved: u8 = self.with_conn(|con| {
            redis::cmd("EVAL")
                .arg(RUN_NOW)
                .arg(1)
                .arg(self.key("scheduled"))
                .arg(format!("{}:", self.prefix))
                .arg(id)
                .query(con)
        })?;
        Ok(moved > 0)
    }

    fn discard(&self, id: &str) -> Result<bool> {
        let state: Option<String> = self.with_conn(|con| {
            redis::cmd("HGET")
                .arg(self.job_key(id))
                .arg("state")
                .query(con)
        })?;
        let finished = state
            .as_deref()
            .and_then(JobState::parse)
            .is_some_and(JobState::is_finished);
        if !finished {
            return Ok(false);
        }
        self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("DEL")
                .arg(self.job_key(id))
                .ignore()
                .cmd("LREM")
                .arg(self.key("dead"))
                .arg(0)
                .arg(id)
                .ignore()
                .cmd("LREM")
                .arg(self.key("recent:done"))
                .arg(0)
                .arg(id)
                .ignore()
                .cmd("LREM")
                .arg(self.key("recent:cancelled"))
                .arg(0)
                .arg(id)
                .ignore()
                .exec(con)
        })?;
        Ok(true)
    }

    fn pause_queue(&self, queue: &str) -> Result<bool> {
        let added: u8 = self.with_conn(|con| {
            redis::cmd("SADD")
                .arg(self.key("paused"))
                .arg(queue)
                .query(con)
        })?;
        Ok(added > 0)
    }

    /// Also wakes idle claims, which can take the queue's jobs again.
    fn resume_queue(&self, queue: &str) -> Result<bool> {
        let (removed,): (u8,) = self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("SREM")
                .arg(self.key("paused"))
                .arg(queue)
                .cmd("PUBLISH")
                .arg(self.key("wake"))
                .arg(queue)
                .ignore()
                .query(con)
        })?;
        Ok(removed > 0)
    }

    fn recurring(&self) -> Result<Vec<RecurringRecord>> {
        let mut keys: Vec<String> =
            self.with_conn(|con| redis::cmd("SMEMBERS").arg(self.key("recurring")).query(con))?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        keys.sort();
        let mut pipe = redis::pipe();
        for key in &keys {
            Self::recurring_fields(&mut pipe, self.recurring_key(key));
        }
        let rows: Vec<RecurringFields> = self.with_conn(|con| pipe.query(con))?;
        // A key removed between the two reads has no data left: skip it.
        Ok(rows
            .into_iter()
            .map(read_recurring)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    fn remove_recurring(&self, key: &str) -> Result<bool> {
        crate::recurring::validate_key(key)?;
        let (removed,): (u8,) = self.with_conn(|con| {
            redis::pipe()
                .atomic()
                .cmd("SREM")
                .arg(self.key("recurring"))
                .arg(key)
                .cmd("DEL")
                .arg(self.recurring_key(key))
                .ignore()
                .query(con)
        })?;
        Ok(removed > 0)
    }

    fn record_metric(&self, metric: &JobMetric) -> Result<()> {
        let base = format!("{}{FIELD_SEP}{}{FIELD_SEP}", metric.queue, metric.job);
        self.with_conn(|con| {
            redis::cmd("EVAL")
                .arg(RECORD_METRIC)
                .arg(3)
                .arg(self.metrics_key(metric.minute))
                .arg(self.key("counter:processed"))
                .arg(self.key("counter:failed"))
                .arg(&base)
                .arg(u8::from(metric.failed))
                .arg(metric.duration_ms)
                .arg(METRICS_RETENTION_MINUTES * 60)
                .exec(con)
        })
    }

    fn metrics(&self, since_minute: u64) -> Result<Vec<MetricBucket>> {
        let now = current_minute();
        let first = since_minute.max(now.saturating_sub(METRICS_RETENTION_MINUTES));
        let minutes: Vec<u64> = (first..=now).collect();
        if minutes.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for minute in &minutes {
            pipe.cmd("HGETALL").arg(self.metrics_key(*minute));
        }
        let hashes: Vec<Vec<(String, u64)>> = self.with_conn(|con| pipe.query(con))?;
        let mut buckets = Vec::new();
        for (minute, fields) in minutes.into_iter().zip(hashes) {
            let mut by_job: BTreeMap<(String, String), MetricBucket> = BTreeMap::new();
            for (field, value) in fields {
                let mut parts = field.splitn(3, FIELD_SEP);
                let (Some(queue), Some(job), Some(name)) =
                    (parts.next(), parts.next(), parts.next())
                else {
                    continue;
                };
                let bucket = by_job
                    .entry((queue.to_owned(), job.to_owned()))
                    .or_insert_with(|| MetricBucket {
                        minute,
                        queue: queue.to_owned(),
                        job: job.to_owned(),
                        ..MetricBucket::default()
                    });
                match name {
                    "processed" => bucket.processed = value,
                    "failed" => bucket.failed = value,
                    "total_ms" => bucket.total_ms = value,
                    "max_ms" => bucket.max_ms = value,
                    _ => {}
                }
            }
            buckets.extend(by_job.into_values());
        }
        Ok(buckets)
    }
}

impl Watch for RedisQueue {
    fn watch_finished(&self, id: &str) -> Option<Arc<Signal>> {
        self.listen_for_signals();
        Some(self.signals.finished.watch(id))
    }
}

/// The job-hash fields for its concurrency and unique keys, as `HSET`
/// arguments: the keys by their hashes, which scripts build Redis keys from.
fn key_fields(job: &JobRecord) -> Vec<String> {
    let mut fields = Vec::new();
    if let Some(concurrency) = &job.concurrency {
        fields.extend([
            "ckey".to_owned(),
            concurrency.hash(),
            "climit".to_owned(),
            concurrency.limit.to_string(),
        ]);
    }
    if let Some(unique) = &job.unique {
        fields.extend([
            "ukey".to_owned(),
            unique.hash(),
            "umode".to_owned(),
            unique.until.as_str().to_owned(),
        ]);
    }
    fields
}

/// A schedule hash's fields, as [`RedisQueue::recurring_fields`] reads them.
type RecurringFields = (
    Option<String>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<String>,
);

/// The schedule in `fields`, or `None` if it has no data (it was removed).
fn read_recurring(
    (data, created_at, seen_at, last_tick, last_job): RecurringFields,
) -> Result<Option<RecurringRecord>> {
    let Some(data) = data else {
        return Ok(None);
    };
    let mut schedule: RecurringRecord = serde_json::from_str(&data)?;
    schedule.created_at_ms = created_at.unwrap_or(schedule.created_at_ms);
    schedule.seen_at_ms = seen_at.unwrap_or(schedule.seen_at_ms);
    schedule.last_tick_ms = last_tick;
    schedule.last_job_id = last_job;
    Ok(Some(schedule))
}

/// Redis counts and lengths, which are never negative here.
fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
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
    #![allow(clippy::unwrap_used)]

    use std::sync::{Arc, Barrier, PoisonError};

    use super::RedisQueue;

    fn idle(queue: &RedisQueue) -> usize {
        queue
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Needs a Redis server, like the integration tests; skipped without one.
    #[test]
    fn keeps_at_most_max_idle_connections() {
        let url = std::env::var("BUTLER_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
        let prefix = format!("butler-pool-{}", std::process::id());
        let queue = match RedisQueue::connect(&url, &prefix) {
            Ok(queue) => Arc::new(queue.max_idle_connections(3)),
            Err(e) => {
                eprintln!("skipping redis: {e}");
                return;
            }
        };
        // Eight calls at once, each on its own connection.
        let calls = 8;
        let barrier = Arc::new(Barrier::new(calls));
        let threads: Vec<_> = (0..calls)
            .map(|_| {
                let (queue, barrier) = (Arc::clone(&queue), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    queue
                        .with_conn(|con| {
                            barrier.wait();
                            redis::cmd("PING").query::<String>(con)
                        })
                        .unwrap()
                })
            })
            .collect();
        for thread in threads {
            assert_eq!(thread.join().unwrap(), "PONG");
        }
        assert_eq!(idle(&queue), 3);

        let queue = RedisQueue::connect(&url, &prefix)
            .unwrap()
            .max_idle_connections(0);
        assert_eq!(idle(&queue), 0, "the connect-time connection is dropped");
        queue
            .with_conn(|con| redis::cmd("PING").query::<String>(con))
            .unwrap();
        assert_eq!(idle(&queue), 0);
    }

    #[test]
    fn redacts_credentials() {
        assert_eq!(
            super::redact("redis://:secret@h:6379/0"),
            "redis://***@h:6379/0"
        );
        assert_eq!(super::redact("redis://127.0.0.1/"), "redis://127.0.0.1/");
    }
}
