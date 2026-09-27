//! Recurring jobs, like Solid Queue's `recurring.yml`: a job, its arguments
//! and a cron schedule. Every worker that has the schedule evaluates it, and
//! the backend makes sure each tick is enqueued exactly once however many of
//! them try (see [`Store::push_recurring`](crate::Store::push_recurring)).
//!
//! ```ignore
//! let worker = Worker::from_config(&config)?
//!     .recurring(nightly_report::prepare("summary")?, "0 3 * * *")?;
//! ```
//!
//! or in `butler.toml`:
//!
//! ```toml
//! [[recurring]]
//! job = "nightly_report"
//! cron = "0 3 * * *"          # minute hour day-of-month month day-of-week
//! args = ["summary"]
//! timezone = "Europe/Paris"   # optional; UTC by default
//! ```
//!
//! **Missed ticks.** A tick that passed while no worker was running is
//! enqueued once, late, when a worker starts: only the latest one, never a
//! backlog. A schedule that did not exist yet at a tick (its first worker
//! started after it) doesn't run for it.

use std::{
    str::FromStr,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Error, JobId, PreparedJob, Result,
    job::{from_millis, is_valid_queue_name, millis},
};

/// How often a running worker registers its schedules with the backend, so
/// the dashboard can tell which ones some worker still runs.
pub const REGISTER_INTERVAL: Duration = Duration::from_secs(60);

/// A schedule no worker registered for this long has no running worker: it
/// was removed from their configuration, or they are all stopped.
pub const ACTIVE_WINDOW: Duration = Duration::from_secs(3 * 60);

/// A five-field cron expression (`minute hour day-of-month month
/// day-of-week`, as in crontab), evaluated in UTC or in a named time zone.
///
/// Fields take numbers, `*`, lists (`1,15`), ranges (`1-5`), steps (`*/15`)
/// and names (`jan`-`dec`, `sun`-`sat`); in the day of week, `0` and `7` are
/// both Sunday. When both day fields are restricted, a day matching either
/// one matches, as in crontab. In a zone with daylight saving time, a time
/// skipped by the spring change doesn't run that day, and one repeated in the
/// autumn runs at both instants.
#[derive(Clone, Debug)]
pub struct Cron {
    schedule: cron_parser::Schedule,
    zone: Tz,
}

impl Cron {
    /// Parses `expression`, evaluated in UTC.
    pub fn parse(expression: &str) -> Result<Self> {
        let schedule = cron_parser::Schedule::parse(expression.trim()).map_err(|source| {
            Error::InvalidCron {
                expression: expression.to_owned(),
                source: source.into(),
            }
        })?;
        Ok(Self {
            schedule,
            zone: Tz::UTC,
        })
    }

    /// The same schedule, evaluated in `zone`: an IANA name such as
    /// `"Europe/Paris"` or `"America/New_York"`, or `"UTC"`.
    pub fn in_time_zone(mut self, zone: &str) -> Result<Self> {
        self.zone = Tz::from_str(zone).map_err(|_| Error::UnknownTimeZone {
            name: zone.to_owned(),
        })?;
        Ok(self)
    }

    pub fn expression(&self) -> &str {
        self.schedule.source()
    }

    /// The time zone's IANA name, `"UTC"` by default.
    pub fn time_zone(&self) -> &str {
        self.zone.name()
    }

    /// The first tick strictly after `at`.
    pub fn next_after(&self, at: SystemTime) -> Option<SystemTime> {
        let next = self.schedule.next_after(&self.zoned(at)?)?;
        Some(from_millis(u64::try_from(next.timestamp_millis()).ok()?))
    }

    /// The latest tick at or before `at`.
    pub fn latest_until(&self, at: SystemTime) -> Option<SystemTime> {
        let just_after = self.zoned(at)? + chrono::Duration::milliseconds(1);
        let tick = self.schedule.previous_before(&just_after)?;
        Some(from_millis(u64::try_from(tick.timestamp_millis()).ok()?))
    }

    fn zoned(&self, at: SystemTime) -> Option<DateTime<Tz>> {
        let ms = i64::try_from(millis(at)).ok()?;
        Some(DateTime::<Utc>::from_timestamp_millis(ms)?.with_timezone(&self.zone))
    }
}

impl FromStr for Cron {
    type Err = Error;

    fn from_str(expression: &str) -> Result<Self> {
        Self::parse(expression)
    }
}

/// A job enqueued on a cron schedule: what [`Worker::recurring`] and
/// `[[recurring]]` entries in `butler.toml` build.
///
/// Its key identifies it in the backend, for exactly-once ticks and for the
/// dashboard. By default it is derived from the job, queue, arguments, cron
/// expression and time zone, so every worker with the same schedule agrees on
/// it. Changing any of them makes it a new schedule; to keep its history, and
/// so that old and new workers can't both run one tick during a rolling
/// deploy, give it a key of your own with [`with_key`](Recurring::with_key).
///
/// [`Worker::recurring`]: crate::Worker::recurring
#[derive(Clone, Debug)]
pub struct Recurring {
    key: String,
    /// Whether `key` was given rather than derived.
    keyed: bool,
    cron: Cron,
    name: String,
    queue: String,
    args: Vec<Value>,
}

impl Recurring {
    /// Enqueues `job`, with its arguments and queue, at every tick of `cron`
    /// (UTC). A run time set on `job` is ignored: the schedule decides.
    pub fn new<T>(job: PreparedJob<T>, cron: &str) -> Result<Self> {
        Self::from_parts(
            job.name().to_owned(),
            job.queue().to_owned(),
            job.args().to_vec(),
            Cron::parse(cron)?,
        )
    }

    /// A schedule for the job registered as `name`, with arguments already
    /// serialized, as `[[recurring]]` entries give them.
    pub fn from_parts(name: String, queue: String, args: Vec<Value>, cron: Cron) -> Result<Self> {
        if !is_valid_queue_name(&queue) {
            return Err(Error::InvalidQueue {
                name: queue,
                reason: "use 1 to 64 of A-Z a-z 0-9 _ - . (not starting with a dot)",
            });
        }
        let mut recurring = Self {
            key: String::new(),
            keyed: false,
            cron,
            name,
            queue,
            args,
        };
        recurring.key = recurring.derived_key();
        Ok(recurring)
    }

    /// Names the schedule: 1 to 128 of `A-Z a-z 0-9 _ - .`, not starting with
    /// a dot.
    pub fn with_key(mut self, key: &str) -> Result<Self> {
        if !is_valid_key(key) {
            return Err(Error::InvalidRecurringKey {
                key: key.to_owned(),
            });
        }
        key.clone_into(&mut self.key);
        self.keyed = true;
        Ok(self)
    }

    /// Evaluates the schedule in `zone` (see [`Cron::in_time_zone`]) rather
    /// than UTC.
    pub fn in_time_zone(mut self, zone: &str) -> Result<Self> {
        self.cron = self.cron.in_time_zone(zone)?;
        if !self.keyed {
            self.key = self.derived_key();
        }
        Ok(self)
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn cron(&self) -> &Cron {
        &self.cron
    }

    /// The name of the job it enqueues.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn args(&self) -> &[Value] {
        &self.args
    }

    /// What a worker registers with the backend at `now`: the schedule, not
    /// yet run. The backend keeps an existing one's creation time and last
    /// run.
    pub fn record(&self, now: SystemTime) -> RecurringRecord {
        let now = millis(now);
        RecurringRecord {
            key: self.key.clone(),
            name: self.name.clone(),
            queue: self.queue.clone(),
            args: self.args.clone(),
            cron: self.cron.expression().to_owned(),
            time_zone: self.cron.time_zone().to_owned(),
            created_at_ms: now,
            seen_at_ms: now,
            last_tick_ms: None,
            last_job_id: None,
        }
    }

    /// When a worker that just started, with the schedule as the backend
    /// knows it (`stored`), should first enqueue it. The latest tick up to
    /// `now` if it was missed: the schedule already existed then, and nobody
    /// enqueued it. Otherwise the next tick.
    pub(crate) fn first_due(
        &self,
        stored: &RecurringRecord,
        now: SystemTime,
    ) -> Option<SystemTime> {
        if let Some(tick) = self.cron.latest_until(now) {
            let tick_ms = millis(tick);
            let missed = tick_ms >= stored.created_at_ms
                && stored.last_tick_ms.is_none_or(|last| last < tick_ms);
            if missed {
                return Some(tick);
            }
        }
        self.cron.next_after(now)
    }

    /// `<job>-<hash>`: readable, and the same in every process for the same
    /// schedule. FNV-1a, because its output is fixed by definition, unlike
    /// the standard library's hasher.
    fn derived_key(&self) -> String {
        let args = serde_json::to_string(&self.args).unwrap_or_default();
        let identity = [
            self.name.as_str(),
            &self.queue,
            self.cron.expression(),
            self.cron.time_zone(),
            &args,
        ];
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for part in identity {
            for byte in part.bytes().chain([0]) {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0100_0000_01b3);
            }
        }
        let mut readable: String = self
            .name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                    c
                } else {
                    '_'
                }
            })
            .take(64)
            .collect();
        if readable.starts_with('.') || readable.is_empty() {
            readable.insert(0, '_');
        }
        format!("{readable}-{hash:016x}")
    }
}

/// Whether `key` can name a recurring schedule: 1 to 128 of
/// `A-Z a-z 0-9 _ - .`, not starting with a dot. Keys become file names and
/// parts of Redis keys.
pub fn is_valid_key(key: &str) -> bool {
    (1..=128).contains(&key.len())
        && !key.starts_with('.')
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// A recurring schedule as a backend stores it: its definition, when workers
/// registered it, and its last run. Times are milliseconds since the Unix
/// epoch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecurringRecord {
    pub key: String,
    /// The job it enqueues.
    pub name: String,
    pub queue: String,
    pub args: Vec<Value>,
    pub cron: String,
    /// IANA name of the zone `cron` is evaluated in.
    pub time_zone: String,
    /// When a worker first registered it. Ticks before that are never run.
    pub created_at_ms: u64,
    /// When a running worker last registered it; see [`ACTIVE_WINDOW`].
    pub seen_at_ms: u64,
    /// The latest tick enqueued, if any.
    #[serde(default)]
    pub last_tick_ms: Option<u64>,
    /// The job enqueued for `last_tick_ms`.
    #[serde(default)]
    pub last_job_id: Option<JobId>,
}

impl RecurringRecord {
    /// Its schedule, parsed.
    pub fn schedule(&self) -> Result<Cron> {
        Cron::parse(&self.cron)?.in_time_zone(&self.time_zone)
    }

    /// Its next tick after `at`, if its schedule still parses.
    pub fn next_run(&self, at: SystemTime) -> Option<SystemTime> {
        self.schedule().ok()?.next_after(at)
    }

    /// Whether a running worker registered it within [`ACTIVE_WINDOW`] of
    /// `now`.
    pub fn is_active(&self, now: SystemTime) -> bool {
        millis(now).saturating_sub(self.seen_at_ms) <= millis_of(ACTIVE_WINDOW)
    }

    /// Takes `stored`'s creation time and last run, for a registration of a
    /// schedule the backend already had.
    pub fn merge_stored(&mut self, stored: &RecurringRecord) {
        self.created_at_ms = stored.created_at_ms.min(self.created_at_ms);
        self.last_tick_ms = stored.last_tick_ms;
        self.last_job_id.clone_from(&stored.last_job_id);
    }

    /// Records `job` as the run of `tick_ms`, unless a later tick already ran.
    pub fn record_run(&mut self, tick_ms: u64, job: &str) {
        if self.last_tick_ms.is_none_or(|last| last < tick_ms) {
            self.last_tick_ms = Some(tick_ms);
            self.last_job_id = Some(job.to_owned());
        }
    }
}

fn millis_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn at(rfc3339: &str) -> SystemTime {
        let ms = DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .timestamp_millis();
        from_millis(u64::try_from(ms).unwrap())
    }

    #[test]
    fn ticks_follow_crontab_semantics_in_utc() {
        let nightly = Cron::parse("0 3 * * *").unwrap();
        assert_eq!(nightly.time_zone(), "UTC");
        assert_eq!(
            nightly.next_after(at("2026-09-26T10:00:00Z")),
            Some(at("2026-09-27T03:00:00Z"))
        );
        // A tick exactly at `at` is the latest one up to it, not the next.
        assert_eq!(
            nightly.latest_until(at("2026-09-26T03:00:00Z")),
            Some(at("2026-09-26T03:00:00Z"))
        );
        assert_eq!(
            nightly.next_after(at("2026-09-26T03:00:00Z")),
            Some(at("2026-09-27T03:00:00Z"))
        );
        // 1 is Monday, as in crontab; 2026-09-28 is a Monday.
        let mondays = Cron::parse("0 9 * * 1").unwrap();
        assert_eq!(
            mondays.next_after(at("2026-09-26T00:00:00Z")),
            Some(at("2026-09-28T09:00:00Z"))
        );
    }

    #[test]
    fn a_time_zone_moves_ticks_and_follows_daylight_saving() {
        let paris = Cron::parse("0 3 * * *")
            .unwrap()
            .in_time_zone("Europe/Paris")
            .unwrap();
        // Summer time: UTC+2.
        assert_eq!(
            paris.next_after(at("2026-09-26T10:00:00Z")),
            Some(at("2026-09-27T01:00:00Z"))
        );
        // Winter time, after the change on 2026-10-25: UTC+1.
        assert_eq!(
            paris.next_after(at("2026-10-26T10:00:00Z")),
            Some(at("2026-10-27T02:00:00Z"))
        );
        assert!(matches!(
            Cron::parse("0 3 * * *")
                .unwrap()
                .in_time_zone("Mars/Olympus"),
            Err(Error::UnknownTimeZone { .. })
        ));
    }

    #[test]
    fn bad_expressions_are_typed_errors() {
        for bad in ["", "* * * *", "61 * * * *", "0 3 * * * *", "*/0 * * * *"] {
            assert!(
                matches!(Cron::parse(bad), Err(Error::InvalidCron { .. })),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn derived_keys_are_stable_readable_and_distinct() {
        let a = Recurring::from_parts(
            "billing.charge".into(),
            "default".into(),
            vec![serde_json::json!(1)],
            Cron::parse("0 3 * * *").unwrap(),
        )
        .unwrap();
        let same = a.clone();
        let other_args = Recurring::from_parts(
            "billing.charge".into(),
            "default".into(),
            vec![serde_json::json!(2)],
            Cron::parse("0 3 * * *").unwrap(),
        )
        .unwrap();
        assert_eq!(a.key(), same.key());
        assert_ne!(a.key(), other_args.key());
        assert!(a.key().starts_with("billing.charge-"), "{}", a.key());
        assert!(is_valid_key(a.key()));
        let zoned = a.clone().in_time_zone("Europe/Paris").unwrap();
        assert_ne!(a.key(), zoned.key());
        // A key of its own survives a change of zone.
        let named = a.with_key("nightly").unwrap();
        assert_eq!(named.in_time_zone("Asia/Tokyo").unwrap().key(), "nightly");
        assert!(
            Recurring::from_parts(
                "x".into(),
                "default".into(),
                vec![],
                Cron::parse("* * * * *").unwrap()
            )
            .unwrap()
            .with_key("../etc")
            .is_err()
        );
    }

    #[test]
    fn only_the_latest_missed_tick_runs_and_only_for_schedules_that_existed() {
        let hourly = Recurring::from_parts(
            "report".into(),
            "default".into(),
            vec![],
            Cron::parse("0 * * * *").unwrap(),
        )
        .unwrap();
        let now = at("2026-09-26T10:30:00Z");
        let mut stored = hourly.record(at("2026-09-26T07:10:00Z"));
        stored.record_run(millis(at("2026-09-26T08:00:00Z")), "job-8");
        // Down since 08:30: 09:00 and 10:00 were missed; only 10:00 runs.
        assert_eq!(
            hourly.first_due(&stored, now),
            Some(at("2026-09-26T10:00:00Z"))
        );
        // Someone already ran 10:00: wait for 11:00.
        stored.record_run(millis(at("2026-09-26T10:00:00Z")), "job-10");
        assert_eq!(
            hourly.first_due(&stored, now),
            Some(at("2026-09-26T11:00:00Z"))
        );
        // A schedule first registered at 10:20 didn't exist at 10:00.
        let new = hourly.record(at("2026-09-26T10:20:00Z"));
        assert_eq!(
            hourly.first_due(&new, now),
            Some(at("2026-09-26T11:00:00Z"))
        );
    }
}
