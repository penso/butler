# Redis Cluster: what support would take

butler's Redis backend targets a single Redis primary (with replicas and
Sentinel-style failover left to your deployment). It does **not** support
Redis Cluster today. This note records the audit behind that, the options, and
what each would cost, so the decision can be made deliberately.

## The rule Cluster imposes

A cluster routes every command, and every `EVAL`, to the node that owns the
hash slot of its keys. The Redis documentation is explicit about scripts
([Scripting with Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/)):

> all names of keys that a script accesses must be explicitly provided as input
> key arguments. [...] Scripts **should never** access keys with
> programmatically-generated names or based on the contents of data structures
> stored in the database.

Transactions (`MULTI`/`EXEC`) and multi-key commands must also stay within one
slot. Keys can be forced into one slot with a hash tag: only the part between
`{` and `}` is hashed, so `{butler}:queue:a` and `{butler}:job:1` share a slot.

## Audit of the Lua scripts

In `crates/butler/src/backend/redis.rs`. "Data-dependent" means the key name
comes from something the script reads (an id popped from a list, a job hash's
`queue`, `ckey` or `ukey` field), so the client can't declare it in advance.

| Script | Declared `KEYS` | Keys built inside Lua | Data-dependent |
|---|---|---|---|
| `RECORD_METRIC` | metric hash, both counters | none | no |
| `CHECKPOINT_IF_HELD` | processing list, job hash | none | no |
| `PUSH_RECURRING` | tick, schedule, job, queue, queue set | none | no |
| `PRUNE_ACTIVE` | the queue's active set | `job:<id>` for ids passed in `ARGV` | no (could be declared) |
| `CLEAN_DEAD` | dead list | `job:<id>` of each popped id, `dead:legacy_finished_at` | yes |
| `CLAIM` | queue, processing list, slots, active | `job:<id>` of the popped id; `running:<ckey>`, `blocked:<ckey>`, `unique:<ukey>` from its hash; `blocked`, `blocked_keys` | yes |
| `RECOVER_ONE` | processing list | `job:<id>` of the popped id; `queue:<q>`, `slots:<q>`, `active:<q>` from its hash; plus `release` | yes |
| `PROMOTE_DUE` | scheduled set | `job:<id>` of each due id; `queue:<q>` from its hash | yes |
| `RUN_NOW` | scheduled set | `job:<id>`; `queue:<q>` from its hash | yes |
| `CANCEL` | **none** | `job:<id>`, `scheduled`; `queue:<q>`, `blocked:<ckey>`, `unique:<ukey>` from its hash; `blocked`, `recent:cancelled` | yes |
| `PUSH_UNIQUE` | **none** | `unique:<key>`, `job:<id>`, `queues`, `scheduled`, `queue:<q>`; `job:<holder>` from the lock | yes |
| `RELEASE` (and `release()` inside others) | **none** | `job:<id>`; `running:<ckey>`, `blocked:<ckey>`, `unique:<ukey>` from its hash; the parked id's `job:` and `queue:<q>`; `blocked` | yes |

Three scripts are fully declared. Eight build keys from data they read, and
four call sites (`CANCEL`, `PUSH_UNIQUE`, and `RELEASE` in the complete and
fail transactions) declare no key at all, so a cluster client would send them
to a random node.

Outside scripts, twelve `MULTI`/`EXEC` transactions touch several keys (a
push writes the job hash, the queue list and the `queues` set), and `stats()`
pipelines reads across every queue and worker. Pub/sub is fine: classic
`PUBLISH` is broadcast cluster-wide, so one subscriber on any node hears it.
Nothing uses `KEYS`, `SCAN` or blocking list commands.

**Conclusion: full support with hash tags *and* declared keys is not
achievable without redesigning the scripts.** The data-dependent keys are the
core of the atomicity guarantees: a claim pops an id and reads its concurrency
key in the same step, recovery pops an id and requeues it on the queue its hash
names, and so on.

## Options

### A. One hash tag for everything (single-slot cluster)

Put every key under one hash tag, e.g. prefix `{butler}` (the prefix is already
configurable: `[queue.redis] prefix`). All keys then live in one slot, so the
scripts' undeclared keys are on the node that runs them.

What it takes:

- Declare at least one key in every script call, so a cluster client routes it
  (the four zero-key call sites above).
- A cluster client: redis-rs's `cluster` feature and `ClusterClient`, pooled
  like today's connections (`ClusterConnection` implements `ConnectionLike`,
  and routes a `MULTI` pipeline to one node), plus configuration for several
  seed URLs and validation that the prefix contains a hash tag.
- The subscriber on one node's plain connection.
- Every backend test in `crates/butler/tests/backends.rs` run against a real
  cluster, including resharding and failover, and a cluster in CI (for
  example three `redis-server --cluster-enabled yes` nodes created with
  `redis-cli --cluster create` on the Linux runner, with host networking).

What it gives: compatibility with cluster-mode deployments (managed services
that only offer cluster mode) and their failover. What it doesn't: any
horizontal scaling, since all of butler's data sits on one primary. And it
relies on Redis tolerating undeclared keys in the same slot, which the
documentation tells scripts not to do. (Scripts with a `#!lua` shebang lose
cross-slot access; butler's have none.) BullMQ takes this approach with a
hash-tagged prefix per queue.

### B. A hash tag per queue (sharded)

Give each queue its own slot (`{butler:mailers}:...`), as BullMQ does, so
queues spread across nodes. Everything that spans queues has to be redone:

- the job hash must live in its queue's slot, so looking a job up by id alone
  (`JobHandle`, the dashboard) needs the queue in the id or a global index;
- per-worker processing lists, the scheduled set and the dead list become
  per queue, and recovery, promotion and cleanup walk every queue;
- concurrency keys (`running:`, `blocked:`) and unique keys can be shared by
  jobs on different queues (`prepare(..).on_queue(..)`), so they'd need their
  own slot and a two-step protocol instead of one script;
- global state (`workers`, `paused`, `recurring`, metrics) sits elsewhere and is
  updated in separate steps.

This is a new backend in all but name, and each atomicity argument in
`redis.rs` would have to be made again.

### C. Declare everything, two steps per operation

Read what a script needs first (a job's queue and keys), then run the script
with every key declared, verifying inside that what was read hasn't changed
and retrying if it has. A claim can't know which id it will pop, so it would
become "peek, then claim that id". It keeps one slot per operation legal
without hash tags, but multiplies round trips on the hot path, and it still
needs every key of one operation in one slot, which brings back option A or B.

## Upgrade story, for whichever option

Key names change (`butler:queue:x` becomes `{butler}:queue:x`), so existing
data doesn't carry over by itself. Standalone deployments can keep their
prefix and never notice. Moving to a cluster means either draining (stop
enqueuing, let workers empty the queues and the scheduled set, then switch
every process to the new configuration at once) or a one-off migration that
copies each key under the new name (`DUMP`/`RESTORE`, since `RENAME` can't
cross slots) while nothing runs.

## Recommendation

Keep Redis Cluster unsupported until someone needs it, and say so. If
cluster-mode compatibility is needed, option A is the smallest correct step
(roughly: the zero-key call sites, a cluster connection type, configuration,
and a cluster CI job), with its limits documented. Option B only makes sense
with a demonstrated need to spread one butler deployment across shards.
