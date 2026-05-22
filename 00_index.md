# Aurora PostgreSQL Wait Events — Advanced Reference Guide

A comprehensive reference for diagnosing, simulating, monitoring, and resolving the top 10 Aurora PostgreSQL wait events.

---

## Index

| # | File | Wait Event | Type | Criticality | Primary Cause |
|---|------|-----------|------|-------------|---------------|
| 1 | `01_client_clientread.md` | `Client:ClientRead` | Client | Medium–High | App slow to send next query |
| 2 | `02_client_clientwrite.md` | `Client:ClientWrite` | Client | Medium | Client slow to consume result set |
| 3 | `03_lock_relation.md` | `Lock:relation` | Lock | High | DDL holding ACCESS EXCLUSIVE lock |
| 4 | `04_lock_transactionid.md` | `Lock:transactionid` | Lock | High | Row-level contention; transactions waiting on XID |
| 5 | `05_lock_tuple.md` | `Lock:tuple` | Lock | High | Hot-row contention; many waiters on one row |
| 6 | `06_lwlock_buffermapping.md` | `LWLock:BufferMapping` | LWLock | High | Buffer pool hash table contention |
| 7 | `07_io_datafileread.md` | `IO:DataFileRead` | IO | High | Cache miss; page read from Aurora storage |
| 8 | `08_io_walwrite.md` | `IO:WALWrite` | IO | Medium–High | High write throughput; WAL flush pressure |
| 9 | `09_cpu.md` | `CPU` | CPU | Medium–High | Unoptimized queries; seq scans; parallel storms |
| 10 | `10_timeout_pgsleep.md` | `Timeout:PgSleep` | Timeout | Low–Medium | Explicit pg_sleep() calls |

---

## Quick Diagnosis Query

Run this first to identify which wait events are currently active on your Aurora cluster:

```sql
SELECT
    COALESCE(wait_event_type, 'Running')   AS wait_type,
    COALESCE(wait_event, '—')              AS wait_event,
    state,
    COUNT(*)                               AS sessions,
    MAX(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS max_wait_secs,
    AVG(EXTRACT(EPOCH FROM (now() - query_start)))::NUMERIC(10,2) AS avg_wait_secs
FROM pg_stat_activity
WHERE pid != pg_backend_pid()
  AND state != 'idle'
GROUP BY 1, 2, 3
ORDER BY sessions DESC, max_wait_secs DESC;
```

---

## Quick Fix Cheat Sheet

### Terminate a Blocking Session

```sql
-- Non-graceful: immediate disconnect
SELECT pg_terminate_backend(<blocking_pid>);

-- Graceful: cancel current query only
SELECT pg_cancel_backend(<blocking_pid>);

-- Find and terminate the root blocker automatically
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid IN (
    SELECT unnest(pg_blocking_pids(pid))
    FROM pg_stat_activity
    WHERE wait_event_type = 'Lock'
)
AND pid != pg_backend_pid();
```

### Emergency Parameter Overrides (Per Session)

```sql
SET lock_timeout = '3s';                        -- Fail fast on lock waits
SET statement_timeout = '30s';                  -- Kill runaway queries
SET idle_in_transaction_session_timeout = '30s'; -- Kill idle-in-txn sessions
SET max_parallel_workers_per_gather = 0;         -- Disable parallel query (CPU relief)
SET work_mem = '64MB';                           -- Tune per-session sort memory
SET synchronous_commit = off;                    -- Reduce WALWrite pressure (with tradeoff)
SET enable_seqscan = off;                        -- Force index usage for testing
```

### Identify Root Blocker Chain

```sql
SELECT
    pid,
    pg_blocking_pids(pid) AS blocked_by,
    wait_event_type,
    wait_event,
    state,
    left(query, 150) AS query
FROM pg_stat_activity
WHERE cardinality(pg_blocking_pids(pid)) > 0
   OR pid IN (
       SELECT unnest(pg_blocking_pids(pid2))
       FROM pg_stat_activity AS sq2
       WHERE cardinality(pg_blocking_pids(sq2.pid)) > 0
   )
ORDER BY cardinality(pg_blocking_pids(pid)) DESC;
```

---

## Aurora-Specific Tips

| Feature | Recommendation |
|---------|---------------|
| **RDS Proxy** | Use for connection pooling (reduces ClientRead/ClientWrite) |
| **Performance Insights** | Enable for 7-day wait event history with per-query breakdown |
| **Blue/Green Deployments** | Use for zero-downtime DDL (eliminates Lock:relation spikes) |
| **pg_prewarm** | Enable `autoprewarm` to restore buffer cache after failover (reduces DataFileRead) |
| **I/O Optimized Storage** | Use `aurora-iopt1` for clusters with sustained high WALWrite/DataFileRead |
| **Read Replicas** | Route analytics queries to reader endpoint (reduces CPU on writer) |
| **Serverless v2** | Cold starts after scale-down cause DataFileRead spikes; use `pg_prewarm` |

---

## Key Parameters Reference

```sql
-- View current settings for all tuning parameters discussed in this guide
SELECT name, setting, unit, short_desc
FROM pg_settings
WHERE name IN (
    'shared_buffers',
    'effective_cache_size',
    'work_mem',
    'wal_buffers',
    'max_wal_size',
    'checkpoint_completion_target',
    'checkpoint_timeout',
    'synchronous_commit',
    'max_connections',
    'max_parallel_workers',
    'max_parallel_workers_per_gather',
    'deadlock_timeout',
    'lock_timeout',
    'statement_timeout',
    'idle_in_transaction_session_timeout',
    'tcp_keepalives_idle',
    'bgwriter_lru_maxpages',
    'default_statistics_target',
    'track_io_timing',
    'log_min_duration_statement'
)
ORDER BY name;
```
