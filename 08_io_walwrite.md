# Aurora PostgreSQL Wait Event: `IO:WALWrite`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | IO |
| **Wait Event**   | WALWrite |
| **IO Object**    | Write-Ahead Log (WAL) segment files |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | Medium–High |
| **Aurora Specific** | Aurora uses a fundamentally different WAL architecture — WAL is sent to storage nodes, not written locally. `IO:WALWrite` behavior and remediation differ significantly. |

---

## Description

`IO:WALWrite` occurs when a backend is waiting for WAL (Write-Ahead Log) records to be written to the WAL buffer and/or flushed to durable storage. WAL is the foundational durability mechanism in PostgreSQL: **no data modification is complete until its WAL record is durable**.

### Standard PostgreSQL WAL Path

```
Backend writes data
   └─ XLogInsert() — insert WAL record into WAL buffers (in shared memory)
       └─ XLogWrite() — write WAL buffers to WAL segment files
           └─ XLogFlush() — fsync WAL to durable storage
               └─ wait event: IO:WALWrite ← backend blocked here
```

### Aurora's WAL Architecture (Key Difference)

In Aurora PostgreSQL, **WAL is not written to local disk**. Instead:

```
Backend writes data
   └─ XLogInsert() — insert WAL record
       └─ Aurora Storage Protocol
           └─ WAL sent to 6 Aurora Storage Nodes (across 3 AZs)
               └─ Acknowledged when 4/6 nodes confirm receipt (quorum write)
               └─ No local fsync needed — durability via distributed quorum
```

This means:
- `IO:WALWrite` in Aurora is actually **network I/O to storage nodes**, not disk I/O
- Aurora is inherently more durable than local-disk PostgreSQL
- `synchronous_commit` settings have different implications in Aurora

### What Triggers IO:WALWrite

- High write throughput (many INSERTs/UPDATEs/DELETEs per second)
- Large `wal_buffers` filling up (forces a write)
- Checkpoint operations flushing WAL
- Replication slots causing WAL retention pressure
- `synchronous_commit = on` (default) — backend waits for WAL flush before returning

---

## Sample Data

### What You'll See in `pg_stat_activity`

```sql
SELECT
    pid, usename, state,
    wait_event_type, wait_event,
    now() - query_start AS wait_duration,
    left(query, 200) AS query
FROM pg_stat_activity
WHERE wait_event = 'WALWrite'
ORDER BY wait_duration DESC;
```

**Sample Output (bulk insert workload):**

```
 pid  | usename | state  | wait_event_type | wait_event | wait_duration  | query
------+---------+--------+-----------------+------------+----------------+-------
 8801 | loader  | active | IO              | WALWrite   | 00:00:00.087   | INSERT INTO events SELECT * FROM staging
 8802 | appuser | active | IO              | WALWrite   | 00:00:00.043   | UPDATE orders SET processed = true ...
 8803 | appuser | active | IO              | WALWrite   | 00:00:00.031   | DELETE FROM audit_log WHERE age > 30
```

---

## Simulate the Wait Event

### Method 1: High-Throughput Bulk Insert

```sql
-- Generate massive WAL volume with rapid inserts
INSERT INTO events (user_id, event_type, payload, created_at)
SELECT
    (random() * 1000000)::INT,
    'pageview',
    jsonb_build_object('url', md5(random()::TEXT), 'ts', now()),
    now() - (random() * INTERVAL '365 days')
FROM generate_series(1, 10000000);
```

### Method 2: Concurrent Write Workload

```bash
# pgbench write-heavy workload
pgbench \
  -h aurora-cluster.xxxx.rds.amazonaws.com \
  -U appuser \
  -d postgres \
  --client=100 \
  --jobs=20 \
  --time=300 \
  --rate=10000 \   # 10,000 TPS target — saturates WAL
  postgres
```

### Method 3: Synchronous Commit Impact Demonstration

```sql
-- With synchronous_commit ON (default): wait for WAL flush every commit
SET synchronous_commit = on;
DO $$
BEGIN
    FOR i IN 1..100000 LOOP
        INSERT INTO test_walwrite (data) VALUES (md5(random()::TEXT));
        COMMIT;  -- Each commit waits for WAL flush
    END LOOP;
END;
$$;

-- With synchronous_commit OFF: no WAL flush wait
SET synchronous_commit = off;
DO $$
BEGIN
    FOR i IN 1..100000 LOOP
        INSERT INTO test_walwrite (data) VALUES (md5(random()::TEXT));
        COMMIT;  -- Returns immediately; WAL flushed asynchronously
    END LOOP;
END;
$$;
```

---

## Monitoring Queries

### 1. Real-Time WALWrite Waiters

```sql
SELECT
    COUNT(*) AS sessions_waiting,
    MAX(EXTRACT(EPOCH FROM (now() - query_start)) * 1000)::INT AS max_wait_ms,
    AVG(EXTRACT(EPOCH FROM (now() - query_start)) * 1000)::NUMERIC(10,2) AS avg_wait_ms
FROM pg_stat_activity
WHERE wait_event = 'WALWrite';
```

### 2. WAL Generation Rate

```sql
-- WAL volume generated since last reset
SELECT
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) AS total_wal_since_reset,
    pg_current_wal_lsn() AS current_lsn,
    pg_walfile_name(pg_current_wal_lsn()) AS current_wal_file;

-- WAL generation rate (requires two measurements)
-- Run this twice, 60 seconds apart:
SELECT pg_current_wal_lsn(), now();
-- Then:
SELECT
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), '<previous_lsn>')
    ) AS wal_generated_last_60s;
```

### 3. WAL Write and Flush Statistics

```sql
SELECT
    wal_records,
    wal_fpi,          -- Full-page writes (high after checkpoint)
    wal_bytes,
    pg_size_pretty(wal_bytes) AS wal_size,
    wal_buffers_full, -- Times WAL buffers filled and forced a write
    wal_write,        -- Total WAL writes
    wal_sync,         -- Total WAL syncs (fsyncs)
    wal_write_time,   -- Milliseconds spent writing WAL
    wal_sync_time,    -- Milliseconds spent syncing WAL
    NOW() - stats_reset AS stats_age
FROM pg_stat_wal;
-- Available in PostgreSQL 14+
```

### 4. Checkpoint Frequency and WAL Pressure

```sql
SELECT
    checkpoints_timed,
    checkpoints_req,              -- Requested checkpoints (bad if high)
    checkpoint_write_time,
    checkpoint_sync_time,
    buffers_checkpoint,
    buffers_clean,
    buffers_backend,              -- Direct backend writes (buffer pressure)
    ROUND(checkpoints_req::NUMERIC /
          NULLIF(checkpoints_timed + checkpoints_req, 0) * 100, 2) AS forced_checkpoint_pct,
    NOW() - stats_reset AS stats_age
FROM pg_stat_bgwriter;
```

> **Key signal:** `checkpoints_req` > 10% of total checkpoints means WAL is filling up too fast. Increase `max_wal_size` or `checkpoint_completion_target`.

### 5. Replication Slot WAL Retention

```sql
-- Replication slots can force WAL retention, causing WAL bloat
SELECT
    slot_name,
    plugin,
    slot_type,
    active,
    restart_lsn,
    confirmed_flush_lsn,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
    ) AS retained_wal_size,
    pg_blocking_pids(active_pid) AS blocking
FROM pg_replication_slots
ORDER BY pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;
```

### 6. Top WAL-Generating Statements

```sql
SELECT
    left(query, 150) AS query,
    calls,
    wal_records,
    wal_bytes,
    pg_size_pretty(wal_bytes) AS wal_size,
    ROUND(wal_bytes::NUMERIC / calls) AS avg_wal_bytes_per_call
FROM pg_stat_statements
WHERE wal_bytes > 0
ORDER BY wal_bytes DESC
LIMIT 20;
-- Requires pg_stat_statements with track_wal = on (PG14+)
```

---

## Resolution & Tuning

### 1. Tune `wal_buffers`

WAL buffers are the in-memory staging area for WAL records. Too small → frequent forced writes.

```sql
-- Aurora parameter group
-- Default: -1 (auto = 1/32 of shared_buffers, max 16MB)
-- Recommended for high-write workloads: 64MB or 128MB
wal_buffers = 67108864   -- 64MB in bytes

-- Verify:
SHOW wal_buffers;
```

### 2. Batch Writes to Reduce Commit Frequency

```python
# Bad: 1,000,000 individual commits = 1,000,000 WAL flushes
for row in data:
    cur.execute("INSERT INTO events ...", row)
    conn.commit()

# Good: Batch commits = far fewer WAL flushes
BATCH_SIZE = 10000
for i, row in enumerate(data):
    cur.execute("INSERT INTO events ...", row)
    if i % BATCH_SIZE == 0:
        conn.commit()

# Best: COPY for bulk inserts (single WAL record for entire COPY)
cur.copy_expert("COPY events FROM STDIN WITH (FORMAT CSV)", csv_file)
conn.commit()
```

### 3. Use `synchronous_commit = off` for Non-Critical Writes

```sql
-- Risk: up to wal_writer_delay (200ms default) of data loss on crash
-- Benefit: 2-10x write throughput improvement for appropriate workloads

-- Per session (for this transaction only):
SET synchronous_commit = off;
INSERT INTO analytics_events ...;
COMMIT;  -- Returns immediately without waiting for WAL flush

-- Per table (via ALTER TABLE or SET LOCAL in transaction):
ALTER TABLE session_tracking SET (autovacuum_enabled = off);

-- Aurora-specific consideration:
-- Aurora's quorum writes provide durability even with synchronous_commit=off
-- because WAL is durably stored in 4/6 storage nodes regardless
```

### 4. Tune Checkpoint Parameters

```sql
-- Aurora parameter group
max_wal_size = 4096        -- 4GB (increase to reduce checkpoint frequency)
checkpoint_completion_target = 0.9  -- Spread checkpoint I/O over 90% of interval
checkpoint_timeout = 300    -- 5 minutes between checkpoints (increase if needed)

-- Check if WAL size is adequate:
-- If checkpoints_req > 0 frequently, increase max_wal_size
```

### 5. Use `UNLOGGED` Tables for Ephemeral Data

```sql
-- Unlogged tables don't write WAL — perfect for temp/staging data
CREATE UNLOGGED TABLE staging_events (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Load data into unlogged table (no WAL = very fast)
COPY staging_events FROM '/tmp/events.csv' WITH (FORMAT CSV);

-- Then move to permanent table in one transaction
INSERT INTO events SELECT * FROM staging_events;
TRUNCATE staging_events;
```

### 6. Aurora-Specific: Use I/O Optimized Storage

For Aurora clusters with consistently high WAL-related I/O:

```bash
# Switch to Aurora I/O Optimized (no per-I/O charges; predictable cost at high I/O)
aws rds modify-db-cluster \
  --db-cluster-identifier my-aurora-cluster \
  --storage-type aurora-iopt1 \
  --apply-immediately
```

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | High write throughput; small `wal_buffers`; frequent single-row commits |
| **Risk** | Write latency increases; checkpoint pressure; storage I/O saturation |
| **Quick Fix** | Batch commits; reduce commit frequency |
| **Long-Term Fix** | Tune `wal_buffers`; `synchronous_commit=off` where safe; COPY for bulk; UNLOGGED tables |
| **Aurora Note** | WAL goes to distributed storage, not disk; `synchronous_commit=off` is still safe in Aurora due to quorum writes |
| **Key Parameters** | `wal_buffers`, `synchronous_commit`, `max_wal_size`, `checkpoint_completion_target` |
