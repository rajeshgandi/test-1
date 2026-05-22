# Aurora PostgreSQL Wait Event: `LWLock:BufferMapping`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | LWLock |
| **Wait Event**   | BufferMapping (also seen as `buffer_mapping`) |
| **Lock Object**  | Shared buffer hash table partitions |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | High |
| **Aurora Specific** | Aurora's distributed storage changes I/O patterns; this LWLock is pure shared memory contention |

---

## Description

`LWLock:BufferMapping` occurs when a session is waiting to acquire a **lightweight lock on one of the 128 partitions of the shared buffer hash table**. This hash table maps `(database, relation, block_number)` tuples to buffer pool slots.

Every time a backend needs to access a page (read or write), it must:
1. Hash the page identifier to find which partition owns it
2. Acquire a **shared or exclusive LWLock** on that partition
3. Look up the buffer pool slot
4. Release the LWLock

Under high concurrency with many sessions accessing the same or adjacent hash partitions, contention on these LWLocks becomes a bottleneck.

### Internal Architecture

```
Shared Buffer Pool (shared_buffers)
  ├─ BufferDescriptors[0..N]    ← array of N buffer descriptors
  └─ BufferBlocks[0..N]         ← actual data pages

Buffer Mapping Hash Table
  ├─ Partition 0  (LWLock 0)   ← protects ~(N/128) hash buckets
  ├─ Partition 1  (LWLock 1)
  ├─ ...
  └─ Partition 127 (LWLock 127)

When accessing a page:
  hash(dbid, relid, blocknum) % 128  → selects partition
  AcquireLWLockShared(partition_lock) → wait event if contended
```

### When It Spikes

- **Small `shared_buffers`**: high eviction rate → every access is a hash table miss+update
- **Hot pages**: many sessions repeatedly accessing the same few blocks
- **Large working sets**: buffer pool thrashing → constant eviction and re-insertion
- **High parallel query concurrency**: many parallel workers scanning the same table
- **Bloated tables**: many blocks to visit, many hash table lookups

---

## Sample Data

### What You'll See in `pg_stat_activity`

```sql
SELECT
    pid, usename, application_name,
    state, wait_event_type, wait_event,
    now() - query_start AS wait_duration,
    left(query, 200) AS query
FROM pg_stat_activity
WHERE wait_event = 'BufferMapping'
ORDER BY wait_duration DESC;
```

**Sample Output (high concurrency scan workload):**

```
 pid  | usename | application_name | state  | wait_event_type | wait_event    | wait_duration | query
------+---------+------------------+--------+-----------------+---------------+---------------+-------
 6601 | appuser | analytics        | active | LWLock          | BufferMapping | 00:00:00.034  | SELECT COUNT(*) FROM events ...
 6602 | appuser | analytics        | active | LWLock          | BufferMapping | 00:00:00.031  | SELECT COUNT(*) FROM events ...
 6603 | appuser | analytics        | active | LWLock          | BufferMapping | 00:00:00.028  | SELECT COUNT(*) FROM events ...
 ... (50+ sessions) ...
```

> **Note:** Individual wait durations are often milliseconds, but the **cumulative impact** on throughput is severe. Look for many sessions simultaneously in this state.

---

## Simulate the Wait Event

### Method 1: Parallel Scans on a Hot Table

```bash
# Set shared_buffers small to increase eviction pressure, then hammer a table
# First, temporarily reduce shared_buffers (requires Aurora parameter group change):
# shared_buffers = 128MB  (instead of typical 25% of RAM)

# Then run many parallel full table scans
for i in $(seq 1 50); do
  psql -c "SELECT COUNT(*) FROM large_events_table;" &
done
wait
```

### Method 2: Cache-Busting Workload (Python)

```python
import psycopg2
import threading

DSN = "host=aurora-cluster... dbname=postgres user=appuser"

def random_page_access():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    while True:
        # Random access pattern → maximum hash table contention
        cur.execute("SELECT * FROM large_table OFFSET random()*1000000 LIMIT 1")
        cur.fetchone()

threads = [threading.Thread(target=random_page_access) for _ in range(100)]
for t in threads: t.start()
```

### Method 3: pgbench with Large Scale Factor

```bash
pgbench -i -s 1000 postgres  # ~15GB database
pgbench \
  -h aurora-cluster.xxxx.rds.amazonaws.com \
  -U appuser \
  -d postgres \
  --client=200 \
  --jobs=20 \
  --time=300 \
  --no-vacuum
```

---

## Monitoring Queries

### 1. Real-Time BufferMapping Contention Count

```sql
SELECT
    wait_event_type,
    wait_event,
    COUNT(*) AS sessions,
    MAX(EXTRACT(EPOCH FROM (now() - query_start)) * 1000)::INT AS max_wait_ms
FROM pg_stat_activity
WHERE wait_event = 'BufferMapping'
GROUP BY wait_event_type, wait_event;
```

### 2. Buffer Pool Efficiency (Cache Hit Ratio)

```sql
SELECT
    sum(heap_blks_read)  AS heap_blks_read,
    sum(heap_blks_hit)   AS heap_blks_hit,
    sum(idx_blks_read)   AS idx_blks_read,
    sum(idx_blks_hit)    AS idx_blks_hit,
    ROUND(
        sum(heap_blks_hit)::NUMERIC /
        NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100, 2
    ) AS heap_cache_hit_pct,
    ROUND(
        sum(idx_blks_hit)::NUMERIC /
        NULLIF(sum(idx_blks_hit) + sum(idx_blks_read), 0) * 100, 2
    ) AS idx_cache_hit_pct
FROM pg_statio_user_tables;
```

### 3. Tables with Highest Buffer Usage

```sql
SELECT
    c.relname AS table_name,
    count(*) AS buffers_used,
    ROUND(count(*) * 8.0 / 1024, 2) AS size_mb,
    ROUND(count(*) * 100.0 /
        (SELECT setting::INT FROM pg_settings WHERE name = 'shared_buffers'), 2
    ) AS pct_of_shared_buffers
FROM pg_buffercache b
JOIN pg_class c ON c.oid = b.relfilenode
JOIN pg_database d ON d.oid = b.reldatabase
WHERE d.datname = current_database()
GROUP BY c.relname
ORDER BY buffers_used DESC
LIMIT 20;
-- Requires: CREATE EXTENSION pg_buffercache;
```

### 4. Buffer Eviction Rate

```sql
-- pg_stat_bgwriter shows buffer eviction activity
SELECT
    buffers_clean,       -- by bgwriter
    buffers_backend,     -- by backends (bad — means backend is doing I/O)
    buffers_alloc,       -- total allocated (proxy for eviction rate)
    maxwritten_clean,    -- bgwriter was throttled (bad)
    ROUND(buffers_backend::NUMERIC / NULLIF(buffers_alloc, 0) * 100, 2) AS backend_write_pct,
    NOW() - stats_reset AS stats_age
FROM pg_stat_bgwriter;
```

> **Key insight:** High `buffers_backend` means backends are directly evicting and writing buffers — a strong signal that `shared_buffers` is too small or the working set doesn't fit.

### 5. LWLock Contention via pg_stat_activity Aggregation

```sql
-- Aggregate all LWLock waits to spot the highest-contention locks
SELECT
    wait_event_type,
    wait_event,
    COUNT(*) AS sessions,
    MAX(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS max_wait_secs
FROM pg_stat_activity
WHERE wait_event_type = 'LWLock'
  AND state != 'idle'
GROUP BY wait_event_type, wait_event
ORDER BY sessions DESC
LIMIT 10;
```

---

## Resolution & Tuning

### 1. Increase `shared_buffers` (Primary Fix)

```sql
-- Aurora parameter group
-- Typical recommendation: 25% of instance RAM
-- For r6g.4xlarge (128GB RAM): shared_buffers = 32GB

-- Check current value
SHOW shared_buffers;

-- Check effective_cache_size (should be ~75% of RAM)
SHOW effective_cache_size;
```

> **Aurora note:** Aurora uses its distributed storage layer, so `shared_buffers` tuning has a slightly different impact than community PostgreSQL. However, increasing it still reduces buffer mapping contention significantly by keeping more pages in the local buffer pool.

### 2. Reduce Working Set Size

```sql
-- Partition large tables to reduce the number of blocks being accessed
CREATE TABLE events_2024 PARTITION OF events
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Add partial indexes to reduce index page scatter
CREATE INDEX CONCURRENTLY idx_events_recent
ON events (created_at, user_id)
WHERE created_at > now() - INTERVAL '90 days';
```

### 3. Use `pg_prewarm` to Keep Hot Data in Cache

```sql
-- Install extension
CREATE EXTENSION pg_prewarm;

-- Prewarm a table into shared_buffers after instance restart
SELECT pg_prewarm('orders');
SELECT pg_prewarm('orders_pkey');

-- Or use autoprewarm (Aurora pg14+)
-- Set in parameter group: pg_prewarm.autoprewarm = on
```

### 4. Reduce Full Table Scans

```sql
-- Identify tables with high sequential scan rates
SELECT
    schemaname,
    relname,
    seq_scan,
    idx_scan,
    n_live_tup,
    ROUND(seq_scan::NUMERIC / NULLIF(seq_scan + idx_scan, 0) * 100, 2) AS seq_scan_pct
FROM pg_stat_user_tables
WHERE seq_scan > 100
ORDER BY seq_scan DESC
LIMIT 20;

-- Add indexes for frequently scanned columns
CREATE INDEX CONCURRENTLY idx_events_user_created
ON events (user_id, created_at DESC);
```

### 5. Tune `bgwriter` to Reduce Backend I/O

```sql
-- Aurora parameter group
bgwriter_lru_maxpages = 200       -- Write more pages per round (default 100)
bgwriter_lru_multiplier = 4.0     -- More aggressive (default 2.0)
bgwriter_delay = 50               -- More frequent runs (default 200ms)
```

### 6. Consider Larger Instance Class

For `LWLock:BufferMapping` caused by genuine working-set pressure:

| Root Cause | Fix |
|-----------|-----|
| Working set > `shared_buffers` | Scale up instance (more RAM → bigger `shared_buffers`) |
| Too many parallel workers | Reduce `max_parallel_workers_per_gather` |
| Full table scans | Add indexes; partition tables |
| Hot pages (index root) | Increase `random_page_cost`; tune `effective_cache_size` |

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | Buffer pool too small; hot pages; excessive eviction; parallel scan storms |
| **Risk** | Global throughput degradation; all concurrent sessions slow down |
| **Quick Fix** | Reduce concurrency temporarily; kill long-running scans |
| **Long-Term Fix** | Increase `shared_buffers`; add indexes; partition large tables; pg_prewarm |
| **Key Parameters** | `shared_buffers`, `effective_cache_size`, `bgwriter_lru_maxpages` |
