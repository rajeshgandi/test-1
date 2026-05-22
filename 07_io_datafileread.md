# Aurora PostgreSQL Wait Event: `IO:DataFileRead`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | IO |
| **Wait Event**   | DataFileRead |
| **IO Object**    | Heap/index data files (relation blocks) |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | High |
| **Aurora Specific** | Aurora reads from distributed storage layer (not local disk); latency characteristics differ from EBS |

---

## Description

`IO:DataFileRead` occurs when a backend process is waiting for a **page to be read from storage into the shared buffer pool**. This is a **cache miss** — the requested page is not in `shared_buffers` and must be fetched from the underlying storage layer.

In standard PostgreSQL, this means EBS disk I/O. In **Aurora PostgreSQL**, this means a read from the **Aurora Distributed Storage Layer** — a 6-way replicated, SSD-backed storage system. Aurora reads are typically faster than EBS but still involve network I/O to the storage nodes.

### Read Path in Aurora

```
Backend Process
   └─ ReadBuffer(relation, blocknum)
       └─ BufferAlloc() — page NOT in shared_buffers (cache miss)
           └─ smgrread(relation, blocknum)
               └─ mdread()
                   └─ pread() system call
                       └─ [Community PG] → EBS disk read
                       └─ [Aurora PG]   → Network read from Aurora Storage Node
                                           (one of 6 storage nodes in the segment)
```

### Aurora-Specific Storage Architecture

```
Writer Instance
   └─ DataFileRead (cache miss)
       └─ Read Request → Aurora Storage Layer
           ├─ Segment A: Node 1 (AZ-a), Node 2 (AZ-a), Node 3 (AZ-b)
           └─ Segment B: Node 4 (AZ-b), Node 5 (AZ-c), Node 6 (AZ-c)

Aurora reads from the closest available storage node.
Typical Aurora storage read latency: ~1-5ms (vs 1-10ms EBS gp3, ~0.1ms NVMe local)
```

### Common Triggers

- Working set larger than `shared_buffers`
- Full table scans (sequential reads)
- Random I/O from unindexed queries
- Post-failover cache warming (cold start)
- VACUUM and autovacuum reading many blocks
- Aurora Serverless v2 scaling events (cold pages after scale-down)

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
WHERE wait_event = 'DataFileRead'
ORDER BY wait_duration DESC;
```

**Sample Output:**

```
 pid  | usename   | state  | wait_event_type | wait_event   | wait_duration  | query
------+-----------+--------+-----------------+--------------+----------------+-------
 7701 | analytics | active | IO              | DataFileRead | 00:02:15.334   | SELECT * FROM orders WHERE created_at > ...
 7702 | etl       | active | IO              | DataFileRead | 00:01:44.221   | SELECT o.*, c.name FROM orders o JOIN ...
 7703 | appuser   | active | IO              | DataFileRead | 00:00:05.112   | SELECT * FROM products WHERE category_id = 5
```

---

## Simulate the Wait Event

### Method 1: Full Table Scan with Cache Bypass

```sql
-- First, flush the buffer cache (requires pg_prewarm extension or restart)
-- On dev/test: use pg_buffercache to identify and avoid cached pages

-- Force sequential scan that bypasses index
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Run a query that touches many uncached pages
SELECT COUNT(*), AVG(amount)
FROM orders
WHERE status = 'completed'
  AND created_at BETWEEN '2020-01-01' AND '2023-12-31';
```

### Method 2: Cache Eviction via Large Scan

```bash
# Read the entire large table to evict other pages from cache
# Then query with a filter that was previously cached

# Step 1: Blow out the cache with a huge sequential scan
psql -c "SELECT COUNT(*) FROM very_large_audit_log;"

# Step 2: Now query the target table — most pages evicted from cache
psql -c "SELECT * FROM orders WHERE id = 12345;"
# DataFileRead will appear as Aurora re-fetches the evicted pages
```

### Method 3: pgbench with Random Access Pattern

```bash
# -M simple avoids prepared statement caching, maximizes page scatter
pgbench \
  -h aurora-cluster.xxxx.rds.amazonaws.com \
  -U appuser \
  -d postgres \
  --client=50 \
  --time=120 \
  -M simple \
  --select-only
```

### Method 4: Post-Failover Simulation

```bash
# Simulate a cold start / failover (all caches wiped)
# In Aurora: trigger a manual failover
aws rds failover-db-cluster \
  --db-cluster-identifier my-aurora-cluster \
  --target-db-instance-identifier my-aurora-replica

# Then immediately run normal workload — massive DataFileRead spike
pgbench -h new-writer-endpoint ... --client=100 --time=60
```

---

## Monitoring Queries

### 1. Real-Time DataFileRead Pressure

```sql
SELECT
    COUNT(*) AS sessions_waiting,
    MAX(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS max_wait_secs,
    AVG(EXTRACT(EPOCH FROM (now() - query_start)))::NUMERIC(10,2) AS avg_wait_secs,
    SUM(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS total_wait_secs
FROM pg_stat_activity
WHERE wait_event = 'DataFileRead';
```

### 2. Cache Hit Ratio Per Table (Find Cache-Miss Offenders)

```sql
SELECT
    relname,
    heap_blks_read,
    heap_blks_hit,
    idx_blks_read,
    idx_blks_hit,
    ROUND(heap_blks_hit::NUMERIC / NULLIF(heap_blks_hit + heap_blks_read, 0) * 100, 2) AS heap_hit_pct,
    ROUND(idx_blks_hit::NUMERIC  / NULLIF(idx_blks_hit  + idx_blks_read,  0) * 100, 2) AS idx_hit_pct
FROM pg_statio_user_tables
WHERE heap_blks_read + idx_blks_read > 0
ORDER BY heap_blks_read + idx_blks_read DESC
LIMIT 20;
```

### 3. Sequential Scan Candidates (Tables Causing Bulk Reads)

```sql
SELECT
    schemaname,
    relname AS table_name,
    seq_scan,
    seq_tup_read,
    ROUND(seq_tup_read::NUMERIC / NULLIF(seq_scan, 0)) AS avg_rows_per_scan,
    n_live_tup,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables
WHERE seq_scan > 10
  AND n_live_tup > 10000
ORDER BY seq_tup_read DESC
LIMIT 20;
```

### 4. Current I/O Load Per Query

```sql
-- Combine pg_stat_activity with pg_stat_statements for I/O profiling
SELECT
    s.query,
    s.calls,
    s.total_exec_time,
    s.blk_read_time,          -- Time blocked on I/O (requires track_io_timing=on)
    s.blk_write_time,
    s.shared_blks_read,       -- Cache misses (went to storage)
    s.shared_blks_hit,        -- Cache hits
    ROUND(s.shared_blks_read::NUMERIC /
          NULLIF(s.shared_blks_read + s.shared_blks_hit, 0) * 100, 2) AS miss_rate_pct,
    ROUND(s.blk_read_time / NULLIF(s.calls, 0), 2) AS avg_io_ms_per_call
FROM pg_stat_statements s
WHERE s.shared_blks_read > 1000
ORDER BY s.shared_blks_read DESC
LIMIT 20;
-- Requires: shared_preload_libraries = 'pg_stat_statements'
--           track_io_timing = on
```

### 5. Aurora-Specific: I/O Metrics via CloudWatch

```bash
# Key Aurora CloudWatch metrics for DataFileRead correlation
aws cloudwatch get-metric-statistics \
  --namespace AWS/RDS \
  --metric-name ReadIOPS \
  --dimensions Name=DBClusterIdentifier,Value=my-aurora-cluster \
  --start-time $(date -d '1 hour ago' -u +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 60 \
  --statistics Average

# Also check: ReadLatency, ReadThroughput, BufferCacheHitRatio
```

---

## Resolution & Tuning

### 1. Increase `shared_buffers` and `effective_cache_size`

```sql
-- Aurora parameter group
-- shared_buffers = 25% of instance RAM (increase to reduce cache misses)
-- effective_cache_size = 75% of instance RAM (helps planner prefer index scans)

-- Verify current settings
SELECT name, setting, unit
FROM pg_settings
WHERE name IN ('shared_buffers', 'effective_cache_size', 'work_mem');
```

### 2. Enable and Use `track_io_timing`

```sql
-- Aurora parameter group: track_io_timing = 1
-- After enabling, query I/O time per statement:

SELECT
    left(query, 100) AS query,
    calls,
    ROUND(blk_read_time::NUMERIC / calls, 2) AS avg_read_ms,
    shared_blks_read AS physical_reads
FROM pg_stat_statements
WHERE blk_read_time > 0
ORDER BY blk_read_time DESC
LIMIT 10;
```

### 3. Add Missing Indexes

```sql
-- Find tables with high sequential scan I/O
-- Then check for missing indexes with pg_stat_statements

SELECT
    schemaname,
    tablename,
    attname AS column_name,
    n_distinct,
    correlation  -- close to 1 = clustered, close to 0 = scattered
FROM pg_stats
WHERE tablename = 'orders'
  AND n_distinct > 100
ORDER BY n_distinct DESC;

-- Create index for high-cardinality columns used in WHERE clauses
CREATE INDEX CONCURRENTLY idx_orders_customer_status
ON orders (customer_id, status)
WHERE status != 'archived';  -- partial index reduces size
```

### 4. Use `pg_prewarm` After Failover

```sql
CREATE EXTENSION IF NOT EXISTS pg_prewarm;

-- Prewarm critical tables and indexes after Aurora failover
SELECT pg_prewarm('orders');
SELECT pg_prewarm('orders_pkey');
SELECT pg_prewarm('idx_orders_customer_status');

-- Automate in Aurora with pg_prewarm.autoprewarm = on
-- Saves and restores buffer state across restarts
```

### 5. Partition Large Tables

```sql
-- Range partition orders by month — queries only scan relevant partition
CREATE TABLE orders (
    id BIGSERIAL,
    customer_id INT,
    created_at TIMESTAMPTZ NOT NULL,
    amount NUMERIC(10,2),
    status TEXT
) PARTITION BY RANGE (created_at);

CREATE TABLE orders_2024_q1 PARTITION OF orders
FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE orders_2024_q2 PARTITION OF orders
FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
-- etc.

-- Query now only reads 1 partition's pages instead of all pages
SELECT * FROM orders WHERE created_at BETWEEN '2024-01-01' AND '2024-03-31';
```

### 6. Aurora-Specific: Instance Sizing

| Symptom | Recommendation |
|---------|---------------|
| Cache hit ratio < 95% | Scale up instance class (more RAM → bigger `shared_buffers`) |
| ReadLatency > 5ms consistently | Check storage node health; consider Aurora I/O Optimized |
| High reads after failover | Enable `pg_prewarm.autoprewarm`; use RDS Proxy for connection continuity |
| Seasonal spikes | Use Aurora Serverless v2 (auto-scales, but cache is cold on scale-up) |

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | Pages not in shared_buffers; full table scans; cold cache after failover |
| **Risk** | Query slowdowns; high Aurora storage I/O costs; latency spikes |
| **Quick Fix** | `pg_prewarm` hot tables; cancel large rogue scans |
| **Long-Term Fix** | Increase `shared_buffers`; add indexes; partition tables; `track_io_timing` |
| **Key Parameters** | `shared_buffers`, `effective_cache_size`, `track_io_timing`, `random_page_cost` |
