# Aurora PostgreSQL Wait Event: `CPU`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | CPU |
| **Wait Event**   | CPU |
| **Nature**       | Active computation — not truly a "wait" |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | Medium–High (depends on utilization level) |
| **Aurora Specific** | Aurora shares vCPUs across the writer and reader instances; CPU saturation has cluster-wide impact |

---

## Description

`CPU` is reported in `pg_stat_activity` when a session is **actively using the CPU** — it is not blocked on I/O, locks, or any other resource. While it is listed as a "wait event," it actually means the session is doing useful (or wasteful) computation.

Seeing many sessions in `CPU` state indicates the **database CPU is saturated** or individual queries are consuming excessive processor time.

### What CPU-Intensive Operations Look Like Internally

```
Backend CPU consumption comes from:
  ├─ Query planning (complex joins, many partitions, subqueries)
  ├─ Query execution
  │   ├─ Sequential scans (reading pages + evaluating predicates)
  │   ├─ Sort operations (quicksort in work_mem)
  │   ├─ Hash joins (building and probing hash tables)
  │   ├─ Aggregation (GROUP BY, DISTINCT, window functions)
  │   ├─ String/regex/JSON processing
  │   └─ Expression evaluation
  ├─ VACUUM and autovacuum
  ├─ Index builds (CREATE INDEX)
  └─ Cryptography (SSL handshakes, pgcrypto)
```

### When CPU Becomes a Problem

| Scenario | Symptom |
|----------|---------|
| Missing indexes → full table scans | Many sessions in CPU doing seq scans |
| Bad query plans | Single session consuming 100% of a vCPU |
| N+1 queries | Many short-lived CPU bursts from many connections |
| Unparameterized queries | High CPU from repeated query planning |
| Bloated tables | More pages to scan = more CPU |
| Aggressive autovacuum | CPU spikes from background cleanup |
| Parallel query storms | All vCPUs consumed by parallel workers |

---

## Sample Data

### What You'll See in `pg_stat_activity`

```sql
SELECT
    pid, usename, application_name,
    state, wait_event_type, wait_event,
    now() - query_start AS runtime,
    left(query, 200) AS query
FROM pg_stat_activity
WHERE wait_event_type = 'CPU'
   OR (state = 'active' AND wait_event IS NULL)
ORDER BY runtime DESC;
```

**Sample Output:**

```
 pid  | usename   | application_name | state  | wait_event_type | wait_event | runtime         | query
------+-----------+------------------+--------+-----------------+------------+-----------------+-------
 9901 | analytics | bi-tool          | active | CPU             | CPU        | 00:04:32.123    | SELECT user_id, COUNT(*), AVG(amount) FROM orders GROUP BY user_id ...
 9902 | appuser   | webapp           | active | CPU             | CPU        | 00:00:05.441    | SELECT * FROM products WHERE description LIKE '%wireless%'
 9903 | appuser   | webapp           | active | CPU             | CPU        | 00:00:02.112    | SELECT json_agg(t) FROM large_table t WHERE ...
```

---

## Simulate the Wait Event

### Method 1: CPU-Intensive Sort + Aggregation

```sql
-- Force sorts and aggregations that consume significant CPU
-- Disable parallel query to isolate single-session CPU usage
SET max_parallel_workers_per_gather = 0;

EXPLAIN ANALYZE
SELECT
    user_id,
    DATE_TRUNC('day', created_at) AS day,
    COUNT(*) AS events,
    SUM(amount) AS total,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) AS p95_amount
FROM orders
WHERE created_at > now() - INTERVAL '2 years'
GROUP BY user_id, DATE_TRUNC('day', created_at)
ORDER BY total DESC;
```

### Method 2: Regex / String Processing

```sql
-- Regex evaluation is CPU-intensive
SELECT *
FROM users
WHERE email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
  AND bio ~* 'senior|principal|staff|architect|lead';
```

### Method 3: JSON Processing at Scale

```sql
-- jsonb processing is CPU-heavy
SELECT
    id,
    payload->>'user_id' AS user_id,
    payload->'metadata'->>'source' AS source,
    jsonb_array_length(payload->'items') AS item_count,
    (SELECT SUM((item->>'price')::NUMERIC)
     FROM jsonb_array_elements(payload->'items') AS item) AS total_price
FROM events
WHERE payload @> '{"type": "purchase"}'
  AND created_at > now() - INTERVAL '30 days';
```

### Method 4: Parallel Query Storm

```sql
-- Enable aggressive parallelism to saturate all vCPUs
SET max_parallel_workers_per_gather = 8;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;

-- Now run a heavy analytical query
SELECT
    product_category,
    region,
    COUNT(DISTINCT customer_id),
    SUM(amount)
FROM orders o
JOIN customers c ON c.id = o.customer_id
JOIN products p ON p.id = o.product_id
WHERE o.created_at > '2023-01-01'
GROUP BY 1, 2
ORDER BY 4 DESC;
```

---

## Monitoring Queries

### 1. Top CPU-Consuming Queries Right Now

```sql
SELECT
    pid,
    usename,
    application_name,
    state,
    wait_event_type,
    wait_event,
    EXTRACT(EPOCH FROM (now() - query_start))::INT AS runtime_secs,
    left(query, 300) AS query
FROM pg_stat_activity
WHERE state = 'active'
  AND query NOT LIKE '%pg_stat_activity%'
ORDER BY runtime_secs DESC
LIMIT 20;
```

### 2. CPU-Heavy Queries via pg_stat_statements

```sql
SELECT
    left(query, 200) AS query,
    calls,
    ROUND(total_exec_time::NUMERIC / 1000, 2) AS total_exec_secs,
    ROUND(mean_exec_time::NUMERIC, 2) AS mean_exec_ms,
    ROUND(stddev_exec_time::NUMERIC, 2) AS stddev_exec_ms,
    rows,
    shared_blks_hit + shared_blks_read AS total_blks,
    -- CPU = total time minus I/O time (approximation)
    ROUND((total_exec_time - blk_read_time - blk_write_time)::NUMERIC / 1000, 2) AS cpu_secs
FROM pg_stat_statements
WHERE calls > 10
ORDER BY (total_exec_time - blk_read_time - blk_write_time) DESC
LIMIT 20;
-- Requires: track_io_timing = on for accurate CPU isolation
```

### 3. Autovacuum CPU Pressure

```sql
SELECT
    pid,
    usename,
    wait_event_type,
    wait_event,
    now() - query_start AS runtime,
    left(query, 200) AS query
FROM pg_stat_activity
WHERE query LIKE 'autovacuum:%'
  OR query LIKE 'autoanalyze:%'
ORDER BY runtime DESC;

-- Also check which tables are being vacuumed
SELECT
    schemaname,
    relname,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    n_dead_tup,
    n_live_tup,
    ROUND(n_dead_tup::NUMERIC / NULLIF(n_live_tup, 0) * 100, 2) AS dead_tup_pct
FROM pg_stat_user_tables
WHERE n_dead_tup > 10000
ORDER BY n_dead_tup DESC
LIMIT 20;
```

### 4. Query Plan Quality (Detect Inefficient Plans)

```sql
-- Find queries with high rows-estimated vs rows-actual discrepancy
-- indicating bad statistics → bad plans → wasted CPU
SELECT
    left(query, 200) AS query,
    calls,
    rows AS total_rows_returned,
    ROUND(rows::NUMERIC / calls) AS avg_rows_per_call,
    shared_blks_read + shared_blks_hit AS total_blks,
    ROUND((shared_blks_read + shared_blks_hit)::NUMERIC / NULLIF(rows, 0), 2) AS blks_per_row
FROM pg_stat_statements
WHERE calls > 100
  AND rows > 0
ORDER BY blks_per_row DESC  -- High blocks-per-row = inefficient scan
LIMIT 20;
```

### 5. Parallel Worker CPU Saturation

```sql
-- Check if parallel workers are overwhelming the CPU
SELECT
    pid,
    leader_pid,
    usename,
    state,
    wait_event_type,
    wait_event,
    left(query, 150) AS query
FROM pg_stat_activity
WHERE leader_pid IS NOT NULL  -- parallel worker processes
ORDER BY leader_pid, pid;

-- Count parallel workers per leader query
SELECT
    leader_pid,
    COUNT(*) AS worker_count,
    left(min(query), 150) AS query
FROM pg_stat_activity
WHERE leader_pid IS NOT NULL
GROUP BY leader_pid
ORDER BY worker_count DESC;
```

---

## Resolution & Tuning

### 1. Identify and Optimize Slow Queries (Primary Fix)

```sql
-- Find top 10 CPU-consuming queries and EXPLAIN ANALYZE them
SELECT
    left(query, 100) AS query_snippet,
    calls,
    ROUND(mean_exec_time::NUMERIC, 2) AS mean_ms,
    ROUND(total_exec_time::NUMERIC / 1000, 2) AS total_secs
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;

-- For each offender:
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT /* your slow query here */;
```

### 2. Fix Sequential Scans with Indexes

```sql
-- Identify seqscan-heavy tables
SELECT relname, seq_scan, idx_scan,
       ROUND(seq_scan::NUMERIC / NULLIF(seq_scan + idx_scan, 0) * 100, 2) AS seq_pct
FROM pg_stat_user_tables
WHERE seq_scan > idx_scan AND n_live_tup > 10000
ORDER BY seq_scan DESC;

-- Add appropriate indexes
CREATE INDEX CONCURRENTLY idx_orders_created_status
ON orders (created_at DESC, status)
WHERE status IN ('pending', 'processing');
```

### 3. Use Parameterized Queries to Avoid Re-Planning

```python
# Bad: New plan every execution (high planning CPU overhead)
cur.execute(f"SELECT * FROM users WHERE email = '{email}'")

# Good: Plan cached after first execution
cur.execute("SELECT * FROM users WHERE email = %s", (email,))
```

```sql
-- Check planning vs execution time ratio
SELECT
    left(query, 100) AS query,
    calls,
    ROUND(total_plan_time::NUMERIC / 1000, 2) AS total_plan_secs,
    ROUND(total_exec_time::NUMERIC / 1000, 2) AS total_exec_secs,
    ROUND(total_plan_time::NUMERIC / NULLIF(total_exec_time, 0) * 100, 2) AS plan_pct
FROM pg_stat_statements
WHERE total_plan_time > 1000
ORDER BY total_plan_time DESC
LIMIT 10;
-- Requires pg_stat_statements with compute_query_id = on (PG14+)
```

### 4. Control Parallel Query Aggressiveness

```sql
-- Limit parallel workers for OLTP workloads
ALTER SYSTEM SET max_parallel_workers_per_gather = 2;
ALTER SYSTEM SET max_parallel_workers = 8;

-- For specific heavy queries, control per session
SET max_parallel_workers_per_gather = 0;  -- disable parallelism for this session
SET parallel_setup_cost = 1000;            -- make planner less eager to parallelize
```

### 5. Update Table Statistics Aggressively

```sql
-- Stale statistics → bad plans → wasted CPU
ANALYZE orders;  -- manual analyze

-- Increase statistics target for skewed columns
ALTER TABLE orders ALTER COLUMN status SET STATISTICS 500;  -- default 100
ANALYZE orders;

-- Aurora parameter group
default_statistics_target = 200   -- increase from default 100
```

### 6. Tune `work_mem` Carefully

```sql
-- Too low: sort spills to disk (IO:DataFileRead instead of CPU)
-- Too high: many sessions × high work_mem = OOM risk
-- Tune per query type

-- For reporting queries:
SET work_mem = '256MB';  -- session-level for BI tool connections

-- For OLTP:
work_mem = 4MB  -- Aurora parameter group (keep low for many connections)

-- Check if sorts are spilling to disk
SELECT
    left(query, 100) AS query,
    temp_blks_read,
    temp_blks_written
FROM pg_stat_statements
WHERE temp_blks_written > 0
ORDER BY temp_blks_written DESC
LIMIT 10;
```

### 7. Aurora-Specific: Right-Size Instance vCPUs

```bash
# Check CPU utilization via CloudWatch
aws cloudwatch get-metric-statistics \
  --namespace AWS/RDS \
  --metric-name CPUUtilization \
  --dimensions Name=DBInstanceIdentifier,Value=my-aurora-writer \
  --start-time $(date -d '24 hours ago' -u +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average Maximum

# If CPU > 70% sustained: scale up instance class
# If CPU spiky: optimize queries or use read replicas for reporting
```

### 8. Offload Analytics to Read Replicas

```python
# Route read-only analytical queries to Aurora Reader endpoint
WRITER_DSN = "host=cluster.cluster-xxxx.rds.amazonaws.com ..."
READER_DSN = "host=cluster.cluster-ro-xxxx.rds.amazonaws.com ..."

def run_analytics_query(sql):
    conn = psycopg2.connect(READER_DSN)  # Never touches writer CPU
    ...

def run_write_query(sql):
    conn = psycopg2.connect(WRITER_DSN)
    ...
```

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | Unoptimized queries; missing indexes; seq scans; bad plans; parallel query storms |
| **Risk** | CPU saturation → all sessions slow; instance becomes unresponsive |
| **Quick Fix** | `pg_terminate_backend()` on runaway queries; `SET max_parallel_workers_per_gather = 0` |
| **Long-Term Fix** | Index optimization; query rewriting; statistics tuning; read replica offload |
| **Key Parameters** | `work_mem`, `max_parallel_workers_per_gather`, `default_statistics_target` |
