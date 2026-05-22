# Aurora PostgreSQL Wait Event: `Lock:relation`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | Lock |
| **Wait Event**   | relation |
| **Lock Object**  | Entire table (relation OID) |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | High |
| **Aurora Specific** | Identical behavior; DDL lock contention common in Aurora Serverless v2 autoscaling |

---

## Description

`Lock:relation` occurs when a session is waiting to acquire a **table-level lock** on a relation (table, index, sequence, or view). PostgreSQL uses a hierarchy of lock modes from `ACCESS SHARE` (lightest) to `ACCESS EXCLUSIVE` (heaviest). Most DDL operations require `ACCESS EXCLUSIVE`, which **conflicts with every other lock mode** — causing any concurrent reader or writer to wait.

### Lock Mode Hierarchy (Conflict Matrix)

```
Lock Mode            | AS  | RS  | RE  | SUE | S   | SRE | E   | AE
---------------------|-----|-----|-----|-----|-----|-----|-----|----
ACCESS SHARE (AS)    |     |     |     |     |     |     |     |  ✗
ROW SHARE (RS)       |     |     |     |     |     |     |  ✗  |  ✗
ROW EXCLUSIVE (RE)   |     |     |     |     |  ✗  |  ✗  |  ✗  |  ✗
SHARE UPDATE EXC(SUE)|     |     |     |  ✗  |  ✗  |  ✗  |  ✗  |  ✗
SHARE (S)            |     |     |  ✗  |  ✗  |     |  ✗  |  ✗  |  ✗
SHARE ROW EXC (SRE)  |     |     |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗
EXCLUSIVE (E)        |     |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗
ACCESS EXCLUSIVE (AE)|  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗
```

### What Holds ACCESS EXCLUSIVE

- `ALTER TABLE` (add column, change type, add constraint)
- `DROP TABLE`
- `TRUNCATE`
- `VACUUM FULL`
- `CLUSTER`
- `REINDEX TABLE` (not `REINDEX CONCURRENTLY`)
- `LOCK TABLE ... IN ACCESS EXCLUSIVE MODE`
- `REFRESH MATERIALIZED VIEW` (not `CONCURRENTLY`)

### The Lock Queue Problem

A single blocked DDL creates a **lock queue** that blocks all subsequent queries:

```
Session A: SELECT (ACCESS SHARE) ───────────────────────────────► runs OK
Session B: ALTER TABLE (ACCESS EXCLUSIVE) ─── BLOCKED waiting for A ──►
Session C: SELECT (ACCESS SHARE) ──────────── BLOCKED waiting for B ──►
Session D: INSERT (ROW EXCLUSIVE) ─────────── BLOCKED waiting for B ──►
```

Even lightweight SELECTs pile up behind the DDL, causing cascading connection exhaustion.

---

## Sample Data

### What You'll See in `pg_stat_activity`

```sql
SELECT pid, usename, state, wait_event_type, wait_event,
       now() - query_start AS wait_duration,
       left(query, 200) AS query
FROM pg_stat_activity
WHERE wait_event_type = 'Lock'
  AND wait_event = 'relation'
ORDER BY wait_duration DESC;
```

**Sample Output:**

```
 pid  | usename  | state  | wait_event_type | wait_event | wait_duration    | query
------+----------+--------+-----------------+------------+------------------+-------
 3301 | migrator | active | Lock            | relation   | 00:00:45.112     | ALTER TABLE orders ADD COLUMN ...
 3302 | appuser  | active | Lock            | relation   | 00:00:44.987     | SELECT COUNT(*) FROM orders
 3303 | appuser  | active | Lock            | relation   | 00:00:44.776     | INSERT INTO orders VALUES (...)
 3304 | appuser  | active | Lock            | relation   | 00:00:43.123     | UPDATE orders SET status = ...
```

---

## Simulate the Wait Event

### Method 1: DDL Blocked by Long-Running Query

```sql
-- Terminal 1: Start a long-running query (holds ACCESS SHARE)
BEGIN;
SELECT COUNT(*), pg_sleep(120) FROM orders;

-- Terminal 2: Try ALTER TABLE (needs ACCESS EXCLUSIVE → blocks)
ALTER TABLE orders ADD COLUMN processed_at TIMESTAMPTZ;

-- Terminal 3: Any subsequent query on orders now also blocks
SELECT * FROM orders LIMIT 1;  -- BLOCKED by lock queue
```

### Method 2: Explicit Lock Simulation

```sql
-- Terminal 1: Hold explicit ACCESS EXCLUSIVE lock
BEGIN;
LOCK TABLE orders IN ACCESS EXCLUSIVE MODE;
SELECT pg_sleep(60);  -- Hold for 60 seconds
COMMIT;

-- Terminal 2: Observe all other sessions pile up
SELECT * FROM orders;         -- blocked
INSERT INTO orders ...;       -- blocked
UPDATE orders SET ...;        -- blocked
```

### Method 3: Concurrent VACUUM FULL

```bash
# VACUUM FULL takes ACCESS EXCLUSIVE — simulate on a busy table
psql -c "VACUUM FULL orders;" &
# Simultaneously run workload
pgbench -T 30 -c 20 postgres
```

---

## Monitoring Queries

### 1. Full Lock Dependency Tree

```sql
WITH RECURSIVE lock_tree AS (
    -- Find blocking sessions
    SELECT
        pid AS blocker_pid,
        NULL::INT AS blocked_pid,
        usename AS blocker_user,
        query AS blocker_query,
        wait_event,
        state,
        0 AS depth
    FROM pg_stat_activity
    WHERE pid IN (
        SELECT DISTINCT pid FROM pg_locks WHERE granted = true
    )
    AND wait_event_type IS DISTINCT FROM 'Lock'

    UNION ALL

    -- Find sessions blocked by the above
    SELECT
        a.pid,
        lt.blocker_pid,
        a.usename,
        a.query,
        a.wait_event,
        a.state,
        lt.depth + 1
    FROM pg_stat_activity a
    JOIN pg_locks bl ON bl.pid = a.pid AND bl.granted = false
    JOIN pg_locks gl ON gl.relation = bl.relation AND gl.granted = true
    JOIN lock_tree lt ON lt.blocker_pid = gl.pid
    WHERE a.wait_event = 'relation'
)
SELECT
    depth,
    blocker_pid,
    blocked_pid,
    blocker_user,
    left(blocker_query, 150) AS query
FROM lock_tree
ORDER BY depth, blocker_pid;
```

### 2. Detailed Lock Waiter Analysis

```sql
SELECT
    waiting.pid                          AS waiting_pid,
    waiting.usename                      AS waiting_user,
    waiting.query                        AS waiting_query,
    blocking.pid                         AS blocking_pid,
    blocking.usename                     AS blocking_user,
    blocking.query                       AS blocking_query,
    blocking.state                       AS blocking_state,
    now() - waiting.query_start          AS wait_duration,
    wl.mode                              AS requested_mode,
    bl.mode                              AS held_mode,
    waiting_rel.relname                  AS locked_table
FROM pg_stat_activity waiting
JOIN pg_locks wl ON wl.pid = waiting.pid AND wl.granted = false
JOIN pg_locks bl ON bl.relation = wl.relation AND bl.granted = true
JOIN pg_stat_activity blocking ON blocking.pid = bl.pid
JOIN pg_class waiting_rel ON waiting_rel.oid = wl.relation
WHERE waiting.wait_event = 'relation'
ORDER BY wait_duration DESC;
```

### 3. Lock Modes Currently Held on Hot Tables

```sql
SELECT
    c.relname AS table_name,
    l.mode,
    l.granted,
    COUNT(*) AS lock_count,
    array_agg(DISTINCT a.usename) AS users
FROM pg_locks l
JOIN pg_class c ON c.oid = l.relation
JOIN pg_stat_activity a ON a.pid = l.pid
WHERE c.relkind = 'r'
  AND c.relname NOT LIKE 'pg_%'
GROUP BY c.relname, l.mode, l.granted
ORDER BY c.relname, l.granted DESC, l.mode;
```

### 4. Identify the Root Blocker (Multi-Level Chains)

```sql
SELECT
    pg_blocking_pids(pid) AS blocked_by,
    pid,
    usename,
    state,
    wait_event_type,
    wait_event,
    left(query, 200) AS query
FROM pg_stat_activity
WHERE cardinality(pg_blocking_pids(pid)) > 0
ORDER BY array_length(pg_blocking_pids(pid), 1) DESC;
```

---

## Resolution & Tuning

### 1. Always Use `lock_timeout` for DDL

```sql
-- Fail fast instead of piling up waiters
SET lock_timeout = '3s';
ALTER TABLE orders ADD COLUMN metadata JSONB;

-- If it fails, retry when traffic is lower
-- In Aurora parameter group:
-- lock_timeout = 3000
```

### 2. Use `pg_terminate_backend` to Unblock

```sql
-- Find and terminate the root blocking session
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid = ANY (
    SELECT unnest(pg_blocking_pids(pid))
    FROM pg_stat_activity
    WHERE wait_event = 'relation'
);
```

### 3. Use Non-Blocking DDL Alternatives

```sql
-- Instead of: ALTER TABLE orders ADD COLUMN is_archived BOOLEAN NOT NULL DEFAULT false;
-- (Takes ACCESS EXCLUSIVE, rewrites table)

-- Use: Add nullable first, then backfill, then add constraint
ALTER TABLE orders ADD COLUMN is_archived BOOLEAN;           -- Fast
UPDATE orders SET is_archived = false WHERE is_archived IS NULL;  -- Batched
ALTER TABLE orders ALTER COLUMN is_archived SET DEFAULT false;    -- Fast
ALTER TABLE orders ALTER COLUMN is_archived SET NOT NULL;         -- Fast (PG14+ with check)
```

```sql
-- Instead of REINDEX TABLE orders; (ACCESS EXCLUSIVE)
-- Use:
REINDEX TABLE CONCURRENTLY orders;  -- SHARE UPDATE EXCLUSIVE, non-blocking

-- Instead of VACUUM FULL orders;
-- Use pg_repack extension:
pg_repack --table=orders mydb
```

### 4. Retry DDL with Exponential Backoff

```python
import psycopg2
import time
import random

def run_ddl_with_retry(conn_str, ddl, max_retries=5, base_delay=1.0):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(conn_str)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET lock_timeout = '5s'")
                cur.execute(ddl)
            print(f"DDL succeeded on attempt {attempt + 1}")
            return
        except psycopg2.errors.LockNotAvailable:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"Lock not available, retrying in {delay:.1f}s...")
            time.sleep(delay)
    raise Exception("DDL failed after max retries")

run_ddl_with_retry(dsn, "ALTER TABLE orders ADD COLUMN archived_at TIMESTAMPTZ")
```

### 5. Aurora-Specific: Use Blue/Green Deployments for Schema Changes

For zero-downtime DDL on large Aurora tables, use **RDS Blue/Green Deployments**:
- Apply schema changes on the Green environment
- Cutover during low-traffic window
- Avoid lock contention on the production Blue cluster entirely

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | DDL holding ACCESS EXCLUSIVE while DML/queries hold incompatible locks |
| **Risk** | Lock queuing cascades; full connection exhaustion in seconds |
| **Quick Fix** | `pg_terminate_backend()` on root blocker; `lock_timeout` on DDL |
| **Long-Term Fix** | Non-blocking DDL patterns; `pg_repack`; Blue/Green deployments |
| **Key Parameters** | `lock_timeout`, `deadlock_timeout`, `statement_timeout` |
