# Aurora PostgreSQL Wait Event: `Lock:tuple`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | Lock |
| **Wait Event**   | tuple |
| **Lock Object**  | Physical heap tuple (page + offset) |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | High |
| **Aurora Specific** | Amplified in Aurora when write throughput is high; seen in hot-row patterns |

---

## Description

`Lock:tuple` is a **second-level lock** that appears when **multiple sessions are simultaneously waiting for the same row**, and the row is already locked by a transaction. Unlike `Lock:transactionid` (where each waiter blocks on the holder's XID), `Lock:tuple` is used to **serialize multiple waiters** among themselves — only one waiter is allowed to wait on the XID at a time.

### The Three-Level Contention Hierarchy

When many sessions want to update the same row:

```
Level 1: Holder
   Transaction A holds row (xmax = XID_A)

Level 2: First Waiter — Lock:transactionid
   Transaction B: waiting on XID_A (one designated waiter)

Level 3: All Remaining Waiters — Lock:tuple
   Transaction C: waiting on the physical tuple location
   Transaction D: waiting on the physical tuple location
   Transaction E: waiting on the physical tuple location
```

When A commits, B gets the row. B's turn releases the tuple lock. C, D, E then compete for the `Lock:transactionid` on XID_B.

### Why PostgreSQL Uses This Two-Level Approach

Without `Lock:tuple`, when Transaction A commits and B gets the row, ALL waiting sessions (C, D, E, ...) would wake up simultaneously and race to grab the XID lock — causing a **thundering herd**. `Lock:tuple` creates an orderly queue, serializing access without stampedes.

```
Without Lock:tuple:  A commits → B, C, D, E all wake up simultaneously → race condition
With Lock:tuple:     A commits → only B wakes up → B gets row → C wakes up → etc.
```

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
WHERE wait_event IN ('tuple', 'transactionid')
ORDER BY wait_event, wait_duration DESC;
```

**Sample Output (classic hot-row pattern):**

```
 pid  | usename | state  | wait_event_type | wait_event    | wait_duration  | query
------+---------+--------+-----------------+---------------+----------------+-------
 5501 | appuser | active | Lock            | transactionid | 00:00:05.223   | UPDATE inventory SET qty=...
 5502 | appuser | active | Lock            | tuple         | 00:00:05.189   | UPDATE inventory SET qty=...
 5503 | appuser | active | Lock            | tuple         | 00:00:05.134   | UPDATE inventory SET qty=...
 5504 | appuser | active | Lock            | tuple         | 00:00:05.091   | UPDATE inventory SET qty=...
 5505 | appuser | active | Lock            | tuple         | 00:00:04.876   | UPDATE inventory SET qty=...
```

> **Signature:** 1 session waiting on `transactionid` + N sessions waiting on `tuple` = **hot row contention**.

---

## Simulate the Wait Event

### Method 1: Flash Sale / Inventory Decrement Pattern

```sql
-- Terminal 1: Holder — slow transaction
BEGIN;
UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 42;
SELECT pg_sleep(60);  -- Hold for 60 seconds
COMMIT;

-- Terminals 2-20: Many sessions hammer the same row simultaneously
-- One will get Lock:transactionid, rest get Lock:tuple
for i in $(seq 1 20); do
  psql -c "UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 42;" &
done
```

### Method 2: Python Concurrent Hot Row

```python
import psycopg2
import threading
import time

DSN = "host=aurora-cluster... dbname=postgres user=appuser password=secret"

def decrement_inventory(product_id):
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute(
            "UPDATE inventory SET quantity = quantity - 1 WHERE product_id = %s AND quantity > 0",
            (product_id,)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        conn.close()

# 50 threads all trying to decrement product_id=42 simultaneously
threads = [threading.Thread(target=decrement_inventory, args=(42,)) for _ in range(50)]
for t in threads: t.start()
for t in threads: t.join()
```

### Method 3: pgbench Custom Script

```sql
-- custom_hot_row.sql
BEGIN;
UPDATE products SET view_count = view_count + 1 WHERE id = 1;
COMMIT;
```

```bash
pgbench \
  -h aurora-cluster.xxxx.rds.amazonaws.com \
  -U appuser \
  -d postgres \
  -f custom_hot_row.sql \
  --client=100 \
  --jobs=10 \
  --time=60
```

---

## Monitoring Queries

### 1. Detect Hot Rows: Tables with Most Tuple Lock Waiters

```sql
SELECT
    c.relname AS table_name,
    l.page,
    l.tuple,
    COUNT(*) FILTER (WHERE NOT l.granted) AS waiters,
    COUNT(*) FILTER (WHERE l.granted)     AS holders,
    array_agg(DISTINCT a.pid ORDER BY a.pid) AS all_pids
FROM pg_locks l
JOIN pg_class c ON c.oid = l.relation
JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'tuple'
GROUP BY c.relname, l.page, l.tuple
HAVING COUNT(*) FILTER (WHERE NOT l.granted) > 0
ORDER BY waiters DESC;
```

### 2. Full Contention Picture: Holder + transactionid waiter + tuple waiters

```sql
WITH lock_info AS (
    SELECT
        l.pid,
        l.locktype,
        l.relation,
        l.page,
        l.tuple,
        l.transactionid,
        l.granted,
        a.query,
        a.usename,
        a.wait_event,
        a.state,
        now() - a.query_start AS duration
    FROM pg_locks l
    JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype IN ('tuple', 'transactionid')
)
SELECT
    locktype,
    granted,
    pid,
    usename,
    wait_event,
    duration,
    left(query, 150) AS query
FROM lock_info
ORDER BY granted DESC, locktype, duration DESC;
```

### 3. Contention Rate Per Table

```sql
SELECT
    schemaname,
    relname AS table_name,
    n_tup_upd AS updates,
    n_tup_del AS deletes,
    n_tup_hot_upd AS hot_updates,
    ROUND(n_tup_hot_upd::NUMERIC / NULLIF(n_tup_upd, 0) * 100, 2) AS hot_update_pct
FROM pg_stat_user_tables
WHERE n_tup_upd > 0
ORDER BY updates DESC
LIMIT 20;
```

> **Insight:** Low `hot_update_pct` on frequently updated tables suggests many cold updates, which can aggravate tuple contention by creating more dead tuple versions.

### 4. pg_stat_activity Correlation with Lock Queue Length

```sql
SELECT
    wait_event,
    COUNT(*) AS waiters,
    MAX(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS max_wait_secs,
    MIN(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS min_wait_secs
FROM pg_stat_activity
WHERE wait_event IN ('tuple', 'transactionid', 'relation')
GROUP BY wait_event
ORDER BY waiters DESC;
```

---

## Resolution & Tuning

### 1. Redesign the Hot Row — Partition the Counter

Instead of a single counter row, use multiple rows and aggregate:

```sql
-- Bad: Single hot row
CREATE TABLE inventory (product_id INT PRIMARY KEY, quantity INT);
UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 42;

-- Good: Sharded counter — distribute writes across N rows
CREATE TABLE inventory_shards (
    product_id INT,
    shard_id   INT,          -- 0 to N-1
    quantity   INT,
    PRIMARY KEY (product_id, shard_id)
);

-- Write to a random shard
UPDATE inventory_shards
SET quantity = quantity - 1
WHERE product_id = 42
  AND shard_id = floor(random() * 10)::INT  -- 10 shards
  AND quantity > 0;

-- Read: aggregate across shards
SELECT SUM(quantity) FROM inventory_shards WHERE product_id = 42;
```

### 2. Use `SELECT FOR UPDATE SKIP LOCKED` (Queue-Based Pattern)

```sql
-- Process items without contention — skip already-locked rows
WITH next_item AS (
    SELECT id FROM job_queue
    WHERE status = 'pending'
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE job_queue
SET status = 'processing', started_at = now()
WHERE id = (SELECT id FROM next_item)
RETURNING *;
```

### 3. Reduce Transaction Hold Time

```sql
-- Move non-DB work outside the transaction
-- Bad:
BEGIN;
SELECT * FROM orders WHERE id = 42 FOR UPDATE;
-- ... validate with external API (slow) ...
UPDATE orders SET status = 'approved' WHERE id = 42;
COMMIT;

-- Good:
-- Fetch data, validate externally, then open a short transaction
data = SELECT * FROM orders WHERE id = 42;
validated = call_external_api(data);  -- outside transaction
BEGIN;
UPDATE orders SET status = 'approved' WHERE id = 42;  -- tiny transaction
COMMIT;
```

### 4. Implement Exponential Backoff + Jitter at Application Level

```python
import random
import time
import psycopg2

def update_with_retry(product_id, decrement, max_attempts=5):
    for attempt in range(max_attempts):
        try:
            conn = psycopg2.connect(dsn="...")
            cur = conn.cursor()
            cur.execute("SET lock_timeout = '500ms'")
            cur.execute("""
                UPDATE inventory
                SET quantity = quantity - %s
                WHERE product_id = %s AND quantity >= %s
            """, (decrement, product_id, decrement))
            conn.commit()
            return True
        except psycopg2.errors.LockNotAvailable:
            conn.rollback()
            # Exponential backoff with jitter
            sleep_time = (2 ** attempt) * 0.1 + random.uniform(0, 0.1)
            time.sleep(sleep_time)
    return False
```

### 5. Enable `fillfactor` to Allow HOT Updates

Setting `fillfactor < 100` leaves free space in pages for Heap Only Tuple (HOT) updates, reducing page version bloat:

```sql
-- Allow 30% free space per page for HOT updates
ALTER TABLE inventory SET (fillfactor = 70);
VACUUM inventory;  -- Reclaim space per new fillfactor

-- Verify HOT update ratio improves
SELECT n_tup_hot_upd, n_tup_upd,
       ROUND(n_tup_hot_upd::NUMERIC / NULLIF(n_tup_upd,0) * 100, 2) AS hot_pct
FROM pg_stat_user_tables
WHERE relname = 'inventory';
```

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | Multiple concurrent sessions updating the same row (hot row pattern) |
| **Risk** | Thundering herd on commit; cascading wait queues; throughput collapse |
| **Quick Fix** | Terminate root holder; reduce transaction duration |
| **Long-Term Fix** | Shard hot counters; SKIP LOCKED; redesign write patterns; fillfactor |
| **Key Parameters** | `deadlock_timeout`, `lock_timeout`, `fillfactor` |
