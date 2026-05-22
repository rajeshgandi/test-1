# Aurora PostgreSQL Wait Event: `Lock:transactionid`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | Lock |
| **Wait Event**   | transactionid |
| **Lock Object**  | Transaction ID (XID) |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | High |
| **Aurora Specific** | Common in Aurora write-heavy workloads; visible in Performance Insights |

---

## Description

`Lock:transactionid` occurs when one transaction is **waiting for another transaction to finish** (commit or rollback). This is the fundamental row-level concurrency mechanism in PostgreSQL's MVCC implementation.

When a transaction wants to modify a row that is currently being modified by another transaction, it must wait for the other transaction's outcome before proceeding. PostgreSQL implements this by having the waiter acquire a lock on the **transaction ID (XID)** of the holder.

### Internal MVCC Mechanics

Every PostgreSQL row stores `xmin` (the XID that inserted it) and `xmax` (the XID that deleted/updated it). When a session tries to update a row:

1. It reads the row's `xmax` to see if another transaction is modifying it
2. If `xmax` is an in-progress XID, the session **waits on that XID's lock**
3. When the holder commits or rolls back, the lock is released
4. The waiter wakes up, re-checks the row's visibility, and proceeds

```
Transaction A: UPDATE orders SET status='shipped' WHERE id=42
   └─ Acquires ExclusiveLock on XID(A)
   └─ Sets orders.id=42 xmax = XID(A)

Transaction B: UPDATE orders SET status='cancelled' WHERE id=42
   └─ Sees orders.id=42 xmax = XID(A) which is in-progress
   └─ Tries to acquire ExclusiveLock on XID(A) → BLOCKS
   └─ Wait event: Lock:transactionid
```

### Key Difference from Lock:tuple

| Event | What's Locked | When |
|-------|--------------|------|
| `Lock:transactionid` | The XID of the holding transaction | Waiting for a transaction to finish |
| `Lock:tuple` | The physical tuple location | Multiple sessions waiting for the same row, first waiter holds tuple lock |

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
WHERE wait_event = 'transactionid'
ORDER BY wait_duration DESC;
```

**Sample Output:**

```
 pid  | usename | state  | wait_event_type | wait_event    | wait_duration  | query
------+---------+--------+-----------------+---------------+----------------+-------
 4401 | appuser | active | Lock            | transactionid | 00:00:12.445   | UPDATE accounts SET balance=...
 4402 | appuser | active | Lock            | transactionid | 00:00:11.231   | UPDATE accounts SET balance=...
 4403 | appuser | active | Lock            | transactionid | 00:00:09.876   | DELETE FROM orders WHERE id=...
```

---

## Simulate the Wait Event

### Method 1: Classic Row Contention

```sql
-- Terminal 1: Start a slow transaction on a specific row
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE account_id = 1001;
SELECT pg_sleep(30);  -- Simulate slow application; hold lock for 30s
COMMIT;

-- Terminal 2: Try to update the same row → Lock:transactionid
BEGIN;
UPDATE accounts SET balance = balance + 50 WHERE account_id = 1001;  -- BLOCKED
COMMIT;

-- Terminal 3: Another session also contending
BEGIN;
DELETE FROM accounts WHERE account_id = 1001;  -- BLOCKED
COMMIT;
```

### Method 2: Hot Row Pattern (Many Waiters)

```bash
# Simulate 50 concurrent sessions all hitting the same row
for i in $(seq 1 50); do
  psql -c "BEGIN; UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 999; SELECT pg_sleep(5); COMMIT;" &
done
wait
```

### Method 3: Payment Processing Pattern

```python
import psycopg2
import threading

def process_payment(account_id, amount):
    conn = psycopg2.connect(dsn="...")
    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.execute("SELECT balance FROM accounts WHERE id = %s FOR UPDATE", (account_id,))
    balance = cur.fetchone()[0]
    if balance >= amount:
        cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, account_id))
    cur.execute("COMMIT")

# 100 threads all contending on account_id=1
threads = [threading.Thread(target=process_payment, args=(1, 10)) for _ in range(100)]
for t in threads: t.start()
for t in threads: t.join()
```

---

## Monitoring Queries

### 1. Lock:transactionid Waiter Chain

```sql
SELECT
    blocked.pid                     AS blocked_pid,
    blocked.usename                 AS blocked_user,
    blocked.query                   AS blocked_query,
    blocking.pid                    AS blocking_pid,
    blocking.usename                AS blocking_user,
    blocking.query                  AS blocking_query,
    blocking.state                  AS blocking_state,
    EXTRACT(EPOCH FROM (now() - blocked.query_start))::INT AS wait_secs,
    EXTRACT(EPOCH FROM (now() - blocking.query_start))::INT AS blocker_age_secs
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking
    ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
WHERE blocked.wait_event = 'transactionid'
ORDER BY wait_secs DESC;
```

### 2. Identify Hot Rows (Rows with Most Contention)

```sql
-- Find which tables/rows are causing transactionid waits
SELECT
    c.relname AS table_name,
    l.page,
    l.tuple,
    COUNT(*) AS waiters,
    array_agg(DISTINCT a.usename) AS waiting_users
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
JOIN pg_class c ON c.oid = l.relation
WHERE l.granted = false
  AND l.locktype = 'transactionid'
GROUP BY c.relname, l.page, l.tuple
ORDER BY waiters DESC
LIMIT 20;
```

### 3. XID Age and Transaction Duration of Blockers

```sql
SELECT
    pid,
    usename,
    application_name,
    state,
    wait_event,
    xact_start,
    EXTRACT(EPOCH FROM (now() - xact_start))::INT AS txn_age_secs,
    EXTRACT(EPOCH FROM (now() - state_change))::INT AS state_age_secs,
    pg_blocking_pids(pid) AS blocked_by,
    left(query, 200) AS query
FROM pg_stat_activity
WHERE state IN ('active', 'idle in transaction')
  AND xact_start IS NOT NULL
ORDER BY txn_age_secs DESC
LIMIT 20;
```

### 4. Deadlock Detection (transactionid + circular waits)

```sql
-- Detect potential deadlock cycles
WITH waits AS (
    SELECT
        a.pid AS waiter,
        unnest(pg_blocking_pids(a.pid)) AS holder
    FROM pg_stat_activity a
    WHERE a.wait_event = 'transactionid'
)
SELECT DISTINCT
    w1.waiter AS pid_a,
    w1.holder AS pid_b,
    'POTENTIAL DEADLOCK' AS note
FROM waits w1
JOIN waits w2 ON w2.waiter = w1.holder AND w2.holder = w1.waiter;
```

### 5. Transaction Throughput vs Contention Ratio

```sql
SELECT
    xact_commit + xact_rollback                AS total_transactions,
    deadlocks,
    conflicts,
    ROUND(deadlocks::NUMERIC / NULLIF(xact_commit + xact_rollback, 0) * 1000000, 2) AS deadlocks_per_million_txns,
    blks_hit,
    blks_read,
    ROUND(blks_hit::NUMERIC / NULLIF(blks_hit + blks_read, 0) * 100, 2) AS cache_hit_pct
FROM pg_stat_database
WHERE datname = current_database();
```

---

## Resolution & Tuning

### 1. Shorten Transaction Duration

The root fix: **the shorter your transactions, the less time others wait**.

```sql
-- Bad: Long transaction with slow application logic in the middle
BEGIN;
  UPDATE accounts SET balance = balance - 100 WHERE id = 1;
  -- application does HTTP call, file I/O, etc.
  UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- Good: Do application logic BEFORE starting transaction
-- Fetch data, compute results, THEN open transaction for minimal time
local_result = fetch_and_compute()
BEGIN;
  UPDATE accounts SET balance = balance - 100 WHERE id = 1;
  UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;  -- transaction open for microseconds, not seconds
```

### 2. Consistent Lock Ordering to Prevent Deadlocks

```sql
-- Bad: Session A locks id=1 then id=2; Session B locks id=2 then id=1 → deadlock
-- Good: Always lock in the same order (ascending id)

BEGIN;
SELECT * FROM accounts WHERE id IN (1, 2) ORDER BY id FOR UPDATE;  -- consistent order
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

### 3. Optimistic Locking (Skip Locks for High Throughput)

```sql
-- Use SKIP LOCKED to implement a work queue without contention
SELECT id, task_data
FROM job_queue
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED;  -- Don't wait; grab next available row
```

### 4. Set `deadlock_timeout`

```sql
-- Detect deadlocks faster (default is 1s; lower for latency-sensitive systems)
-- Aurora parameter group:
deadlock_timeout = 500   -- 500ms; PostgreSQL checks for deadlocks after this
```

### 5. Use `NOWAIT` for Fail-Fast Behavior

```sql
-- Instead of waiting indefinitely, fail immediately and retry at app level
BEGIN;
SELECT * FROM orders WHERE id = 42 FOR UPDATE NOWAIT;
-- If locked: ERROR: could not obtain lock on row in relation "orders"
-- Catch in application and implement retry with backoff
COMMIT;
```

### 6. Aurora-Specific: Tune `max_connections` and Use RDS Proxy

High `transactionid` waits combined with many connections can cascade:

```
-- RDS Proxy reduces connection overhead, freeing the database
-- to process existing transactions faster, reducing wait time
```

```bash
# Aurora parameter group recommendations for high-contention workloads
deadlock_timeout = 500
lock_timeout = 10000       # 10 seconds max wait for any lock
statement_timeout = 30000  # 30 seconds max per statement
```

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | Multiple sessions modifying the same rows; long-running transactions |
| **Risk** | Deadlocks; cascading waits; throughput collapse on hot rows |
| **Quick Fix** | `pg_terminate_backend()` on long-running blockers |
| **Long-Term Fix** | Shorter transactions; consistent lock ordering; SKIP LOCKED; optimistic locking |
| **Key Parameters** | `deadlock_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout` |
