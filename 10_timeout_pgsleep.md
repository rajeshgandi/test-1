# Aurora PostgreSQL Wait Event: `Timeout:PgSleep`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | Timeout |
| **Wait Event**   | PgSleep |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | Low–Medium (usually benign, but can mask patterns) |
| **Aurora Specific** | Can appear in Aurora maintenance procedures, connection pool health checks, and scheduled jobs |

---

## Description

`Timeout:PgSleep` occurs when a session is explicitly sleeping via the `pg_sleep()`, `pg_sleep_for()`, or `pg_sleep_until()` functions. Unlike other wait events that indicate resource contention, this is an **intentional delay** requested by the session itself or by an application.

While individually harmless, many sessions in `PgSleep` simultaneously can:
- Consume connection slots unnecessarily
- Mask real connection pool sizing issues
- Indicate anti-patterns in application code or test harnesses
- Appear in health check queries, rate-limiting logic, or retry loops

### pg_sleep Function Family

```sql
-- Sleep for N seconds (fractional allowed)
SELECT pg_sleep(5.5);           -- sleeps 5.5 seconds

-- Sleep for an interval
SELECT pg_sleep_for('2 minutes 30 seconds');

-- Sleep until a specific timestamp
SELECT pg_sleep_until('2024-12-31 23:59:59+00');
```

### Common Legitimate Use Cases

| Use Case | Pattern |
|----------|---------|
| Connection pool health checks | `SELECT 1` (no sleep) or `SELECT pg_sleep(0)` |
| Rate-limiting retry loops | `pg_sleep(0.1)` between retries |
| Test simulation | Simulating slow queries or lock contention |
| Scheduled job pacing | Sleeping between processing batches |
| Maintenance scripts | Pausing between DDL operations |
| Lock wait simulation | Holding a transaction open for testing |

---

## Sample Data

### What You'll See in `pg_stat_activity`

```sql
SELECT
    pid, usename, application_name, client_addr,
    state, wait_event_type, wait_event,
    now() - query_start AS sleep_duration,
    left(query, 200) AS query
FROM pg_stat_activity
WHERE wait_event = 'PgSleep'
ORDER BY sleep_duration DESC;
```

**Sample Output:**

```
 pid   | usename      | application_name | client_addr   | state  | wait_event_type | wait_event | sleep_duration  | query
-------+--------------+------------------+---------------+--------+-----------------+------------+-----------------+-------
 10001 | health-check | pgbouncer        | 10.0.0.5      | active | Timeout         | PgSleep    | 00:00:04.991    | SELECT pg_sleep(5)
 10002 | batch-job    | etl-runner       | 10.0.1.20     | active | Timeout         | PgSleep    | 00:00:00.234    | SELECT pg_sleep(0.5)
 10003 | test-user    | pytest           | 10.0.2.99     | active | Timeout         | PgSleep    | 00:00:29.112    | BEGIN; UPDATE ...; SELECT pg_sleep(30);
```

---

## Simulate the Wait Event

### Method 1: Simple Sleep

```sql
-- Single session sleep
SELECT pg_sleep(60);  -- sleeps 60 seconds; wait_event = 'PgSleep'
```

### Method 2: Sleep Inside a Transaction (Lock Hold Simulation)

```sql
-- Terminal 1: Hold a lock while sleeping (dangerous anti-pattern)
BEGIN;
UPDATE accounts SET balance = 1000 WHERE id = 1;
SELECT pg_sleep(120);  -- holds lock for 2 minutes
COMMIT;
```

### Method 3: Application Health Check Pattern

```python
import psycopg2
import time

# Simulating a connection pool health check that uses pg_sleep
def health_check(conn):
    cur = conn.cursor()
    cur.execute("SELECT pg_sleep(5)")  # PgSleep wait event appears
    cur.fetchone()

# PgBouncer's server_check_query configuration
# pgbouncer.ini: server_check_query = select pg_sleep(0)
```

### Method 4: Batch Processing with Sleep Between Batches

```python
import psycopg2
import time

conn = psycopg2.connect(dsn="...")
cur = conn.cursor()

while True:
    # Process a batch
    cur.execute("""
        UPDATE events
        SET processed = true
        WHERE id IN (
            SELECT id FROM events WHERE processed = false LIMIT 1000
        )
    """)
    conn.commit()
    processed = cur.rowcount

    if processed == 0:
        break

    # Rate limit: sleep between batches
    cur.execute("SELECT pg_sleep(0.1)")  # PgSleep appears here
    cur.fetchone()
```

### Method 5: Many Concurrent Sleepers (Connection Exhaustion Test)

```bash
# Test how many sleeping connections exhaust your connection pool
for i in $(seq 1 200); do
  psql -c "SELECT pg_sleep(300);" &  # each sleeps 5 minutes
done
# Observe max_connections being reached
psql -c "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep';"
```

---

## Monitoring Queries

### 1. PgSleep Session Inventory

```sql
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    wait_event,
    xact_start,
    query_start,
    EXTRACT(EPOCH FROM (now() - query_start))::INT AS sleep_secs,
    EXTRACT(EPOCH FROM (now() - xact_start))::INT AS txn_secs,
    left(query, 200) AS query,
    -- Is this inside an active transaction? (dangerous)
    (xact_start IS NOT NULL AND
     xact_start < query_start - INTERVAL '1 second') AS has_open_transaction
FROM pg_stat_activity
WHERE wait_event = 'PgSleep'
ORDER BY sleep_secs DESC;
```

### 2. PgSleep Sessions Holding Locks (Most Dangerous)

```sql
SELECT
    a.pid,
    a.usename,
    a.application_name,
    EXTRACT(EPOCH FROM (now() - a.query_start))::INT AS sleep_secs,
    c.relname AS locked_table,
    l.mode AS lock_mode,
    l.granted,
    a.query
FROM pg_stat_activity a
JOIN pg_locks l ON l.pid = a.pid
JOIN pg_class c ON c.oid = l.relation
WHERE a.wait_event = 'PgSleep'
  AND l.granted = true
  AND l.relation IS NOT NULL
  AND c.relkind = 'r'  -- regular tables only
ORDER BY sleep_secs DESC;
```

### 3. Connection Slot Consumption by Wait Event

```sql
SELECT
    COALESCE(wait_event_type, 'None') AS wait_type,
    COALESCE(wait_event, 'None') AS wait_event,
    state,
    COUNT(*) AS connection_count,
    ROUND(COUNT(*) * 100.0 /
        (SELECT setting::INT FROM pg_settings WHERE name = 'max_connections'), 2) AS pct_of_max
FROM pg_stat_activity
GROUP BY COALESCE(wait_event_type, 'None'), COALESCE(wait_event, 'None'), state
ORDER BY connection_count DESC
LIMIT 20;
```

### 4. PgSleep by Application Source

```sql
SELECT
    application_name,
    client_addr,
    COUNT(*) AS sleeping_sessions,
    MAX(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS max_sleep_secs,
    AVG(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS avg_sleep_secs
FROM pg_stat_activity
WHERE wait_event = 'PgSleep'
GROUP BY application_name, client_addr
ORDER BY sleeping_sessions DESC;
```

### 5. Historical PgSleep Trend (snapshot-based)

```sql
-- Run this every minute via cron/Lambda; store in a monitoring table
INSERT INTO wait_event_snapshots (snapshot_time, wait_event, session_count)
SELECT
    now(),
    'PgSleep',
    COUNT(*)
FROM pg_stat_activity
WHERE wait_event = 'PgSleep';

-- Query trend
SELECT
    date_trunc('minute', snapshot_time) AS minute,
    AVG(session_count)::INT AS avg_pgsleep_sessions
FROM wait_event_snapshots
WHERE wait_event = 'PgSleep'
  AND snapshot_time > now() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1;
```

---

## Resolution & Tuning

### 1. Identify and Terminate PgSleep Sessions with Open Transactions

```sql
-- Find PgSleep sessions with open transactions (most dangerous)
SELECT pid, usename, application_name,
       EXTRACT(EPOCH FROM (now() - xact_start))::INT AS txn_age_secs,
       query
FROM pg_stat_activity
WHERE wait_event = 'PgSleep'
  AND state = 'idle in transaction'
ORDER BY txn_age_secs DESC;

-- Terminate them
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE wait_event = 'PgSleep'
  AND state = 'idle in transaction'
  AND xact_start < now() - INTERVAL '30 seconds';
```

### 2. Replace Health Check Sleeps with Lightweight Alternatives

```sql
-- Bad: PgBouncer health check with pg_sleep (holds connection, appears in wait events)
-- server_check_query = select pg_sleep(0)

-- Good: Instant no-op health check
-- server_check_query = select 1

-- In code:
# Bad
cur.execute("SELECT pg_sleep(1)")

# Good: Zero-cost health check
cur.execute("SELECT 1")
```

### 3. Move Retry Logic Outside the Database

```python
# Bad: Using pg_sleep for retry pacing inside a database session
def retry_with_db_sleep(cur, max_retries=5):
    for attempt in range(max_retries):
        try:
            cur.execute("UPDATE queue SET status='processing' WHERE ...")
            return
        except Exception:
            cur.execute("SELECT pg_sleep(1)")  # holds DB connection during sleep!

# Good: Sleep at application level, release connection during wait
def retry_with_app_sleep(dsn, max_retries=5):
    for attempt in range(max_retries):
        try:
            conn = get_connection_from_pool(dsn)  # get fresh connection
            cur = conn.cursor()
            cur.execute("UPDATE queue SET status='processing' WHERE ...")
            conn.commit()
            return_connection_to_pool(conn)
            return
        except Exception:
            return_connection_to_pool(conn)  # release before sleeping!
            time.sleep(2 ** attempt)          # application-level sleep, no DB connection held
```

### 4. Set `idle_in_transaction_session_timeout`

Automatically terminate sessions that go idle (including PgSleep inside BEGIN) after a threshold:

```sql
-- Aurora parameter group
idle_in_transaction_session_timeout = 30000   -- 30 seconds

-- Or set per session:
SET idle_in_transaction_session_timeout = '30s';
BEGIN;
UPDATE orders SET status = 'processing' WHERE id = 1;
SELECT pg_sleep(60);  -- Will be terminated after 30s → ERROR
```

### 5. Audit Applications Using pg_sleep in Production

```sql
-- Find all distinct queries that contain pg_sleep in production
SELECT
    left(query, 200) AS query,
    calls,
    application_name,
    usename
FROM pg_stat_statements s
JOIN pg_stat_activity a ON a.query = s.query
WHERE s.query ILIKE '%pg_sleep%'
GROUP BY 1, 2, 3, 4
ORDER BY calls DESC;
```

### 6. Rate Limiting via Advisory Locks Instead of Sleep

```sql
-- Better rate limiting pattern: use advisory locks, not pg_sleep
-- Acquire a slot, do work, release slot — no sleeping needed

-- Try to acquire one of 10 "rate limit slots"
SELECT pg_try_advisory_lock(12345, slot_id)
FROM generate_series(0, 9) AS slot_id
LIMIT 1;

-- If acquired, do work; on completion, release
SELECT pg_advisory_unlock(12345, :slot_id);
```

---

## Real-World Scenarios Where PgSleep Masks Problems

### Scenario 1: PgBouncer Pool Exhaustion

```
Symptom: Many PgSleep sessions from PgBouncer health checks
Root Cause: server_check_query = SELECT pg_sleep(N) using N > 0
Fix: Change to server_check_query = select 1
```

### Scenario 2: Test Suite Bleeding into Production

```
Symptom: PgSleep sessions from pytest or test framework
Root Cause: Integration tests running against production DB with sleep-based assertions
Fix: Separate test database; detect via application_name filter
```

### Scenario 3: Deadlock Avoidance Anti-Pattern

```
Symptom: Application sleeping inside transactions to "avoid conflicts"
Root Cause: Developer using pg_sleep as a poor man's retry mechanism
Fix: Implement proper optimistic locking with application-layer retry
```

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | Explicit `pg_sleep()` calls in queries, health checks, or test code |
| **Risk** | Connection slot waste; locks held during sleep; masks pool sizing issues |
| **Quick Fix** | `pg_terminate_backend()` for PgSleep sessions with open transactions |
| **Long-Term Fix** | Move sleeps to application layer; `idle_in_transaction_session_timeout`; lightweight health checks |
| **Key Parameters** | `idle_in_transaction_session_timeout`, `statement_timeout` |
| **Detection Signal** | PgSleep + `state = 'idle in transaction'` = high-priority fix needed |
