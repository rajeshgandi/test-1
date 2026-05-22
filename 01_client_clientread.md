# Aurora PostgreSQL Wait Event: `Client:ClientRead`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | Client |
| **Wait Event**   | ClientRead |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | Medium–High |
| **Aurora Specific** | Behavior identical to community PostgreSQL |

---

## Description

`Client:ClientRead` occurs when a PostgreSQL backend process is **blocked waiting to receive data from the client**. The server has finished processing its previous command and is sitting idle, waiting for the client to send the next query, parameter binding, or protocol message.

This is fundamentally a **network or application-side bottleneck**, not a database-side one. The backend process holds all its memory, open transactions, and locks while waiting — making this event dangerous in high-concurrency environments.

### Internal Mechanics

PostgreSQL uses a **synchronous request-response protocol**. After each query completes, the backend calls `recv()` on the client socket. If the client delays sending the next message (due to application logic, network round-trips, or slow processing), the backend registers `Client:ClientRead`.

Under the hood, this maps to the kernel-level `poll()` or `select()` system call waiting on the socket file descriptor.

```
Backend Process
   └─ ProcessCommand()
       └─ ReadCommand()
           └─ pq_getbyte()         ← blocks here waiting on socket
               └─ syscall: recv()  ← kernel: waiting for client bytes
```

### Why It's Dangerous

- Backend holds **open transactions** and potentially **row-level locks** while waiting
- Idle backends consume **`max_connections`** slots
- In OLTP systems, a spike in `ClientRead` often precedes **connection exhaustion**
- Can mask application-level N+1 query patterns or chatty ORM behavior

---

## Sample Data

### What You'll See in `pg_stat_activity`

```sql
SELECT pid, usename, application_name, client_addr, state,
       wait_event_type, wait_event,
       now() - state_change AS waiting_duration,
       query
FROM pg_stat_activity
WHERE wait_event = 'ClientRead'
ORDER BY waiting_duration DESC;
```

**Sample Output:**

```
 pid  | usename | application_name | client_addr     | state | wait_event_type | wait_event | waiting_duration | query
------+---------+------------------+-----------------+-------+-----------------+------------+------------------+-------
 1234 | appuser | myapp            | 10.0.1.45       | idle  | Client          | ClientRead | 00:02:34.123     | SELECT ...
 1235 | appuser | myapp            | 10.0.1.46       | idle  | Client          | ClientRead | 00:01:12.456     | UPDATE ...
 1236 | appuser | myapp            | 10.0.1.47       | idle  | Client          | ClientRead | 00:00:03.789     | BEGIN
```

> **Note:** `state = 'idle'` + `wait_event = 'ClientRead'` means the backend is between commands. `state = 'idle in transaction'` + `ClientRead` is **much more dangerous** — it holds locks.

---

## Simulate the Wait Event

### Method 1: Slow Client Simulation (Python)

```python
import psycopg2
import time

conn = psycopg2.connect(
    host="your-aurora-cluster.cluster-xxxx.us-east-1.rds.amazonaws.com",
    database="postgres",
    user="appuser",
    password="secret"
)
cur = conn.cursor()

# Execute a query, then intentionally delay before sending the next one
# This simulates a slow application that processes results before asking for more
while True:
    cur.execute("SELECT pg_sleep(0)")  # instant query
    conn.commit()
    time.sleep(30)  # Application sleeps 30 seconds — backend registers ClientRead
```

### Method 2: Idle in Transaction with ClientRead (Most Dangerous)

```python
import psycopg2
import time

conn = psycopg2.connect(dsn="...")
cur = conn.cursor()

cur.execute("BEGIN")
cur.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
# Don't COMMIT — simulate slow application logic holding lock
time.sleep(120)  # 2 minutes of ClientRead with an open transaction
conn.commit()
```

### Method 3: Generate Multiple Slow Clients (pgbench style)

```bash
# Install pgbench and run with extreme think time
pgbench \
  -h your-aurora-cluster.cluster-xxxx.us-east-1.rds.amazonaws.com \
  -U appuser \
  -d postgres \
  --client=100 \
  --jobs=10 \
  --time=300 \
  --rate=1 \          # only 1 TPS per client = lots of idle time
  postgres
```

---

## Monitoring Queries

### 1. Real-Time ClientRead Count and Duration

```sql
SELECT
    wait_event,
    state,
    COUNT(*) AS session_count,
    MAX(EXTRACT(EPOCH FROM (now() - state_change)))::INT AS max_wait_secs,
    AVG(EXTRACT(EPOCH FROM (now() - state_change)))::INT AS avg_wait_secs,
    SUM(EXTRACT(EPOCH FROM (now() - state_change)))::INT AS total_wait_secs
FROM pg_stat_activity
WHERE wait_event = 'ClientRead'
GROUP BY wait_event, state
ORDER BY session_count DESC;
```

### 2. Identify ClientRead Sessions Holding Locks (Critical!)

```sql
SELECT
    a.pid,
    a.usename,
    a.application_name,
    a.client_addr,
    a.state,
    a.wait_event,
    EXTRACT(EPOCH FROM (now() - a.state_change))::INT AS idle_secs,
    l.relation::regclass AS locked_table,
    l.mode AS lock_mode,
    a.query AS last_query
FROM pg_stat_activity a
JOIN pg_locks l ON l.pid = a.pid
WHERE a.wait_event = 'ClientRead'
  AND l.granted = true
  AND l.relation IS NOT NULL
ORDER BY idle_secs DESC;
```

### 3. ClientRead by Application and Client IP

```sql
SELECT
    application_name,
    client_addr,
    state,
    COUNT(*) AS connections,
    MAX(EXTRACT(EPOCH FROM (now() - state_change)))::INT AS max_idle_secs
FROM pg_stat_activity
WHERE wait_event = 'ClientRead'
GROUP BY application_name, client_addr, state
ORDER BY connections DESC
LIMIT 20;
```

### 4. Connection Saturation Risk

```sql
SELECT
    total_connections,
    clientread_connections,
    active_connections,
    idle_in_txn_clientread,
    ROUND(clientread_connections::NUMERIC / max_connections * 100, 2) AS clientread_pct_of_max,
    max_connections
FROM (
    SELECT
        COUNT(*) FILTER (WHERE wait_event = 'ClientRead') AS clientread_connections,
        COUNT(*) FILTER (WHERE wait_event = 'ClientRead' AND state = 'idle in transaction') AS idle_in_txn_clientread,
        COUNT(*) FILTER (WHERE state = 'active') AS active_connections,
        COUNT(*) AS total_connections,
        (SELECT setting::INT FROM pg_settings WHERE name = 'max_connections') AS max_connections
    FROM pg_stat_activity
) sub;
```

### 5. Historical Trend via Performance Insights (Aurora)

```sql
-- Use Aurora's performance_schema equivalent or query the PI API
-- This query approximates wait event history using pg_stat_activity snapshots
-- Best implemented as a cron job or CloudWatch metric

SELECT
    date_trunc('minute', now()) AS snapshot_time,
    COUNT(*) FILTER (WHERE wait_event = 'ClientRead') AS clientread_count
FROM pg_stat_activity;
```

---

## Resolution & Tuning

### 1. Deploy a Connection Pooler (Highest Impact)

**PgBouncer** in `transaction` mode is the #1 fix. It multiplexes many application connections onto fewer backend connections, drastically reducing the number of backends stuck in `ClientRead`.

```ini
# pgbouncer.ini
[databases]
mydb = host=aurora-cluster.xxxx.rds.amazonaws.com dbname=mydb

[pgbouncer]
pool_mode = transaction          # Return connection to pool after each transaction
max_client_conn = 5000           # Accept up to 5000 app connections
default_pool_size = 100          # Only 100 real backend connections
reserve_pool_size = 10
reserve_pool_timeout = 3
server_idle_timeout = 600
client_idle_timeout = 0          # Do not timeout idle clients (manage at app layer)
log_connections = 0
log_disconnections = 0
```

> **Aurora note:** RDS Proxy is the AWS-native alternative — it's built on PgBouncer principles and supports IAM authentication natively.

### 2. Set `idle_in_transaction_session_timeout`

Terminate sessions that go idle inside a transaction after a threshold:

```sql
-- Per session
SET idle_in_transaction_session_timeout = '30s';

-- Cluster-wide (Aurora parameter group)
-- idle_in_transaction_session_timeout = 30000  (milliseconds)
```

### 3. Set `tcp_keepalives_idle` to Detect Dead Clients

```sql
-- Detect dead clients faster; stop backends from waiting on ghost connections
SET tcp_keepalives_idle = 60;       -- Start probing after 60s idle
SET tcp_keepalives_interval = 10;   -- Probe every 10s
SET tcp_keepalives_count = 5;       -- Kill after 5 failed probes
```

### 4. Terminate Long-Running ClientRead Sessions

```sql
-- Identify and terminate sessions idle in ClientRead > 10 minutes
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE wait_event = 'ClientRead'
  AND state = 'idle in transaction'
  AND state_change < now() - INTERVAL '10 minutes';
```

### 5. Application-Level Fixes

- **Use connection pools** in your ORM (HikariCP, SQLAlchemy pool, pg, etc.)
- **Avoid long application logic inside transactions** — commit fast, process outside
- **Batch queries** to reduce round-trips
- **Use `statement_timeout`** to cap per-query duration

```sql
-- Aurora parameter group recommendation
statement_timeout = 30000          -- 30 seconds max per statement
idle_in_transaction_session_timeout = 30000   -- 30 seconds max idle-in-txn
```

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | Application slow to send next query; long think-time; network latency |
| **Risk** | Connection exhaustion; lock retention; blocking other sessions |
| **Quick Fix** | Terminate idle-in-transaction sessions with `pg_terminate_backend()` |
| **Long-Term Fix** | PgBouncer / RDS Proxy; `idle_in_transaction_session_timeout`; application connection pooling |
| **CloudWatch Metric** | `DatabaseConnections`, `DBLoad` broken by wait event via Performance Insights |
