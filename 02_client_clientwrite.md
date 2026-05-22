# Aurora PostgreSQL Wait Event: `Client:ClientWrite`

## Overview

| Attribute        | Detail |
|------------------|--------|
| **Wait Type**    | Client |
| **Wait Event**   | ClientWrite |
| **Introduced**   | PostgreSQL 9.6+ |
| **Criticality**  | Medium |
| **Aurora Specific** | Behavior identical to community PostgreSQL |

---

## Description

`Client:ClientWrite` occurs when a PostgreSQL backend process is **blocked trying to send data to the client** and the client is not reading fast enough. The server has query results ready but cannot deliver them because the client-side TCP receive buffer is full.

This is the **inverse of ClientRead**: instead of the server waiting for the client to speak, the server is trying to deliver data and the client isn't consuming it quickly enough.

### Internal Mechanics

When PostgreSQL sends query results, it writes to the client socket. If the client's TCP receive buffer fills up (because the application is processing rows slowly or is busy), the kernel `send()` call blocks. PostgreSQL registers this as `Client:ClientWrite`.

```
Backend Process
   └─ ExecuteQuery()
       └─ pq_flush()
           └─ pq_write()
               └─ secure_write()
                   └─ syscall: send()  ← kernel: client buffer full, blocking
```

### TCP Buffer Chain

```
PostgreSQL Backend
      │
      ▼
  send() blocked ← kernel send buffer full
      │
      ▼
  Network (TCP)
      │
      ▼
  Client TCP recv buffer ← FULL (client not reading)
      │
      ▼
  Application (slow to call recv/read on socket)
```

### Common Triggers

- Large result sets returned to a slow application
- Clients on high-latency or low-bandwidth connections
- Application processing rows one-by-one instead of streaming
- JDBC/psycopg2 clients fetching all rows into memory before processing
- Clients in a GC pause or CPU-intensive operation

---

## Sample Data

### What You'll See in `pg_stat_activity`

```sql
SELECT pid, usename, application_name, client_addr,
       state, wait_event_type, wait_event,
       now() - query_start AS query_duration,
       left(query, 120) AS query_snippet
FROM pg_stat_activity
WHERE wait_event = 'ClientWrite'
ORDER BY query_duration DESC;
```

**Sample Output:**

```
 pid  | usename | application_name | client_addr   | state  | wait_event_type | wait_event  | query_duration   | query_snippet
------+---------+------------------+---------------+--------+-----------------+-------------+------------------+--------------
 2210 | appuser | reporting-svc    | 10.0.2.31     | active | Client          | ClientWrite | 00:01:45.231     | SELECT * FROM orders WHERE ...
 2211 | appuser | etl-job          | 10.0.2.32     | active | Client          | ClientWrite | 00:03:22.012     | SELECT id, payload FROM events ...
```

> **Note:** Unlike `ClientRead`, the state here is usually `active` — the query is running/finished, but delivery is stalled.

---

## Simulate the Wait Event

### Method 1: Large Result Set with Slow Consumer (Python)

```python
import psycopg2
import time

conn = psycopg2.connect(dsn="host=aurora-cluster... dbname=postgres user=appuser password=secret")
cur = conn.cursor()

# Fetch a huge result set but process each row very slowly
cur.execute("SELECT generate_series(1, 10000000)")  # 10 million rows

for row in cur:
    time.sleep(0.001)  # 10ms per row = 100,000 seconds total; backend stuck in ClientWrite
    process(row)
```

### Method 2: Fill Client Buffer (Socket-Level)

```python
import socket
import time

# Open a raw TCP connection to PostgreSQL
sock = socket.create_connection(("aurora-cluster.xxxx.rds.amazonaws.com", 5432))
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)  # tiny receive buffer

# Send a startup message and a large query
# ... (startup handshake)
# Then just stop reading — backend will block on ClientWrite
time.sleep(300)
```

### Method 3: Using pg_read_file to Force Large Writes

```sql
-- From another session, trigger a query that returns huge data to a slow client
-- Run this from a client that has its receive buffer artificially throttled
SELECT pg_read_file('pg_wal/' || name)
FROM pg_ls_waldir()
LIMIT 5;
```

---

## Monitoring Queries

### 1. Active ClientWrite Sessions with Duration

```sql
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    wait_event,
    EXTRACT(EPOCH FROM (now() - query_start))::INT AS query_secs,
    EXTRACT(EPOCH FROM (now() - state_change))::INT AS state_secs,
    left(query, 200) AS query_snippet
FROM pg_stat_activity
WHERE wait_event = 'ClientWrite'
ORDER BY query_secs DESC;
```

### 2. Bytes Sent vs Expected (using pg_stat_replication as a proxy)

```sql
-- For replication-related ClientWrite, check lag
SELECT
    application_name,
    client_addr,
    state,
    sent_lsn,
    write_lsn,
    pg_wal_lsn_diff(sent_lsn, write_lsn) AS pending_bytes
FROM pg_stat_replication
WHERE state = 'streaming';
```

### 3. Correlate ClientWrite with Large Queries

```sql
SELECT
    a.pid,
    a.wait_event,
    a.query_start,
    EXTRACT(EPOCH FROM (now() - a.query_start))::INT AS runtime_secs,
    s.rows_returned,
    s.temp_blks_written,
    left(a.query, 300) AS query
FROM pg_stat_activity a
JOIN pg_stat_statements s
    ON md5(a.query) = md5(s.query)  -- approximate match
WHERE a.wait_event = 'ClientWrite'
ORDER BY runtime_secs DESC;
```

### 4. Network-Level Diagnosis

```bash
# On the Aurora host (via SSM or EC2 jump host):
# Check socket send-Q buildup for the backend PID
ss -tnp | grep <backend_pid>

# Expected output when ClientWrite is occurring:
# State    Recv-Q   Send-Q   Local Address:Port   Peer Address:Port
# ESTAB    0        212992   10.0.1.10:5432       10.0.2.31:54321
#                   ^^^^^^ Non-zero Send-Q means kernel buffer is filling
```

### 5. Wait Event Frequency Over Time

```sql
-- Snapshot-based; run every minute via cron/Lambda and store results
SELECT
    now() AS snapshot_at,
    COUNT(*) FILTER (WHERE wait_event = 'ClientWrite') AS clientwrite_count,
    COUNT(*) FILTER (WHERE wait_event = 'ClientRead')  AS clientread_count,
    COUNT(*) FILTER (WHERE state = 'active')           AS active_count
FROM pg_stat_activity;
```

---

## Resolution & Tuning

### 1. Use Server-Side Cursors to Stream Large Results

Instead of fetching all rows at once, use cursors to stream in chunks:

```python
# psycopg2 server-side cursor — only fetches `itersize` rows at a time
conn = psycopg2.connect(dsn="...")
cur = conn.cursor(name='large_result_cursor')  # named = server-side
cur.itersize = 2000  # fetch 2000 rows per round-trip

cur.execute("SELECT * FROM large_table WHERE created_at > '2024-01-01'")

for row in cur:
    process(row)  # backend sends 2000 rows, waits, sends next 2000
```

```sql
-- SQL-level cursor
BEGIN;
DECLARE my_cursor CURSOR FOR SELECT * FROM orders WHERE status = 'pending';
FETCH 1000 FROM my_cursor;
-- Process...
FETCH 1000 FROM my_cursor;
-- Continue...
CLOSE my_cursor;
COMMIT;
```

### 2. Increase TCP Kernel Buffer Sizes

```bash
# On application server (not Aurora) — increase socket receive buffers
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.rmem_default=262144
sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"
```

### 3. Set `statement_timeout` to Kill Runaway Queries

```sql
-- Prevent any single query from holding a backend in ClientWrite indefinitely
SET statement_timeout = '120s';

-- Aurora parameter group
statement_timeout = 120000
```

### 4. Use LIMIT and Pagination

```sql
-- Instead of SELECT * FROM billion_row_table
-- Use keyset pagination
SELECT id, data
FROM events
WHERE id > :last_seen_id
  AND created_at > now() - INTERVAL '7 days'
ORDER BY id
LIMIT 1000;
```

### 5. Compress Result Sets

```python
# psycopg2: enable SSL compression or use application-level compression
import gzip
import json

cur.execute("SELECT json_agg(t) FROM large_table t")
result = cur.fetchone()[0]
compressed = gzip.compress(json.dumps(result).encode())
# Smaller payload = less time for backend to write to socket
```

### 6. Move Large Exports to S3 (Aurora-Specific)

For ETL/reporting workloads, avoid sending large result sets over TCP entirely:

```sql
-- Aurora PostgreSQL: export directly to S3
SELECT * FROM aws_s3.query_export_to_s3(
    'SELECT * FROM orders WHERE created_at > ''2024-01-01''',
    aws_commons.create_s3_uri('my-bucket', 'exports/orders.csv', 'us-east-1'),
    options := 'format csv, header true'
);
```

---

## Summary

| Dimension | Detail |
|-----------|--------|
| **Root Cause** | Client too slow to consume query results; large result sets; network constraints |
| **Risk** | Backend held active; wastes connections; query appears "stuck" |
| **Quick Fix** | `pg_terminate_backend()` on long-running ClientWrite sessions |
| **Long-Term Fix** | Server-side cursors; pagination; S3 export for bulk; larger client TCP buffers |
| **Key Tuning** | `statement_timeout`; cursor `itersize`; network buffer tuning |
