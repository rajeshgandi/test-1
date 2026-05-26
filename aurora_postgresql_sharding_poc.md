# Proof of Concept: Sharding in Aurora PostgreSQL

**Document Type:** Technical POC Guide  
**Target Audience:** PostgreSQL Database Engineers  
**Aurora Version:** Aurora PostgreSQL 15.x / 16.x  
**Date:** May 2026  

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Sharding Approaches for Aurora PostgreSQL](#2-sharding-approaches-for-aurora-postgresql)
3. [Architecture Overview](#3-architecture-overview)
4. [Approach 1: Application-Level Sharding](#4-approach-1-application-level-sharding)
5. [Approach 2: pg_partman + Foreign Data Wrappers (FDW)](#5-approach-2-pg_partman--foreign-data-wrappers-fdw)
6. [Approach 3: Citus on Aurora PostgreSQL](#6-approach-3-citus-on-aurora-postgresql)
7. [Approach 4: AWS RDS Proxy + Lambda Shard Router](#7-approach-4-aws-rds-proxy--lambda-shard-router)
8. [Simulation Environment Setup](#8-simulation-environment-setup)
9. [Validation & Testing](#9-validation--testing)
10. [Performance Benchmarking](#10-performance-benchmarking)
11. [Failure Simulation & Resilience Testing](#11-failure-simulation--resilience-testing)
12. [Monitoring & Observability](#12-monitoring--observability)
13. [Decision Matrix](#13-decision-matrix)
14. [Recommendations](#14-recommendations)

---

## 1. Executive Summary

Amazon Aurora PostgreSQL does not natively provide automatic horizontal sharding (unlike Aurora Limitless, which is a separate product). However, sharding can be achieved through several patterns depending on your workload, scale, and operational tolerance.

This POC covers four viable sharding strategies, with complete SQL scripts, simulation steps, and validation procedures for each approach.

**Key Goals of This POC:**
- Distribute write load across multiple Aurora clusters or nodes
- Achieve linear scalability beyond a single Aurora cluster's capacity
- Maintain ACID guarantees within a shard
- Enable transparent or semi-transparent query routing
- Validate shard rebalancing, failure recovery, and cross-shard query patterns

---

## 2. Sharding Approaches for Aurora PostgreSQL

| Approach | Sharding Layer | Cross-Shard Queries | Operational Complexity | Best For |
|---|---|---|---|---|
| Application-Level Sharding | Application code | Manual | Low | Greenfield, full control |
| pg_partman + FDW | PostgreSQL native | Via FDW | Medium | Existing schemas |
| Citus Extension | PostgreSQL extension | Native | Medium-High | OLAP + OLTP hybrid |
| RDS Proxy + Lambda Router | AWS infrastructure | Via aggregation | High | Serverless architectures |

---

## 3. Architecture Overview

### Conceptual Sharding Layout

```
                        ┌─────────────────────────────────┐
                        │        Application / ORM         │
                        └──────────────┬──────────────────┘
                                       │
                        ┌──────────────▼──────────────────┐
                        │      Shard Router / Coordinator  │
                        │  (App logic / FDW / Citus / AWS) │
                        └──────┬──────────┬───────────────┘
                               │          │
              ┌────────────────▼──┐   ┌───▼────────────────┐
              │  Aurora Cluster 1  │   │  Aurora Cluster 2   │
              │  Shard: users 0-M  │   │  Shard: users M+1-N │
              │  (Writer + Reader) │   │  (Writer + Reader)  │
              └────────────────────┘   └─────────────────────┘
```

### Shard Key Selection Principles

- **High cardinality:** `user_id`, `tenant_id`, `order_id` — avoid low-cardinality keys like `status` or `region`
- **Even distribution:** Hash-based sharding prevents hot spots
- **Immutability:** Never change the shard key value after insert
- **Query locality:** Most queries should touch only one shard

---

## 4. Approach 1: Application-Level Sharding

### 4.1 Concept

The application computes the target shard using a hash function on the shard key, then routes the connection to the corresponding Aurora cluster.

### 4.2 Setup: Multiple Aurora Clusters

```bash
# Provision 3 Aurora PostgreSQL clusters via AWS CLI
for i in 1 2 3; do
  aws rds create-db-cluster \
    --db-cluster-identifier "shard-cluster-${i}" \
    --engine aurora-postgresql \
    --engine-version 16.1 \
    --master-username postgres \
    --master-user-password "SecurePass${i}!" \
    --db-subnet-group-name my-subnet-group \
    --vpc-security-group-ids sg-xxxxxxxx \
    --serverless-v2-scaling-configuration MinCapacity=0.5,MaxCapacity=16
done
```

### 4.3 Shard Configuration Table (Central Registry)

Create a central config database (separate small Aurora cluster or RDS instance):

```sql
-- On the config cluster
CREATE DATABASE shard_registry;
\c shard_registry

CREATE TABLE shard_map (
    shard_id        INT PRIMARY KEY,
    shard_name      TEXT NOT NULL,
    host            TEXT NOT NULL,
    port            INT  NOT NULL DEFAULT 5432,
    database_name   TEXT NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT now(),
    key_range_start BIGINT,
    key_range_end   BIGINT
);

INSERT INTO shard_map (shard_id, shard_name, host, port, database_name, key_range_start, key_range_end)
VALUES
  (1, 'shard-1', 'shard-cluster-1.cluster-xxxx.us-east-1.rds.amazonaws.com', 5432, 'appdb', 0,           333333333),
  (2, 'shard-2', 'shard-cluster-2.cluster-xxxx.us-east-1.rds.amazonaws.com', 5432, 'appdb', 333333334,   666666666),
  (3, 'shard-3', 'shard-cluster-3.cluster-xxxx.us-east-1.rds.amazonaws.com', 5432, 'appdb', 666666667,   999999999);
```

### 4.4 Schema on Each Shard

Run identical DDL on all shard clusters:

```sql
-- Execute on shard-1, shard-2, shard-3
CREATE TABLE users (
    user_id     BIGINT      NOT NULL,
    tenant_id   INT         NOT NULL,
    username    TEXT        NOT NULL,
    email       TEXT        NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now(),
    shard_id    INT         NOT NULL,
    PRIMARY KEY (user_id, shard_id)  -- Include shard_id to enforce locality
);

CREATE TABLE orders (
    order_id    BIGSERIAL   NOT NULL,
    user_id     BIGINT      NOT NULL,
    shard_id    INT         NOT NULL,
    amount      NUMERIC(12,2),
    status      TEXT        DEFAULT 'pending',
    created_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (order_id, shard_id)
);

CREATE INDEX idx_users_user_id   ON users(user_id);
CREATE INDEX idx_orders_user_id  ON orders(user_id);
CREATE INDEX idx_orders_status   ON orders(status);
```

### 4.5 Application-Side Shard Router (Python)

```python
import hashlib
import psycopg2
from functools import lru_cache

SHARD_CONFIGS = {
    1: {"host": "shard-cluster-1.cluster-xxxx.us-east-1.rds.amazonaws.com", "dbname": "appdb"},
    2: {"host": "shard-cluster-2.cluster-xxxx.us-east-1.rds.amazonaws.com", "dbname": "appdb"},
    3: {"host": "shard-cluster-3.cluster-xxxx.us-east-1.rds.amazonaws.com", "dbname": "appdb"},
}
NUM_SHARDS = 3

def get_shard_id(shard_key: int) -> int:
    """Consistent hash-based shard routing."""
    hash_val = int(hashlib.md5(str(shard_key).encode()).hexdigest(), 16)
    return (hash_val % NUM_SHARDS) + 1

@lru_cache(maxsize=NUM_SHARDS)
def get_connection(shard_id: int):
    cfg = SHARD_CONFIGS[shard_id]
    return psycopg2.connect(
        host=cfg["host"],
        dbname=cfg["dbname"],
        user="postgres",
        password="SecurePass!",
        port=5432,
        connect_timeout=5
    )

def insert_user(user_id: int, username: str, email: str):
    shard_id = get_shard_id(user_id)
    conn = get_connection(shard_id)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, tenant_id, username, email, shard_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id, shard_id) DO NOTHING
        """, (user_id, 1, username, email, shard_id))
    conn.commit()
    return shard_id

def get_user(user_id: int):
    shard_id = get_shard_id(user_id)
    conn = get_connection(shard_id)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        return cur.fetchone()
```

---

## 5. Approach 2: pg_partman + Foreign Data Wrappers (FDW)

### 5.1 Concept

Use a single Aurora coordinator cluster with `postgres_fdw` pointing to multiple shard Aurora clusters. Native partitioning (declarative) combined with FDW foreign partitions creates a transparent sharding layer.

### 5.2 Install Extensions on Coordinator

```sql
-- On the coordinator Aurora cluster
CREATE EXTENSION postgres_fdw;
CREATE EXTENSION pg_partman SCHEMA partman;
```

### 5.3 Create Foreign Servers (One Per Shard)

```sql
-- Foreign server for shard 1
CREATE SERVER shard1_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'shard-cluster-1.cluster-xxxx.us-east-1.rds.amazonaws.com',
    port '5432',
    dbname 'appdb',
    fetch_size '10000',
    use_remote_estimate 'true',
    async_capable 'true'    -- enables parallel FDW scans (PG 14+)
  );

CREATE USER MAPPING FOR postgres
  SERVER shard1_server
  OPTIONS (user 'fdw_user', password 'fdwpassword1');

-- Foreign server for shard 2
CREATE SERVER shard2_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'shard-cluster-2.cluster-xxxx.us-east-1.rds.amazonaws.com',
    port '5432',
    dbname 'appdb',
    fetch_size '10000',
    use_remote_estimate 'true',
    async_capable 'true'
  );

CREATE USER MAPPING FOR postgres
  SERVER shard2_server
  OPTIONS (user 'fdw_user', password 'fdwpassword2');

-- Foreign server for shard 3
CREATE SERVER shard3_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'shard-cluster-3.cluster-xxxx.us-east-1.rds.amazonaws.com',
    port '5432',
    dbname 'appdb',
    fetch_size '10000',
    use_remote_estimate 'true',
    async_capable 'true'
  );

CREATE USER MAPPING FOR postgres
  SERVER shard3_server
  OPTIONS (user 'fdw_user', password 'fdwpassword3');
```

### 5.4 Create Local Tables on Each Shard

```sql
-- Run on shard-1, shard-2, shard-3 (identical DDL)
CREATE TABLE users_local (
    user_id    BIGINT NOT NULL,
    username   TEXT   NOT NULL,
    email      TEXT   NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON users_local (user_id);
```

### 5.5 Create Partitioned Parent Table on Coordinator

```sql
-- On coordinator: partitioned parent (hash partitioning by user_id)
CREATE TABLE users (
    user_id    BIGINT NOT NULL,
    username   TEXT   NOT NULL,
    email      TEXT   NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
) PARTITION BY HASH (user_id);

-- Attach foreign tables as partitions
CREATE FOREIGN TABLE users_shard1
  PARTITION OF users
  FOR VALUES WITH (MODULUS 3, REMAINDER 0)
  SERVER shard1_server
  OPTIONS (table_name 'users_local');

CREATE FOREIGN TABLE users_shard2
  PARTITION OF users
  FOR VALUES WITH (MODULUS 3, REMAINDER 1)
  SERVER shard2_server
  OPTIONS (table_name 'users_local');

CREATE FOREIGN TABLE users_shard3
  PARTITION OF users
  FOR VALUES WITH (MODULUS 3, REMAINDER 2)
  SERVER shard3_server
  OPTIONS (table_name 'users_local');
```

### 5.6 Verify Partition Pruning

```sql
-- Explain should show only 1 shard accessed
EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT * FROM users WHERE user_id = 12345;

-- Expected output excerpt:
-- ->  Foreign Scan on users_shard2  (cost=100.00..200.00 rows=1)
--     Remote SQL: SELECT user_id, username, email, created_at
--                 FROM users_local WHERE user_id = 12345
```

### 5.7 FDW User Permissions on Each Shard

```sql
-- On each shard cluster
CREATE USER fdw_user WITH PASSWORD 'fdwpassword1';
GRANT CONNECT ON DATABASE appdb TO fdw_user;
GRANT USAGE ON SCHEMA public TO fdw_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON users_local TO fdw_user;
```

---

## 6. Approach 3: Citus on Aurora PostgreSQL

> **Note:** Citus requires installing the extension. On Aurora, this is supported via custom parameter groups and the `citus` shared library. Verify your Aurora version supports Citus before proceeding.

### 6.1 Enable Citus

```sql
-- On coordinator node
CREATE EXTENSION citus;
SELECT citus_version();

-- Add worker nodes (Aurora Read Replicas or separate clusters)
SELECT citus_add_node('worker-1.xxxx.rds.amazonaws.com', 5432);
SELECT citus_add_node('worker-2.xxxx.rds.amazonaws.com', 5432);
SELECT citus_add_node('worker-3.xxxx.rds.amazonaws.com', 5432);

-- Verify nodes
SELECT * FROM citus_get_active_worker_nodes();
```

### 6.2 Create and Distribute Tables

```sql
-- Create table on coordinator
CREATE TABLE users (
    user_id    BIGINT      NOT NULL,
    tenant_id  INT         NOT NULL,
    username   TEXT        NOT NULL,
    email      TEXT        NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, tenant_id)
);

-- Distribute using hash sharding on user_id
SELECT create_distributed_table('users', 'user_id', shard_count := 32);

-- Co-locate orders with users (same shard key = same physical shard)
CREATE TABLE orders (
    order_id   BIGSERIAL   NOT NULL,
    user_id    BIGINT      NOT NULL,
    amount     NUMERIC(12,2),
    status     TEXT        DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (order_id, user_id)
);

SELECT create_distributed_table('orders', 'user_id',
  colocate_with := 'users');  -- critical: ensures JOIN stays on one shard
```

### 6.3 Reference Tables (Broadcast to All Shards)

```sql
-- Small lookup tables: replicated to every shard
CREATE TABLE product_categories (
    category_id   INT PRIMARY KEY,
    category_name TEXT NOT NULL
);

SELECT create_reference_table('product_categories');
```

### 6.4 Shard Rebalancing

```sql
-- Check shard distribution
SELECT nodename, count(*) as shard_count
FROM citus_shards
GROUP BY nodename
ORDER BY shard_count DESC;

-- Rebalance shards across workers (non-blocking with pg 16+)
SELECT citus_rebalance_start();

-- Monitor rebalance progress
SELECT * FROM citus_rebalance_status();
```

---

## 7. Approach 4: AWS RDS Proxy + Lambda Shard Router

### 7.1 Concept

AWS Lambda acts as the shard-routing middleware. RDS Proxy pools connections per shard, and Lambda computes routing before forwarding queries.

### 7.2 Lambda Shard Router (Node.js)

```javascript
const { Client } = require('pg');
const crypto = require('crypto');

const SHARDS = {
  1: process.env.SHARD1_PROXY_ENDPOINT,
  2: process.env.SHARD2_PROXY_ENDPOINT,
  3: process.env.SHARD3_PROXY_ENDPOINT,
};

function computeShard(userId) {
  const hash = crypto.createHash('md5').update(String(userId)).digest('hex');
  return (parseInt(hash.substring(0, 8), 16) % 3) + 1;
}

exports.handler = async (event) => {
  const { operation, user_id, data } = event;
  const shardId = computeShard(user_id);
  const endpoint = SHARDS[shardId];

  const client = new Client({
    host: endpoint,
    database: 'appdb',
    user: 'app_user',
    password: process.env.DB_PASSWORD,
    port: 5432,
    ssl: { rejectUnauthorized: true },
  });

  await client.connect();
  let result;

  try {
    if (operation === 'INSERT_USER') {
      result = await client.query(
        'INSERT INTO users (user_id, username, email) VALUES ($1, $2, $3) RETURNING *',
        [user_id, data.username, data.email]
      );
    } else if (operation === 'GET_USER') {
      result = await client.query(
        'SELECT * FROM users WHERE user_id = $1',
        [user_id]
      );
    }
    return { statusCode: 200, body: result.rows, shard_id: shardId };
  } finally {
    await client.end();
  }
};
```

---

## 8. Simulation Environment Setup

This section creates a local simulation using Docker containers mimicking multiple Aurora clusters.

### 8.1 Docker Compose: 3-Shard Simulation

```yaml
# docker-compose.yml
version: '3.8'

services:
  shard1:
    image: postgres:16
    container_name: pg_shard1
    environment:
      POSTGRES_DB: appdb
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: shard1pass
    ports:
      - "5441:5432"
    volumes:
      - shard1_data:/var/lib/postgresql/data
      - ./init/shard_schema.sql:/docker-entrypoint-initdb.d/01_schema.sql
    command: >
      postgres
        -c shared_preload_libraries='pg_stat_statements'
        -c pg_stat_statements.track=all
        -c max_connections=200
        -c shared_buffers=256MB
        -c work_mem=16MB

  shard2:
    image: postgres:16
    container_name: pg_shard2
    environment:
      POSTGRES_DB: appdb
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: shard2pass
    ports:
      - "5442:5432"
    volumes:
      - shard2_data:/var/lib/postgresql/data
      - ./init/shard_schema.sql:/docker-entrypoint-initdb.d/01_schema.sql
    command: >
      postgres
        -c shared_preload_libraries='pg_stat_statements'
        -c pg_stat_statements.track=all
        -c max_connections=200
        -c shared_buffers=256MB

  shard3:
    image: postgres:16
    container_name: pg_shard3
    environment:
      POSTGRES_DB: appdb
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: shard3pass
    ports:
      - "5443:5432"
    volumes:
      - shard3_data:/var/lib/postgresql/data
      - ./init/shard_schema.sql:/docker-entrypoint-initdb.d/01_schema.sql
    command: >
      postgres
        -c shared_preload_libraries='pg_stat_statements'
        -c pg_stat_statements.track=all
        -c max_connections=200
        -c shared_buffers=256MB

  coordinator:
    image: postgres:16
    container_name: pg_coordinator
    environment:
      POSTGRES_DB: coordinator
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: coordpass
    ports:
      - "5440:5432"
    depends_on:
      - shard1
      - shard2
      - shard3
    volumes:
      - coordinator_data:/var/lib/postgresql/data
      - ./init/coordinator_setup.sql:/docker-entrypoint-initdb.d/01_setup.sql

volumes:
  shard1_data:
  shard2_data:
  shard3_data:
  coordinator_data:
```

### 8.2 Shard Schema Init Script

```sql
-- init/shard_schema.sql (applied to all shards)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE users (
    user_id    BIGINT      NOT NULL,
    username   TEXT        NOT NULL,
    email      TEXT        UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    shard_id   SMALLINT    NOT NULL,
    PRIMARY KEY (user_id)
);

CREATE TABLE orders (
    order_id   BIGSERIAL   PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users(user_id),
    amount     NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    status     TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending','paid','shipped','cancelled')),
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status  ON orders(status);
CREATE INDEX idx_orders_created ON orders(created_at DESC);

-- FDW user for coordinator access
CREATE USER fdw_user WITH PASSWORD 'fdwpass';
GRANT CONNECT ON DATABASE appdb TO fdw_user;
GRANT USAGE ON SCHEMA public TO fdw_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fdw_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fdw_user;

-- Monitoring view
CREATE VIEW shard_stats AS
SELECT
    schemaname,
    relname AS table_name,
    n_live_tup AS live_rows,
    n_dead_tup AS dead_rows,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    last_vacuum,
    last_autovacuum,
    last_analyze
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
```

### 8.3 Coordinator Setup Script

```sql
-- init/coordinator_setup.sql
CREATE EXTENSION postgres_fdw;

-- Foreign servers pointing to Docker shards
CREATE SERVER shard1 FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'pg_shard1', port '5432', dbname 'appdb',
           use_remote_estimate 'true', async_capable 'true');

CREATE SERVER shard2 FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'pg_shard2', port '5432', dbname 'appdb',
           use_remote_estimate 'true', async_capable 'true');

CREATE SERVER shard3 FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'pg_shard3', port '5432', dbname 'appdb',
           use_remote_estimate 'true', async_capable 'true');

CREATE USER MAPPING FOR postgres SERVER shard1 OPTIONS (user 'fdw_user', password 'fdwpass');
CREATE USER MAPPING FOR postgres SERVER shard2 OPTIONS (user 'fdw_user', password 'fdwpass');
CREATE USER MAPPING FOR postgres SERVER shard3 OPTIONS (user 'fdw_user', password 'fdwpass');

-- Partitioned parent
CREATE TABLE users (
    user_id    BIGINT      NOT NULL,
    username   TEXT        NOT NULL,
    email      TEXT        NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    shard_id   SMALLINT    NOT NULL
) PARTITION BY HASH (user_id);

CREATE FOREIGN TABLE users_p0 PARTITION OF users
  FOR VALUES WITH (MODULUS 3, REMAINDER 0)
  SERVER shard1 OPTIONS (table_name 'users');

CREATE FOREIGN TABLE users_p1 PARTITION OF users
  FOR VALUES WITH (MODULUS 3, REMAINDER 1)
  SERVER shard2 OPTIONS (table_name 'users');

CREATE FOREIGN TABLE users_p2 PARTITION OF users
  FOR VALUES WITH (MODULUS 3, REMAINDER 2)
  SERVER shard3 OPTIONS (table_name 'users');
```

### 8.4 Start the Simulation

```bash
# Start all containers
docker-compose up -d

# Wait for health
sleep 5

# Verify all shards are up
for port in 5440 5441 5442 5443; do
  psql -h localhost -p $port -U postgres -c "SELECT current_database(), version();" 2>&1 | head -3
done
```

---

## 9. Validation & Testing

### 9.1 Data Distribution Validation

```python
#!/usr/bin/env python3
"""
validate_sharding.py
Inserts N users and validates distribution across shards.
"""
import hashlib
import psycopg2
import random
import string
from collections import defaultdict

SHARD_CONNS = {
    1: {"host": "localhost", "port": 5441, "dbname": "appdb", "user": "postgres", "password": "shard1pass"},
    2: {"host": "localhost", "port": 5442, "dbname": "appdb", "user": "postgres", "password": "shard2pass"},
    3: {"host": "localhost", "port": 5443, "dbname": "appdb", "user": "postgres", "password": "shard3pass"},
}

def get_shard(user_id: int, num_shards: int = 3) -> int:
    h = int(hashlib.md5(str(user_id).encode()).hexdigest(), 16)
    return (h % num_shards) + 1

def random_str(n=8):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

def run_validation(num_users: int = 10000):
    connections = {sid: psycopg2.connect(**cfg) for sid, cfg in SHARD_CONNS.items()}
    distribution = defaultdict(int)
    errors = 0

    print(f"Inserting {num_users} users...")
    for i in range(1, num_users + 1):
        user_id = i
        shard_id = get_shard(user_id)
        conn = connections[shard_id]
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (user_id, username, email, shard_id) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (user_id, f"user_{random_str()}", f"{random_str()}@example.com", shard_id)
                )
            conn.commit()
            distribution[shard_id] += 1
        except Exception as e:
            errors += 1
            conn.rollback()

    print("\n=== Distribution Report ===")
    total = sum(distribution.values())
    for shard_id, count in sorted(distribution.items()):
        pct = (count / total) * 100
        bar = "█" * int(pct / 2)
        print(f"  Shard {shard_id}: {count:6d} rows ({pct:.1f}%) {bar}")
    print(f"  Total:   {total:6d} rows")
    print(f"  Errors:  {errors}")

    # Validate read-back from correct shard
    print("\n=== Read Validation (sample 100 users) ===")
    misrouted = 0
    for user_id in random.sample(range(1, num_users+1), min(100, num_users)):
        expected_shard = get_shard(user_id)
        conn = connections[expected_shard]
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, shard_id FROM users WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            if row is None or row[1] != expected_shard:
                misrouted += 1
                print(f"  MISMATCH: user_id={user_id}, expected shard={expected_shard}, got={row}")

    print(f"  Misrouted reads: {misrouted}/100")

    for conn in connections.values():
        conn.close()

if __name__ == "__main__":
    run_validation(10000)
```

### 9.2 Cross-Shard Query Validation

```sql
-- On coordinator: verify partition pruning eliminates irrelevant shards
EXPLAIN (ANALYZE, VERBOSE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM users WHERE user_id = 42;

-- Expected: only 1 foreign scan, not 3
-- Output should show only one of: users_p0, users_p1, or users_p2

-- Full scatter-gather (all shards): confirm all rows returned
SELECT COUNT(*) FROM users;  -- Should equal total inserts

-- Aggregate across shards
SELECT
    shard_id,
    COUNT(*) AS user_count,
    MIN(created_at) AS oldest,
    MAX(created_at) AS newest
FROM users
GROUP BY shard_id
ORDER BY shard_id;
```

### 9.3 Write Consistency Validation

```sql
-- Verify no duplicate user_ids exist across shards (run on each shard)
-- On shard1
SELECT user_id, COUNT(*) FROM users GROUP BY user_id HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Verify referential integrity within shard
SELECT o.order_id, o.user_id
FROM orders o
LEFT JOIN users u ON u.user_id = o.user_id
WHERE u.user_id IS NULL;
-- Expected: 0 rows (all orders have valid users on same shard)
```

### 9.4 Shard Balance Validation Query

```sql
-- Run on coordinator to get per-shard metrics
WITH shard_counts AS (
    SELECT 1 AS shard_id, (SELECT count(*) FROM users_p0) AS row_count UNION ALL
    SELECT 2,             (SELECT count(*) FROM users_p1)              UNION ALL
    SELECT 3,             (SELECT count(*) FROM users_p2)
),
stats AS (
    SELECT
        avg(row_count) AS avg_rows,
        stddev(row_count) AS stddev_rows,
        max(row_count) AS max_rows,
        min(row_count) AS min_rows
    FROM shard_counts
)
SELECT
    sc.shard_id,
    sc.row_count,
    round((sc.row_count / s.avg_rows - 1) * 100, 2) AS pct_deviation_from_avg,
    CASE
        WHEN abs(sc.row_count / s.avg_rows - 1) > 0.20 THEN 'REBALANCE NEEDED'
        WHEN abs(sc.row_count / s.avg_rows - 1) > 0.10 THEN 'MONITOR'
        ELSE 'OK'
    END AS balance_status
FROM shard_counts sc CROSS JOIN stats s
ORDER BY sc.shard_id;
```

---

## 10. Performance Benchmarking

### 10.1 pgbench Custom Script for Sharded Insert

```bash
# Create custom pgbench script
cat > /tmp/shard_bench.sql << 'EOF'
\set user_id random(1, 1000000)
\set shard_id ((:user_id % 3) + 1)
INSERT INTO users (user_id, username, email, shard_id)
VALUES (:user_id, 'bench_user_' || :user_id::text, 'user' || :user_id::text || '@bench.com', :shard_id)
ON CONFLICT DO NOTHING;
EOF

# Run against each shard (shard1 example)
pgbench -h localhost -p 5441 -U postgres -d appdb \
  -c 50 -j 10 -T 60 \
  -f /tmp/shard_bench.sql \
  --report-latencies \
  2>&1 | tee /tmp/shard1_bench.log

# Run on all shards simultaneously (parallel write test)
for port in 5441 5442 5443; do
  pgbench -h localhost -p $port -U postgres -d appdb \
    -c 50 -j 10 -T 60 \
    -f /tmp/shard_bench.sql &
done
wait
echo "Parallel benchmark complete"
```

### 10.2 Latency Comparison SQL

```sql
-- Measure FDW round-trip latency per shard
\timing on

-- Single-shard point lookup (should be < 5ms)
SELECT * FROM users WHERE user_id = 1234;

-- Full scatter-gather aggregate (sum of all shards)
SELECT COUNT(*), AVG(user_id) FROM users;

-- Per-shard timing: run directly on each shard for comparison
-- (connect to port 5441/5442/5443 directly)
SELECT COUNT(*) FROM users;
```

### 10.3 TPS Measurement Script

```bash
#!/bin/bash
# measure_tps.sh - Measure aggregate TPS across all shards

echo "Starting 60-second TPS test across 3 shards..."
start_time=$(date +%s)

total_inserts=0
for port in 5441 5442 5443; do
  before=$(psql -h localhost -p $port -U postgres -d appdb -t -c "SELECT count(*) FROM users;")
done

sleep 60  # Wait while pgbench runs in background

for port in 5441 5442 5443; do
  after=$(psql -h localhost -p $port -U postgres -d appdb -t -c "SELECT count(*) FROM users;")
  delta=$((after - before))
  total_inserts=$((total_inserts + delta))
  echo "Shard $port: +$delta inserts"
done

echo "Total aggregate TPS: $((total_inserts / 60))"
```

---

## 11. Failure Simulation & Resilience Testing

### 11.1 Shard Failure Simulation

```bash
# Simulate shard2 going offline
docker-compose stop shard2
echo "Shard 2 stopped at $(date)"

# Test: queries to shard1 and shard3 should succeed
psql -h localhost -p 5440 -U postgres -d coordinator -c \
  "SELECT user_id FROM users WHERE user_id = 1;"  -- routes to shard1

# Test: queries routed to shard2 should fail gracefully
psql -h localhost -p 5440 -U postgres -d coordinator -c \
  "SELECT user_id FROM users WHERE user_id = 2;"  -- routes to shard2 -> error expected

# Restore shard2
docker-compose start shard2
sleep 3
echo "Shard 2 restored at $(date)"

# Verify shard2 is accessible again
psql -h localhost -p 5440 -U postgres -d coordinator -c \
  "SELECT COUNT(*) FROM users_p1;"
```

### 11.2 Application-Level Circuit Breaker

```python
import time
import logging
from functools import wraps

class ShardCircuitBreaker:
    """
    Per-shard circuit breaker to handle shard failures gracefully.
    States: CLOSED (normal), OPEN (failing), HALF_OPEN (testing recovery)
    """
    def __init__(self, failure_threshold=5, recovery_timeout=30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_counts = {}
        self.last_failure_time = {}
        self.state = {}  # 'CLOSED', 'OPEN', 'HALF_OPEN'

    def _get_state(self, shard_id):
        if shard_id not in self.state:
            self.state[shard_id] = 'CLOSED'
            self.failure_counts[shard_id] = 0

        if self.state[shard_id] == 'OPEN':
            elapsed = time.time() - self.last_failure_time.get(shard_id, 0)
            if elapsed > self.recovery_timeout:
                self.state[shard_id] = 'HALF_OPEN'
                logging.info(f"Shard {shard_id}: OPEN -> HALF_OPEN (testing recovery)")

        return self.state[shard_id]

    def record_success(self, shard_id):
        self.failure_counts[shard_id] = 0
        self.state[shard_id] = 'CLOSED'

    def record_failure(self, shard_id):
        self.failure_counts[shard_id] = self.failure_counts.get(shard_id, 0) + 1
        self.last_failure_time[shard_id] = time.time()
        if self.failure_counts[shard_id] >= self.failure_threshold:
            self.state[shard_id] = 'OPEN'
            logging.warning(f"Shard {shard_id}: CLOSED -> OPEN (circuit breaker tripped)")

    def is_available(self, shard_id):
        state = self._get_state(shard_id)
        return state in ('CLOSED', 'HALF_OPEN')

breaker = ShardCircuitBreaker()

def execute_with_breaker(shard_id, query_fn):
    if not breaker.is_available(shard_id):
        raise Exception(f"Shard {shard_id} circuit breaker is OPEN — failing fast")
    try:
        result = query_fn()
        breaker.record_success(shard_id)
        return result
    except Exception as e:
        breaker.record_failure(shard_id)
        raise
```

### 11.3 Data Integrity After Recovery

```sql
-- After shard recovery, verify data integrity

-- 1. Check for row count consistency
SELECT
    'shard1' AS shard, count(*) AS users FROM users_p0
UNION ALL
SELECT 'shard2', count(*) FROM users_p1
UNION ALL
SELECT 'shard3', count(*) FROM users_p2;

-- 2. Check for orphaned orders (only within each shard directly)
-- Run on shard2 after recovery
SELECT count(*) AS orphaned_orders
FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM users u WHERE u.user_id = o.user_id
);

-- 3. Verify sequences are not duplicated across shards
-- (bigserial should be independent per shard; use application-generated IDs
--  or a central sequence service for global uniqueness)
SELECT min(order_id), max(order_id), count(*) FROM orders;
```

### 11.4 Write-Ahead Log (WAL) & Replication Lag Check

```sql
-- On each Aurora shard's writer: check replication lag to readers
SELECT
    client_addr,
    state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    (sent_lsn - replay_lsn) AS replication_lag_bytes,
    write_lag,
    flush_lag,
    replay_lag
FROM pg_stat_replication
ORDER BY replication_lag_bytes DESC;

-- Alert if lag > 100MB
SELECT client_addr,
       pg_size_pretty(sent_lsn - replay_lsn) AS lag_size
FROM pg_stat_replication
WHERE (sent_lsn - replay_lsn) > 100 * 1024 * 1024;
```

---

## 12. Monitoring & Observability

### 12.1 Per-Shard Metrics Dashboard Query

```sql
-- Comprehensive shard health view (run on each shard)
CREATE OR REPLACE VIEW shard_health AS
WITH table_stats AS (
    SELECT
        relname,
        n_live_tup,
        n_dead_tup,
        pg_size_pretty(pg_total_relation_size(relid)) AS size,
        round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS bloat_pct
    FROM pg_stat_user_tables
    WHERE relname IN ('users', 'orders')
),
conn_stats AS (
    SELECT count(*) AS total_connections,
           count(*) FILTER (WHERE state = 'active') AS active_queries,
           count(*) FILTER (WHERE state = 'idle') AS idle_connections,
           count(*) FILTER (WHERE wait_event_type = 'Lock') AS blocked_queries
    FROM pg_stat_activity
    WHERE datname = current_database()
),
cache_hit AS (
    SELECT
        round(sum(heap_blks_hit) * 100.0 / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0), 2) AS cache_hit_pct
    FROM pg_statio_user_tables
)
SELECT
    current_setting('server_version') AS pg_version,
    ts.*,
    cs.*,
    ch.cache_hit_pct
FROM table_stats ts, conn_stats cs, cache_hit ch;

SELECT * FROM shard_health;
```

### 12.2 Slow Query Detection

```sql
-- Top 10 slowest queries across shards
SELECT
    round(mean_exec_time::numeric, 2) AS avg_ms,
    round(total_exec_time::numeric, 2) AS total_ms,
    calls,
    rows,
    round(100.0 * total_exec_time / sum(total_exec_time) OVER (), 2) AS pct_total,
    query
FROM pg_stat_statements
WHERE query NOT LIKE '%pg_%'
ORDER BY mean_exec_time DESC
LIMIT 10;
```

### 12.3 CloudWatch Metrics for Aurora Shards (Terraform)

```hcl
# monitoring.tf
resource "aws_cloudwatch_metric_alarm" "shard_cpu" {
  for_each = { "1" = "shard-cluster-1", "2" = "shard-cluster-2", "3" = "shard-cluster-3" }

  alarm_name          = "aurora-shard-${each.key}-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = 60
  statistic           = "Average"
  threshold           = 80

  dimensions = {
    DBClusterIdentifier = each.value
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "shard_connections" {
  for_each = { "1" = "shard-cluster-1", "2" = "shard-cluster-2", "3" = "shard-cluster-3" }

  alarm_name          = "aurora-shard-${each.key}-connections"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DatabaseConnections"
  namespace           = "AWS/RDS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 900

  dimensions = {
    DBClusterIdentifier = each.value
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
}
```

---

## 13. Decision Matrix

| Criterion | App-Level | FDW + Partitioning | Citus | Lambda Router |
|---|---|---|---|---|
| Setup Complexity | Low | Medium | Medium-High | High |
| Cross-Shard JOIN | Manual / not supported | Slow (FDW overhead) | Native | Aggregation in app |
| Transactions | Per-shard only | Per-shard only | Distributed (2PC) | Per-shard only |
| Read Scaling | Via Aurora readers | Via Aurora readers | Via Citus workers | Via RDS Proxy |
| Write Scaling | Excellent | Excellent | Excellent | Excellent |
| Rebalancing | Manual data migration | Manual or FDW remapping | `citus_rebalance_start()` | Lambda config update |
| Operational Overhead | Low | Low-Medium | Medium | High (Lambda + VPC) |
| Aurora Native | Yes | Yes (FDW built-in) | Extension required | Yes |
| Cost | Lowest | Low | Medium (Citus licensing) | Medium-High |
| Best Fit | Multi-tenant SaaS | Existing schemas | OLAP + analytics | Serverless workloads |

---

## 14. Recommendations

### For Most Production Use Cases: FDW + Hash Partitioning

This is the recommended starting point for Aurora PostgreSQL sharding because it uses only native PostgreSQL features (no additional extensions), Aurora's FDW is production-tested, partition pruning eliminates unnecessary shard scans, and it integrates cleanly with existing ORMs and connection pools.

### When to Upgrade to Citus

Consider Citus when you need distributed aggregations (GROUP BY, COUNT across all shards), complex analytical queries across tenants, or automated shard rebalancing with minimal downtime.

### Consider Aurora Limitless

If your workload requires fully transparent, auto-managed horizontal write scaling without explicit sharding logic, evaluate **Aurora Limitless** (now GA). It handles sharding automatically at the storage layer and presents a single endpoint to your application.

### Shard Key Guidelines

Always choose a shard key that appears in every hot query's WHERE clause, never changes after insert, and produces an even hash distribution. The key must co-locate related data — for example, `user_id` for a user-centric application so that all orders, sessions, and preferences for a user land on the same shard.

### Avoid These Anti-Patterns

Do not shard on low-cardinality columns like `status`, `region`, or `boolean` flags. Never allow cross-shard foreign keys. Do not use sequential IDs (SERIAL) as shard keys — they create hotspots on the highest-numbered shard. Avoid global transactions spanning multiple shards; redesign your schema to co-locate data that participates in the same transaction.

---

*End of Aurora PostgreSQL Sharding POC*
