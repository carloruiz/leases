# Lease Module

> **Note:** PostgreSQL support is implemented but not yet tested against a live database.

## Overview

The lease module provides distributed, time-bound ownership of named resources.

A lease represents exclusive ownership of a resource by a single claimant.
stringship is maintained by periodically extending an expiration timestamp
(`expires_at`). If the lease is not renewed, it becomes claimable by another
agent.

This module is implemented in Go backed by CockroachDB. The `Store` interface
defines all lease operations, and each database backend provides its own
implementation (e.g. `CRDBStore` for CockroachDB).

---

## Goals

- Exclusive ownership per resource
- Time-bounded leases with expiration
- Simple and efficient heartbeat model (extend expiry)
- Visibility into current owner
- Grouping for bulk acquisition
- Batch operations (`AcquireMany`, `HeartbeatMany`)
- Caller-controlled transactions (every operation accepts a `DBTX`)
- Pluggable store implementations (one `Store` interface, per-database backends)

---

## Getting Started

### 1. Install the module

```
go get github.com/carloruiz/leases
```

### 2. Add the schema to your migrations

Copy the schema file that matches your database into your migration framework:

```
schema.crdb.sql    — CockroachDB
schema.pg.sql      — PostgreSQL (future)
```

### 3. Use the store

```go
import "github.com/carloruiz/leases"

db, _ := sql.Open("pgx", dsn)
store := leases.NewCRDBStore()

// Pass db directly for standalone operations.
store.Create(ctx, db, "workers", "worker-1")
lease, _ := store.Acquire(ctx, db, "worker-1", "host-a", 30*time.Second)

// Or pass a *sql.Tx to participate in a caller-controlled transaction.
tx, _ := db.BeginTx(ctx, nil)
store.Heartbeat(ctx, tx, "worker-1", *lease.LeaseToken, 30*time.Second)
// ... other application writes ...
tx.Commit()
```

---

## Non-Goals

- Fair scheduling
- FIFO ordering
- Re-entrant locks
- Shared/read locks
- Instant revocation

---

## Data Model

```sql
CREATE TABLE leases (
    resource_name STRING NOT NULL PRIMARY KEY,
    group_name    STRING NOT NULL,

    lease_token   UUID,
    claimed_by    STRING,
    claimed_at    TIMESTAMPTZ,
    expires_at    TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    FAMILY default (resource_name, group_name, lease_token, claimed_by, claimed_at, created_at),
    FAMILY f_expires (expires_at),

    INDEX leases_claimable_by_group (group_name, expires_at, resource_name)
);
```

---

## State Model

### Unclaimed
- `expires_at = 'epoch'`

### Claimed (active)
- `expires_at > now()`

### Claimed (expired)
- `expires_at <= now()` (and `expires_at > 'epoch'`)

Expired leases remain in the table and can be reclaimed.

---

## API

```go
type Lease struct {
    Resource   string
    Group      string
    LeaseToken *LeaseToken
    ClaimedBy  *string
    ClaimedAt  *time.Time
    ExpiresAt  *time.Time
}

type HeartbeatRequest struct {
    Resource   string
    LeaseToken LeaseToken
}

type LeaseToken struct{ /* unexported */ }

func NewLeaseToken() (LeaseToken, error)

type Store interface {
    Create(ctx context.Context, db DBTX, group, resource string) error
    Delete(ctx context.Context, db DBTX, resource string) error

    Acquire(ctx context.Context, db DBTX, resource string, owner string, duration time.Duration) (*Lease, error)
    Release(ctx context.Context, db DBTX, resource string, token LeaseToken) error

    AcquireMany(ctx context.Context, db DBTX, group string, limit int, owner string, duration time.Duration) ([]Lease, error)

    Heartbeat(ctx context.Context, db DBTX, resource string, token LeaseToken, duration time.Duration) (*Lease, error)
    HeartbeatMany(ctx context.Context, db DBTX, items []HeartbeatRequest, duration time.Duration) ([]Lease, error)
}
```

---

## Operations

### Create

```sql
INSERT INTO leases (resource_name, group_name)
VALUES ($1, $2)
ON CONFLICT (resource_name) DO NOTHING;
```

---

### Delete

```sql
DELETE FROM leases
WHERE resource_name = $1
  AND expires_at <= now();
```

---

### Acquire

```sql
UPDATE leases
SET
    lease_token = $2,
    claimed_by  = $3,
    claimed_at  = now(),
    expires_at  = now() + $4
WHERE resource_name = $1
  AND expires_at <= now()
RETURNING *;
```

---

### Release

```sql
UPDATE leases
SET
    lease_token = NULL,
    claimed_by  = NULL,
    claimed_at  = NULL,
    expires_at  = '1970-01-01T00:00:00Z'
WHERE resource_name = $1
  AND lease_token = $2;
```

---

### Heartbeat

```sql
UPDATE leases
SET
    expires_at = now() + $3
WHERE resource_name = $1
  AND lease_token = $2
RETURNING *;
```

---

### HeartbeatMany

```sql
WITH req(resource_name, lease_token) AS (
    VALUES
        ($1, $2),
        ($3, $4)
)
UPDATE leases AS l
SET
    expires_at = now() + $5
FROM req
WHERE l.resource_name = req.resource_name
  AND l.lease_token = req.lease_token
RETURNING l.*;
```

---

### AcquireMany

```sql
WITH candidates AS (
    SELECT resource_name
    FROM leases
    WHERE group_name = $1
      AND expires_at <= now()
    ORDER BY resource_name
    LIMIT $2
)
UPDATE leases
SET
    lease_token = gen_random_uuid(),
    claimed_by  = $3,
    claimed_at  = now(),
    expires_at  = now() + $4
WHERE resource_name IN (SELECT resource_name FROM candidates)
RETURNING *;
```

---

## Schema Management

Each database backend has its own schema file, named `schema.<backend>.sql`:

```
schema.crdb.sql    — CockroachDB
schema.pg.sql      — PostgreSQL (future)
```

Schema files contain idempotent `CREATE TABLE IF NOT EXISTS` statements and can
be run repeatedly without side effects. The module does not run schema changes
at runtime — callers apply them via their own migration tooling or init code.

When the schema needs to change, update the relevant `schema.<backend>.sql` file
and apply the alteration manually (e.g. `ALTER TABLE` in a migration script).

---

## Design Notes

- `lease_token` provides fencing and prevents stale writes
- `expires_at` is the only hot column (isolated in its own column family)
- caller fully controls lease duration and renewal policy
- expired leases are not deleted automatically
- every method accepts `DBTX` — works with `*sql.DB`, `*sql.Tx`, or `*sql.Conn`

---

## Summary

- Single table (`leases`)
- One row per resource
- Expiry-based ownership
- Caller-controlled lease policy
- Caller-controlled transactions via `DBTX`
- Pluggable backends via `Store` interface
- Per-database schema files (`schema.<backend>.sql`)
- Optimized for simplicity and performance
