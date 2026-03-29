// Package leases provides distributed, time-bound ownership of named resources.
package leases

import (
	"context"
	"database/sql"
	"time"
)

// DBTX is implemented by *sql.DB, *sql.Tx, and *sql.Conn.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Lease represents exclusive ownership of a resource by a single claimant.
type Lease struct {
	Resource   string
	Group      string
	LeaseToken *LeaseToken
	ClaimedBy  *string
	ClaimedAt  *time.Time
	ExpiresAt  *time.Time
}

// HeartbeatRequest identifies a lease to renew.
type HeartbeatRequest struct {
	Resource   string
	LeaseToken LeaseToken
}

// Store defines the interface for lease operations.
type Store interface {
	Create(ctx context.Context, db DBTX, group, resource string) error
	CreateAndAcquire(ctx context.Context, db DBTX, group, resource, owner string, duration time.Duration) (*Lease, error)
	Delete(ctx context.Context, db DBTX, resource string) error

	Acquire(ctx context.Context, db DBTX, resource string, owner string, duration time.Duration) (*Lease, error)
	Release(ctx context.Context, db DBTX, resource string, token LeaseToken) error

	AcquireMany(ctx context.Context, db DBTX, group string, limit int, owner string, duration time.Duration) ([]Lease, error)

	Heartbeat(ctx context.Context, db DBTX, resource string, token LeaseToken, duration time.Duration) (*Lease, error)
	HeartbeatMany(ctx context.Context, db DBTX, items []HeartbeatRequest, duration time.Duration) ([]Lease, error)
}
