package leases

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.crdb.sql
var crdbSchema string

func testCRDBStore(t *testing.T) (*CRDBStore, *sql.DB, context.Context) {
	t.Helper()
	dsn := os.Getenv("LEASES_TEST_CRDB_DSN")
	if dsn == "" {
		t.Skip("LEASES_TEST_CRDB_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()

	// Create table (idempotent).
	if _, err := db.ExecContext(ctx, crdbSchema); err != nil {
		t.Fatal(err)
	}
	// Truncate between tests.
	if _, err := db.ExecContext(ctx, "DELETE FROM leases"); err != nil {
		t.Fatal(err)
	}

	return NewCRDBStore(), db, ctx
}

func TestCRDBCreateAndDelete(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	// Create a resource.
	if err := store.Create(ctx, db, "group-a", "res-1"); err != nil {
		t.Fatal(err)
	}

	// Create again is a no-op (ON CONFLICT DO NOTHING).
	if err := store.Create(ctx, db, "group-a", "res-1"); err != nil {
		t.Fatal(err)
	}

	// Delete unclaimed resource succeeds.
	if err := store.Delete(ctx, db, "res-1"); err != nil {
		t.Fatal(err)
	}

	// Delete again returns ErrNotFound.
	if err := store.Delete(ctx, db, "res-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCRDBCreateAndAcquire(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	// CreateAndAcquire returns a held lease.
	lease, err := store.CreateAndAcquire(ctx, db, "g", "res-1", "owner-a", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.LeaseToken == nil {
		t.Fatal("expected non-nil LeaseToken")
	}
	if lease.ClaimedBy == nil || *lease.ClaimedBy != "owner-a" {
		t.Fatalf("expected claimed_by=owner-a, got %v", lease.ClaimedBy)
	}
	if lease.ExpiresAt == nil || lease.ExpiresAt.Before(time.Now()) {
		t.Fatal("expected expires_at in the future")
	}

	// Second CreateAndAcquire on same resource returns ErrNotAcquired.
	if _, err := store.CreateAndAcquire(ctx, db, "g", "res-1", "owner-b", 30*time.Second); !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("expected ErrNotAcquired, got %v", err)
	}

	// The original lease is still valid — heartbeat succeeds.
	if _, err := store.Heartbeat(ctx, db, "res-1", *lease.LeaseToken, 30*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestCRDBDeleteActiveLeaseBlocked(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	if err := store.Create(ctx, db, "g", "res-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, db, "res-1", "owner-a", 10*time.Minute); err != nil {
		t.Fatal(err)
	}

	// Should not delete an active (non-expired) lease.
	if err := store.Delete(ctx, db, "res-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for active lease, got %v", err)
	}
}

func TestCRDBAcquireAndRelease(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	if err := store.Create(ctx, db, "g", "res-1"); err != nil {
		t.Fatal(err)
	}

	// Acquire the lease.
	lease, err := store.Acquire(ctx, db, "res-1", "owner-a", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.LeaseToken == nil {
		t.Fatal("expected non-nil LeaseToken")
	}
	if lease.ClaimedBy == nil || *lease.ClaimedBy != "owner-a" {
		t.Fatalf("expected claimed_by=owner-a, got %v", lease.ClaimedBy)
	}
	if lease.ExpiresAt == nil || lease.ExpiresAt.Before(time.Now()) {
		t.Fatal("expected expires_at in the future")
	}

	// Second acquire should fail (already held).
	if _, err := store.Acquire(ctx, db, "res-1", "owner-b", 30*time.Second); !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("expected ErrNotAcquired, got %v", err)
	}

	// Release with the correct token.
	if err := store.Release(ctx, db, "res-1", *lease.LeaseToken); err != nil {
		t.Fatal(err)
	}

	// After release, another owner can acquire.
	lease2, err := store.Acquire(ctx, db, "res-1", "owner-b", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease2.ClaimedBy == nil || *lease2.ClaimedBy != "owner-b" {
		t.Fatal("expected owner-b to acquire after release")
	}
}

func TestCRDBReleaseWithWrongToken(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	if err := store.Create(ctx, db, "g", "res-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, db, "res-1", "owner-a", 10*time.Minute); err != nil {
		t.Fatal(err)
	}

	bogus, _ := NewLeaseToken()
	if err := store.Release(ctx, db, "res-1", bogus); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for wrong token, got %v", err)
	}
}

func TestCRDBHeartbeat(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	if err := store.Create(ctx, db, "g", "res-1"); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, db, "res-1", "owner-a", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	origExpiry := *lease.ExpiresAt

	// Heartbeat extends expiry.
	updated, err := store.Heartbeat(ctx, db, "res-1", *lease.LeaseToken, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.ExpiresAt.After(origExpiry) {
		t.Fatalf("heartbeat should extend expiry: orig=%v new=%v", origExpiry, *updated.ExpiresAt)
	}
}

func TestCRDBHeartbeatWrongToken(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	if err := store.Create(ctx, db, "g", "res-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, db, "res-1", "owner-a", 10*time.Minute); err != nil {
		t.Fatal(err)
	}

	bogus, _ := NewLeaseToken()
	if _, err := store.Heartbeat(ctx, db, "res-1", bogus, 10*time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for wrong token, got %v", err)
	}
}

func TestCRDBAcquireMany(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	for _, r := range []string{"res-1", "res-2", "res-3"} {
		if err := store.Create(ctx, db, "batch", r); err != nil {
			t.Fatal(err)
		}
	}

	// Acquire up to 2.
	leases, err := store.AcquireMany(ctx, db, "batch", 2, "owner-a", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 2 {
		t.Fatalf("expected 2 leases, got %d", len(leases))
	}
	for _, l := range leases {
		if l.ClaimedBy == nil || *l.ClaimedBy != "owner-a" {
			t.Fatal("expected owner-a")
		}
	}

	// Only 1 remains unclaimed.
	remaining, err := store.AcquireMany(ctx, db, "batch", 10, "owner-b", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining, got %d", len(remaining))
	}
}

func TestCRDBHeartbeatMany(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	for _, r := range []string{"res-1", "res-2"} {
		if err := store.Create(ctx, db, "g", r); err != nil {
			t.Fatal(err)
		}
	}

	leases, err := store.AcquireMany(ctx, db, "g", 2, "owner-a", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 2 {
		t.Fatal("expected 2 leases")
	}

	reqs := make([]HeartbeatRequest, len(leases))
	for i, l := range leases {
		reqs[i] = HeartbeatRequest{Resource: l.Resource, LeaseToken: *l.LeaseToken}
	}

	updated, err := store.HeartbeatMany(ctx, db, reqs, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(updated) != 2 {
		t.Fatalf("expected 2 updated, got %d", len(updated))
	}
	for _, u := range updated {
		if u.ExpiresAt == nil || u.ExpiresAt.Before(time.Now().Add(50*time.Second)) {
			t.Fatal("heartbeat should have extended expiry")
		}
	}
}

func TestCRDBHeartbeatManyEmpty(t *testing.T) {
	store, db, ctx := testCRDBStore(t)
	leases, err := store.HeartbeatMany(ctx, db, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if leases != nil {
		t.Fatalf("expected nil, got %v", leases)
	}
}

func TestCRDBAcquireNonexistent(t *testing.T) {
	store, db, ctx := testCRDBStore(t)

	// Acquire a resource that was never created.
	if _, err := store.Acquire(ctx, db, "ghost", "owner-a", time.Minute); !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("expected ErrNotAcquired, got %v", err)
	}
}
