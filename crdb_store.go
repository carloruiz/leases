package leases

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// CRDBStore implements Store backed by CockroachDB.
type CRDBStore struct{}

// NewCRDBStore creates a new CockroachDB-backed lease store.
func NewCRDBStore() *CRDBStore {
	return &CRDBStore{}
}

func (s *CRDBStore) Create(ctx context.Context, db DBTX, group, resource string) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO leases (resource_name, group_name)
		 VALUES ($1, $2)
		 ON CONFLICT (resource_name) DO NOTHING`,
		resource, group)
	return err
}

func (s *CRDBStore) Delete(ctx context.Context, db DBTX, resource string) error {
	res, err := db.ExecContext(ctx,
		`DELETE FROM leases
		 WHERE resource_name = $1
		   AND expires_at <= now()`,
		resource)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *CRDBStore) Acquire(ctx context.Context, db DBTX, resource string, owner string, duration time.Duration) (*Lease, error) {
	token, err := NewLeaseToken()
	if err != nil {
		return nil, fmt.Errorf("generating token: %w", err)
	}
	interval := formatInterval(duration)

	row := db.QueryRowContext(ctx,
		`UPDATE leases
		 SET
		     lease_token = $2,
		     claimed_by  = $3,
		     claimed_at  = now(),
		     expires_at  = now() + $4::INTERVAL
		 WHERE resource_name = $1
		   AND expires_at <= now()
		 RETURNING resource_name, group_name, lease_token, claimed_by, claimed_at, expires_at`,
		resource, token.String(), owner, interval)

	lease, err := scanLease(row)
	if err == sql.ErrNoRows {
		return nil, ErrNotAcquired
	}
	return lease, err
}

func (s *CRDBStore) Release(ctx context.Context, db DBTX, resource string, token LeaseToken) error {
	res, err := db.ExecContext(ctx,
		`UPDATE leases
		 SET
		     lease_token = NULL,
		     claimed_by  = NULL,
		     claimed_at  = NULL,
		     expires_at  = '1970-01-01T00:00:00Z'
		 WHERE resource_name = $1
		   AND lease_token = $2`,
		resource, token.String())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *CRDBStore) AcquireMany(ctx context.Context, db DBTX, group string, limit int, owner string, duration time.Duration) ([]Lease, error) {
	interval := formatInterval(duration)
	rows, err := db.QueryContext(ctx,
		`WITH candidates AS (
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
		     expires_at  = now() + $4::INTERVAL
		 WHERE resource_name IN (SELECT resource_name FROM candidates)
		 RETURNING resource_name, group_name, lease_token, claimed_by, claimed_at, expires_at`,
		group, limit, owner, interval)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLeases(rows)
}

func (s *CRDBStore) Heartbeat(ctx context.Context, db DBTX, resource string, token LeaseToken, duration time.Duration) (*Lease, error) {
	interval := formatInterval(duration)

	row := db.QueryRowContext(ctx,
		`UPDATE leases
		 SET
		     expires_at = now() + $3::INTERVAL
		 WHERE resource_name = $1
		   AND lease_token = $2
		 RETURNING resource_name, group_name, lease_token, claimed_by, claimed_at, expires_at`,
		resource, token.String(), interval)

	lease, err := scanLease(row)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	return lease, err
}

func (s *CRDBStore) HeartbeatMany(ctx context.Context, db DBTX, items []HeartbeatRequest, duration time.Duration) ([]Lease, error) {
	if len(items) == 0 {
		return nil, nil
	}
	interval := formatInterval(duration)

	// Build VALUES clause: ($1, $2), ($3, $4), ...
	var b strings.Builder
	args := make([]interface{}, 0, len(items)*2+1)
	for i, item := range items {
		if i > 0 {
			b.WriteString(", ")
		}
		p := i * 2
		fmt.Fprintf(&b, "($%d, $%d)", p+1, p+2)
		args = append(args, item.Resource, item.LeaseToken.String())
	}
	intervalParam := len(args) + 1
	args = append(args, interval)

	query := fmt.Sprintf(
		`WITH req(resource_name, lease_token) AS (
		     VALUES %s
		 )
		 UPDATE leases AS l
		 SET
		     expires_at = now() + $%d::INTERVAL
		 FROM req
		 WHERE l.resource_name = req.resource_name
		   AND l.lease_token = req.lease_token::UUID
		 RETURNING l.resource_name, l.group_name, l.lease_token, l.claimed_by, l.claimed_at, l.expires_at`,
		b.String(), intervalParam)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLeases(rows)
}

func scanLease(row *sql.Row) (*Lease, error) {
	var l Lease
	var tokenStr *string
	err := row.Scan(&l.Resource, &l.Group, &tokenStr, &l.ClaimedBy, &l.ClaimedAt, &l.ExpiresAt)
	if err != nil {
		return nil, err
	}
	if tokenStr != nil {
		parsed, err := parseLeaseToken(*tokenStr)
		if err != nil {
			return nil, fmt.Errorf("parsing lease_token: %w", err)
		}
		l.LeaseToken = &parsed
	}
	return &l, nil
}

func scanLeases(rows *sql.Rows) ([]Lease, error) {
	var out []Lease
	for rows.Next() {
		var l Lease
		var tokenStr *string
		if err := rows.Scan(&l.Resource, &l.Group, &tokenStr, &l.ClaimedBy, &l.ClaimedAt, &l.ExpiresAt); err != nil {
			return nil, err
		}
		if tokenStr != nil {
			parsed, err := parseLeaseToken(*tokenStr)
			if err != nil {
				return nil, fmt.Errorf("parsing lease_token: %w", err)
			}
			l.LeaseToken = &parsed
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

func formatInterval(d time.Duration) string {
	return fmt.Sprintf("%d seconds", int(d.Seconds()))
}
