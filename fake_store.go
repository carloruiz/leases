package leases

import (
	"context"
	"sync"
	"time"
)

// FakeStore is an in-memory implementation of Store for use in tests.
// Each method has an optional hook field that, when non-nil, is called
// instead of the default behaviour. This lets callers inject errors or
// custom logic on a per-method basis.
type FakeStore struct {
	mu     sync.Mutex
	leases map[string]*Lease // keyed by resource
	now    func() time.Time  // injectable clock; defaults to time.Now

	// Hooks — when non-nil the corresponding method delegates to the hook
	// and skips the default in-memory logic.
	CreateFunc           func(ctx context.Context, db DBTX, group, resource string) error
	CreateAndAcquireFunc func(ctx context.Context, db DBTX, group, resource, owner string, duration time.Duration) (*Lease, error)
	DeleteFunc           func(ctx context.Context, db DBTX, resource string) error
	AcquireFunc          func(ctx context.Context, db DBTX, resource string, owner string, duration time.Duration) (*Lease, error)
	ReleaseFunc          func(ctx context.Context, db DBTX, resource string, token LeaseToken) error
	AcquireManyFunc      func(ctx context.Context, db DBTX, group string, limit int, owner string, duration time.Duration) ([]Lease, error)
	HeartbeatFunc        func(ctx context.Context, db DBTX, resource string, token LeaseToken, duration time.Duration) (*Lease, error)
	HeartbeatManyFunc    func(ctx context.Context, db DBTX, items []HeartbeatRequest, duration time.Duration) ([]Lease, error)
}

// NewFakeStore returns a FakeStore ready for use.
func NewFakeStore() *FakeStore {
	return &FakeStore{
		leases: make(map[string]*Lease),
		now:    time.Now,
	}
}

// NewFakeStoreWithClock returns a FakeStore that uses the provided function
// as its clock, which is useful for deterministic time-based tests.
func NewFakeStoreWithClock(now func() time.Time) *FakeStore {
	return &FakeStore{
		leases: make(map[string]*Lease),
		now:    now,
	}
}

func (f *FakeStore) Create(ctx context.Context, db DBTX, group, resource string) error {
	if f.CreateFunc != nil {
		return f.CreateFunc(ctx, db, group, resource)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	f.leases[resource] = &Lease{
		Resource: resource,
		Group:    group,
	}
	return nil
}

func (f *FakeStore) CreateAndAcquire(ctx context.Context, db DBTX, group, resource, owner string, duration time.Duration) (*Lease, error) {
	if f.CreateAndAcquireFunc != nil {
		return f.CreateAndAcquireFunc(ctx, db, group, resource, owner, duration)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	token, err := NewLeaseToken()
	if err != nil {
		return nil, err
	}
	now := f.now()
	exp := now.Add(duration)
	l := &Lease{
		Resource:   resource,
		Group:      group,
		LeaseToken: &token,
		ClaimedBy:  &owner,
		ClaimedAt:  &now,
		ExpiresAt:  &exp,
	}
	f.leases[resource] = l
	return copyLease(l), nil
}

func (f *FakeStore) Delete(ctx context.Context, db DBTX, resource string) error {
	if f.DeleteFunc != nil {
		return f.DeleteFunc(ctx, db, resource)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.leases[resource]; !ok {
		return ErrNotFound
	}
	delete(f.leases, resource)
	return nil
}

func (f *FakeStore) Acquire(ctx context.Context, db DBTX, resource string, owner string, duration time.Duration) (*Lease, error) {
	if f.AcquireFunc != nil {
		return f.AcquireFunc(ctx, db, resource, owner, duration)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	l, ok := f.leases[resource]
	if !ok {
		return nil, ErrNotAcquired
	}
	if l.ExpiresAt != nil && f.now().Before(*l.ExpiresAt) {
		return nil, ErrNotAcquired
	}

	token, err := NewLeaseToken()
	if err != nil {
		return nil, err
	}
	now := f.now()
	exp := now.Add(duration)
	l.LeaseToken = &token
	l.ClaimedBy = &owner
	l.ClaimedAt = &now
	l.ExpiresAt = &exp
	return copyLease(l), nil
}

func (f *FakeStore) Release(ctx context.Context, db DBTX, resource string, token LeaseToken) error {
	if f.ReleaseFunc != nil {
		return f.ReleaseFunc(ctx, db, resource, token)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	l, ok := f.leases[resource]
	if !ok || l.LeaseToken == nil || *l.LeaseToken != token {
		return ErrNotFound
	}
	l.LeaseToken = nil
	l.ClaimedBy = nil
	l.ClaimedAt = nil
	l.ExpiresAt = nil
	return nil
}

func (f *FakeStore) AcquireMany(ctx context.Context, db DBTX, group string, limit int, owner string, duration time.Duration) ([]Lease, error) {
	if f.AcquireManyFunc != nil {
		return f.AcquireManyFunc(ctx, db, group, limit, owner, duration)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.now()
	var acquired []Lease
	for _, l := range f.leases {
		if len(acquired) >= limit {
			break
		}
		if l.Group != group {
			continue
		}
		if l.ExpiresAt != nil && now.Before(*l.ExpiresAt) {
			continue
		}
		token, err := NewLeaseToken()
		if err != nil {
			return nil, err
		}
		exp := now.Add(duration)
		l.LeaseToken = &token
		l.ClaimedBy = &owner
		l.ClaimedAt = &now
		l.ExpiresAt = &exp
		acquired = append(acquired, *copyLease(l))
	}
	return acquired, nil
}

func (f *FakeStore) Heartbeat(ctx context.Context, db DBTX, resource string, token LeaseToken, duration time.Duration) (*Lease, error) {
	if f.HeartbeatFunc != nil {
		return f.HeartbeatFunc(ctx, db, resource, token, duration)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	l, ok := f.leases[resource]
	if !ok || l.LeaseToken == nil || *l.LeaseToken != token {
		return nil, ErrNotFound
	}
	now := f.now()
	exp := now.Add(duration)
	l.ExpiresAt = &exp
	return copyLease(l), nil
}

func (f *FakeStore) HeartbeatMany(ctx context.Context, db DBTX, items []HeartbeatRequest, duration time.Duration) ([]Lease, error) {
	if f.HeartbeatManyFunc != nil {
		return f.HeartbeatManyFunc(ctx, db, items, duration)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.now()
	var results []Lease
	for _, item := range items {
		l, ok := f.leases[item.Resource]
		if !ok || l.LeaseToken == nil || *l.LeaseToken != item.LeaseToken {
			continue
		}
		exp := now.Add(duration)
		l.ExpiresAt = &exp
		results = append(results, *copyLease(l))
	}
	return results, nil
}

// copyLease returns a shallow copy so callers can't mutate internal state.
func copyLease(l *Lease) *Lease {
	cp := *l
	return &cp
}
