package leases

import "errors"

var (
	// ErrNotFound indicates the resource was not found or the token didn't match.
	ErrNotFound = errors.New("lease not found")

	// ErrNotAcquired indicates the lease could not be acquired (already held or doesn't exist).
	ErrNotAcquired = errors.New("lease not acquired")
)
