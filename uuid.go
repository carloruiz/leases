package leases

import (
	"crypto/rand"
	"fmt"
)

// LeaseToken is an opaque token that identifies a specific lease claim.
// It acts as a fencing token — operations that mutate a lease require
// the caller to present the token they received at acquisition time.
type LeaseToken struct {
	b [16]byte
}

// NewLeaseToken generates a new random lease token.
func NewLeaseToken() (LeaseToken, error) {
	var t LeaseToken
	if _, err := rand.Read(t.b[:]); err != nil {
		return t, err
	}
	// Set version 4 and variant bits so the token is a valid UUID on the wire.
	t.b[6] = (t.b[6] & 0x0f) | 0x40
	t.b[8] = (t.b[8] & 0x3f) | 0x80
	return t, nil
}

// String returns the token in the standard 8-4-4-4-12 hex format expected by CockroachDB's UUID column.
func (t LeaseToken) String() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		t.b[0:4], t.b[4:6], t.b[6:8], t.b[8:10], t.b[10:16])
}

// parseLeaseToken parses the hex string returned by CockroachDB into a LeaseToken.
func parseLeaseToken(s string) (LeaseToken, error) {
	var t LeaseToken
	if len(s) != 36 {
		return t, fmt.Errorf("invalid token length: %d", len(s))
	}
	hexStr := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	if len(hexStr) != 32 {
		return t, fmt.Errorf("invalid token format")
	}
	for i := 0; i < 16; i++ {
		hi, err := hexVal(hexStr[i*2])
		if err != nil {
			return t, err
		}
		lo, err := hexVal(hexStr[i*2+1])
		if err != nil {
			return t, err
		}
		t.b[i] = hi<<4 | lo
	}
	return t, nil
}

func hexVal(b byte) (byte, error) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', nil
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, nil
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, nil
	default:
		return 0, fmt.Errorf("invalid hex char: %c", b)
	}
}
