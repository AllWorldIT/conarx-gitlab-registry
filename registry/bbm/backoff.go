//go:generate mockgen -package mocks -destination mocks/backoff.go . Backoff

package bbm

import "time"

// Backoff represents a back off generator.
type Backoff interface {
	// Reset resets the interval back to the initial retry interval.
	Reset()
	// NextBackOff calculates the next backoff interval.
	NextBackOff() time.Duration
}
