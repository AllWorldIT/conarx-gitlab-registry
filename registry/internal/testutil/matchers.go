package testutil

import (
	"context"
	"fmt"
	"time"
)

// IsContextWithDeadline is a gomock Matcher to validate that an argument is a context.Context with a given Deadline.
type IsContextWithDeadline struct {
	Deadline time.Time
}

// Matches implements gomock.Matcher.
func (m IsContextWithDeadline) Matches(x any) bool {
	ctx, ok := x.(context.Context)
	if !ok {
		return false
	}
	d, ok := ctx.Deadline()
	if !ok {
		return false
	}

	return d == m.Deadline
}

// String implements gomock.Matcher.
func (m IsContextWithDeadline) String() string {
	return fmt.Sprintf("is context.Context with a deadline of %q", m.Deadline)
}
