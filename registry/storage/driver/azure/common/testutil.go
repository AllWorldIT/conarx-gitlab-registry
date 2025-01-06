package common

import (
	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/registry/internal"
)

// SystemClock is meant for testing purposes - running tests with custom clock.
// This variable is updated/changed during the tests.
var SystemClock internal.Clock = clock.New()
