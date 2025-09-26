package checks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileChecker(t *testing.T) {
	// nolint: testifylint // require-error
	assert.Error(t, FileChecker("/tmp").Check(), "/tmp was expected as exists")
	assert.NoError(t, FileChecker("NoSuchFileFromMoon").Check(), "NoSuchFileFromMoon was expected as not exists")
}

func TestHTTPChecker(t *testing.T) {
	// nolint: testifylint // require-error
	assert.Error(t, HTTPChecker("https://www.google.cybertron", 200, 0, nil).Check(), "Google on Cybertron was expected as not exists")
	assert.NoError(t, HTTPChecker("https://www.google.pt", 200, 0, nil).Check(), "Google at Portugal was expected as exists")
}
