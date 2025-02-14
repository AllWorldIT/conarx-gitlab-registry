package testutil

import (
	"context"
	"errors"
	mrand "math/rand/v2"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

type Opts struct {
	Defaultt          any
	Required          bool
	NilAllowed        bool
	EmptyAllowed      bool
	NonTypeAllowed    bool
	ParamName         string
	DriverParamName   string
	OriginalParams    map[string]any
	ParseParametersFn func(map[string]any) (any, error)
}

func AssertByDefaultType(t *testing.T, opts Opts) {
	switch tt := opts.Defaultt.(type) {
	case bool:
		TestBoolValue(t, opts)
	case string:
		TestStringValue(t, opts)
	default:
		require.FailNowf(t, "unknown type", "%v", tt)
	}
}

func TestBoolValue(t *testing.T, opts Opts) {
	// Keep OriginalParams intact for idempotency.
	params := CopyMap(opts.OriginalParams)

	driverParams, err := opts.ParseParametersFn(params)
	require.NoError(t, err)

	AssertParam(t, driverParams, opts.DriverParamName, opts.Defaultt, "default value mismatch")

	params[opts.ParamName] = true
	driverParams, err = opts.ParseParametersFn(params)
	require.NoError(t, err)

	AssertParam(t, driverParams, opts.DriverParamName, true, "boolean true")

	params[opts.ParamName] = false
	driverParams, err = opts.ParseParametersFn(params)
	require.NoError(t, err)

	AssertParam(t, driverParams, opts.DriverParamName, false, "boolean false")

	params[opts.ParamName] = nil
	driverParams, err = opts.ParseParametersFn(params)
	require.NoError(t, err, "nil does not return: %v", err)

	AssertParam(t, driverParams, opts.DriverParamName, opts.Defaultt, "param is nil")

	params[opts.ParamName] = ""
	_, err = opts.ParseParametersFn(params)
	require.Error(t, err, "empty string")

	params[opts.ParamName] = "invalid"
	_, err = opts.ParseParametersFn(params)
	require.Error(t, err, "not boolean string")

	params[opts.ParamName] = 12
	_, err = opts.ParseParametersFn(params)
	require.Error(t, err, "not boolean type")
}

func TestStringValue(t *testing.T, opts Opts) {
	// Keep OriginalParams intact for idempotency.
	params := CopyMap(opts.OriginalParams)

	value := "value"
	if opts.Required {
		// nolint: revive // unchecked-type-assertion
		value = params[opts.ParamName].(string)
	} else if opts.Defaultt != nil && opts.Defaultt.(string) != "" {
		// nolint: revive // unchecked-type-assertion
		value = opts.Defaultt.(string)
	}

	params[opts.ParamName] = value
	driverParams, err := opts.ParseParametersFn(params)
	require.NoError(t, err)

	AssertParam(t, driverParams, opts.DriverParamName, value, "string value")

	params[opts.ParamName] = nil
	driverParams, err = opts.ParseParametersFn(params)
	if opts.NilAllowed {
		require.NoError(t, err, "nil does not return: %v", err)
		AssertParam(t, driverParams, opts.DriverParamName, opts.Defaultt, "param is nil")
	} else {
		require.Error(t, err, "nil value should error")
	}

	params[opts.ParamName] = ""
	driverParams, err = opts.ParseParametersFn(params)
	if opts.EmptyAllowed {
		require.NoError(t, err, "empty does not return: %v", err)
		AssertParam(t, driverParams, opts.DriverParamName, opts.Defaultt, "param is empty")
	} else {
		require.Error(t, err, "empty string")
	}

	params[opts.ParamName] = 12
	driverParams, err = opts.ParseParametersFn(params)
	if opts.NonTypeAllowed {
		require.NoError(t, err, "not string type")
		AssertParam(t, driverParams, opts.DriverParamName, "12", "param is empty")
	} else {
		require.Error(t, err, "non string type")
	}
}

func AssertParam(t *testing.T, params any, fieldName string, expected any, msgs ...any) {
	r := reflect.ValueOf(params)
	field := reflect.Indirect(r).FieldByName(fieldName)

	switch e := expected.(type) {
	case string:
		require.Equal(t, e, field.String(), msgs...)
	case bool:
		require.Equal(t, e, field.Bool(), msgs...)
	default:
		require.FailNowf(t, "unhandled expected type", "%T", e)
	}
}

func CopyMap(original map[string]any) map[string]any {
	newMap := make(map[string]any)
	for k, v := range original {
		newMap[k] = v
	}

	return newMap
}

func EnsurePathDeleted(
	ctx context.Context,
	tb testing.TB,
	driver storagedriver.StorageDriver,
	targetPath string,
) {
	// NOTE(prozlach): We want to make sure that we do not do an accidental
	// retry of the Delete call, hence it is outside of the
	// require.EventuallyWithT block.
	err := driver.Delete(ctx, targetPath)
	if err != nil {
		if !errors.As(err, new(storagedriver.PathNotFoundError)) {
			// Handover the termination of the execution to require.NoError call
			require.NoError(tb, err)
		}

		// Path does not exist, in theory we are done, but in practice
		// let's also confirm that with .List() call
	}

	// NOTE(prozlach): do the first check imediatelly as an optimization, so
	// that we avoid the 1s wait even if the blobs were removed, and thus speed
	// up the tests.
	paths, err := driver.List(ctx, targetPath)
	if errors.As(err, new(storagedriver.PathNotFoundError)) {
		return
	}
	tb.Logf("files were not cleaned up (%s), entering wait loop", strings.Join(paths, ","))

	startTime := time.Now()
	require.EventuallyWithT(
		tb,
		func(c *assert.CollectT) {
			_, err := driver.List(ctx, targetPath)
			assert.ErrorAs(c, err, new(storagedriver.PathNotFoundError))
		},
		4200*time.Millisecond,
		1*time.Second,
	)
	tb.Logf("waited %s for container to return an empty list of files", time.Since(startTime))
}

var (
	filenameChars  = []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	separatorChars = []byte("._-")
)

func RandomPath(minTldLen, length int) string {
	// NOTE(prozlach): randomPath() is called in some tests concurrently and it
	// may happen that the top-level directory of the path returned is not
	// unique given enough calls to it. This leads to wonky race conditions as
	// the tests are running concurrently and are trying to remove top level
	// directory to clean up after themselves while others are still writing to
	// it, leading to errors where paths are not being cleaned up.
	//
	// The solution is simple - enforce the minimal length of the top level dir
	// enough so that the chance that there are collisions(i.e. same top-level
	// directories) are very low for tests that require it.
	//
	// In my test script doing one million iterations I was not able to get a
	// collision anymore for directories with minimal length of 4 and 32 calls
	// to randomPath() function.
	p := "/"
	for len(p) < length {
		/* #nosec G404 */
		chunkLength := mrand.IntN(length-len(p)) + 1
		if len(p) == 1 && chunkLength < minTldLen {
			// First component is too short - retry
			continue
		}
		chunk := RandomFilename(chunkLength)
		p += chunk
		remaining := length - len(p)
		if remaining == 1 {
			p += RandomFilename(1)
		} else if remaining > 1 {
			p += "/"
		}
	}
	return p
}

func RandomFilename(length int) string {
	b := make([]byte, length)
	wasSeparator := true
	for i := range b {
		/* #nosec G404 */
		if !wasSeparator && i < len(b)-1 && mrand.IntN(4) == 0 {
			b[i] = separatorChars[mrand.IntN(len(separatorChars))]
			wasSeparator = true
		} else {
			b[i] = filenameChars[mrand.IntN(len(filenameChars))]
			wasSeparator = false
		}
	}
	return string(b)
}

// RandomFilenameRange returns a random file with a length between min and max
// chars long inclusive.
func RandomFilenameRange(minimum, maximum int) string { // nolint:unparam //(min always receives 8)
	/* #nosec G404 */
	return RandomFilename(minimum + (mrand.IntN(maximum + 1)))
}

// RandomBranchingFiles creates n number of randomly named files at the end of
// a binary tree of randomly named directories.
func RandomBranchingFiles(root string, n int) []string {
	var files []string

	subDirectory := path.Join(root, RandomFilenameRange(8, 8))

	if n <= 1 {
		files = append(files, path.Join(subDirectory, RandomFilenameRange(8, 8)))
		return files
	}

	half := n / 2
	remainder := n % 2

	files = append(files, RandomBranchingFiles(subDirectory, half+remainder)...)
	files = append(files, RandomBranchingFiles(subDirectory, half)...)

	return files
}
