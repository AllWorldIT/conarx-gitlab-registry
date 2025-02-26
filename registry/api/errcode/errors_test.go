package errcode

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestErrorsManagement does a quick check of the Errors type to ensure that
// members are properly pushed and marshaled.
var ErrorCodeTest1 = Register("test.errors", ErrorDescriptor{
	Value:          "TEST1",
	Message:        "test error 1",
	Description:    `Just a test message #1.`,
	HTTPStatusCode: http.StatusInternalServerError,
})

var ErrorCodeTest2 = Register("test.errors", ErrorDescriptor{
	Value:          "TEST2",
	Message:        "test error 2",
	Description:    `Just a test message #2.`,
	HTTPStatusCode: http.StatusNotFound,
})

var ErrorCodeTest3 = Register("test.errors", ErrorDescriptor{
	Value:          "TEST3",
	Message:        "Sorry %q isn't valid",
	Description:    `Just a test message #3.`,
	HTTPStatusCode: http.StatusNotFound,
})

// TestErrorCodes ensures that error code format, mappings and
// marshaling/unmarshaling. round trips are stable.
func TestErrorCodes(t *testing.T) {
	require.NotEmpty(t, errorCodeToDescriptors, "errors aren't loaded")

	for ec, desc := range errorCodeToDescriptors {
		require.Equal(t, ec, desc.Code, "error code in descriptor isn't correct")
		require.Equal(t, ec, idToDescriptors[desc.Value].Code, "error code in idToDesc isn't correct")
		require.Equal(t, ec.Message(), desc.Message, "ec.Message doesn't match desc.Message")

		// Test (de)serializing the ErrorCode
		p, err := json.Marshal(ec)
		require.NoErrorf(t, err, "couldn't marshal ec %v", ec)
		require.NotEmptyf(t, p, "expected content in marshaled before for error code %v", ec)

		// First, unmarshal to interface and ensure we have a string.
		var ecUnspecified any
		require.NoErrorf(t, json.Unmarshal(p, &ecUnspecified), "error unmarshaling error code %v", ec)
		require.IsType(t, "", ecUnspecified, "expected a string for error code %v on unmarshal", ec)

		// Now, unmarshal with the error code type and ensure they are equal
		var ecUnmarshaled ErrorCode
		require.NoErrorf(t, json.Unmarshal(p, &ecUnmarshaled), "error unmarshaling error code %v", ec)
		require.Equal(t, ec, ecUnmarshaled, "unexpected error code during error code marshal/unmarshal")

		expectedErrorString := strings.ToLower(strings.ReplaceAll(ec.Descriptor().Value, "_", " "))
		require.Equalf(t, expectedErrorString, ec.Error(), "unexpected return from %v.Error()", ec)
	}
}

func TestErrorsManagement(t *testing.T) {
	var errs Errors

	errs = append(errs, ErrorCodeTest1)
	errs = append(errs, ErrorCodeTest2.WithDetail(
		map[string]any{"digest": "sometestblobsumdoesntmatter"}))
	errs = append(errs, ErrorCodeTest3.WithArgs("BOOGIE"))
	errs = append(errs, ErrorCodeTest3.WithArgs("BOOGIE").WithDetail("data"))

	p, err := json.Marshal(errs)
	require.NoError(t, err, "error marshaling errors")

	expectedJSON := `{"errors":[` +
		`{"code":"TEST1","message":"test error 1"},` +
		`{"code":"TEST2","message":"test error 2","detail":{"digest":"sometestblobsumdoesntmatter"}},` +
		`{"code":"TEST3","message":"Sorry \"BOOGIE\" isn't valid"},` +
		`{"code":"TEST3","message":"Sorry \"BOOGIE\" isn't valid","detail":"data"}` +
		`]}`

	require.JSONEq(t, expectedJSON, string(p), "unexpected json")

	// Now test the reverse
	var unmarshaled Errors
	require.NoError(t, json.Unmarshal(p, &unmarshaled), "unexpected error unmarshaling error envelope")
	require.Equal(t, unmarshaled, errs, "errors not equal after round trip")

	// Test the arg substitution stuff
	e1 := unmarshaled[3].(Error)
	exp1 := `Sorry "BOOGIE" isn't valid`
	require.Equal(t, exp1, e1.Message, "wrong msg")
	require.Equal(t, "test3: "+exp1, e1.Error(), "Error() didn't return the right string")

	// Test again with a single value this time
	errs = Errors{ErrorCodeUnknown}
	expectedJSON = "{\"errors\":[{\"code\":\"UNKNOWN\",\"message\":\"unknown error\"}]}"
	p, err = json.Marshal(errs)
	require.NoError(t, err, "error marshaling errors")
	require.JSONEq(t, expectedJSON, string(p), "unexpected json")

	// Now test the reverse
	unmarshaled = nil
	require.NoError(t, json.Unmarshal(p, &unmarshaled), "unexpected error unmarshaling error envelope")
	require.Equal(t, unmarshaled, errs, "errors not equal after round trip")

	// Verify that calling WithArgs() more than once does the right thing.
	// Meaning creates a new Error and uses the ErrorCode Message
	e1 = ErrorCodeTest3.WithArgs("test1")
	e2 := e1.WithArgs("test2")
	require.NotSame(t, &e1, &e2, "args: e2 and e1 should not be the same")
	require.Equal(t, `Sorry "test2" isn't valid`, e2.Message, "e2 had wrong message")

	// Verify that calling WithDetail() more than once does the right thing.
	// Meaning creates a new Error and overwrites the old detail field
	e1 = ErrorCodeTest3.WithDetail("stuff1")
	e2 = e1.WithDetail("stuff2")
	require.NotSame(t, &e1, &e2, "detail: e2 and e1 should not be the same")
	require.Equal(t, `stuff2`, e2.Detail, "e2 had wrong detail")
}

func TestFromUnknownError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected int
	}{
		{
			name: "write connection reset",
			err: &net.OpError{
				Op:  "write",
				Net: "tcp",
				Source: &net.TCPAddr{
					IP:   net.IPv4(192, 168, 1, 127),
					Port: 9001,
				},
				Addr: &net.TCPAddr{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 5001,
				},
				Err: os.NewSyscallError("write", syscall.ECONNRESET),
			},
			expected: http.StatusBadRequest,
		},
		{
			name: "read connection reset",
			err: &net.OpError{
				Op:  "write",
				Net: "tcp",
				Source: &net.TCPAddr{
					IP:   net.IPv4(192, 168, 1, 127),
					Port: 9001,
				},
				Addr: &net.TCPAddr{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 5001,
				},
				Err: os.NewSyscallError("read", syscall.ECONNRESET),
			},
			expected: http.StatusBadRequest,
		},
		{
			name: "unknown op error",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Source: &net.TCPAddr{
					IP:   net.IPv4(192, 168, 1, 127),
					Port: 9001,
				},
				Addr: &net.TCPAddr{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 5001,
				},
				Err: net.UnknownNetworkError("tcp"),
			},
			expected: http.StatusServiceUnavailable,
		},
		{
			name:     "already an errcode.Error",
			err:      ErrorCodeUnauthorized.WithDetail("don't do that"),
			expected: http.StatusUnauthorized,
		},
		{
			name:     "unknown error",
			err:      fmt.Errorf("division by zero"),
			expected: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := FromUnknownError(tt.err)

			require.Equal(t, tt.expected, err.Code.Descriptor().HTTPStatusCode)
		})
	}
}
