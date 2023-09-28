package handlers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/api/errcode"
)

// closeResources closes all the provided resources after running the target
// handler.
func closeResources(handler http.Handler, closers ...io.Closer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, closer := range closers {
			defer closer.Close()
		}
		handler.ServeHTTP(w, r)
	})
}

// copyFullPayload copies the payload of an HTTP request to destWriter. If it
// receives less content than expected, and the client disconnected during the
// upload, it avoids sending a 400 error to keep the logs cleaner.
//
// The copy will be limited to `limit` bytes, if limit is greater than zero.
func copyFullPayload(ctx context.Context, responseWriter http.ResponseWriter, r *http.Request, destWriter io.Writer, limit int64, action string) error {
	// Get a channel that tells us if the client disconnects
	clientClosed := r.Context().Done()
	var body = r.Body
	if limit > 0 {
		body = http.MaxBytesReader(responseWriter, body, limit)
	}

	// Read in the data, if any.
	copied, err := io.Copy(destWriter, body)
	if clientClosed != nil && (err != nil || (r.ContentLength > 0 && copied < r.ContentLength)) {
		// Didn't receive as much content as expected. Did the client
		// disconnect during the request? If so, avoid returning a 400
		// error to keep the logs cleaner.
		select {
		case <-clientClosed:
			// Set the response code to "499 Client Closed Request"
			// Even though the connection has already been closed,
			// this causes the logger to pick up a 499 error
			// instead of showing 0 for the HTTP status.
			responseWriter.WriteHeader(499)

			dcontext.GetLoggerWithFields(ctx, map[interface{}]interface{}{
				"error":         err,
				"copied":        copied,
				"contentLength": r.ContentLength,
			}, "error", "copied", "contentLength").Warn("client disconnected during " + action)
			return errcode.ErrorCodeConnectionReset.WithDetail("client disconnected")
		default:
		}
	}

	if err != nil {
		dcontext.GetLogger(ctx).Errorf("unknown error reading request payload: %v", err)
		return err
	}

	return nil
}

// parseContentRange parses the value of a Content-Range header (contentRange) of assumed format: "<start of range>-<end of range>"
// into startRange and endRange. It returns an error (err) whenever the contentRange argument can not be parsed.
func parseContentRange(contentRange string) (startRange int64, endRange int64, err error) {
	ranges := strings.Split(contentRange, "-")
	if len(ranges) != 2 {
		err = fmt.Errorf("invalid content range format, %s", contentRange)
		return
	}
	startRange, err = strconv.ParseInt(ranges[0], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid content range values, %s: %v", contentRange, err)
		return
	}
	endRange, err = strconv.ParseInt(ranges[1], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid content range values, %s: %v", contentRange, err)
		return
	}

	return
}
