package checks

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/docker/distribution/health"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/version"
	"github.com/hashicorp/go-multierror"
)

var registryHTTPCheckUserAgent = fmt.Sprintf("container-registry-httpcheck/%s-%s", version.Version, version.Revision)

// FileChecker checks the existence of a file and returns an error
// if the file exists.
func FileChecker(f string) health.Checker {
	return health.CheckFunc(func() error {
		absoluteFilePath, err := filepath.Abs(f)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for %q: %v", f, err)
		}

		_, err = os.Stat(absoluteFilePath)
		if err == nil {
			return errors.New("file exists")
		} else if os.IsNotExist(err) {
			return nil
		}

		return err
	})
}

// HTTPChecker does a HEAD request and verifies that the HTTP status code
// returned matches statusCode.
func HTTPChecker(r string, statusCode int, timeout time.Duration, headers http.Header) health.Checker {
	return health.CheckFunc(func() error {
		client := http.Client{
			Timeout: timeout,
		}
		req, err := http.NewRequest(http.MethodHead, r, nil)
		if err != nil {
			return errors.New("error creating request: " + r)
		}
		// NOTE(prozlach): In case of Golang, only first call to `Add/Set` sets
		// the `User-Agent` header, hence the extra logic to not to break
		// User-Agent header that the user might have already set
		userAgentSet := false
		for headerName, headerValues := range headers {
			if http.CanonicalHeaderKey(headerName) == "User-Agent" {
				userAgentSet = true
			}
			for _, headerValue := range headerValues {
				req.Header.Add(headerName, headerValue)
			}
		}
		if !userAgentSet {
			req.Header.Set("User-Agent", registryHTTPCheckUserAgent)
		}
		response, err := client.Do(req)
		if err != nil {
			return errors.New("error while checking: " + r)
		}
		defer response.Body.Close()
		if response.StatusCode != statusCode {
			return errors.New("downstream service returned unexpected status: " + strconv.Itoa(response.StatusCode))
		}
		return nil
	})
}

// TCPChecker attempts to open a TCP connection.
func TCPChecker(addr string, timeout time.Duration) health.Checker {
	return health.CheckFunc(func() error {
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			return errors.New("connection to " + addr + " failed")
		}
		conn.Close()
		return nil
	})
}

func DBChecker(ctx context.Context, timeout time.Duration, db datastore.LoadBalancer) health.CheckFunc {
	// NOTE(prozlach): We cant register replicas and the primary individually,
	// as they may change in time and we would need to be able to de-register
	// them. Hence we put them in one bag/check and alway fetch a fresh list
	// from DB LB.
	return func() error {
		f := func(db *datastore.DB) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			err := db.PingContext(ctx)
			if err != nil {
				return fmt.Errorf("executing query for db %s: %w", db.Address(), err)
			}
			return nil
		}

		dbs := []*datastore.DB{db.Primary()}
		dbs = append(dbs, db.Replicas()...)

		var errs *multierror.Error
		for _, db := range dbs {
			err := f(db)
			if err != nil {
				errs = multierror.Append(errs, err)
			}
		}
		return errs.ErrorOrNil()
	}
}
