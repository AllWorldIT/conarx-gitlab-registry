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
	"github.com/hashicorp/go-multierror"
)

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
		for headerName, headerValues := range headers {
			for _, headerValue := range headerValues {
				req.Header.Add(headerName, headerValue)
			}
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

func DBChecker(ctx context.Context, timeout time.Duration, db datastore.LoadBalancer, dbName string) health.CheckFunc {
	q := fmt.Sprintf(`SELECT EXISTS (
				SELECT
					1
				FROM
					pg_stat_database
				WHERE
					datname = '%s'
				LIMIT 1
			)`, dbName)

	// NOTE(prozlach): We cant register replicas and the primary individually,
	// as they may change in time and we would need to be able to de-register
	// them. Hence we put them in one bag/check and alway fetch a fresh list
	// from DB LB.
	return func() error {
		f := func(db *datastore.DB) error {
			var ok bool

			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			err := db.QueryRowContext(ctx, q).Scan(&ok)
			if err != nil {
				return fmt.Errorf("executing query for db %s: %w", db.Address(), err)
			}
			if !ok {
				return fmt.Errorf("cannot access database schema for db %s: %w", db.Address(), err)
			}
			return nil
		}

		dbs := []*datastore.DB{db.Primary()}
		// NOTE(prozlach): Should we keep checking replicas status in the
		// healthchecks?
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
