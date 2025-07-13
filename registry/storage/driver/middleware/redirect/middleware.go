package middleware

import (
	"context"
	"fmt"
	"net/url"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	storagemiddleware "github.com/docker/distribution/registry/storage/driver/middleware"
)

type redirectStorageMiddleware struct {
	storagedriver.StorageDriver
	scheme string
	host   string
}

var _ storagedriver.StorageDriver = &redirectStorageMiddleware{}

func newRedirectStorageMiddleware(sd storagedriver.StorageDriver, options map[string]any) (storagedriver.StorageDriver, func() error, error) {
	o, ok := options["baseurl"]
	if !ok {
		return nil, nil, fmt.Errorf("no baseurl provided")
	}
	b, ok := o.(string)
	if !ok {
		return nil, nil, fmt.Errorf("baseurl must be a string")
	}
	u, err := url.Parse(b)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse redirect baseurl: %s", b)
	}
	if u.Scheme == "" {
		return nil, nil, fmt.Errorf("no scheme specified for redirect baseurl")
	}
	if u.Host == "" {
		return nil, nil, fmt.Errorf("no host specified for redirect baseurl")
	}

	return &redirectStorageMiddleware{StorageDriver: sd, scheme: u.Scheme, host: u.Host}, nil, nil
}

func (r *redirectStorageMiddleware) URLFor(_ context.Context, path string, _ map[string]any) (string, error) {
	u := &url.URL{Scheme: r.scheme, Host: r.host, Path: path}
	return u.String(), nil
}

func init() {
	// nolint: gosec // ignore when backend is already registered
	storagemiddleware.Register("redirect", storagemiddleware.InitFunc(newRedirectStorageMiddleware))
}
