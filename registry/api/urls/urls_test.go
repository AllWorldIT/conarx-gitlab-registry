package urls

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/docker/distribution/reference"
)

type urlBuilderTestCase struct {
	description  string
	expectedPath string
	expectedErr  error
	build        func() (string, error)
}

func makeBuilderTestCases(builder *Builder) []urlBuilderTestCase {
	fooBarRef, _ := reference.WithName("foo/bar")
	return []urlBuilderTestCase{
		{
			description:  "test base url",
			expectedPath: "/v2/",
			expectedErr:  nil,
			build:        builder.BuildBaseURL,
		},
		{
			description:  "test tags url",
			expectedPath: "/v2/foo/bar/tags/list",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildTagsURL(fooBarRef)
			},
		},
		{
			description:  "test tags url with n query parameter",
			expectedPath: "/v2/foo/bar/tags/list?n=10",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildTagsURL(fooBarRef, url.Values{
					"n": []string{"10"},
				})
			},
		},
		{
			description:  "test tags url with last query parameter",
			expectedPath: "/v2/foo/bar/tags/list?last=abc-def",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildTagsURL(fooBarRef, url.Values{
					"last": []string{"abc-def"},
				})
			},
		},
		{
			description:  "test tags url with n and last query parameters",
			expectedPath: "/v2/foo/bar/tags/list?last=abc-def&n=10",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildTagsURL(fooBarRef, url.Values{
					"n":    []string{"10"},
					"last": []string{"abc-def"},
				})
			},
		},
		{
			description:  "test manifest url tagged ref",
			expectedPath: "/v2/foo/bar/manifests/tag",
			expectedErr:  nil,
			build: func() (string, error) {
				ref, _ := reference.WithTag(fooBarRef, "tag")
				return builder.BuildManifestURL(ref)
			},
		},
		{
			description:  "test manifest url bare ref",
			expectedPath: "",
			expectedErr:  fmt.Errorf("reference must have a tag or digest"),
			build: func() (string, error) {
				return builder.BuildManifestURL(fooBarRef)
			},
		},
		{
			description:  "build blob url",
			expectedPath: "/v2/foo/bar/blobs/sha256:3b3692957d439ac1928219a83fac91e7bf96c153725526874673ae1f2023f8d5",
			expectedErr:  nil,
			build: func() (string, error) {
				ref, _ := reference.WithDigest(fooBarRef, "sha256:3b3692957d439ac1928219a83fac91e7bf96c153725526874673ae1f2023f8d5")
				return builder.BuildBlobURL(ref)
			},
		},
		{
			description:  "build blob upload url",
			expectedPath: "/v2/foo/bar/blobs/uploads/",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildBlobUploadURL(fooBarRef)
			},
		},
		{
			description:  "build blob upload url with digest and size",
			expectedPath: "/v2/foo/bar/blobs/uploads/?digest=sha256%3A3b3692957d439ac1928219a83fac91e7bf96c153725526874673ae1f2023f8d5&size=10000",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildBlobUploadURL(fooBarRef, url.Values{
					"size":   []string{"10000"},
					"digest": []string{"sha256:3b3692957d439ac1928219a83fac91e7bf96c153725526874673ae1f2023f8d5"},
				})
			},
		},
		{
			description:  "build blob upload chunk url",
			expectedPath: "/v2/foo/bar/blobs/uploads/uuid-part",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildBlobUploadChunkURL(fooBarRef, "uuid-part")
			},
		},
		{
			description:  "build blob upload chunk url with digest and size",
			expectedPath: "/v2/foo/bar/blobs/uploads/uuid-part?digest=sha256%3A3b3692957d439ac1928219a83fac91e7bf96c153725526874673ae1f2023f8d5&size=10000",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildBlobUploadChunkURL(fooBarRef, "uuid-part", url.Values{
					"size":   []string{"10000"},
					"digest": []string{"sha256:3b3692957d439ac1928219a83fac91e7bf96c153725526874673ae1f2023f8d5"},
				})
			},
		},
		{
			description:  "test Gitlab v1 base url",
			expectedPath: "/gitlab/v1/",
			expectedErr:  nil,
			build:        builder.BuildGitlabV1BaseURL,
		},
		{
			description:  "test Gitlab v1 repository url",
			expectedPath: "/gitlab/v1/repositories/foo/bar/",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildGitlabV1RepositoryURL(fooBarRef)
			},
		},
		{
			description:  "test Gitlab v1 repository url with size",
			expectedPath: "/gitlab/v1/repositories/foo/bar/?size=self",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildGitlabV1RepositoryURL(fooBarRef, url.Values{
					"size": []string{"self"},
				})
			},
		},
		{
			description:  "test Gitlab v1 repository url with self with descendants size",
			expectedPath: "/gitlab/v1/repositories/foo/bar/?size=self_with_descendants",
			expectedErr:  nil,
			build: func() (string, error) {
				return builder.BuildGitlabV1RepositoryURL(fooBarRef, url.Values{
					"size": []string{"self_with_descendants"},
				})
			},
		},
	}
}

// TestBuilder tests the various url building functions, ensuring they are
// returning the expected values.
func TestBuilder(t *testing.T) {
	roots := []string{
		"http://example.com",
		"https://example.com",
		"http://localhost:5000",
		"https://localhost:5443",
	}

	doTest := func(relative bool) {
		for _, root := range roots {
			builder, err := NewBuilderFromString(root, relative)
			if err != nil {
				t.Fatalf("unexpected error creating builder: %v", err)
			}

			for _, testCase := range makeBuilderTestCases(builder) {
				url, err := testCase.build()
				expectedErr := testCase.expectedErr
				if !reflect.DeepEqual(expectedErr, err) {
					t.Fatalf("%s: Expecting %v but got error %v", testCase.description, expectedErr, err)
				}
				if expectedErr != nil {
					continue
				}

				expectedURL := testCase.expectedPath
				if !relative {
					expectedURL = root + expectedURL
				}

				if url != expectedURL {
					t.Fatalf("%s: %q != %q", testCase.description, url, expectedURL)
				}
			}
		}
	}
	doTest(true)
	doTest(false)
}

func TestBuilderWithPrefix(t *testing.T) {
	roots := []string{
		"http://example.com/prefix/",
		"https://example.com/prefix/",
		"http://localhost:5000/prefix/",
		"https://localhost:5443/prefix/",
	}

	doTest := func(relative bool) {
		for _, root := range roots {
			builder, err := NewBuilderFromString(root, relative)
			if err != nil {
				t.Fatalf("unexpected error creating builder: %v", err)
			}

			for _, testCase := range makeBuilderTestCases(builder) {
				url, err := testCase.build()
				expectedErr := testCase.expectedErr
				if !reflect.DeepEqual(expectedErr, err) {
					t.Fatalf("%s: Expecting %v but got error %v", testCase.description, expectedErr, err)
				}
				if expectedErr != nil {
					continue
				}

				expectedURL := testCase.expectedPath
				if !relative {
					expectedURL = root[0:len(root)-1] + expectedURL
				}
				if url != expectedURL {
					t.Fatalf("%s: %q != %q", testCase.description, url, expectedURL)
				}
			}
		}
	}
	doTest(true)
	doTest(false)
}

type builderFromRequestTestCase struct {
	request *http.Request
	base    string
}

type testRequests struct {
	name       string
	request    *http.Request
	base       string
	configHost url.URL
}

func makeTestRequests(t testing.TB) []testRequests {
	u, err := url.Parse("http://example.com")
	if err != nil {
		t.Fatal(err)
	}

	return []testRequests{
		{
			name:    "no forwarded header",
			request: &http.Request{URL: u, Host: u.Host},
			base:    "http://example.com",
		},
		{
			name: "https protocol forwarded with a non-standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Custom-Forwarded-Proto": []string{"https"},
			}},
			base: "http://example.com",
		},
		{
			name: "forwarded protocol is the same",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Proto": []string{"https"},
			}},
			base: "https://example.com",
		},
		{
			name: "forwarded host with a non-standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Host": []string{"first.example.com"},
			}},
			base: "http://first.example.com",
		},
		{
			name: "forwarded multiple hosts a with non-standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Host": []string{"first.example.com, proxy1.example.com"},
			}},
			base: "http://first.example.com",
		},
		{
			name: "host configured in config file takes priority",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Host": []string{"first.example.com, proxy1.example.com"},
			}},
			base: "https://third.example.com:5000",
			configHost: url.URL{
				Scheme: "https",
				Host:   "third.example.com:5000",
			},
		},
		{
			name: "forwarded host and port with just one non-standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Host": []string{"first.example.com:443"},
			}},
			base: "http://first.example.com:443",
		},
		{
			name: "forwarded port with a non-standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Host": []string{"example.com:5000"},
				"X-Forwarded-Port": []string{"5000"},
			}},
			base: "http://example.com:5000",
		},
		{
			name: "forwarded multiple ports with a non-standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Port": []string{"443 , 5001"},
			}},
			base: "http://example.com",
		},
		{
			name: "forwarded standard port with non-standard headers",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Proto": []string{"https"},
				"X-Forwarded-Host":  []string{"example.com"},
				"X-Forwarded-Port":  []string{"443"},
			}},
			base: "https://example.com",
		},
		{
			name: "forwarded standard port with non-standard headers and explicit port",
			request: &http.Request{URL: u, Host: u.Host + ":443", Header: http.Header{
				"X-Forwarded-Proto": []string{"https"},
				"X-Forwarded-Host":  []string{u.Host + ":443"},
				"X-Forwarded-Port":  []string{"443"},
			}},
			base: "https://example.com:443",
		},
		{
			name: "several non-standard headers",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Proto": []string{"https"},
				"X-Forwarded-Host":  []string{" first.example.com:12345 "},
			}},
			base: "https://first.example.com:12345",
		},
		{
			name: "forwarded host with port supplied takes priority",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Host": []string{"first.example.com:5000"},
				"X-Forwarded-Port": []string{"80"},
			}},
			base: "http://first.example.com:5000",
		},
		{
			name: "malformed forwarded port",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Host": []string{"first.example.com"},
				"X-Forwarded-Port": []string{"abcd"},
			}},
			base: "http://first.example.com",
		},
		{
			name: "forwarded protocol and addr using standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"Forwarded": []string{`proto=https;host="192.168.22.30:80"`},
			}},
			base: "https://192.168.22.30:80",
		},
		{
			name: "forwarded host takes priority over for",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"Forwarded": []string{`host="reg.example.com:5000";for="192.168.22.30"`},
			}},
			base: "http://reg.example.com:5000",
		},
		{
			name: "forwarded host and protocol using standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"Forwarded": []string{`host=reg.example.com;proto=https`},
			}},
			base: "https://reg.example.com",
		},
		{
			name: "process just the first standard forwarded header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"Forwarded": []string{`host="reg.example.com:88";proto=http`, `host=reg.example.com;proto=https`},
			}},
			base: "http://reg.example.com:88",
		},
		{
			name: "process just the first list element of standard header",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"Forwarded": []string{`host="reg.example.com:443";proto=https, host="reg.example.com:80";proto=http`},
			}},
			base: "https://reg.example.com:443",
		},
		{
			name: "IPv6 address use host",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"Forwarded":        []string{`for="2607:f0d0:1002:51::4";host="[2607:f0d0:1002:51::4]:5001"`},
				"X-Forwarded-Port": []string{"5002"},
			}},
			base: "http://[2607:f0d0:1002:51::4]:5001",
		},
		{
			name: "IPv6 address with port",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"Forwarded":        []string{`host="[2607:f0d0:1002:51::4]:4000"`},
				"X-Forwarded-Port": []string{"5001"},
			}},
			base: "http://[2607:f0d0:1002:51::4]:4000",
		},
		{
			name: "non-standard and standard forward headers",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Proto": []string{`https`},
				"X-Forwarded-Host":  []string{`first.example.com`},
				"X-Forwarded-Port":  []string{``},
				"Forwarded":         []string{`host=first.example.com; proto=https`},
			}},
			base: "https://first.example.com",
		},
		{
			name: "standard header takes precedence over non-standard headers",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Proto": []string{`http`},
				"Forwarded":         []string{`host=second.example.com; proto=https`},
				"X-Forwarded-Host":  []string{`first.example.com`},
				"X-Forwarded-Port":  []string{`4000`},
			}},
			base: "https://second.example.com",
		},
		{
			name: "incomplete standard header uses default",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Proto": []string{`https`},
				"Forwarded":         []string{`for=127.0.0.1`},
				"X-Forwarded-Host":  []string{`first.example.com`},
				"X-Forwarded-Port":  []string{`4000`},
			}},
			base: "http://" + u.Host,
		},
		{
			name: "standard with just proto",
			request: &http.Request{URL: u, Host: u.Host, Header: http.Header{
				"X-Forwarded-Proto": []string{`https`},
				"Forwarded":         []string{`proto=https`},
				"X-Forwarded-Host":  []string{`first.example.com`},
				"X-Forwarded-Port":  []string{`4000`},
			}},
			base: "https://" + u.Host,
		},
	}
}

func TestBuilderFromRequest(t *testing.T) {
	testRequests := makeTestRequests(t)

	doTest := func(relative bool) {
		for _, tr := range testRequests {
			var builder *Builder
			if tr.configHost.Scheme != "" && tr.configHost.Host != "" {
				builder = NewBuilder(&tr.configHost, relative)
			} else {
				builder = NewBuilderFromRequest(tr.request, relative)
			}

			for _, testCase := range makeBuilderTestCases(builder) {
				buildURL, err := testCase.build()
				expectedErr := testCase.expectedErr
				if !reflect.DeepEqual(expectedErr, err) {
					t.Fatalf("%s: Expecting %v but got error %v", testCase.description, expectedErr, err)
				}
				if expectedErr != nil {
					continue
				}

				expectedURL := testCase.expectedPath
				if !relative {
					expectedURL = tr.base + expectedURL
				}

				if buildURL != expectedURL {
					t.Errorf("[relative=%t, request=%q, case=%q]: %q != %q", relative, tr.name, testCase.description, buildURL, expectedURL)
				}
			}
		}
	}

	doTest(true)
	doTest(false)
}

// Prevent Compiler optimizations from altering benchmark results
// https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go
var result string
var builder *Builder

func BenchmarkBuilderFromRequest(b *testing.B) {
	doTest := func(relative bool) {
		var pathType string
		if relative {
			pathType = "relative"
		} else {
			pathType = "absolute"
		}

		b.Run(pathType, func(b *testing.B) {
			b.ReportAllocs()

			var ub *Builder
			request := makeTestRequests(b)[0].request

			for i := 0; i < b.N; i++ {
				ub = NewBuilderFromRequest(request, relative)
			}
			builder = ub
		})
	}

	doTest(true)
	doTest(false)
}

func BenchmarkBuilderFromRequestURLs(b *testing.B) {
	doTest := func(relative bool) {
		var builder *Builder

		var pathType string
		if relative {
			pathType = "relative"
		} else {
			pathType = "absolute"
		}

		request := makeTestRequests(b)[0].request
		builder = NewBuilderFromRequest(request, relative)

		for _, testCase := range makeBuilderTestCases(builder) {
			b.Run(testCase.description+" "+pathType, func(b *testing.B) {
				b.ReportAllocs()
				var r string
				for i := 0; i < b.N; i++ {
					// This will occasionally throw expected error values, ignore them.
					r, _ = testCase.build()
				}
				result = r
			})
		}
	}

	doTest(true)
	doTest(false)
}

func TestBuilderFromRequestWithPrefix(t *testing.T) {
	u, err := url.Parse("http://example.com/prefix/v2/")
	if err != nil {
		t.Fatal(err)
	}

	forwardedProtoHeader := make(http.Header, 1)
	forwardedProtoHeader.Set("X-Forwarded-Proto", "https")

	testRequests := []struct {
		request    *http.Request
		base       string
		configHost url.URL
	}{
		{
			request: &http.Request{URL: u, Host: u.Host},
			base:    "http://example.com/prefix/",
		},

		{
			request: &http.Request{URL: u, Host: u.Host, Header: forwardedProtoHeader},
			base:    "http://example.com/prefix/",
		},
		{
			request: &http.Request{URL: u, Host: u.Host, Header: forwardedProtoHeader},
			base:    "https://example.com/prefix/",
		},
		{
			request: &http.Request{URL: u, Host: u.Host, Header: forwardedProtoHeader},
			base:    "https://subdomain.example.com/prefix/",
			configHost: url.URL{
				Scheme: "https",
				Host:   "subdomain.example.com",
				Path:   "/prefix/",
			},
		},
	}

	var relative bool
	for _, tr := range testRequests {
		var builder *Builder
		if tr.configHost.Scheme != "" && tr.configHost.Host != "" {
			builder = NewBuilder(&tr.configHost, false)
		} else {
			builder = NewBuilderFromRequest(tr.request, false)
		}

		for _, testCase := range makeBuilderTestCases(builder) {
			buildURL, err := testCase.build()
			expectedErr := testCase.expectedErr
			if !reflect.DeepEqual(expectedErr, err) {
				t.Fatalf("%s: Expecting %v but got error %v", testCase.description, expectedErr, err)
			}
			if expectedErr != nil {
				continue
			}

			var expectedURL string
			proto, ok := tr.request.Header["X-Forwarded-Proto"]
			if !ok {
				expectedURL = testCase.expectedPath
				if !relative {
					expectedURL = tr.base[0:len(tr.base)-1] + expectedURL
				}
			} else {
				urlBase, err := url.Parse(tr.base)
				if err != nil {
					t.Fatal(err)
				}
				urlBase.Scheme = proto[0]
				expectedURL = testCase.expectedPath
				if !relative {
					expectedURL = urlBase.String()[0:len(urlBase.String())-1] + expectedURL
				}
			}

			if buildURL != expectedURL {
				t.Fatalf("%s: %q != %q", testCase.description, buildURL, expectedURL)
			}
		}
	}
}
