package googlecdn

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dcontext "github.com/docker/distribution/context"
)

type mockIPRangeHandler struct {
	data googleIPResponse
}

func (m mockIPRangeHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	bytes, err := json.Marshal(m.data)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.Write(bytes)
}

func newTestHandler(data googleIPResponse) *httptest.Server {
	return httptest.NewServer(mockIPRangeHandler{
		data: data,
	})
}

func serverIPRanges(server *httptest.Server) string {
	return fmt.Sprintf("%s/", server.URL)
}

func setupTest(data googleIPResponse) *httptest.Server {
	// This is a basic schema which only claims the exact IP is in GCP.
	server := newTestHandler(data)
	return server
}

func TestTryUpdate(t *testing.T) {
	t.Parallel()
	server := setupTest(googleIPResponse{
		Prefixes: []prefixEntry{
			{IPV4Prefix: "123.231.123.231/32"},
		},
	})
	defer server.Close()

	ips := newGoogleIPs(serverIPRanges(server), time.Hour)

	require.Equal(t, 1, len(ips.ipv4))
	require.Equal(t, 0, len(ips.ipv6))
}

func TestMatchIPV6(t *testing.T) {
	t.Parallel()
	server := setupTest(googleIPResponse{
		Prefixes: []prefixEntry{
			{IPV6Prefix: "ff00::/16"},
		},
	})
	defer server.Close()

	ips := newGoogleIPs(serverIPRanges(server), time.Hour)
	err := ips.tryUpdate()
	require.NoError(t, err)

	require.Equal(t, true, ips.contains(net.ParseIP("ff00::")))
	require.Equal(t, 1, len(ips.ipv6))
	require.Equal(t, 0, len(ips.ipv4))
}

func TestMatchIPV4(t *testing.T) {
	t.Parallel()
	server := setupTest(googleIPResponse{
		Prefixes: []prefixEntry{
			{IPV4Prefix: "192.168.0.0/24"},
		},
	})
	defer server.Close()

	ips := newGoogleIPs(serverIPRanges(server), time.Hour)
	err := ips.tryUpdate()
	require.NoError(t, err)

	require.Equal(t, true, ips.contains(net.ParseIP("192.168.0.0")))
	require.Equal(t, true, ips.contains(net.ParseIP("192.168.0.1")))
	require.Equal(t, false, ips.contains(net.ParseIP("192.169.0.0")))
}

func TestInvalidData(t *testing.T) {
	t.Parallel()
	// Invalid entries from GCP should be ignored.
	server := setupTest(googleIPResponse{
		Prefixes: []prefixEntry{
			{IPV4Prefix: "9000"},
			{IPV4Prefix: "192.168.0.0/24"},
		},
	})
	defer server.Close()

	ips := newGoogleIPs(serverIPRanges(server), time.Hour)
	err := ips.tryUpdate()
	require.NoError(t, err)

	require.Equal(t, 1, len(ips.ipv4))
}

func TestInvalidNetworkType(t *testing.T) {
	t.Parallel()
	server := setupTest(googleIPResponse{
		Prefixes: []prefixEntry{
			{IPV4Prefix: "192.168.0.0/24"},
			{IPV6Prefix: "ff00::/8"},
			{IPV6Prefix: "fe00::/8"},
		},
	})
	defer server.Close()

	ips := newGoogleIPs(serverIPRanges(server), time.Hour)
	require.Equal(t, 0, len(ips.getCandidateNetworks(make([]byte, 17)))) // 17 bytes does not correspond to any net type
	require.Equal(t, 1, len(ips.getCandidateNetworks(make([]byte, 4))))  // netv4 networks
	require.Equal(t, 2, len(ips.getCandidateNetworks(make([]byte, 16)))) // netv6 networks
}

func TestParsing(t *testing.T) {
	t.Parallel()

	data := `{
	   "syncToken": "1640628111578",
	   "creationTime": "2021-12-27T10:01:51.578099",
	   "prefixes": [{
		 "ipv4Prefix": "8.8.4.0/24"
	   }, {
		 "ipv4Prefix": "8.8.8.0/24"
	   }, {
		 "ipv6Prefix": "2620:11a:a000::/40"
	   }]
	 }`
	rawMockHandler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(data))
		},
	)

	server := httptest.NewServer(rawMockHandler)
	defer server.Close()

	schema, err := fetchGoogleIPs(server.URL)
	require.NoError(t, err)

	require.Len(t, schema.Prefixes, 3)
	require.Equal(t, []prefixEntry{
		{IPV4Prefix: "8.8.4.0/24"},
		{IPV4Prefix: "8.8.8.0/24"},
		{IPV6Prefix: "2620:11a:a000::/40"},
	}, schema.Prefixes)
}

func TestUpdateCalledRegularly(t *testing.T) {
	t.Parallel()

	updateCount := 0
	server := httptest.NewServer(http.HandlerFunc(
		func(rw http.ResponseWriter, req *http.Request) {
			updateCount++
			rw.Write([]byte("ok"))
		},
	))
	defer server.Close()

	newGoogleIPs(fmt.Sprintf("%s/", server.URL), time.Second)
	time.Sleep(time.Second*4 + time.Millisecond*500)
	require.GreaterOrEqual(t, updateCount, 4, "update should have been called at least 4 times")
}

func TestEligibleForGCS(t *testing.T) {
	googleIPs := &googleIPs{
		ipv4: []net.IPNet{{
			IP:   net.ParseIP("192.168.1.1"),
			Mask: net.IPv4Mask(255, 255, 255, 0),
		}},
		initialized: true,
	}
	empty := context.TODO()
	makeContext := func(ip string) context.Context {
		req := &http.Request{
			RemoteAddr: ip,
		}

		return dcontext.WithRequest(empty, req)
	}

	cases := []struct {
		Context  context.Context
		Expected bool
	}{
		{Context: empty, Expected: false},
		{Context: makeContext("192.168.1.2"), Expected: true},
		{Context: makeContext("192.168.0.2"), Expected: false},
	}

	for _, testCase := range cases {
		name := fmt.Sprintf("Client IP = %v",
			testCase.Context.Value("http.request.ip"))
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.Expected, eligibleForGCS(testCase.Context, googleIPs))
		})
	}
}

func TestEligibleForGCSWithGCPIPNotInitialized(t *testing.T) {
	googleIPs := &googleIPs{
		ipv4: []net.IPNet{{
			IP:   net.ParseIP("192.168.1.1"),
			Mask: net.IPv4Mask(255, 255, 255, 0),
		}},
		initialized: false,
	}
	empty := context.TODO()
	makeContext := func(ip string) context.Context {
		req := &http.Request{
			RemoteAddr: ip,
		}

		return dcontext.WithRequest(empty, req)
	}

	cases := []struct {
		Context  context.Context
		Expected bool
	}{
		{Context: empty, Expected: false},
		{Context: makeContext("192.168.1.2"), Expected: false},
		{Context: makeContext("192.168.0.2"), Expected: false},
	}

	for _, testCase := range cases {
		name := fmt.Sprintf("Client IP = %v",
			testCase.Context.Value("http.request.ip"))
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.Expected, eligibleForGCS(testCase.Context, googleIPs))
		})
	}
}

func TestResponseNotOK(t *testing.T) {
	t.Parallel()

	mockHandler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
	)

	server := httptest.NewServer(mockHandler)
	defer server.Close()

	resp, err := fetchGoogleIPs(server.URL)
	require.EqualError(t, err, `request failed with status "404 Not Found", check logs for more details`)
	require.Empty(t, resp)
}

// populate ips with a number of different ipv4 and ipv6 networks, for the purposes
// of benchmarking contains() performance.
func populateRandomNetworks(b *testing.B, ips *googleIPs, ipv4Count, ipv6Count int) {
	generateNetworks := func(dest *[]net.IPNet, bytes int, count int) {
		for i := 0; i < count; i++ {
			ip := make([]byte, bytes)
			_, err := rand.Read(ip)
			if err != nil {
				b.Fatalf("failed to generate network for test : %s", err.Error())
			}
			mask := make([]byte, bytes)
			for i := 0; i < bytes; i++ {
				mask[i] = 0xff
			}
			*dest = append(*dest, net.IPNet{
				IP:   ip,
				Mask: mask,
			})
		}
	}

	generateNetworks(&ips.ipv4, 4, ipv4Count)
	generateNetworks(&ips.ipv6, 16, ipv6Count)
}

func BenchmarkContainsRandom(b *testing.B) {
	// Generate a random network configuration, of size comparable to
	// GCPs official networks list
	// curl -s https://www.gstatic.com/ipranges/goog.json | jq '.prefixes | length'
	// 72
	numNetworksPerType := 100 // keep in sync with the above
	// intentionally skip constructor when creating googleIPs, to avoid updater routine.
	// This benchmark is only concerned with contains() performance.
	googleIPs := googleIPs{}
	populateRandomNetworks(b, &googleIPs, numNetworksPerType, numNetworksPerType)

	ipv4 := make([][]byte, b.N)
	ipv6 := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		ipv4[i] = make([]byte, 4)
		ipv6[i] = make([]byte, 16)
		rand.Read(ipv4[i])
		rand.Read(ipv6[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		googleIPs.contains(ipv4[i])
		googleIPs.contains(ipv6[i])
	}
}

func BenchmarkContainsProd(b *testing.B) {
	googleIPs := newGoogleIPs(defaultIPRangesURL, defaultUpdateFrequency)
	ipv4 := make([][]byte, b.N)
	ipv6 := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		ipv4[i] = make([]byte, 4)
		ipv6[i] = make([]byte, 16)
		rand.Read(ipv4[i])
		rand.Read(ipv6[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		googleIPs.contains(ipv4[i])
		googleIPs.contains(ipv6[i])
	}
}
