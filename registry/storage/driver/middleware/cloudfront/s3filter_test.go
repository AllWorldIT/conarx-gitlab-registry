package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockIPRangeHandler struct {
	data awsIPResponse
}

func (m mockIPRangeHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	bytes, err := json.Marshal(m.data)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	// nolint: revive // unhandled-error
	w.Write(bytes)
}

func newTestHandler(data awsIPResponse) *httptest.Server {
	return httptest.NewServer(mockIPRangeHandler{
		data: data,
	})
}

func serverIPRanges(server *httptest.Server) string {
	return fmt.Sprintf("%s/", server.URL)
}

func setupTest(data awsIPResponse) *httptest.Server {
	// This is a basic schema which only claims the exact ip
	// is in aws.
	server := newTestHandler(data)
	return server
}

func TestS3TryUpdate(t *testing.T) {
	t.Parallel()
	server := setupTest(awsIPResponse{
		Prefixes: []prefixEntry{
			{IPV4Prefix: "123.231.123.231/32"},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, nil)

	assert.Len(t, ips.ipv4, 1)
	assert.Empty(t, ips.ipv6)
}

func TestMatchIPV6(t *testing.T) {
	t.Parallel()
	server := setupTest(awsIPResponse{
		V6Prefixes: []prefixEntry{
			{IPV6Prefix: "ff00::/16"},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, nil)
	err := ips.tryUpdate()
	require.NoError(t, err)
	assert.True(t, ips.contains(net.ParseIP("ff00::")))
	assert.Len(t, ips.ipv6, 1)
	assert.Empty(t, ips.ipv4)
}

func TestMatchIPV4(t *testing.T) {
	t.Parallel()
	server := setupTest(awsIPResponse{
		Prefixes: []prefixEntry{
			{IPV4Prefix: "192.168.0.0/24"},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, nil)
	err := ips.tryUpdate()
	require.NoError(t, err)
	assert.True(t, ips.contains(net.ParseIP("192.168.0.0")))
	assert.True(t, ips.contains(net.ParseIP("192.168.0.1")))
	assert.False(t, ips.contains(net.ParseIP("192.169.0.0")))
}

func TestMatchIPV4_2(t *testing.T) {
	t.Parallel()
	server := setupTest(awsIPResponse{
		Prefixes: []prefixEntry{
			{
				IPV4Prefix: "192.168.0.0/24",
				Region:     "us-east-1",
			},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, nil)
	err := ips.tryUpdate()
	require.NoError(t, err)
	assert.True(t, ips.contains(net.ParseIP("192.168.0.0")))
	assert.True(t, ips.contains(net.ParseIP("192.168.0.1")))
	assert.False(t, ips.contains(net.ParseIP("192.169.0.0")))
}

func TestMatchIPV4WithRegionMatched(t *testing.T) {
	t.Parallel()
	server := setupTest(awsIPResponse{
		Prefixes: []prefixEntry{
			{
				IPV4Prefix: "192.168.0.0/24",
				Region:     "us-east-1",
			},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, []string{"us-east-1"})
	err := ips.tryUpdate()
	require.NoError(t, err)
	assert.True(t, ips.contains(net.ParseIP("192.168.0.0")))
	assert.True(t, ips.contains(net.ParseIP("192.168.0.1")))
	assert.False(t, ips.contains(net.ParseIP("192.169.0.0")))
}

func TestMatchIPV4WithRegionMatch_2(t *testing.T) {
	t.Parallel()
	server := setupTest(awsIPResponse{
		Prefixes: []prefixEntry{
			{
				IPV4Prefix: "192.168.0.0/24",
				Region:     "us-east-1",
			},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, []string{"us-west-2", "us-east-1"})
	err := ips.tryUpdate()
	require.NoError(t, err)
	assert.True(t, ips.contains(net.ParseIP("192.168.0.0")))
	assert.True(t, ips.contains(net.ParseIP("192.168.0.1")))
	assert.False(t, ips.contains(net.ParseIP("192.169.0.0")))
}

func TestMatchIPV4WithRegionNotMatched(t *testing.T) {
	t.Parallel()
	server := setupTest(awsIPResponse{
		Prefixes: []prefixEntry{
			{
				IPV4Prefix: "192.168.0.0/24",
				Region:     "us-east-1",
			},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, []string{"us-west-2"})
	err := ips.tryUpdate()
	require.NoError(t, err)
	assert.False(t, ips.contains(net.ParseIP("192.168.0.0")))
	assert.False(t, ips.contains(net.ParseIP("192.168.0.1")))
	assert.False(t, ips.contains(net.ParseIP("192.169.0.0")))
}

func TestInvalidData(t *testing.T) {
	t.Parallel()
	// Invalid entries from aws should be ignored.
	server := setupTest(awsIPResponse{
		Prefixes: []prefixEntry{
			{IPV4Prefix: "9000"},
			{IPV4Prefix: "192.168.0.0/24"},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, nil)
	err := ips.tryUpdate()
	require.NoError(t, err)
	assert.Len(t, ips.ipv4, 1)
}

func TestInvalidNetworkType(t *testing.T) {
	t.Parallel()
	server := setupTest(awsIPResponse{
		Prefixes: []prefixEntry{
			{IPV4Prefix: "192.168.0.0/24"},
		},
		V6Prefixes: []prefixEntry{
			{IPV6Prefix: "ff00::/8"},
			{IPV6Prefix: "fe00::/8"},
		},
	})
	defer server.Close()

	ips := newAWSIPs(serverIPRanges(server), time.Hour, nil)
	assert.Empty(t, ips.getCandidateNetworks(make([]byte, 17)))  // 17 bytes does not correspond to any net type
	assert.Len(t, ips.getCandidateNetworks(make([]byte, 4)), 1)  // netv4 networks
	assert.Len(t, ips.getCandidateNetworks(make([]byte, 16)), 2) // netv6 networks
}

func TestParsing(t *testing.T) {
	data := `{
      "prefixes": [{
        "ip_prefix": "192.168.0.0",
        "region": "someregion",
        "service": "s3"}],
      "ipv6_prefixes": [{
        "ipv6_prefix": "2001:4860:4860::8888",
        "region": "anotherregion",
        "service": "ec2"}]
    }`
	rawMockHandler := http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			// nolint: revive // unhandled-error
			w.Write([]byte(data))
		},
	)
	t.Parallel()
	server := httptest.NewServer(rawMockHandler)
	defer server.Close()
	schema, err := fetchAWSIPs(server.URL)

	require.NoError(t, err)
	assert.Len(t, schema.Prefixes, 1)
	assert.Equal(t, prefixEntry{
		IPV4Prefix: "192.168.0.0",
		Region:     "someregion",
		Service:    "s3",
	}, schema.Prefixes[0])
	assert.Len(t, schema.V6Prefixes, 1)
	assert.Equal(t, prefixEntry{
		IPV6Prefix: "2001:4860:4860::8888",
		Region:     "anotherregion",
		Service:    "ec2",
	}, schema.V6Prefixes[0])
}

func TestUpdateCalledRegularly(t *testing.T) {
	t.Parallel()

	updateCount := 0
	server := httptest.NewServer(http.HandlerFunc(
		func(rw http.ResponseWriter, _ *http.Request) {
			updateCount++
			// nolint: revive // unhandled-error
			rw.Write([]byte("ok"))
		}))
	defer server.Close()
	newAWSIPs(fmt.Sprintf("%s/", server.URL), time.Second, nil)
	time.Sleep(time.Second*4 + time.Millisecond*500)
	assert.GreaterOrEqual(t, updateCount, 4, "update should have been called at least 4 times")
}

func TestEligibleForS3(t *testing.T) {
	awsIPs := &awsIPs{
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
			assert.Equal(t, testCase.Expected, eligibleForS3(testCase.Context, awsIPs))
		})
	}
}

func TestEligibleForS3WithAWSIPNotInitialized(t *testing.T) {
	awsIPs := &awsIPs{
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
			assert.Equal(t, testCase.Expected, eligibleForS3(testCase.Context, awsIPs))
		})
	}
}

// populate ips with a number of different ipv4 and ipv6 networks, for the purposes
// of benchmarking contains() performance.
func populateRandomNetworks(b *testing.B, ips *awsIPs, ipv4Count, ipv6Count int) {
	generateNetworks := func(dest *[]net.IPNet, bytes, count int) {
		rng := rand.NewChaCha8([32]byte(testutil.MustChaChaSeed(b)))
		for i := 0; i < count; i++ {
			ip := make([]byte, bytes)
			_, _ = rng.Read(ip)
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
	// aws official networks list
	// curl -s https://ip-ranges.amazonaws.com/ip-ranges.json | jq '.prefixes | length'
	// 941
	numNetworksPerType := 1000 // keep in sync with the above
	// intentionally skip constructor when creating awsIPs, to avoid updater routine.
	// This benchmark is only concerned with contains() performance.
	awsIPs := awsIPs{}
	populateRandomNetworks(b, &awsIPs, numNetworksPerType, numNetworksPerType)

	ipv4 := make([][]byte, b.N)
	ipv6 := make([][]byte, b.N)
	rng := rand.NewChaCha8([32]byte(testutil.MustChaChaSeed(b)))
	for i := 0; i < b.N; i++ {
		ipv4[i] = make([]byte, 4)
		ipv6[i] = make([]byte, 16)
		_, _ = rng.Read(ipv4[i])
		_, _ = rng.Read(ipv6[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		awsIPs.contains(ipv4[i])
		awsIPs.contains(ipv6[i])
	}
}

func BenchmarkContainsProd(b *testing.B) {
	awsIPs := newAWSIPs(defaultIPRangesURL, defaultUpdateFrequency, nil)
	ipv4 := make([][]byte, b.N)
	ipv6 := make([][]byte, b.N)
	rng := rand.NewChaCha8([32]byte(testutil.MustChaChaSeed(b)))
	for i := 0; i < b.N; i++ {
		ipv4[i] = make([]byte, 4)
		ipv6[i] = make([]byte, 16)
		_, _ = rng.Read(ipv4[i])
		_, _ = rng.Read(ipv6[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		awsIPs.contains(ipv4[i])
		awsIPs.contains(ipv6[i])
	}
}
