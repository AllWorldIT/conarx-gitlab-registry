package health

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fakeTimestamp = time.Date(2025, 1, 2, 12, 24, 5, 123456789, time.UTC)

func TestDBHealthCheck(t *testing.T) {
	tests := []struct {
		description string
		db          LoadBalancer
		pingInfo    map[string]*pingInfo
		expectedErr string
	}{{
		description: "both primary and replicas succeed",
		db: &mockDB{
			primary: &mockReplica{address: "primary"},
			replicas: []Replica{
				&mockReplica{address: "replica1"},
				&mockReplica{address: "replica2"},
			},
		},
		pingInfo: map[string]*pingInfo{
			"primary":  {err: nil},
			"replica1": {err: nil},
			"replica2": {err: nil},
		},
	}, {
		description: "primary fails replicas succeed",
		db: &mockDB{
			primary: &mockReplica{address: "primary"},
			replicas: []Replica{
				&mockReplica{address: "replica1"},
				&mockReplica{address: "replica2"},
			},
		},
		pingInfo: map[string]*pingInfo{
			"primary":  {err: errors.New("Maryna is Boryna")},
			"replica1": {err: nil},
			"replica2": {err: nil},
		},
		expectedErr: "Maryna is Boryna",
	}, {
		description: "primary succeeds replica fails",
		db: &mockDB{
			primary: &mockReplica{address: "primary"},
			replicas: []Replica{
				&mockReplica{address: "replica1"},
				&mockReplica{address: "replica2"},
			},
		},
		pingInfo: map[string]*pingInfo{
			"primary":  {err: nil},
			"replica1": {err: errors.New("ping timed out")},
			"replica2": {err: nil},
		},
		expectedErr: "ping timed out",
	}}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			su := DBStatusChecker{
				db:       test.db,
				pingInfo: test.pingInfo,
			}
			err := su.HealthCheck()
			if test.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.expectedErr)
			}
		})
	}
}

func TestDLBHandler(t *testing.T) {
	su := &DBStatusChecker{
		db: &mockDB{
			primary: &mockReplica{address: "primary"},
			replicas: []Replica{
				&mockReplica{address: "primary"},
				&mockReplica{address: "replica1"},
				&mockReplica{address: "replica2"},
			},
			lagInfo: map[string]*datastore.ReplicaLagInfo{
				"replica1": {Quarantined: false},
				"replica2": {Quarantined: true, QuarantinedAt: fakeTimestamp},
			},
		},
	}

	svr := httptest.NewServer(su)
	defer svr.Close()

	resp, err := http.Get(svr.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header["Content-Type"][0])
}

func TestDLBStatusUpdaterRace(t *testing.T) {
	db := &mockDB{
		primary: &mockReplica{address: "primary"},
		replicas: []Replica{
			&mockReplica{address: "replica1"},
			&mockReplica{address: "replica2"},
		},
		lagInfo: map[string]*datastore.ReplicaLagInfo{
			"replica1": {Quarantined: false},
			"replica2": {Quarantined: true, QuarantinedAt: fakeTimestamp},
		},
	}

	dbStatusChecker := NewDBStatusChecker(db, 10*time.Microsecond, time.Microsecond, dcontext.GetLogger(context.Background()))
	dbStatusChecker.Start(context.Background())
	svr := httptest.NewServer(dbStatusChecker)
	defer svr.Close()

	for range 100 {
		resp, err := http.Get(svr.URL)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		err = resp.Body.Close()
		require.NoError(t, err)

		_ = dbStatusChecker.HealthCheck()

		time.Sleep(time.Duration(rand.IntN(10)) * time.Microsecond)
	}
}

func TestUpdatePingInfo(t *testing.T) {
	db := &mockDB{
		primary: &mockReplica{address: "primary"},
		replicas: []Replica{
			&mockReplica{address: "replica1"},
			&mockReplica{address: "replica2", pingErr: errors.New("foo")},
		},
	}
	expected := map[string]*pingInfo{
		"primary":  {err: nil},
		"replica1": {err: nil},
		"replica2": {err: errors.New("foo")},
	}

	su := DBStatusChecker{db: db}
	su.doPings(context.Background())
	// Strip timestamps
	for _, i := range su.pingInfo {
		i.pingedAt = time.Time{}
	}
	assert.Equal(t, expected, su.pingInfo)
}

func TestGetDBStatus(t *testing.T) {
	tests := []struct {
		description string
		db          LoadBalancer
		pingInfo    map[string]*pingInfo
		expected    *DBStatus
	}{
		{
			description: "nil primary and no replicas",
			db: &mockDB{
				primary:  nil,
				replicas: []Replica(nil),
			},
			expected: &DBStatus{
				OverallStatus: "unhealthy",
			},
		},
		{
			description: "primary has no ping info yet",
			db: &mockDB{
				primary: &mockReplica{address: "primary"},
			},
			pingInfo: make(map[string]*pingInfo),
			expected: &DBStatus{
				OverallStatus: "unhealthy",
				Primary: &ReplicaStatus{
					Address: "primary",
					Status:  "unknown",
				},
			},
		},
		{
			description: "primary not pingable",
			db: &mockDB{
				primary: &mockReplica{address: "primary"},
			},
			pingInfo: map[string]*pingInfo{
				"primary": {err: errors.New("connection failed"), pingedAt: fakeTimestamp},
			},
			expected: &DBStatus{
				OverallStatus: "unhealthy",
				Primary: &ReplicaStatus{
					Address:      "primary",
					Status:       "unreachable",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				},
			},
		},
		{
			description: "no healthy replicas",
			db: &mockDB{
				primary: &mockReplica{address: "primary"},
				replicas: []Replica{
					&mockReplica{address: "nilLagInfo"},
					&mockReplica{address: "quarantined"},
					&mockReplica{address: "nilPingInfo"},
					&mockReplica{address: "unpingable"},
				},
				lagInfo: map[string]*datastore.ReplicaLagInfo{
					"quarantined": {Quarantined: true, QuarantinedAt: fakeTimestamp},
					"nilPingInfo": {Quarantined: false},
					"unpingable":  {Quarantined: false},
				},
			},
			pingInfo: map[string]*pingInfo{
				"primary":    {err: nil, pingedAt: fakeTimestamp},
				"unpingable": {err: errors.New("foobar"), pingedAt: fakeTimestamp},
			},
			expected: &DBStatus{
				OverallStatus: "unknown",
				Primary: &ReplicaStatus{
					Address:      "primary",
					Status:       "online",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				},
				Replicas: []*ReplicaStatus{{
					Address: "nilLagInfo",
					Status:  "unknown",
				}, {
					Address:       "quarantined",
					Status:        "quarantined",
					QuarantinedAt: (*timestamp)(&fakeTimestamp),
				}, {
					Address: "nilPingInfo",
					Status:  "unknown",
				}, {
					Address:      "unpingable",
					Status:       "unreachable",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				}},
			},
		},
		{
			description: "load balancer healthy",
			db: &mockDB{
				primary: &mockReplica{address: "primary"},
				replicas: []Replica{
					&mockReplica{address: "replica1"},
					&mockReplica{address: "replica2"},
				},
				lagInfo: map[string]*datastore.ReplicaLagInfo{
					"replica1": {Quarantined: false},
					"replica2": {Quarantined: true, QuarantinedAt: fakeTimestamp},
				},
			},
			pingInfo: map[string]*pingInfo{
				"primary":  {err: nil, pingedAt: fakeTimestamp},
				"replica1": {err: nil, pingedAt: fakeTimestamp},
			},
			expected: &DBStatus{
				OverallStatus: "healthy",
				Primary: &ReplicaStatus{
					Address:      "primary",
					Status:       "online",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				},
				Replicas: []*ReplicaStatus{{
					Address:      "replica1",
					Status:       "online",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				}, {
					Address:       "replica2",
					Status:        "quarantined",
					QuarantinedAt: (*timestamp)(&fakeTimestamp),
				}},
			},
		},
		{
			description: "new replica just added, no ping info",
			db: &mockDB{
				primary: &mockReplica{address: "primary"},
				replicas: []Replica{
					&mockReplica{address: "replica1"},
				},
				lagInfo: map[string]*datastore.ReplicaLagInfo{
					"replica1": {Quarantined: false},
				},
			},
			pingInfo: map[string]*pingInfo{
				"primary": {err: nil, pingedAt: fakeTimestamp},
			},
			expected: &DBStatus{
				OverallStatus: "unknown",
				Primary: &ReplicaStatus{
					Address:      "primary",
					Status:       "online",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				},
				Replicas: []*ReplicaStatus{{
					Address: "replica1",
					Status:  "unknown",
				}},
			},
		},
		{
			description: "starting up, replica statuses unknown",
			db: &mockDB{
				primary: &mockReplica{address: "primary"},
				replicas: []Replica{
					&mockReplica{address: "replica1"},
					&mockReplica{address: "replica2"},
				},
				lagInfo: map[string]*datastore.ReplicaLagInfo{
					"replica1": {Quarantined: false},
					"replica2": {Quarantined: false},
				},
			},
			pingInfo: map[string]*pingInfo{
				"primary": {err: nil, pingedAt: fakeTimestamp},
			},
			expected: &DBStatus{
				OverallStatus: "unknown",
				Primary: &ReplicaStatus{
					Address:      "primary",
					Status:       "online",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				},
				Replicas: []*ReplicaStatus{{
					Address: "replica1",
					Status:  "unknown",
				}, {
					Address: "replica2",
					Status:  "unknown",
				}},
			},
		},
		{
			description: "all replicas unhealthy",
			db: &mockDB{
				primary: &mockReplica{address: "primary"},
				replicas: []Replica{
					&mockReplica{address: "replica1"},
					&mockReplica{address: "replica2"},
				},
				lagInfo: map[string]*datastore.ReplicaLagInfo{
					"replica1": {Quarantined: true, QuarantinedAt: fakeTimestamp},
					"replica2": {Quarantined: false},
				},
			},
			pingInfo: map[string]*pingInfo{
				"primary":  {err: nil, pingedAt: fakeTimestamp},
				"replica2": {err: errors.New("ping failed"), pingedAt: fakeTimestamp},
			},
			expected: &DBStatus{
				OverallStatus: "unhealthy",
				Primary: &ReplicaStatus{
					Address:      "primary",
					Status:       "online",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				},
				Replicas: []*ReplicaStatus{{
					Address:       "replica1",
					Status:        "quarantined",
					QuarantinedAt: (*timestamp)(&fakeTimestamp),
				}, {
					Address:      "replica2",
					Status:       "unreachable",
					LastPingedAt: (*timestamp)(&fakeTimestamp),
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			su := &DBStatusChecker{
				db:       test.db,
				pingInfo: test.pingInfo,
				timeout:  10 * time.Millisecond,
			}
			obtained := su.getStatus()
			assert.Equal(t, test.expected, obtained, test.description)
		})
	}
}

func TestDBHealthCheckResponseFormat(t *testing.T) {
	db := &mockDB{
		primary: &mockReplica{address: "primary"},
		replicas: []Replica{
			&mockReplica{address: "replica1"},
			&mockReplica{address: "replica2"},
		},
		lagInfo: map[string]*datastore.ReplicaLagInfo{
			"replica1": {Quarantined: true, QuarantinedAt: fakeTimestamp},
			"replica2": {Quarantined: false},
		},
	}
	pingInfo := map[string]*pingInfo{
		"primary":  {err: nil, pingedAt: fakeTimestamp},
		"replica2": {err: nil, pingedAt: fakeTimestamp},
	}

	su := &DBStatusChecker{
		db:       db,
		pingInfo: pingInfo,
	}
	svr := httptest.NewServer(su)
	defer svr.Close()

	resp, err := http.Get(svr.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, stripSpaces(`
{
  "overall_status": "healthy",
  "primary": {
    "address": "primary",
    "status": "online",
    "last_pinged_at": "2025-01-02T12:24:05.123Z"
  },
  "replicas": [
    {
      "address": "replica1",
      "status": "quarantined",
      "quarantined_at": "2025-01-02T12:24:05.123Z"
    },
    {
      "address": "replica2",
      "status": "online",
      "last_pinged_at": "2025-01-02T12:24:05.123Z"
    }
  ]
}
	`), string(payload))
}

func stripSpaces(s string) string {
	spacesRemoved := strings.ReplaceAll(s, " ", "")
	newlinesRemoved := strings.ReplaceAll(spacesRemoved, "\n", "")
	tabsRemoved := strings.ReplaceAll(newlinesRemoved, "\t", "")
	return tabsRemoved
}

type mockDB struct {
	primary  Replica
	replicas []Replica
	lagInfo  map[string]*datastore.ReplicaLagInfo
}

func (db *mockDB) Primary() Replica { return db.primary }

func (db *mockDB) Replicas() []Replica { return db.replicas }

func (db *mockDB) GetReplicaLagInfo(addr string) *datastore.ReplicaLagInfo { return db.lagInfo[addr] }

type mockReplica struct {
	address string
	pingErr error
}

func (r *mockReplica) Address() string {
	return r.address
}

func (r *mockReplica) PingContext(context.Context) error {
	return r.pingErr
}
