package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore"
	"github.com/hashicorp/go-multierror"
)

// DBStatusChecker asynchronously checks and stores the status of the DB, load
// balancer and replicas, returning the status when required.
type DBStatusChecker struct {
	db       LoadBalancer
	interval time.Duration
	timeout  time.Duration // for replica pings

	mu       sync.RWMutex
	pingInfo map[string]*pingInfo
	logger   dcontext.Logger
}

type pingInfo struct {
	err      error
	pingedAt time.Time
}

func NewDBStatusChecker(db LoadBalancer, interval, timeout time.Duration, logger dcontext.Logger) *DBStatusChecker {
	return &DBStatusChecker{
		db:       db,
		interval: interval,
		timeout:  timeout,
		pingInfo: make(map[string]*pingInfo),
		logger:   logger,
	}
}

func (s *DBStatusChecker) Start(ctx context.Context) {
	go s.updateStatusInBackground(ctx)
}

func (s *DBStatusChecker) updateStatusInBackground(ctx context.Context) {
	// First, initialize the status right away
	s.doPings(ctx)

	// Then update the status every interval
	ticker := time.NewTicker(s.interval)
	for {
		select {
		case <-ticker.C:
			s.doPings(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *DBStatusChecker) doPings(ctx context.Context) {
	// Re-create the entire pingInfo map in order to flush out old replicas which
	// no longer exist
	pingInfos := make(map[string]*pingInfo)

	// Ping DBs concurrently
	var wg sync.WaitGroup
	type pingResult struct {
		address string
		info    *pingInfo
	}
	results := make(chan pingResult)

	for _, db := range s.primaryAndReplicas() {
		if db == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			timestamp := time.Now()
			pingCtx, cancel := context.WithTimeout(ctx, s.timeout)
			err := db.PingContext(pingCtx)
			cancel()

			results <- pingResult{
				address: db.Address(),
				info: &pingInfo{
					pingedAt: timestamp,
					err:      err,
				},
			}
		}()
	}

	go func() {
		// Close results chan once all the pings have returned
		wg.Wait()
		close(results)
	}()

	for r := range results {
		pingInfos[r.address] = r.info
	}

	s.mu.Lock()
	s.pingInfo = pingInfos
	s.mu.Unlock()
}

func (s *DBStatusChecker) primaryAndReplicas() []Replica {
	return append([]Replica{s.db.Primary()}, s.db.Replicas()...)
}

// HealthCheck is a CheckFunc to be used in the standard health check at
// /debug/health.
func (s *DBStatusChecker) HealthCheck() error {
	// Get up-to-date list of all replicas
	dbs := s.primaryAndReplicas()
	var errs *multierror.Error

	s.mu.RLock()
	pingInfo := s.pingInfo
	s.mu.RUnlock()

	for _, db := range dbs {
		address := db.Address()
		info := pingInfo[address]
		if info == nil {
			// This could happen if a replica was just added and the checker hasn't
			// had time to ping it yet. We have to assume it's healthy for now. Log
			// it just in case but continue.
			s.logger.WithFields(log.Fields{
				"path":            "/debug/health",
				"db_replica_addr": address,
			}).Info("status unknown for db replica, haven't pinged it yet, returning OK")
			continue
		}

		if info.err != nil {
			errs = multierror.Append(errs, info.err)
		}
	}
	return errs.ErrorOrNil()
}

// ServeHTTP is a HTTP handler that reports on the status of the load balancer
// mechanism and all replicas. This will be served at /debug/health/db.
func (s *DBStatusChecker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// If the response writing causes a write error, it's already too late to
	// handle it. Instead, this function helps us nicely log the error.
	maybeLogWriteErr := func(err error) {
		if err != nil {
			s.logger.WithFields(log.Fields{"path": "/debug/health/db"}).WithError(err).
				Error("error writing response")
		}
	}

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, err := fmt.Fprintf(w, "must be a GET request, not %s", r.Method)
		maybeLogWriteErr(err)
		return
	}

	encoded, err := json.Marshal(s.getStatus())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, writeErr := fmt.Fprint(w, err)
		maybeLogWriteErr(writeErr)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = fmt.Fprint(w, string(encoded))
	maybeLogWriteErr(err)
}

func (s *DBStatusChecker) getStatus() *DBStatus {
	status := &DBStatus{
		OverallStatus: s.getLoadBalancerStatus(),
	}

	setStatusFromPingInfo := func(st *ReplicaStatus, addr string) {
		s.mu.RLock()
		pingInfo := s.pingInfo[addr]
		s.mu.RUnlock()

		if pingInfo == nil {
			st.Status = ReplicaStatusUnknown
		} else {
			st.LastPingedAt = (*timestamp)(&pingInfo.pingedAt)
			if pingInfo.err == nil {
				st.Status = ReplicaOnline
			} else {
				st.Status = ReplicaUnreachable
			}
		}
	}

	// Getting the primary status is different to the replicas because there is
	// no lag tracker info. We just have to check if it was pingable.
	primary := s.db.Primary()
	if primary != nil {
		primaryAddress := primary.Address()
		status.Primary = &ReplicaStatus{
			Address: primaryAddress,
		}
		setStatusFromPingInfo(status.Primary, primaryAddress)
	}

	for _, replica := range s.db.Replicas() {
		if replica == nil {
			continue
		}

		address := replica.Address()
		replicaStatus := &ReplicaStatus{
			Address: address,
		}

		// Check if replica is quarantined
		info := s.db.GetReplicaLagInfo(address)
		switch {
		case info == nil:
			replicaStatus.Status = ReplicaStatusUnknown
		case info.Quarantined:
			replicaStatus.Status = ReplicaQuarantined
			replicaStatus.QuarantinedAt = (*timestamp)(&info.QuarantinedAt)
		default:
			// In theory, replica is online - but were we able to ping it?
			setStatusFromPingInfo(replicaStatus, address)
		}

		status.Replicas = append(status.Replicas, replicaStatus)
	}

	return status
}

func (s *DBStatusChecker) getLoadBalancerStatus() string {
	primary := s.db.Primary()
	if primary == nil {
		return LoadBalancerUnhealthy
	}

	s.mu.RLock()
	primaryPingInfo := s.pingInfo[primary.Address()]
	s.mu.RUnlock()

	if primaryPingInfo == nil || primaryPingInfo.err != nil {
		return LoadBalancerUnhealthy
	}

	// Need to check there is an online, pingable replica
	onlineReplicaExists := false
	unknownReplicaExists := false
	for _, replica := range s.db.Replicas() {
		// Check if quarantined
		info := s.db.GetReplicaLagInfo(replica.Address())
		if info == nil {
			unknownReplicaExists = true
			continue
		}
		if info.Quarantined {
			continue
		}

		s.mu.RLock()
		rPingInfo := s.pingInfo[replica.Address()]
		s.mu.RUnlock()

		if rPingInfo == nil {
			// This must be a new replica that we haven't had time to ping yet.
			unknownReplicaExists = true
			continue
		}
		if rPingInfo.err == nil {
			onlineReplicaExists = true
			break
		}
	}

	if !onlineReplicaExists {
		if unknownReplicaExists {
			// The status of some replicas is still unknown, so we can't say the load
			// balancer is unhealthy, at this stage the overall status is unknown.
			return LoadBalancerStatusUnknown
		}
		return LoadBalancerUnhealthy
	}
	return LoadBalancerHealthy
}

type DBStatus struct {
	OverallStatus string           `json:"overall_status"`
	Primary       *ReplicaStatus   `json:"primary"`
	Replicas      []*ReplicaStatus `json:"replicas,omitempty"`
}

const (
	LoadBalancerHealthy       = "healthy"
	LoadBalancerUnhealthy     = "unhealthy"
	LoadBalancerStatusUnknown = "unknown"
)

type ReplicaStatus struct {
	Address       string     `json:"address"`
	Status        string     `json:"status"`
	QuarantinedAt *timestamp `json:"quarantined_at,omitempty"`
	LastPingedAt  *timestamp `json:"last_pinged_at,omitempty"`
}

const (
	ReplicaOnline        = "online"
	ReplicaQuarantined   = "quarantined"
	ReplicaStatusUnknown = "unknown"
	ReplicaUnreachable   = "unreachable"
)

// timestamp is a time.Time that marshals into an ISO8601 timestamp with
// millisecond precision.
type timestamp time.Time

// MarshalJSON outputs the timestamp in ISO8601 format with millisecond precision.
func (t *timestamp) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0)
	b = append(b, '"')
	b = (*time.Time)(t).AppendFormat(b, "2006-01-02T15:04:05.999Z")
	b = append(b, '"')
	return b, nil
}

// LoadBalancer is a sub-interface of datastore.LoadBalancer that allows for
// easy testing of the code in this package.
type LoadBalancer interface {
	Primary() Replica
	Replicas() []Replica
	GetReplicaLagInfo(addr string) *datastore.ReplicaLagInfo
}

// Replica is an interface (implemented by *datastore.DB) that allows for easy
// testing of the code in this package.
type Replica interface {
	Address() string
	PingContext(context.Context) error
}

// LoadBalancerShim allows a datastore.LoadBalancer to implement the
// LoadBalancer interface defined in this package. Specifically, the Replicas
// method is changed to return a slice of type []Replica instead of
// []*datastore.DB.
type LoadBalancerShim struct {
	DB datastore.LoadBalancer
}

func (l *LoadBalancerShim) Primary() Replica {
	return l.DB.Primary()
}

func (l *LoadBalancerShim) Replicas() []Replica {
	rs := l.DB.Replicas()
	out := make([]Replica, 0, len(rs))
	for _, r := range rs {
		out = append(out, r)
	}
	return out
}

func (l *LoadBalancerShim) GetReplicaLagInfo(addr string) *datastore.ReplicaLagInfo {
	return l.DB.GetReplicaLagInfo(addr)
}
