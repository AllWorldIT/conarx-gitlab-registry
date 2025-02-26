package googlecdn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/log"
)

const (
	// defaultIPRangesURL is the default URL to get a list of all known GCP IPs
	defaultIPRangesURL = "https://www.gstatic.com/ipranges/goog.json"
	// defaultUpdateFrequency is the default frequency at which the list of know GCP IPs should be updated
	defaultUpdateFrequency = time.Hour * 12
)

// newGoogleIPs returns a new googleIP object.
func newGoogleIPs(host string, updateFrequency time.Duration) *googleIPs {
	ips := &googleIPs{
		host:            host,
		updateFrequency: updateFrequency,
		updaterStopCh:   make(chan bool),
	}
	if err := ips.tryUpdate(); err != nil {
		log.GetLogger().WithError(err).Error("failed to update list of GCP IPs")
	}
	go ips.updater()
	return ips
}

// googleIPs tracks a list of GCP IPs
type googleIPs struct {
	host            string
	updateFrequency time.Duration
	ipv4            []net.IPNet
	ipv6            []net.IPNet
	mutex           sync.RWMutex
	updaterStopCh   chan bool
	initialized     bool
}

type googleIPResponse struct {
	Prefixes []prefixEntry `json:"prefixes"`
}

type prefixEntry struct {
	IPV4Prefix string `json:"ipv4Prefix"`
	IPV6Prefix string `json:"ipv6Prefix"`
}

func fetchGoogleIPs(url string) (googleIPResponse, error) {
	l := log.GetLogger()
	l.WithFields(log.Fields{"url": url}).Debug("fetching list of known Google IPs")

	var response googleIPResponse
	// nolint: gosec
	resp, err := http.Get(url)
	if err != nil {
		return response, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return response, fmt.Errorf("failed to read response body: %w", err)
		}
		l.WithFields(log.Fields{
			"request_url":     url,
			"response_status": resp.Status,
			"response_body":   string(body),
		}).Error("failed to fetch list of GCP IPs")
		return response, fmt.Errorf("request failed with status %q, check logs for more details", resp.Status)
	}

	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&response); err != nil {
		return response, err
	}

	return response, nil
}

func processAddress(output *[]net.IPNet, prefix string) {
	if prefix == "" {
		return
	}

	_, network, err := net.ParseCIDR(prefix)
	if err != nil {
		log.GetLogger().WithFields(log.Fields{"cidr": prefix}).WithError(err).Error("unparseable cidr")
		return
	}

	*output = append(*output, *network)
}

// tryUpdate attempts to download the new set of IP addresses. Must be thread safe with contains.
func (s *googleIPs) tryUpdate() error {
	resp, err := fetchGoogleIPs(s.host)
	if err != nil {
		return err
	}

	var ipv4 []net.IPNet
	var ipv6 []net.IPNet
	for _, prefix := range resp.Prefixes {
		processAddress(&ipv4, prefix.IPV4Prefix)
		processAddress(&ipv6, prefix.IPV6Prefix)
	}

	// Update each attr of googleIPs atomically.
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.ipv4 = ipv4
	s.ipv6 = ipv6
	s.initialized = true

	return nil
}

// updater will periodically update the list of known GCP IPs. This is meant to be run in a background goroutine.
func (s *googleIPs) updater() {
	l := log.GetLogger()
	defer close(s.updaterStopCh)

	for {
		time.Sleep(s.updateFrequency)
		select {
		case <-s.updaterStopCh:
			l.Info("Google IP updater received stop signal")
			return
		default:
			if err := s.tryUpdate(); err != nil {
				l.WithError(err).Error("error updating Google IP list")
			}
		}
	}
}

// getCandidateNetworks returns either the ipv4 or ipv6 networks that were last read from GCP. The networks returned
// have the same type as the IP address provided.
func (s *googleIPs) getCandidateNetworks(ip net.IP) []net.IPNet {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	switch {
	case ip.To4() != nil:
		return s.ipv4
	case ip.To16() != nil:
		return s.ipv6
	default:
		log.GetLogger().WithFields(log.Fields{"ip": ip}).Error("unknown IP address format")
		// assume mismatch, pass through Google CDN
		return nil
	}
}

// contains determines whether the host is within GCP.
func (s *googleIPs) contains(ip net.IP) bool {
	nn := s.getCandidateNetworks(ip)
	for _, n := range nn {
		if n.Contains(ip) {
			return true
		}
	}

	return false
}

// parseIPFromRequest attempts to extract the IP address of the client that made the request.
func parseIPFromRequest(ctx context.Context) (net.IP, error) {
	req, err := dcontext.GetRequest(ctx)
	if err != nil {
		return nil, err
	}
	ipStr := dcontext.RemoteIP(req)
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP address from requester: %s", ipStr)
	}

	return ip, nil
}

// eligibleForGCS checks if a request is eligible for using GCS directly. Returns true only when the IP belongs to GCP.
func eligibleForGCS(ctx context.Context, googleIPs *googleIPs) bool {
	if googleIPs == nil || !googleIPs.initialized {
		return false
	}

	l := log.GetLogger(log.WithContext(ctx))
	addr, err := parseIPFromRequest(ctx)
	if err != nil {
		l.WithError(err).Warn("failed to parse IP address from context, fallback to Google CDN")
		return false
	}

	req, err := dcontext.GetRequest(ctx)
	if err != nil {
		l.WithError(err).Warn("failed to parse HTTP request, fallback to Google CDN")
		return false
	}

	fields := log.Fields{
		"user-agent": req.UserAgent(),
		"ip":         addr.String(),
	}
	if googleIPs.contains(addr) {
		l.WithFields(fields).Info("request from Google IP, skipping Google CDN")
		return true
	}
	l.WithFields(fields).Info("request not from a Google IP, fallback to Google CDN")
	return false
}
