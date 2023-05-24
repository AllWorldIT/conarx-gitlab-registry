package dns

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/miekg/dns"
)

const defaultResolverTimeout = 5 * time.Second

var errNotFound = errors.New("could not find DNS record for target")

type Resolver struct {
	Nameserver string
	Port       string
	Network    string
	Timeout    time.Duration
}

func NewResolver(nameserver, port, network string) *Resolver {
	if port == "" {
		port = "53"
	}

	return &Resolver{
		Nameserver: nameserver,
		Port:       port,
		Network:    network,
		Timeout:    defaultResolverTimeout,
	}
}

// LookupSRV attempts to get the DNS configuration of type SRV from the
// provided server and specified port. It defaults to using port 53 if no port is supplied.
func (r *Resolver) LookupSRV(target string) ([]*dns.SRV, error) {
	return resolve[*dns.SRV](r, target, dns.TypeSRV)
}

// LookupA attempts to get the record type A for the target from the DNS server.
// It defaults to using port 53 if no port is supplied.
func (r *Resolver) LookupA(target string) ([]*dns.A, error) {
	return resolve[*dns.A](r, target, dns.TypeA)
}

// resolve attempts to get the DNS configuration of type SRV from the
// provided server and specified port. It defaults to using port 53 if no port is supplied.
func resolve[DNSType *dns.SRV | *dns.A](resolver *Resolver, target string, recordType uint16) ([]DNSType, error) {
	c := dns.Client{
		Net:     resolver.Network,
		Timeout: resolver.Timeout,
	}

	m := dns.Msg{}
	m.SetQuestion(target, recordType)

	msg, _, err := c.Exchange(&m, net.JoinHostPort(resolver.Nameserver, resolver.Port))
	if err != nil {
		return nil, err
	}

	if len(msg.Answer) == 0 {
		return nil, errNotFound
	}

	result := make([]DNSType, len(msg.Answer), len(msg.Answer))
	for k, ans := range msg.Answer {
		res, ok := ans.(DNSType)
		if !ok {
			return nil, fmt.Errorf("got an invalid DNS record type: %T", ans)
		}

		result[k] = res
	}

	return result, nil
}
