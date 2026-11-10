package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
)

// Upper bound on how long a DNS answer is cached, regardless of its TTL.
const dnsMaxCacheTTL = 60 * time.Second

// Number of times an upstream query is attempted (per server) before giving up.
const dnsAttempts = 3

// dnsRcodeError is returned when upstream answers with a non-success rcode.
type dnsRcodeError struct {
	rcode int
}

func (e *dnsRcodeError) Error() string {
	return fmt.Sprintf("upstream returned %s", dns.RcodeToString[e.rcode])
}

type dnsCacheEntry struct {
	// closed once the lookup has finished and the fields below are set.
	done    chan struct{}
	ips     []string
	err     error
	expires time.Time
}

// dnsResolver resolves A records through the upstream resolver, caching answers
// for their TTL (capped to dnsMaxCacheTTL) and coalescing concurrent lookups of
// the same name into a single upstream query.
type dnsResolver struct {
	// sends a query upstream.
	exchange func(m *dns.Msg) (*dns.Msg, error)
	// called with the IPs of every fresh upstream answer, before any caller sees them.
	onResolved func(ips []string)
	now        func() time.Time

	mu      sync.Mutex
	entries map[string]*dnsCacheEntry
}

func newDNSResolver(resolvConf string, onResolved func(ips []string)) (*dnsResolver, error) {
	cfg, err := dns.ClientConfigFromFile(resolvConf)
	if err != nil {
		return nil, err
	}
	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("no nameservers in %s", resolvConf)
	}
	var servers []string
	for _, s := range cfg.Servers {
		servers = append(servers, net.JoinHostPort(s, cfg.Port))
	}

	udp := &dns.Client{Net: "udp", Timeout: 2 * time.Second}
	tcp := &dns.Client{Net: "tcp", Timeout: 2 * time.Second}
	exchange := func(m *dns.Msg) (*dns.Msg, error) {
		var lastErr error
		for attempt := 0; attempt < dnsAttempts; attempt++ {
			for _, server := range servers {
				resp, _, err := udp.Exchange(m, server)
				if err == nil && resp.Truncated {
					resp, _, err = tcp.Exchange(m, server)
				}
				if err != nil {
					lastErr = err
					continue
				}
				// Transient server-side failures are worth retrying, anything else is an answer.
				if resp.Rcode == dns.RcodeServerFailure || resp.Rcode == dns.RcodeRefused {
					lastErr = &dnsRcodeError{resp.Rcode}
					continue
				}
				return resp, nil
			}
		}
		return nil, lastErr
	}

	return &dnsResolver{
		exchange:   exchange,
		onResolved: onResolved,
		now:        time.Now,
		entries:    make(map[string]*dnsCacheEntry),
	}, nil
}

// lookupA returns the IPv4 addresses of name, and the TTL to hand out to clients.
func (r *dnsResolver) lookupA(name string) ([]string, uint32, error) {
	name = strings.ToLower(dns.Fqdn(name))

	r.mu.Lock()
	e, ok := r.entries[name]
	if ok {
		select {
		case <-e.done:
			if e.err == nil && r.now().Before(e.expires) {
				r.mu.Unlock()
				return e.ips, r.remainingTTL(e), nil
			}
			// Expired or failed: resolve again below.
		default:
			// Lookup in flight: wait for it.
			r.mu.Unlock()
			<-e.done
			return e.ips, r.remainingTTL(e), e.err
		}
	}
	e = &dnsCacheEntry{done: make(chan struct{})}
	r.entries[name] = e
	r.mu.Unlock()

	ips, ttl, err := r.resolve(name)
	if err == nil && len(ips) > 0 && r.onResolved != nil {
		r.onResolved(ips)
	}
	e.ips = ips
	e.err = err
	e.expires = r.now().Add(min(time.Duration(ttl)*time.Second, dnsMaxCacheTTL))
	close(e.done)

	return e.ips, r.remainingTTL(e), e.err
}

func (r *dnsResolver) remainingTTL(e *dnsCacheEntry) uint32 {
	d := e.expires.Sub(r.now())
	if d <= 0 {
		return 0
	}
	// Round up so we never tell clients 0 for an entry we still consider valid.
	return uint32((d + time.Second - 1) / time.Second)
}

// resolve queries upstream. The returned TTL is the smallest of all records in
// the answer (CNAMEs included), or 0 if there are no A records.
func (r *dnsResolver) resolve(name string) ([]string, uint32, error) {
	m := new(dns.Msg)
	m.SetQuestion(name, dns.TypeA)
	m.RecursionDesired = true

	resp, err := r.exchange(m)
	if err != nil {
		return nil, 0, err
	}
	if resp.Rcode != dns.RcodeSuccess {
		return nil, 0, &dnsRcodeError{resp.Rcode}
	}

	var ips []string
	var ttl uint32
	for i, rr := range resp.Answer {
		if i == 0 || rr.Header().Ttl < ttl {
			ttl = rr.Header().Ttl
		}
		if a, ok := rr.(*dns.A); ok {
			ips = append(ips, a.A.String())
		}
	}
	if len(ips) == 0 {
		ttl = 0
	}
	return ips, ttl, nil
}
