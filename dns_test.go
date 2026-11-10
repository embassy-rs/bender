package main

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
)

type fakeUpstream struct {
	queries atomic.Int32
	// blocks each query until closed, if set.
	gate  chan struct{}
	reply func(m *dns.Msg) (*dns.Msg, error)
}

func (f *fakeUpstream) exchange(m *dns.Msg) (*dns.Msg, error) {
	f.queries.Add(1)
	if f.gate != nil {
		<-f.gate
	}
	return f.reply(m)
}

func answer(m *dns.Msg, rrs ...dns.RR) *dns.Msg {
	resp := new(dns.Msg)
	resp.SetReply(m)
	resp.Answer = rrs
	return resp
}

func cname(name, target string, ttl uint32) dns.RR {
	return &dns.CNAME{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: ttl}, Target: target}
}

func a(name, ip string, ttl uint32) dns.RR {
	return &dns.A{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl}, A: net.ParseIP(ip)}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestResolver(up *fakeUpstream, resolved *[][]string) (*dnsResolver, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	var mu sync.Mutex
	return &dnsResolver{
		exchange: up.exchange,
		onResolved: func(ips []string) {
			mu.Lock()
			defer mu.Unlock()
			*resolved = append(*resolved, ips)
		},
		now:     clock.now,
		entries: make(map[string]*dnsCacheEntry),
	}, clock
}

func TestDNSCacheRespectsMinTTLOfChain(t *testing.T) {
	up := &fakeUpstream{reply: func(m *dns.Msg) (*dns.Msg, error) {
		return answer(m,
			cname("teleprobe.embassy.dev.", "bcnma.akiles.xyz.", 600),
			a("bcnma.akiles.xyz.", "5.5.5.5", 20),
		), nil
	}}
	var resolved [][]string
	r, clock := newTestResolver(up, &resolved)

	ips, ttl, err := r.lookupA("Teleprobe.Embassy.Dev")
	if err != nil || len(ips) != 1 || ips[0] != "5.5.5.5" || ttl != 20 {
		t.Fatalf("got %v %v %v", ips, ttl, err)
	}

	clock.advance(15 * time.Second)
	_, ttl, _ = r.lookupA("teleprobe.embassy.dev.")
	if ttl != 5 {
		t.Fatalf("remaining ttl = %d, want 5", ttl)
	}
	if up.queries.Load() != 1 {
		t.Fatalf("queries = %d, want 1", up.queries.Load())
	}

	clock.advance(5 * time.Second)
	r.lookupA("teleprobe.embassy.dev.")
	if up.queries.Load() != 2 {
		t.Fatalf("queries = %d after expiry, want 2", up.queries.Load())
	}
	if len(resolved) != 2 {
		t.Fatalf("onResolved called %d times, want 2", len(resolved))
	}
}

func TestDNSCacheCapsTTL(t *testing.T) {
	up := &fakeUpstream{reply: func(m *dns.Msg) (*dns.Msg, error) {
		return answer(m, a("example.com.", "1.2.3.4", 86400)), nil
	}}
	var resolved [][]string
	r, clock := newTestResolver(up, &resolved)

	_, ttl, _ := r.lookupA("example.com.")
	if ttl != 60 {
		t.Fatalf("ttl = %d, want 60", ttl)
	}
	clock.advance(59 * time.Second)
	r.lookupA("example.com.")
	if up.queries.Load() != 1 {
		t.Fatalf("queries = %d, want 1", up.queries.Load())
	}
	clock.advance(time.Second)
	r.lookupA("example.com.")
	if up.queries.Load() != 2 {
		t.Fatalf("queries = %d after cap, want 2", up.queries.Load())
	}
}

func TestDNSCacheCoalescesConcurrentLookups(t *testing.T) {
	up := &fakeUpstream{
		gate: make(chan struct{}),
		reply: func(m *dns.Msg) (*dns.Msg, error) {
			return answer(m, a("example.com.", "1.2.3.4", 30)), nil
		},
	}
	var resolved [][]string
	r, _ := newTestResolver(up, &resolved)

	const n = 60
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ips, _, err := r.lookupA("example.com.")
			if err == nil && (len(ips) != 1 || ips[0] != "1.2.3.4") {
				err = errors.New("bad ips")
			}
			errs <- err
		}()
	}
	for up.queries.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	close(up.gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if up.queries.Load() != 1 {
		t.Fatalf("queries = %d, want 1", up.queries.Load())
	}
	if len(resolved) != 1 {
		t.Fatalf("onResolved called %d times, want 1", len(resolved))
	}
}

func TestDNSCacheDoesNotCacheFailures(t *testing.T) {
	fail := true
	up := &fakeUpstream{reply: func(m *dns.Msg) (*dns.Msg, error) {
		if fail {
			return nil, errors.New("timeout")
		}
		return answer(m, a("example.com.", "1.2.3.4", 30)), nil
	}}
	var resolved [][]string
	r, _ := newTestResolver(up, &resolved)

	if _, _, err := r.lookupA("example.com."); err == nil {
		t.Fatal("expected error")
	}
	fail = false
	ips, _, err := r.lookupA("example.com.")
	if err != nil || len(ips) != 1 {
		t.Fatalf("got %v %v", ips, err)
	}
	if up.queries.Load() != 2 {
		t.Fatalf("queries = %d, want 2", up.queries.Load())
	}
}

func TestDNSCacheNXDOMAIN(t *testing.T) {
	up := &fakeUpstream{reply: func(m *dns.Msg) (*dns.Msg, error) {
		resp := answer(m)
		resp.Rcode = dns.RcodeNameError
		return resp, nil
	}}
	var resolved [][]string
	r, _ := newTestResolver(up, &resolved)

	_, _, err := r.lookupA("nope.example.com.")
	var rcodeErr *dnsRcodeError
	if !errors.As(err, &rcodeErr) || rcodeErr.rcode != dns.RcodeNameError {
		t.Fatalf("err = %v, want NXDOMAIN", err)
	}
	if len(resolved) != 0 {
		t.Fatal("onResolved called for NXDOMAIN")
	}
}
