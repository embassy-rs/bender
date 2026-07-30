package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testService(secret string) *Service {
	return &Service{config: Config{
		ExternalURL: "https://bender.example.com",
		Github: GithubConfig{
			ClientID:      "cid",
			ClientSecret:  "csecret",
			SessionSecret: secret,
		},
	}}
}

func TestSignRoundTrip(t *testing.T) {
	s := testService("hunter2")

	signed := s.sign([]byte("hello"))
	got, ok := s.unsign(signed)
	if !ok || string(got) != "hello" {
		t.Fatalf("unsign(sign(x)) = %q, %v; want \"hello\", true", got, ok)
	}
}

func TestUnsignRejectsTampering(t *testing.T) {
	s := testService("hunter2")
	signed := s.sign([]byte(`{"u":"alice"}`))
	payload, sig, _ := strings.Cut(signed, ".")

	// A forged payload with the original signature must not verify.
	forged := s.sign([]byte(`{"u":"mallory"}`))
	forgedPayload, _, _ := strings.Cut(forged, ".")
	if _, ok := s.unsign(forgedPayload + "." + sig); ok {
		t.Error("accepted a payload that doesn't match its signature")
	}

	// Neither may a corrupted signature, or garbage.
	for _, bad := range []string{
		payload + ".",
		payload + ".AAAA",
		payload,
		"",
		".",
	} {
		if _, ok := s.unsign(bad); ok {
			t.Errorf("accepted malformed cookie %q", bad)
		}
	}

	// A cookie signed with a different secret must not verify: this is what
	// makes rotating session_secret log everyone out.
	if _, ok := testService("other-secret").unsign(signed); ok {
		t.Error("accepted a cookie signed with a different secret")
	}
}

func TestNewSession(t *testing.T) {
	// Apps without expiring user tokens get no refresh token and no expiry, and
	// must never be treated as needing a refresh.
	sess := newSession("alice", &oauthToken{AccessToken: "tok"})
	if sess.Refresh != "" || sess.ExpiresAt != 0 {
		t.Errorf("non-expiring token stored refresh=%q expires=%d, want empty", sess.Refresh, sess.ExpiresAt)
	}
	if sess.needsRefresh() {
		t.Error("non-expiring token wants a refresh")
	}

	sess = newSession("alice", &oauthToken{AccessToken: "tok", RefreshToken: "ref", ExpiresIn: 28800})
	if sess.Refresh != "ref" {
		t.Errorf("got refresh %q, want \"ref\"", sess.Refresh)
	}
	if got := time.Until(time.Unix(sess.ExpiresAt, 0)); got < 7*time.Hour || got > 9*time.Hour {
		t.Errorf("expiry is %v away, want ~8h", got)
	}
	if sess.needsRefresh() {
		t.Error("a freshly issued token wants a refresh")
	}
}

func TestNeedsRefresh(t *testing.T) {
	tests := []struct {
		name string
		sess session
		want bool
	}{
		{"fresh", session{Refresh: "r", ExpiresAt: time.Now().Add(time.Hour).Unix()}, false},
		{"expired", session{Refresh: "r", ExpiresAt: time.Now().Add(-time.Hour).Unix()}, true},
		// Inside the margin we refresh early, so a request can't expire mid-flight.
		{"expiring now", session{Refresh: "r", ExpiresAt: time.Now().Add(30 * time.Second).Unix()}, true},
		// Old cookies from before refresh tokens were stored: nothing to refresh
		// with, so they're left to the 401 path instead.
		{"no refresh token", session{ExpiresAt: time.Now().Add(-time.Hour).Unix()}, false},
		{"no expiry", session{Refresh: "r"}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.sess.needsRefresh(); got != test.want {
				t.Errorf("needsRefresh() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestRefreshIfNeededNoop(t *testing.T) {
	// With nothing to refresh, no network call happens and the cookie is left
	// untouched. (Any HTTP attempt here would fail the test by erroring.)
	s := testService("hunter2")
	sess := session{User: "alice", Token: "tok"}
	w := httptest.NewRecorder()

	if err := s.refreshIfNeeded(context.Background(), w, &sess); err != nil {
		t.Fatalf("refreshIfNeeded() = %v, want nil", err)
	}
	if got := w.Result().Cookies(); len(got) != 0 {
		t.Errorf("rewrote %d cookies, want 0", len(got))
	}
	if sess.Token != "tok" {
		t.Errorf("token changed to %q", sess.Token)
	}
}

func TestSessionRoundTripsThroughCookie(t *testing.T) {
	// A refreshed session has to survive the cookie, or the next request would
	// arrive with the old token and 401.
	s := testService("hunter2")
	want := newSession("alice", &oauthToken{AccessToken: "tok2", RefreshToken: "ref2", ExpiresIn: 28800})

	w := httptest.NewRecorder()
	if err := s.saveSession(w, want); err != nil {
		t.Fatal(err)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("wrote %d cookies, want 1", len(cookies))
	}

	r, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.AddCookie(cookies[0])

	got := s.currentUser(r)
	if got == nil {
		t.Fatal("saved session didn't come back")
	}
	if *got != *want {
		t.Errorf("got %+v, want %+v", *got, *want)
	}
}

func TestCurrentUser(t *testing.T) {
	s := testService("hunter2")

	buf, err := json.Marshal(session{User: "alice", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	valid := s.sign(buf)

	tests := []struct {
		name   string
		svc    *Service
		cookie string
		want   string // "" means not logged in
	}{
		{"valid", s, valid, "alice"},
		{"no cookie", s, "", ""},
		{"garbage", s, "not-a-cookie", ""},
		{"wrong secret", testService("other-secret"), valid, ""},
		{"login disabled", &Service{}, valid, ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, err := http.NewRequest("GET", "/", nil)
			if err != nil {
				t.Fatal(err)
			}
			if test.cookie != "" {
				r.AddCookie(&http.Cookie{Name: sessionCookie, Value: test.cookie})
			}

			got := test.svc.currentUser(r)
			if test.want == "" {
				if got != nil {
					t.Fatalf("want not logged in, got user %q", got.User)
				}
				return
			}
			if got == nil {
				t.Fatal("want logged in, got nil")
			}
			if got.User != test.want {
				t.Errorf("got user %q, want %q", got.User, test.want)
			}
		})
	}
}

func TestSafeReturnPath(t *testing.T) {
	tests := map[string]string{
		"/jobs/abc123": "/jobs/abc123",
		"/":            "/",
		// Anything that could leave the site collapses to "/".
		"//evil.example.com":       "/",
		"https://evil.example.com": "/",
		"evil.example.com":         "/",
		"":                         "/",
	}

	for in, want := range tests {
		if got := safeReturnPath(in); got != want {
			t.Errorf("safeReturnPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// The Cancel button and log-out control must appear only for a logged-in user,
// and the login link only when login is actually configured.
func TestDashboardTemplateAuthStates(t *testing.T) {
	jobs := []*JobDisplayInfo{{ID: "abc123", Name: "test", Status: "running"}}

	tests := []struct {
		name       string
		data       DashboardData
		wantSubstr []string
		notSubstr  []string
	}{
		{
			name: "logged in",
			data: DashboardData{AllJobs: jobs, User: "alice", LoginEnabled: true},
			wantSubstr: []string{
				`action="/jobs/abc123/cancel"`,
				`action="/logout"`,
				"alice",
			},
			notSubstr: []string{`href="/login"`},
		},
		{
			name:       "logged out",
			data:       DashboardData{AllJobs: jobs, LoginEnabled: true},
			wantSubstr: []string{`href="/login"`},
			notSubstr:  []string{"/cancel", `action="/logout"`},
		},
		{
			name:      "login not configured",
			data:      DashboardData{AllJobs: jobs},
			notSubstr: []string{"/cancel", `href="/login"`, `action="/logout"`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out strings.Builder
			if err := dashboardTemplate.Execute(&out, test.data); err != nil {
				t.Fatal(err)
			}
			for _, want := range test.wantSubstr {
				if !strings.Contains(out.String(), want) {
					t.Errorf("output missing %q", want)
				}
			}
			for _, bad := range test.notSubstr {
				if strings.Contains(out.String(), bad) {
					t.Errorf("output unexpectedly contains %q", bad)
				}
			}
		})
	}
}
