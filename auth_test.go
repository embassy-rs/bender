package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
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
