package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v88/github"
)

// A final status must win even if an earlier one is still being retried when
// it's set.
func TestStatusSenderNewestWins(t *testing.T) {
	statusRetryInitialDelay = 10 * time.Millisecond

	var mu sync.Mutex
	var got []string
	failPending := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var st github.RepoStatus
		json.NewDecoder(r.Body).Decode(&st)
		mu.Lock()
		defer mu.Unlock()
		if st.GetState() == "pending" && failPending {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		got = append(got, st.GetState())
		w.Write([]byte("{}"))
	}))
	defer srv.Close()

	base := srv.URL + "/"
	gh, err := github.NewClient(github.WithURLs(&base, &base))
	if err != nil {
		t.Fatal(err)
	}

	var ss statusSender
	key := statusKey{"o", "r", "sha", "ci/test"}
	ss.set(key, &statusUpdate{gh: gh, jobID: "j", status: github.RepoStatus{State: github.Ptr("pending")}})
	time.Sleep(50 * time.Millisecond) // let pending fail a few times
	ss.set(key, &statusUpdate{gh: gh, jobID: "j", status: github.RepoStatus{State: github.Ptr("success")}})
	mu.Lock()
	failPending = false // a stray pending retry would now succeed
	mu.Unlock()

	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "success" {
		t.Fatalf("got %v, want [success]", got)
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.slots) != 0 {
		t.Fatalf("worker didn't exit, %d slots left", len(ss.slots))
	}
}
