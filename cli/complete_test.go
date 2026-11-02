package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A complete the server no longer knows (404) is not yet a failure: an earlier ask may have placed the file after
// this client stopped waiting for it, and on 2026-11-02 that is exactly how two 362 GB files that had landed were
// reported as failed. The bridge's plan says whether the file is there with this checksum.
func fakeBridge(t *testing.T, has bool) (*Client, *int) {
	t.Helper()
	plans := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/complete"):
			w.WriteHeader(404)
			w.Write([]byte(`{"error":"no such upload"}`))
		case r.URL.Path == "/api/plan":
			plans++
			if has {
				w.Write([]byte(`{"have":["exvivo/arr.npy"],"missing":[],"resumable":[]}`))
			} else {
				w.Write([]byte(`{"have":[],"missing":[{"path":"exvivo/arr.npy","size":10}],"resumable":[]}`))
			}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)
	return &Client{cfg: Config{URL: srv.URL, Token: "t"}, http: srv.Client(), patient: srv.Client()}, &plans
}

func TestCompleteForgottenButLandedIsDone(t *testing.T) {
	c, plans := fakeBridge(t, true)
	f := &localFile{rel: "arr.npy", size: 10, sha: strings.Repeat("a", 64)}
	if err := completeLarge(context.Background(), c, "u1", f, "exvivo/arr.npy"); err != nil {
		t.Fatalf("the file is on the bridge with this checksum, yet: %v", err)
	}
	if *plans != 1 {
		t.Fatalf("asked the plan %d times, want 1", *plans)
	}
}

func TestCompleteForgottenAndMissingStaysFailed(t *testing.T) {
	c, _ := fakeBridge(t, false)
	f := &localFile{rel: "arr.npy", size: 10, sha: strings.Repeat("a", 64)}
	err := completeLarge(context.Background(), c, "u1", f, "exvivo/arr.npy")
	var ae *apiError
	if !errors.As(err, &ae) || ae.Status != 404 {
		t.Fatalf("want the 404 back when the file is not there, got %v", err)
	}
}

func TestCompleteWaitGrowsWithTheFile(t *testing.T) {
	if got := completeWait(0); got != answerWait {
		t.Fatalf("an empty file waits %v, want %v", got, answerWait)
	}
	// The 362 GB files of 2026-11-02 took the server 11 to 15 minutes to check.
	if got := completeWait(362_387_865_728); got < answerWait+45*time.Minute {
		t.Fatalf("a 362 GB file waits only %v", got)
	}
}
