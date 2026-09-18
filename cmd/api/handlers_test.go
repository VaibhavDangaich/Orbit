package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/store"
)

// newTestStore mirrors internal/store's own test helper -- same
// skip-if-unreachable, same TRUNCATE-before-each-test isolation. It can't
// just call that one directly: it's unexported in a different package.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()

	dsn := os.Getenv("ORBIT_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = store.DefaultDevDSN
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := store.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not reachable at %s (start it: docker compose -f deploy/compose/docker-compose.yml up -d): %v", dsn, err)
	}
	t.Cleanup(s.Close)

	unlock, err := s.TruncateForTest(ctx)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	t.Cleanup(unlock)

	return s
}

func doRequest(t *testing.T, mux http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var r *http.Request
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		r = httptest.NewRequest(method, path, bytes.NewReader(b))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, r)
	return rec
}

func TestCreateJob(t *testing.T) {
	s := newTestStore(t)
	mux := newMux(s)

	rec := doRequest(t, mux, "POST", "/jobs", map[string]any{
		"tenant_id": "acme",
		"name":      "nightly-report",
		"schedule":  "@every 1h",
		"payload":   map[string]any{"message": "hello"},
	})

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var got jobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v (body: %s)", err, rec.Body.String())
	}

	if got.ID == 0 {
		t.Errorf("ID = 0, want a real generated ID")
	}
	if got.TenantID != "acme" {
		t.Errorf("TenantID = %q, want %q", got.TenantID, "acme")
	}
	if got.Schedule != "@every 1h" {
		t.Errorf("Schedule = %q, want %q", got.Schedule, "@every 1h")
	}
	// The whole point of jobResponse's json.RawMessage field: this must
	// come back as a real JSON object, not the base64 string a plain
	// []byte field would produce.
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"payload":{"message":"hello"}`)) {
		t.Errorf("payload wasn't embedded as real JSON, got body: %s", rec.Body.String())
	}
	if !got.Enabled {
		t.Errorf("Enabled = false, want true (default)")
	}
	if got.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3 (default)", got.MaxAttempts)
	}
	// NextRunAt should be ~1h out (the schedule interval), not "now" --
	// NextRun's own contract is "the job's creation time, if it has never
	// run". A wide tolerance (a few seconds) absorbs real clock/DB
	// round-trip time without asserting on exact equality.
	wantNextRun := got.CreatedAt.Add(time.Hour)
	if diff := got.NextRunAt.Sub(wantNextRun); diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("NextRunAt = %v, want ~%v (CreatedAt + 1h)", got.NextRunAt, wantNextRun)
	}
}

func TestCreateJob_Validation(t *testing.T) {
	s := newTestStore(t)
	mux := newMux(s)

	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing tenant_id", map[string]any{"name": "x", "schedule": "@every 1h"}},
		{"missing name", map[string]any{"tenant_id": "acme", "schedule": "@every 1h"}},
		{"invalid schedule", map[string]any{"tenant_id": "acme", "name": "x", "schedule": "not a schedule"}},
		{"negative max_attempts", map[string]any{"tenant_id": "acme", "name": "x", "schedule": "@every 1h", "max_attempts": -1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, mux, "POST", "/jobs", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

func TestCreateJob_MalformedJSON(t *testing.T) {
	s := newTestStore(t)
	mux := newMux(s)

	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(`{not json`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, r)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGetJob(t *testing.T) {
	s := newTestStore(t)
	mux := newMux(s)

	created := doRequest(t, mux, "POST", "/jobs", map[string]any{
		"tenant_id": "acme",
		"name":      "x",
		"schedule":  "@every 5m",
	})
	var want jobResponse
	if err := json.Unmarshal(created.Body.Bytes(), &want); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	rec := doRequest(t, mux, "GET", "/jobs/"+strconv.FormatInt(int64(want.ID), 10), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var got jobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("ID = %d, want %d", got.ID, want.ID)
	}
	if got.Name != "x" {
		t.Errorf("Name = %q, want %q", got.Name, "x")
	}
}

func TestGetJob_NotFound(t *testing.T) {
	s := newTestStore(t)
	mux := newMux(s)

	rec := doRequest(t, mux, "GET", "/jobs/999999", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func TestGetJob_InvalidID(t *testing.T) {
	s := newTestStore(t)
	mux := newMux(s)

	rec := doRequest(t, mux, "GET", "/jobs/not-a-number", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestListJobs(t *testing.T) {
	s := newTestStore(t)
	mux := newMux(s)

	for i := 0; i < 3; i++ {
		rec := doRequest(t, mux, "POST", "/jobs", map[string]any{
			"tenant_id": "acme",
			"name":      "job-" + strconv.Itoa(i),
			"schedule":  "@every 1h",
		})
		if rec.Code != http.StatusCreated {
			t.Fatalf("seed job %d: status = %d; body: %s", i, rec.Code, rec.Body.String())
		}
	}

	rec := doRequest(t, mux, "GET", "/jobs", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var got listJobsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(got.Jobs) != 3 {
		t.Fatalf("len(Jobs) = %d, want 3", len(got.Jobs))
	}
	// Checking a field is non-zero, not just that decoding didn't error:
	// json.Unmarshal silently leaves a field at its zero value when the
	// response uses a different key than the struct tag expects (e.g. a
	// body with "TenantID" against a field tagged "tenant_id") -- it does
	// NOT error. len(Jobs) == 3 alone would pass even if every field in
	// the response came back empty; this is the assertion that actually
	// catches a JSON-tag mismatch between this test's struct and what the
	// handler serializes.
	for _, j := range got.Jobs {
		if j.TenantID == "" || j.Name == "" || j.Schedule == "" {
			t.Fatalf("job summary has an empty field, response keys likely don't match snake_case tags: %+v", j)
		}
	}
}

func TestListJobs_InvalidLimit(t *testing.T) {
	s := newTestStore(t)
	mux := newMux(s)

	rec := doRequest(t, mux, "GET", "/jobs?limit=abc", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}
