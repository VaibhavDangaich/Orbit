package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The probe endpoints are the whole reason Serve grew a second argument,
// and the case that matters is the one that's awkward to reproduce by
// hand: a dependency that's down. Driving handler() directly means that
// case is a one-line stub instead of a stopped Postgres.
func TestProbes(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		ready    func(context.Context) error
		wantCode int
		wantBody string
	}{
		{
			name:     "healthz ignores a failing dependency",
			path:     "/healthz",
			ready:    func(context.Context) error { return errors.New("postgres is down") },
			wantCode: http.StatusOK,
			// The point of the whole split: liveness must NOT fail when a
			// dependency does, because a failed liveness probe kills the
			// container and a Postgres blip would restart the fleet.
			wantBody: "ok",
		},
		{
			name:     "readyz reports ready when the check passes",
			path:     "/readyz",
			ready:    func(context.Context) error { return nil },
			wantCode: http.StatusOK,
			wantBody: "ok",
		},
		{
			name:     "readyz reports 503 and names the failure",
			path:     "/readyz",
			ready:    func(context.Context) error { return errors.New("postgres is down") },
			wantCode: http.StatusServiceUnavailable,
			// Named, not just a bare status: this body is what shows up in
			// `kubectl describe pod`.
			wantBody: "postgres is down",
		},
		{
			name:     "readyz with no check configured is ready",
			path:     "/readyz",
			ready:    nil,
			wantCode: http.StatusOK,
			wantBody: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler(tt.ready).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))

			if rec.Code != tt.wantCode {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			if got := rec.Body.String(); !strings.Contains(got, tt.wantBody) {
				t.Errorf("body = %q, want it to contain %q", got, tt.wantBody)
			}
		})
	}
}

// A readiness check that hangs must not hang the handler with it -- the
// 2s bound inside /readyz is what stops a blocked database call from
// pinning the goroutine indefinitely.
func TestReadyzBoundsASlowCheck(t *testing.T) {
	rec := httptest.NewRecorder()
	handler(func(ctx context.Context) error {
		<-ctx.Done() // never returns on its own
		return ctx.Err()
	}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
