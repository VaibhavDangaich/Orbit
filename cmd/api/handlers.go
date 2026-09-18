package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
	"github.com/vaibhavdangaich/orbit/internal/store"
)

// newMux wires every route to a Store, the same shape cmd/scheduler and
// cmd/worker use for their own dependencies -- no framework, no router
// library. Go 1.22's ServeMux understands "METHOD /path" patterns and
// {name} path variables natively, which is all three of these routes
// need; reaching for something heavier would be solving a problem this
// project doesn't have.
func newMux(s *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", createJobHandler(s))
	mux.HandleFunc("GET /jobs/{id}", getJobHandler(s))
	mux.HandleFunc("GET /jobs", listJobsHandler(s))
	return mux
}

// jobResponse is the API's own shape for a Job, not job.Job reused
// directly. Two reasons: job.Job.Payload is a plain []byte, which
// encoding/json base64-encodes by default (the reflect-based encoder's
// standard treatment of a byte slice) -- exactly wrong for a field that's
// already JSON and should appear as one in the response, not as an opaque
// blob. json.RawMessage is also a []byte under the hood, but its
// MarshalJSON is a no-op that emits the bytes verbatim. Second: job.Job
// carries internal/job.Schedule, an unexported-field type with no JSON
// encoding of its own -- the API's contract is the schedule STRING
// ("@every 30s"), the same spec a caller sends in, not Go's in-memory
// representation of it.
type jobResponse struct {
	ID          job.ID          `json:"id"`
	TenantID    string          `json:"tenant_id"`
	Name        string          `json:"name"`
	Schedule    string          `json:"schedule"`
	Payload     json.RawMessage `json:"payload"`
	Enabled     bool            `json:"enabled"`
	MaxAttempts int             `json:"max_attempts"`
	NextRunAt   time.Time       `json:"next_run_at"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func toJobResponse(j job.Job) jobResponse {
	return jobResponse{
		ID:          j.ID,
		TenantID:    j.TenantID,
		Name:        j.Name,
		Schedule:    j.Schedule.String(),
		Payload:     json.RawMessage(j.Payload),
		Enabled:     j.Enabled,
		MaxAttempts: j.MaxAttempts,
		NextRunAt:   j.NextRunAt,
		CreatedAt:   j.CreatedAt,
		UpdatedAt:   j.UpdatedAt,
	}
}

// createJobRequest mirrors jobResponse's Payload reasoning in the other
// direction: json.RawMessage lets the request body embed the payload as
// real JSON ({"payload": {"message": "hi"}}) instead of forcing callers
// to pre-encode it as a base64 string to satisfy a []byte field.
//
// Enabled is a *bool, not bool, specifically so the handler can tell "the
// caller wrote false" apart from "the caller didn't send this field at
// all" -- a plain bool can't distinguish those, and defaulting a new job
// to disabled because a client omitted an optional field would be a
// surprising, easy-to-hit foot-gun.
type createJobRequest struct {
	TenantID    string          `json:"tenant_id"`
	Name        string          `json:"name"`
	Schedule    string          `json:"schedule"`
	Payload     json.RawMessage `json:"payload"`
	Enabled     *bool           `json:"enabled"`
	MaxAttempts int             `json:"max_attempts"`
}

func createJobHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}

		if req.TenantID == "" {
			writeError(w, http.StatusBadRequest, "tenant_id is required")
			return
		}
		if req.Name == "" {
			writeError(w, http.StatusBadRequest, "name is required")
			return
		}
		sched, err := job.ParseSchedule(req.Schedule)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid schedule: "+err.Error())
			return
		}

		enabled := true
		if req.Enabled != nil {
			enabled = *req.Enabled
		}
		maxAttempts := req.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 3 // matches migrations/0001_init.up.sql's column default
		}
		if maxAttempts < 1 {
			writeError(w, http.StatusBadRequest, "max_attempts must be at least 1")
			return
		}
		payload := []byte(req.Payload)
		if len(payload) == 0 {
			payload = []byte("{}") // matches the column's own default
		}

		now := time.Now().UTC()
		created, err := s.CreateJob(r.Context(), job.Job{
			TenantID:    req.TenantID,
			Name:        req.Name,
			Schedule:    sched,
			Payload:     payload,
			Enabled:     enabled,
			MaxAttempts: maxAttempts,
			// A job's first run is one interval after it's created, not
			// immediately -- NextRun's own doc comment calls this out
			// explicitly ("the job's creation time, if it has never
			// run"). Using this handler's own clock instead of waiting
			// for Postgres's CreatedAt to come back avoids a chicken-
			// and-egg problem: NextRunAt is a NOT NULL column that has
			// to be part of the same INSERT that generates CreatedAt.
			NextRunAt: sched.NextRun(now),
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "create job: "+err.Error())
			return
		}

		writeJSON(w, http.StatusCreated, toJobResponse(created))
	}
}

func getJobHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := parseJobID(r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		found, err := s.GetJob(r.Context(), id)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "get job: "+err.Error())
			return
		}

		writeJSON(w, http.StatusOK, toJobResponse(found))
	}
}

// jobSummaryResponse is the list endpoint's own shape, for the same
// reason jobResponse exists instead of marshaling job.Job directly:
// store.JobSummary carries no json tags at all (cmd/tui, its only other
// caller, never serializes it), so marshaling it as-is would emit its Go
// field names verbatim -- "TenantID", not "tenant_id" -- silently
// inconsistent with every other endpoint's snake_case, and something no
// compiler or vet pass catches, only reading the actual response body
// does.
type jobSummaryResponse struct {
	ID        job.ID    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	Name      string    `json:"name"`
	Schedule  string    `json:"schedule"`
	Enabled   bool      `json:"enabled"`
	NextRunAt time.Time `json:"next_run_at"`
}

func toJobSummaryResponse(j store.JobSummary) jobSummaryResponse {
	return jobSummaryResponse{
		ID:        j.ID,
		TenantID:  j.TenantID,
		Name:      j.Name,
		Schedule:  j.Schedule,
		Enabled:   j.Enabled,
		NextRunAt: j.NextRunAt,
	}
}

// listJobsResponse wraps the slice rather than returning a bare JSON
// array. A bare top-level array can never grow an extra field later
// (pagination metadata, a total count) without becoming a breaking
// change for every existing caller -- wrapping it in an object costs
// nothing today and leaves that door open.
type listJobsResponse struct {
	Jobs []jobSummaryResponse `json:"jobs"`
}

func listJobsHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const defaultLimit = 50
		const maxLimit = 500

		limit := defaultLimit
		if v := r.URL.Query().Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				writeError(w, http.StatusBadRequest, "limit must be a positive integer")
				return
			}
			limit = min(n, maxLimit)
		}

		jobs, err := s.ListJobs(r.Context(), limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "list jobs: "+err.Error())
			return
		}
		// make(), not nil-then-append: make with a length always returns a
		// non-nil slice, even for len(jobs) == 0 -- so an empty result
		// still marshals as "[]", not the JSON null a genuinely nil slice
		// would produce.
		resp := make([]jobSummaryResponse, len(jobs))
		for i, j := range jobs {
			resp[i] = toJobSummaryResponse(j)
		}

		writeJSON(w, http.StatusOK, listJobsResponse{Jobs: resp})
	}
}

func parseJobID(raw string) (job.ID, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, errors.New("invalid job id")
	}
	return job.ID(n), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
