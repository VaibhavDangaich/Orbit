package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// execute runs a Run's payload. There's only one kind of work right now,
// so this is a plain function instead of a pluggable Executor interface +
// registry -- that's real machinery worth building the moment a second
// kind of job shows up (a "call this webhook" job alongside a "log this
// message" job), not before. Introducing the interface now, with exactly
// one implementation behind it, would be solving a problem we don't have
// yet.
//
// Two payload fields exist purely to make orbit's OWN behavior
// demonstrable: {"sleep_ms": 2000} simulates a slow job (useful for
// triggering lease expiry on purpose), and {"fail": true} simulates a
// failing job (useful for watching the retry countdown happen for real).
func execute(ctx context.Context, payload []byte) error {
	var p struct {
		Message string `json:"message"`
		SleepMs int     `json:"sleep_ms"`
		Fail    bool    `json:"fail"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("execute: invalid payload: %w", err)
	}

	if p.SleepMs > 0 {
		select {
		case <-time.After(time.Duration(p.SleepMs) * time.Millisecond):
		case <-ctx.Done():
			// Shutdown was requested mid-execution. We don't have a way
			// to actually cancel "real" work yet (there isn't any) -- for
			// now this just stops waiting and reports the run as failed
			// so it gets retried by whichever worker picks it up next.
			return ctx.Err()
		}
	}

	if p.Fail {
		return fmt.Errorf("execute: simulated failure")
	}

	log.Printf("executed: %s", p.Message)
	return nil
}
