// Package env holds the ORBIT_* config-parsing helpers every cmd/*/main.go
// needs, so they're defined once instead of copy-pasted five times.
package env

import (
	"log"
	"os"
	"strconv"
	"time"
)

func Or(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func IntOr(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
}

func DurationOr(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return d
}
