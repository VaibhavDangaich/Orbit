package queue

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestHashBalancerIsDeterministic(t *testing.T) {
	b := NewHashBalancer()
	partitions := []int{0, 1, 2}

	want := b.Balance(kafka.Message{Key: []byte("job-42")}, partitions...)
	for i := 0; i < 50; i++ {
		got := b.Balance(kafka.Message{Key: []byte("job-42")}, partitions...)
		if got != want {
			t.Fatalf("Balance(%q) = %d, want %d (must be stable across calls)", "job-42", got, want)
		}
	}
}

func TestHashBalancerUsesAllPartitions(t *testing.T) {
	b := NewHashBalancer()
	partitions := []int{0, 1, 2}

	seen := make(map[int]bool)
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		p := b.Balance(kafka.Message{Key: key}, partitions...)
		seen[p] = true
	}

	if len(seen) != len(partitions) {
		t.Fatalf("used %d/%d partitions across 1000 keys: %v", len(seen), len(partitions), seen)
	}
}

func TestHashBalancerAlwaysReturnsALivePartition(t *testing.T) {
	b := NewHashBalancer()
	partitions := []int{0, 1, 2}

	for i := 0; i < 200; i++ {
		key := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		p := b.Balance(kafka.Message{Key: key}, partitions...)
		valid := false
		for _, want := range partitions {
			if p == want {
				valid = true
				break
			}
		}
		if !valid {
			t.Fatalf("Balance returned partition %d, not in %v", p, partitions)
		}
	}
}
