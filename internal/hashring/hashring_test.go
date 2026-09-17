package hashring

import (
	"fmt"
	"testing"
)

func TestGetIsDeterministic(t *testing.T) {
	r := New(100)
	r.Add("a")
	r.Add("b")
	r.Add("c")

	want, ok := r.Get("tenant-42")
	if !ok {
		t.Fatal("Get: ok = false, want true")
	}
	for i := 0; i < 100; i++ {
		got, ok := r.Get("tenant-42")
		if !ok || got != want {
			t.Fatalf("Get(%q) = %q, want %q (must be stable across repeated calls)", "tenant-42", got, want)
		}
	}
}

func TestGetOnEmptyRing(t *testing.T) {
	r := New(100)
	if _, ok := r.Get("anything"); ok {
		t.Fatal("Get on empty ring: ok = true, want false")
	}
}

func TestGetDistributesAcrossAllNodes(t *testing.T) {
	r := New(150)
	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	for _, n := range nodes {
		r.Add(n)
	}

	counts := make(map[string]int)
	const numKeys = 10_000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, ok := r.Get(key)
		if !ok {
			t.Fatalf("Get(%q): ok = false", key)
		}
		counts[node]++
	}

	if len(counts) != len(nodes) {
		t.Fatalf("only %d/%d nodes received any keys: %v", len(counts), len(nodes), counts)
	}

	// With 150 virtual nodes per real node, balance should be reasonably
	// close to the 1/5 = 2000-keys-per-node ideal. This isn't a tight
	// bound -- hashing is still probabilistic -- just a sanity check that
	// no node is wildly starved or overloaded.
	want := numKeys / len(nodes)
	for node, got := range counts {
		if got < want/2 || got > want*2 {
			t.Errorf("node %q got %d keys, want roughly %d (within 2x)", node, got, want)
		}
	}
}

// TestMinimalRemappingOnNodeChange is the actual point of this package:
// prove, with a number, that adding a node remaps roughly 1/(N+1) of
// keys -- not nearly all of them, which is what plain hash(key) % N
// would do. This is the property that makes consistent hashing worth
// using at all instead of a modulus.
func TestMinimalRemappingOnNodeChange(t *testing.T) {
	r := New(150)
	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	for _, n := range nodes {
		r.Add(n)
	}

	const numKeys = 10_000
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.Get(key)
		before[key] = node
	}

	r.Add("node-6") // ring goes from 5 nodes to 6

	remapped := 0
	for key, oldNode := range before {
		newNode, _ := r.Get(key)
		if newNode != oldNode {
			remapped++
		}
	}

	// Theoretical ideal: exactly 1/6 of keys move to the new node, 5/6
	// stay put. Allow a generous band around that (10%-30% of numKeys)
	// rather than asserting the exact ratio -- hashing is probabilistic,
	// and the point being proven is "small fraction," not "exact fraction."
	fraction := float64(remapped) / float64(numKeys)
	t.Logf("remapped %d/%d keys (%.1f%%) after adding a 6th node to 5", remapped, numKeys, fraction*100)
	if fraction > 0.30 {
		t.Errorf("remapped %.1f%% of keys, want well under the ~83%% (5/6) a naive hash%%N would remap", fraction*100)
	}
}

func TestRemoveNodeReassignsItsKeys(t *testing.T) {
	r := New(150)
	nodes := []string{"node-1", "node-2", "node-3"}
	for _, n := range nodes {
		r.Add(n)
	}

	// Find a key that currently belongs to node-1 specifically.
	var targetKey string
	for i := 0; ; i++ {
		key := fmt.Sprintf("key-%d", i)
		if node, _ := r.Get(key); node == "node-1" {
			targetKey = key
			break
		}
	}

	r.Remove("node-1")

	newNode, ok := r.Get(targetKey)
	if !ok {
		t.Fatal("Get after Remove: ok = false, want true (ring still has 2 nodes)")
	}
	if newNode == "node-1" {
		t.Fatalf("Get(%q) after removing node-1 = %q, want reassignment to a surviving node", targetKey, newNode)
	}
}
