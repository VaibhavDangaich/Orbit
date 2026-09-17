// Package hashring implements consistent hashing: a way to map keys onto
// a changing set of nodes (partitions, shards, cache servers -- whatever
// you're distributing load across) such that adding or removing a node
// only remaps a SMALL FRACTION of keys, roughly 1/N of them, instead of
// nearly all of them.
//
// The naive approach, hash(key) % nodeCount, doesn't have that property:
// changing nodeCount from 5 to 6 changes the modulus, which changes
// almost every key's assignment -- for a cache, that's a near-total cache
// wipe every time you scale; for a sharded store, it's a near-total
// reshuffle of data. Consistent hashing exists specifically to avoid that.
package hashring

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Ring is a consistent hash ring. The zero value is not useful -- use New.
type Ring struct {
	vnodes int
	hashes []uint32          // every virtual node's position, kept sorted
	nodeOf map[uint32]string // position -> which real node owns it
}

// New creates a Ring. vnodes is how many positions each real node occupies
// on the ring -- not how many real nodes you'll add.
//
// Why virtual nodes at all: with exactly one position per real node, the
// ring's balance depends entirely on hash luck -- a node could easily end
// up owning far more (or less) than its fair 1/N share of the keyspace,
// especially with few nodes. Giving each real node many positions,
// scattered across the ring, averages that luck out, so each node's total
// share converges toward 1/N even with only a handful of real nodes. 100-
// 150 is a common real-world default; this trades a bit more memory and a
// slightly larger sorted slice to search for meaningfully better balance.
func New(vnodes int) *Ring {
	return &Ring{
		vnodes: vnodes,
		nodeOf: make(map[uint32]string),
	}
}

// hashKey uses crc32, not a cryptographic hash (sha256 and friends) --
// deliberately. The property this package needs is good statistical
// distribution across uint32 space, not resistance to an adversary
// crafting keys to collide on purpose. crc32 is fast and stdlib; reaching
// for a cryptographic hash here would cost real CPU for a security
// property nothing in this package's use case requires.
func (r *Ring) hashKey(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// Add places node onto the ring at r.vnodes positions. Adding a node that's
// already present adds it again (a caller-level bug, not guarded against
// here -- see the package tests for what a clean Add/Remove sequence
// looks like).
func (r *Ring) Add(node string) {
	for i := 0; i < r.vnodes; i++ {
		h := r.hashKey(node + "#" + strconv.Itoa(i))
		r.nodeOf[h] = node
		r.hashes = append(r.hashes, h)
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// Remove takes node, and every virtual position it owns, off the ring.
func (r *Ring) Remove(node string) {
	var kept []uint32
	for _, h := range r.hashes {
		if r.nodeOf[h] == node {
			delete(r.nodeOf, h)
			continue
		}
		kept = append(kept, h)
	}
	r.hashes = kept
}

// Get returns which node owns key: walk clockwise from hash(key) around
// the ring and return the first virtual node position you land on,
// wrapping back to position zero if hash(key) is past every node's
// position. ok is false only when the ring has no nodes at all.
//
// sort.Search is Go's binary search over anything sorted: given a
// monotonic predicate (false, false, ..., false, true, true, ...), it
// returns the index of the first true. Here the predicate is "is this
// position >= hash(key)" -- exactly "find the next virtual node clockwise
// from key's position," in O(log n) instead of scanning the whole ring.
func (r *Ring) Get(key string) (node string, ok bool) {
	if len(r.hashes) == 0 {
		return "", false
	}
	h := r.hashKey(key)
	i := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if i == len(r.hashes) {
		i = 0
	}
	return r.nodeOf[r.hashes[i]], true
}
