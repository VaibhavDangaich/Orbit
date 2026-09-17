package queue

import (
	"strconv"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/vaibhavdangaich/orbit/internal/hashring"
)

// HashBalancer picks a Kafka partition for each message by consistently
// hashing its key (see internal/hashring) instead of kafka-go's default
// strategies (round-robin, or LeastBytes -- whichever partition currently
// has the smallest backlog). The ring's "nodes" here are partition
// numbers, not servers.
//
// What this buys over LeastBytes: the SAME key (we key by JobID -- see
// Publisher.Publish) always lands on the SAME partition, for as long as
// the partition count doesn't change. Since one partition is owned by one
// consumer-group member at a time, that means a given job's runs
// consistently route to the same worker over time, instead of scattering
// across whichever worker happened to be least busy at that exact moment.
// If the topic is ever repartitioned (3 partitions -> 6, say), the ring's
// minimal-remapping property means most jobs keep their existing
// partition -- only the fraction that maps into the new partitions moves.
type HashBalancer struct {
	mu    sync.Mutex
	ring  *hashring.Ring
	known map[int]bool // which partition numbers are currently in the ring
}

func NewHashBalancer() *HashBalancer {
	return &HashBalancer{
		ring:  hashring.New(150),
		known: make(map[int]bool),
	}
}

// Balance implements kafka.Balancer. kafka-go calls this for every
// message, passing the CURRENT live partition list each time -- syncing
// the ring against that list on every call (cheap: it's a no-op unless
// the set actually changed) is what lets this balancer react correctly
// if the topic is ever repartitioned while the writer is running.
func (b *HashBalancer) Balance(msg kafka.Message, partitions ...int) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.syncPartitions(partitions)

	node, ok := b.ring.Get(string(msg.Key))
	if !ok {
		// Only reachable if partitions was empty, which kafka-go itself
		// wouldn't call this with -- fall back rather than panic.
		return partitions[0]
	}
	p, _ := strconv.Atoi(node) // node is always one of our own strconv.Itoa(partition) additions below
	return p
}

func (b *HashBalancer) syncPartitions(partitions []int) {
	current := make(map[int]bool, len(partitions))
	for _, p := range partitions {
		current[p] = true
		if !b.known[p] {
			b.ring.Add(strconv.Itoa(p))
		}
	}
	for p := range b.known {
		if !current[p] {
			b.ring.Remove(strconv.Itoa(p))
		}
	}
	b.known = current
}
