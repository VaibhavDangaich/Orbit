// Package queue is orbit's only place that knows Kafka exists -- the same
// containment principle as internal/store for Postgres and
// internal/election for etcd. It knows nothing about job scheduling: it
// publishes and consumes "a run is ready to be worked on" messages, full
// stop. internal/store.DispatchOutbox never imports this package at all --
// it takes a plain publish func as a parameter instead, so the dependency
// points the other way (cmd/scheduler wires queue.Publisher.Publish into
// store.DispatchOutbox, not the reverse).
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

// RunsTopic is the single topic orbit dispatches run-ready notifications
// on. Partition count is set once, at topic creation (see EnsureTopic) --
// it's the unit of parallelism for consumer groups, so it has to be
// decided before there's meaningful traffic to fan out.
const RunsTopic = "orbit.runs"

// DefaultDevBrokers matches deploy/compose/docker-compose.yml (host port
// 19092, not the default 9092 -- see the compose file for why). Same
// centralize-the-dev-connection-string lesson as store.DefaultDevDSN.
var DefaultDevBrokers = []string{"localhost:19092"}

type runMessage struct {
	RunID job.RunID `json:"run_id"`
}

// EnsureTopic creates topic with the given partition count if it doesn't
// already exist. Idempotent: creating a topic that already exists is
// treated as success, not an error, so this is safe to call on every
// startup rather than requiring a separate manual setup step.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("queue: dial: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("queue: find controller: %w", err)
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("queue: dial controller: %w", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("queue: create topic: %w", err)
	}
	return nil
}

// Publisher publishes "this run is ready to be claimed" messages.
type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string, topic string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
			// HashBalancer, not LeastBytes: Publish keys each message by
			// JobID, so every run belonging to the same job consistently
			// lands on the same partition (and, in the steady state, the
			// same worker) instead of scattering across whichever
			// partition looked least busy at that instant. Postgres
			// fencing is still what makes cross-partition/cross-worker
			// claims safe either way -- this is about locality and
			// minimal remapping on repartitioning, not correctness.
			Balancer: NewHashBalancer(),
			// Bounds how long a single publish can block. This matters
			// beyond the obvious "don't hang forever": DispatchOutbox
			// calls Publish while holding a Postgres transaction open
			// (FOR UPDATE SKIP LOCKED row locks included) -- an
			// unbounded write here would mean a slow/unreachable broker
			// stalls Postgres locks too, not just Kafka.
			WriteTimeout: 5 * time.Second,
			// Found by load testing, not by inspection: kafka-go's
			// Writer batches internally, and its BatchTimeout default
			// (1s if left unset) is how long WriteMessages waits for
			// more messages to arrive before flushing whatever it has.
			// DispatchOutbox calls Publish for ONE row at a time, in a
			// sequential loop, holding a Postgres transaction open for
			// the duration -- so every single dispatched run was paying
			// a full second of pure linger, serialized, with nothing
			// else to batch with. A load test seeding 1000 due jobs
			// materialized only ~200 in 3 minutes, an order of
			// magnitude under the ORBIT_BATCH_SIZE/ORBIT_POLL_INTERVAL
			// ceiling documented in cmd/loadtest -- this was why. 10ms
			// is short enough that a single-message write returns
			// promptly instead of lingering, while still leaving room
			// for the library to coalesce genuinely concurrent writes
			// if this code path is ever changed to publish more than
			// one row per call.
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// Publish starts the PRODUCER span for this run's trace -- the root of
// whatever this message's trace ends up looking like once it's stitched
// together with the consumer span on the other side. See
// kafkaHeaderCarrier for how the two ever find each other.
func (p *Publisher) Publish(ctx context.Context, runID job.RunID, jobID job.ID) error {
	ctx, span := tracer.Start(ctx, "orbit.runs publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(RunsTopic),
			attribute.Int64("orbit.run_id", int64(runID)),
			attribute.Int64("orbit.job_id", int64(jobID)),
		),
	)
	defer span.End()

	body, err := json.Marshal(runMessage{RunID: runID})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal failed")
		return fmt.Errorf("queue: marshal run %d: %w", runID, err)
	}

	msg := kafka.Message{
		Key:   fmt.Appendf(nil, "%d", jobID),
		Value: body,
	}
	// The actual hand-off: serialize the span we just started into this
	// specific message's headers. Nothing on the consuming side knows
	// this span exists until it reads these bytes back out.
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeaderCarrier{headers: &msg.Headers})

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		return err
	}
	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

// Consumer reads run-ready messages as part of a consumer group -- the
// mechanism that replaces manual polling entirely. Kafka assigns each of
// the topic's partitions to exactly one live member of the group, and
// reassigns them automatically when a consumer joins or leaves (a
// "rebalance"). No worker coordinates with any other worker directly;
// Kafka's group coordinator does that for all of them, the same way
// internal/election's etcd session does leader coordination for
// schedulers -- different mechanism, same shape of problem.
type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, groupID, topic string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
		}),
	}
}

// Next blocks until a message is available or ctx is cancelled, and
// returns a context carrying the consumer span (see below), the RunID,
// and a commit function.
//
// Deliberately NOT auto-committing: the offset should only advance once
// the run has actually been claimed (or found already-claimed) in
// Postgres. Committing before that would mean a crash between fetching
// the message and claiming the run silently drops it forever -- Kafka
// never redelivers an already-committed offset. This is also exactly why
// duplicate delivery is an expected case, not a bug: if the process
// crashes AFTER claiming but BEFORE committing, the message gets
// redelivered on restart, and the second claim attempt simply finds the
// run already 'running' (or finished) and does nothing -- the same
// fencing built for CompleteRun/FailRun is what makes Kafka's
// at-least-once guarantee safe to build on here, for free.
//
// The returned context is NOT ctx enriched -- it's a fresh context whose
// span is a CHILD of whatever the producer injected into this message's
// headers, extracted via kafkaHeaderCarrier. The incoming ctx (the
// consume loop's own, unrelated context) has nothing to do with the
// producer's trace; the whole point of the extract step is to reconnect
// to a trace that started in a different process, at a different time,
// using nothing but the string the producer wrote into this message. The
// consumer span stays open until commit runs, so it brackets the full
// claim-execute-report cycle -- callers should use the returned context
// for any further spans (claim, execute, complete) so they nest correctly
// underneath it in the trace viewer.
func (c *Consumer) Next(ctx context.Context) (context.Context, job.RunID, func(context.Context) error, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return ctx, 0, nil, fmt.Errorf("queue: fetch: %w", err)
	}

	msgCtx := otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &msg.Headers})
	msgCtx, span := tracer.Start(msgCtx, "orbit.runs consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(RunsTopic),
		),
	)

	var m runMessage
	if err := json.Unmarshal(msg.Value, &m); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "unmarshal failed")
		span.End()
		return ctx, 0, nil, fmt.Errorf("queue: unmarshal: %w", err)
	}
	span.SetAttributes(attribute.Int64("orbit.run_id", int64(m.RunID)))

	commit := func(ctx context.Context) error {
		defer span.End()
		return c.reader.CommitMessages(ctx, msg)
	}
	return msgCtx, m.RunID, commit, nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
