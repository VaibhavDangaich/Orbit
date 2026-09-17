package queue

import (
	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/otel"
)

// tracer is this package's handle into the global TracerProvider that
// internal/tracing.Init installs at startup. Calling otel.Tracer directly
// here -- not through internal/tracing -- is the same deliberate exception
// documented on internal/metrics: the OpenTelemetry API is designed to be
// used from anywhere once the provider exists, the same way business logic
// calls a Prometheus counter directly rather than through a wrapper.
var tracer = otel.Tracer("orbit/queue")

// kafkaHeaderCarrier bridges OpenTelemetry's propagation.TextMapCarrier
// interface (Get/Set/Keys over string key-value pairs) to a Kafka
// message's Headers field. This is the entire mechanism that lets a trace
// survive crossing an async message queue: HTTP has a standard place to
// put a "traceparent" header, and every HTTP client/server library has
// middleware that does the inject/extract for you automatically. Kafka
// has neither -- headers are just an arbitrary []Header slice attached to
// each message, so something has to explicitly serialize the current
// span's identity into one of those key-value pairs on publish, and
// deserialize it back out on consume. This type is that "something," and
// it's the specific piece of code that makes a trace span two processes
// connected only by a message that sat in a topic for an arbitrary amount
// of time in between.
type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}
