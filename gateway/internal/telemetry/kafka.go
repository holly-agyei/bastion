package telemetry

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Rejection is the event we publish to Kafka when a request is throttled.
// We keep the payload small and stable so the autoscaler-monitor (and any
// downstream consumer) can parse it without a schema registry.
type Rejection struct {
	Ts         int64  `json:"ts_ms"`
	ClientID   string `json:"client_id"`
	Path       string `json:"path"`
	RetryMs    int64  `json:"retry_ms"`
	CountInWin int64  `json:"count_in_window"`
	NodeID     string `json:"node_id"`
}

// Producer is an async, buffered Kafka producer. We do not block the request
// path on broker acks — rejections are best-effort telemetry. If the buffer
// fills (broker outage, network partition), we drop the event and increment a
// counter. The alternative — blocking the HTTP response on Kafka — would let
// a broker incident take down the gateway's rejection path, which is exactly
// the moment we most need it to stay up.
type Producer struct {
	w       *kafka.Writer
	ch      chan Rejection
	log     *slog.Logger
	dropped uint64
}

func NewProducer(brokers []string, topic string, log *slog.Logger) *Producer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		BatchSize:              500,
		BatchBytes:             1 << 20,
		BatchTimeout:           5 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		Async:                  true,
		Compression:            kafka.Snappy,
	}
	p := &Producer{
		w:   w,
		ch:  make(chan Rejection, 16384),
		log: log,
	}
	go p.run()
	return p
}

// Publish enqueues a rejection event without blocking. If the buffer is full
// the event is dropped; the dropped counter is logged periodically.
func (p *Producer) Publish(ev Rejection) {
	select {
	case p.ch <- ev:
	default:
		// non-blocking drop
		p.dropped++
	}
}

func (p *Producer) run() {
	ctx := context.Background()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case ev, ok := <-p.ch:
			if !ok {
				return
			}
			b, err := json.Marshal(ev)
			if err != nil {
				p.log.Error("telemetry marshal", "err", err)
				continue
			}
			msg := kafka.Message{
				Key:   []byte(ev.ClientID),
				Value: b,
				Time:  time.UnixMilli(ev.Ts),
			}
			if err := p.w.WriteMessages(ctx, msg); err != nil {
				p.log.Error("kafka write", "err", err)
			}
		case <-ticker.C:
			if p.dropped > 0 {
				p.log.Warn("telemetry drops since last report", "dropped", p.dropped)
				p.dropped = 0
			}
		}
	}
}

func (p *Producer) Close() error {
	close(p.ch)
	// Drain remaining items: kafka-go's Writer.Close flushes pending
	// async writes before returning.
	return p.w.Close()
}
