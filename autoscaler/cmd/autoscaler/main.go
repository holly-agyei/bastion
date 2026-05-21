package main

// autoscaler-monitor consumes the gateway's rejection telemetry topic and
// fires a critical alert whenever the recent rejection rate exceeds a
// configurable threshold over a configurable window. In production this
// would call out to an AWS Auto-Scaling group or Kubernetes HPA webhook —
// here it logs a structured alert event so we can verify end-to-end latency
// from rejection to alert in the load test.

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Rejection struct {
	Ts       int64  `json:"ts_ms"`
	ClientID string `json:"client_id"`
	Path     string `json:"path"`
}

// slidingCounter is a ring-buffer of per-second counts covering windowSec
// seconds. It's lock-free under the hot path (only the consumer goroutine
// mutates), and lets us compute the rolling sum in O(windowSec) time.
type slidingCounter struct {
	mu        sync.Mutex
	buckets   []int64
	timestamp []int64
	windowSec int64
}

func newSlidingCounter(windowSec int64) *slidingCounter {
	return &slidingCounter{
		buckets:   make([]int64, windowSec),
		timestamp: make([]int64, windowSec),
		windowSec: windowSec,
	}
}

func (s *slidingCounter) add(nowSec int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := nowSec % s.windowSec
	if s.timestamp[idx] != nowSec {
		s.timestamp[idx] = nowSec
		s.buckets[idx] = 0
	}
	s.buckets[idx]++
}

func (s *slidingCounter) total(nowSec int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sum int64
	for i := range s.buckets {
		if nowSec-s.timestamp[i] < s.windowSec {
			sum += s.buckets[i]
		}
	}
	return sum
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	brokers := splitCSV(getEnv("KAFKA_BROKERS", "kafka:9092"))
	topic := getEnv("KAFKA_TOPIC", "telemetry.rejections")
	group := getEnv("KAFKA_GROUP", "autoscaler-monitor")
	windowSec := int64(getEnvInt("WINDOW_SEC", 5))
	threshold := int64(getEnvInt("THRESHOLD", 1000))
	cooldownSec := int64(getEnvInt("COOLDOWN_SEC", 30))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        group,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: 1 * time.Second,
		// Start at the latest offset so a fresh boot doesn't replay
		// stale rejection history and trigger a false alarm.
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	counter := newSlidingCounter(windowSec)
	var lastAlertSec int64

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Info("autoscaler-monitor starting",
		"brokers", brokers, "topic", topic, "window_sec", windowSec, "threshold", threshold)

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("shutdown")
				return
			}
			log.Error("kafka fetch", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var ev Rejection
		if err := json.Unmarshal(m.Value, &ev); err == nil {
			now := time.Now().Unix()
			counter.add(now)
			total := counter.total(now)
			if total >= threshold && now-lastAlertSec >= cooldownSec {
				log.Warn("ALERT: rejection spike — triggering scale-out",
					"window_sec", windowSec,
					"total_rejections", total,
					"threshold", threshold,
					"first_seen_client", ev.ClientID,
					"path", ev.Path,
					"action", "asg:increase_desired_capacity",
				)
				lastAlertSec = now
			}
		}
		if err := reader.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			log.Error("kafka commit", "err", err)
		}
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func getEnvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
