package metrics

import (
	"sort"
	"sync"
	"time"
)

// Recorder keeps per-second buckets of request counts and latency samples
// in a small ring buffer, plus an in-process copy of the same spike-detect
// rule the autoscaler-monitor runs against Kafka. The point is to give the
// dashboard a single, low-cost source of truth without making it depend on
// a Kafka roundtrip.
//
// Sample retention: we keep up to `samplesPerBucket` latency samples per
// second; under heavy load we sample-reservoir down to that. p50/p95 are
// computed by sorting the in-window samples — fine for ~60×128 = 7.6k
// numbers, which is what a 60-second window with the default sample cap
// caps out at.
type Recorder struct {
	mu             sync.Mutex
	snapshotWindow int // seconds of history shown on the UI (e.g. 60)
	spikeWindow    int // seconds used by the spike detector (e.g. 5)
	now            func() time.Time
	buckets        []bucket
	alerts         []Alert
	maxAlerts      int
	threshold      int64
	cooldown       time.Duration
	lastAlert      time.Time
}

type bucket struct {
	secEpoch int64
	allowed  int64
	rejected int64
	samples  []float64 // ms
}

// Alert is a recorded spike-detection event the dashboard can surface.
type Alert struct {
	TsMs      int64 `json:"ts_ms"`
	Window    int   `json:"window_sec"`
	Count     int64 `json:"count_in_window"`
	Threshold int64 `json:"threshold"`
}

// Snapshot is what the API returns to the UI on each poll.
type Snapshot struct {
	NowMs     int64         `json:"now_ms"`
	WindowSec int           `json:"window_sec"`
	Points    []SecondPoint `json:"points"`
	Cumul     Cumul         `json:"cumul"`
}

type SecondPoint struct {
	TsMs     int64   `json:"ts_ms"`
	Allowed  int64   `json:"allowed"`
	Rejected int64   `json:"rejected"`
	P50Ms    float64 `json:"p50_ms"`
	P95Ms    float64 `json:"p95_ms"`
	Samples  int     `json:"samples"`
}

type Cumul struct {
	Allowed  int64 `json:"allowed"`
	Rejected int64 `json:"rejected"`
}

const samplesPerBucket = 128

// New constructs a Recorder. snapshotWindow is how many seconds of history
// the UI gets back from Snapshot(); spikeWindow is the shorter window the
// in-process alerter uses (matching the autoscaler-monitor).
func New(snapshotWindow, spikeWindow int, threshold int64, cooldown time.Duration) *Recorder {
	if snapshotWindow < 5 {
		snapshotWindow = 5
	}
	if spikeWindow < 1 {
		spikeWindow = 1
	}
	if spikeWindow > snapshotWindow {
		spikeWindow = snapshotWindow
	}
	return &Recorder{
		snapshotWindow: snapshotWindow,
		spikeWindow:    spikeWindow,
		now:            time.Now,
		buckets:        make([]bucket, snapshotWindow*2),
		maxAlerts:      32,
		threshold:      threshold,
		cooldown:       cooldown,
	}
}

func (r *Recorder) bucketFor(sec int64) *bucket {
	idx := int(sec) % len(r.buckets)
	b := &r.buckets[idx]
	if b.secEpoch != sec {
		b.secEpoch = sec
		b.allowed = 0
		b.rejected = 0
		b.samples = b.samples[:0]
	}
	return b
}

// RecordAllowed logs one successful request and (best-effort) its latency.
func (r *Recorder) RecordAllowed(latency time.Duration) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.bucketFor(now.Unix())
	b.allowed++
	if len(b.samples) < samplesPerBucket {
		b.samples = append(b.samples, float64(latency.Microseconds())/1000.0)
	}
}

// RecordRejected logs one 429 and (best-effort) the time the handler took
// to make that decision. It also runs the spike detector.
func (r *Recorder) RecordRejected(latency time.Duration) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.bucketFor(now.Unix())
	b.rejected++
	if len(b.samples) < samplesPerBucket {
		b.samples = append(b.samples, float64(latency.Microseconds())/1000.0)
	}
	// Spike detect: sum rejected over spikeWindow seconds, fire if over
	// threshold and outside cooldown. Mirrors autoscaler-monitor exactly so
	// the UI shows the same alerts the Kafka consumer would have fired.
	var rejectedInWindow int64
	cutoff := now.Unix() - int64(r.spikeWindow)
	for i := range r.buckets {
		if r.buckets[i].secEpoch > cutoff {
			rejectedInWindow += r.buckets[i].rejected
		}
	}
	if rejectedInWindow >= r.threshold && now.Sub(r.lastAlert) >= r.cooldown {
		r.lastAlert = now
		alert := Alert{
			TsMs:      now.UnixMilli(),
			Window:    r.spikeWindow,
			Count:     rejectedInWindow,
			Threshold: r.threshold,
		}
		r.alerts = append(r.alerts, alert)
		if len(r.alerts) > r.maxAlerts {
			r.alerts = r.alerts[len(r.alerts)-r.maxAlerts:]
		}
	}
}

// Snapshot returns the last `snapshotWindow` seconds of data plus
// cumulative counters. Bucket samples are sorted to compute p50/p95.
func (r *Recorder) Snapshot() Snapshot {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoff := now.Unix() - int64(r.snapshotWindow)
	points := make([]SecondPoint, 0, r.snapshotWindow)
	var cumA, cumR int64
	for sec := cutoff + 1; sec <= now.Unix(); sec++ {
		idx := int(sec) % len(r.buckets)
		b := r.buckets[idx]
		if b.secEpoch != sec {
			// Empty second — surface a zero row so the chart doesn't
			// silently compress over gaps.
			points = append(points, SecondPoint{TsMs: sec * 1000})
			continue
		}
		cumA += b.allowed
		cumR += b.rejected
		p50, p95 := quantiles(b.samples)
		points = append(points, SecondPoint{
			TsMs:     sec * 1000,
			Allowed:  b.allowed,
			Rejected: b.rejected,
			P50Ms:    p50,
			P95Ms:    p95,
			Samples:  len(b.samples),
		})
	}
	return Snapshot{
		NowMs:     now.UnixMilli(),
		WindowSec: r.snapshotWindow,
		Points:    points,
		Cumul:     Cumul{Allowed: cumA, Rejected: cumR},
	}
}

// Alerts returns up to maxAlerts most-recent spike alerts, oldest first.
func (r *Recorder) Alerts() []Alert {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Alert, len(r.alerts))
	copy(out, r.alerts)
	return out
}

func quantiles(s []float64) (p50, p95 float64) {
	if len(s) == 0 {
		return 0, 0
	}
	// Make a copy so the sort doesn't disturb the live samples.
	c := make([]float64, len(s))
	copy(c, s)
	sort.Float64s(c)
	p50 = c[len(c)/2]
	idx95 := int(0.95 * float64(len(c)))
	if idx95 >= len(c) {
		idx95 = len(c) - 1
	}
	p95 = c[idx95]
	return
}
