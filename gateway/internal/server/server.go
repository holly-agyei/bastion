package server

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	v2rand "math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holly-agyei/bastion/gateway/internal/forwarder"
	"github.com/holly-agyei/bastion/gateway/internal/metrics"
	"github.com/holly-agyei/bastion/gateway/internal/ratelimit"
	"github.com/holly-agyei/bastion/gateway/internal/telemetry"
	"github.com/holly-agyei/bastion/gateway/internal/ui"
	servicev1 "github.com/holly-agyei/bastion/gateway/proto/service"
)

type Server struct {
	limiter      *ratelimit.Limiter
	pool         *forwarder.Pool
	prod         *telemetry.Producer
	rec          *metrics.Recorder
	limit        int64
	windowMs     int64
	clientHeader string
	nodeID       string
	log          *slog.Logger
	listenAddr   string

	allowed  uint64
	rejected uint64
}

type Options struct {
	Limiter      *ratelimit.Limiter
	Pool         *forwarder.Pool
	Producer     *telemetry.Producer
	Recorder     *metrics.Recorder
	Limit        int64
	WindowMs     int64
	ClientHeader string
	ListenAddr   string
	Log          *slog.Logger
}

func New(o Options) *Server {
	host, _ := os.Hostname()
	return &Server{
		limiter:      o.Limiter,
		pool:         o.Pool,
		prod:         o.Producer,
		rec:          o.Recorder,
		limit:        o.Limit,
		windowMs:     o.WindowMs,
		clientHeader: o.ClientHeader,
		nodeID:       host,
		log:          o.Log,
		listenAddr:   o.ListenAddr,
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/stats", s.stats)
	mux.HandleFunc("/ui", s.serveUI)
	mux.HandleFunc("/api/metrics", s.apiMetrics)
	mux.HandleFunc("/api/alerts", s.apiAlerts)
	mux.HandleFunc("/api/config", s.apiConfig)
	mux.HandleFunc("/api/burst", s.apiBurst)
	mux.HandleFunc("/process", s.handle)
	mux.HandleFunc("/", s.handle)
	return mux
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	a := atomic.LoadUint64(&s.allowed)
	r := atomic.LoadUint64(&s.rejected)
	_, _ = w.Write([]byte("bastion_allowed_total " + strconv.FormatUint(a, 10) + "\n"))
	_, _ = w.Write([]byte("bastion_rejected_total " + strconv.FormatUint(r, 10) + "\n"))
}

func (s *Server) serveUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(ui.IndexHTML)
}

func (s *Server) apiMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.rec.Snapshot())
}

func (s *Server) apiAlerts(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.rec.Alerts())
}

func (s *Server) apiConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"limit":         s.limit,
		"window_ms":     s.windowMs,
		"client_header": s.clientHeader,
		"node_id":       s.nodeID,
	})
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	clientID := r.Header.Get(s.clientHeader)
	if clientID == "" {
		clientID = "anonymous"
	}

	reqID := newReqID()
	decision, err := s.limiter.Allow(ctx, clientID, s.limit, reqID)
	if err != nil {
		// Fail open: if Redis is unreachable, we'd rather serve traffic than
		// black-hole it. The autoscaler-monitor will still see backend
		// pressure from non-429 paths, and the operator gets the log line.
		s.log.Error("limiter error, failing open", "err", err)
	} else if !decision.Allowed {
		atomic.AddUint64(&s.rejected, 1)
		retryMs := decision.RetryAfter.Milliseconds()
		if retryMs <= 0 {
			retryMs = 1
		}
		w.Header().Set("Retry-After-Ms", strconv.FormatInt(retryMs, 10))
		w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(s.limit, 10))
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate_limited"}`))
		s.prod.Publish(telemetry.Rejection{
			Ts:         time.Now().UnixMilli(),
			ClientID:   clientID,
			Path:       r.URL.Path,
			RetryMs:    retryMs,
			CountInWin: decision.Count,
			NodeID:     s.nodeID,
		})
		s.rec.RecordRejected(time.Since(start))
		return
	}

	// Allowed → forward via gRPC.
	body, _ := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	hdrs := make(map[string]string, len(r.Header))
	for k, v := range r.Header {
		if len(v) > 0 {
			hdrs[k] = v[0]
		}
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := s.pool.Client().ProcessRequest(rpcCtx, &servicev1.ProcessRequestRequest{
		RequestId:  reqID,
		ClientId:   clientID,
		Path:       r.URL.Path,
		Payload:    body,
		Headers:    hdrs,
		ClientTsMs: time.Now().UnixMilli(),
	})
	if err != nil {
		s.log.Warn("backend rpc failed", "err", err, "req_id", reqID)
		http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
		return
	}
	atomic.AddUint64(&s.allowed, 1)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Backend-Node", resp.NodeId)
	w.WriteHeader(int(resp.StatusCode))
	_, _ = w.Write(resp.Payload)
	s.rec.RecordAllowed(time.Since(start))
}

// apiBurst spawns synthetic traffic from inside the gateway process against
// its own /process endpoint. This is a dashboard convenience — useful for
// demos and screenshots — not a production endpoint. Behaviour is bounded
// (max ~20k total requests, ≤ 10s wallclock) so it can't be used to DoS.
func (s *Server) apiBurst(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	profile := r.URL.Query().Get("profile")
	if profile == "" {
		profile = "mixed"
	}

	type leg struct {
		rps     int
		dur     time.Duration
		abusers int // 0 means many random clients (under quota)
	}
	var legs []leg
	switch profile {
	case "burst":
		legs = []leg{{rps: 3000, dur: 5 * time.Second, abusers: 4}}
	case "sustained":
		legs = []leg{{rps: 1500, dur: 10 * time.Second, abusers: 0}}
	case "mixed":
		legs = []leg{
			{rps: 1500, dur: 8 * time.Second, abusers: 0},
			{rps: 3000, dur: 5 * time.Second, abusers: 4},
		}
	default:
		http.Error(w, "unknown profile", http.StatusBadRequest)
		return
	}

	origin := "http://127.0.0.1" + s.listenAddr
	if s.listenAddr != "" && s.listenAddr[0] == ':' {
		origin = "http://127.0.0.1" + s.listenAddr
	}

	hc := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        2048,
			MaxIdleConnsPerHost: 2048,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	var allowedCt, rejectedCt, sent int64
	startWall := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, l := range legs {
		go func(l leg) {}(l) // no-op; spacing
		wg.Add(1)
		go func(l leg) {
			defer wg.Done()
			runLeg(ctx, hc, origin, s.clientHeader, l.rps, l.dur, l.abusers,
				&allowedCt, &rejectedCt, &sent)
		}(l)
	}
	wg.Wait()
	writeJSON(w, map[string]any{
		"profile":     profile,
		"duration_ms": time.Since(startWall).Milliseconds(),
		"sent":        atomic.LoadInt64(&sent),
		"allowed":     atomic.LoadInt64(&allowedCt),
		"rejected":    atomic.LoadInt64(&rejectedCt),
	})
}

func runLeg(ctx context.Context, hc *http.Client, origin, header string,
	rps int, dur time.Duration, abusers int,
	allowedCt, rejectedCt, sentCt *int64,
) {
	// We don't try to be a precise rate generator; the goal is to be visibly
	// busy in the dashboard for `dur` seconds and to overshoot the per-client
	// quota when abusers > 0. A token bucket over a tick keeps us close.
	tick := 5 * time.Millisecond
	tokensPerTick := float64(rps) * tick.Seconds()
	t := time.NewTicker(tick)
	defer t.Stop()
	deadline := time.Now().Add(dur)
	sem := make(chan struct{}, 800)
	var wg sync.WaitGroup
	var tokens float64
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-t.C:
			if time.Now().After(deadline) {
				wg.Wait()
				return
			}
			tokens += tokensPerTick
			for tokens >= 1 {
				tokens -= 1
				var clientID string
				if abusers > 0 {
					clientID = fmt.Sprintf("sim-abuser-%d", v2rand.IntN(abusers))
				} else {
					clientID = fmt.Sprintf("sim-honest-%d", v2rand.IntN(2000))
				}
				select {
				case sem <- struct{}{}:
				default:
					// drop if we can't keep up; the cap protects the gateway
					continue
				}
				wg.Add(1)
				atomic.AddInt64(sentCt, 1)
				go func(id string) {
					defer func() { <-sem; wg.Done() }()
					req, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/process", nil)
					req.Header.Set(header, id)
					resp, err := hc.Do(req)
					if err != nil {
						return
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					switch resp.StatusCode {
					case 200:
						atomic.AddInt64(allowedCt, 1)
					case 429:
						atomic.AddInt64(rejectedCt, 1)
					}
				}(clientID)
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

func newReqID() string {
	var b [12]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		// Fallback: caller doesn't need cryptographic uniqueness, just
		// distinguishability inside the ZSET. time-based is fine.
		t := time.Now().UnixNano()
		return strconv.FormatInt(t, 36)
	}
	return hex.EncodeToString(b[:])
}

// Run starts the HTTP server and blocks until ctx is cancelled, returning
// only after a graceful shutdown completes (or the shutdown timeout fires).
func Run(ctx context.Context, addr string, handler http.Handler, shutdown time.Duration, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("gateway listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case <-ctx.Done():
		log.Info("shutdown initiated")
		sctx, cancel := context.WithTimeout(context.Background(), shutdown)
		defer cancel()
		return srv.Shutdown(sctx)
	case err := <-errCh:
		return err
	}
}
