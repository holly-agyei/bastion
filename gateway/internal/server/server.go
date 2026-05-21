package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/holly-agyei/bastion/gateway/internal/forwarder"
	"github.com/holly-agyei/bastion/gateway/internal/ratelimit"
	"github.com/holly-agyei/bastion/gateway/internal/telemetry"
	servicev1 "github.com/holly-agyei/bastion/gateway/proto/service"
)

type Server struct {
	limiter      *ratelimit.Limiter
	pool         *forwarder.Pool
	prod         *telemetry.Producer
	limit        int64
	clientHeader string
	nodeID       string
	log          *slog.Logger

	allowed  uint64
	rejected uint64
}

func New(limiter *ratelimit.Limiter, pool *forwarder.Pool, prod *telemetry.Producer,
	limit int64, clientHeader string, log *slog.Logger,
) *Server {
	host, _ := os.Hostname()
	return &Server{
		limiter:      limiter,
		pool:         pool,
		prod:         prod,
		limit:        limit,
		clientHeader: clientHeader,
		nodeID:       host,
		log:          log,
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/stats", s.stats)
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

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
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
}

func newReqID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
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
