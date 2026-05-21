package forwarder

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	servicev1 "github.com/holly-agyei/bastion/gateway/proto/service"
)

// Pool is a fixed-size set of HTTP/2 gRPC connections, fronting one or more
// backend targets. We deliberately open multiple connections per target so a
// single TCP stream's flow control / HOL-blocking window does not become the
// bottleneck under heavy concurrency. Each gRPC ClientConn multiplexes many
// concurrent RPCs over its single HTTP/2 connection — but the pool lets us
// shard goroutines across N connections to keep the per-conn stream count
// manageable.
type Pool struct {
	conns   []*grpc.ClientConn
	clients []servicev1.BackendServiceClient
	next    uint64
}

func NewPool(ctx context.Context, targets []string, perTarget int) (*Pool, error) {
	if perTarget < 1 {
		perTarget = 1
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("forwarder: no targets")
	}
	p := &Pool{}
	for _, t := range targets {
		for i := 0; i < perTarget; i++ {
			cc, err := grpc.NewClient(t,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(8*1024*1024),
					grpc.MaxCallSendMsgSize(8*1024*1024),
				),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:                30 * time.Second,
					Timeout:             10 * time.Second,
					PermitWithoutStream: true,
				}),
				grpc.WithConnectParams(grpc.ConnectParams{
					Backoff: backoff.Config{
						BaseDelay:  200 * time.Millisecond,
						Multiplier: 1.6,
						Jitter:     0.2,
						MaxDelay:   5 * time.Second,
					},
					MinConnectTimeout: 5 * time.Second,
				}),
			)
			if err != nil {
				p.Close()
				return nil, fmt.Errorf("dial %s: %w", t, err)
			}
			p.conns = append(p.conns, cc)
			p.clients = append(p.clients, servicev1.NewBackendServiceClient(cc))
		}
	}
	return p, nil
}

// Client returns one of the pooled clients in round-robin order. Callers
// invoke the unary RPC; the gRPC library multiplexes it over the chosen
// connection's HTTP/2 streams.
func (p *Pool) Client() servicev1.BackendServiceClient {
	i := atomic.AddUint64(&p.next, 1)
	return p.clients[(i-1)%uint64(len(p.clients))]
}

func (p *Pool) Close() {
	for _, c := range p.conns {
		_ = c.Close()
	}
}
