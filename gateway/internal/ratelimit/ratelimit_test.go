package ratelimit

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisAddr returns the Redis address used by tests. Defaults to a local
// container exposed on 6379. Tests that need Redis skip if it's not up.
func redisAddr() string {
	if v := os.Getenv("REDIS_TEST_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr()})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis not reachable at %s: %v", redisAddr(), err)
	}
	return rdb
}

func uniqueClient(t *testing.T, suffix string) string {
	t.Helper()
	return "test-" + t.Name() + "-" + suffix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func TestAllow_AllowsUpToLimit(t *testing.T) {
	rdb := newClient(t)
	defer rdb.Close()
	ctx := context.Background()

	l, err := New(ctx, rdb, 8, time.Second)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client := uniqueClient(t, "allow")

	const limit = 5
	for i := 0; i < limit; i++ {
		d, err := l.Allow(ctx, client, limit, fmt.Sprintf("req-%d", i))
		if err != nil {
			t.Fatalf("allow %d: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("request %d should be allowed (count=%d)", i, d.Count)
		}
	}
	d, err := l.Allow(ctx, client, limit, "req-over")
	if err != nil {
		t.Fatalf("over: %v", err)
	}
	if d.Allowed {
		t.Fatalf("request beyond limit should be rejected")
	}
	if d.RetryAfter <= 0 {
		t.Fatalf("expected positive retry-after, got %v", d.RetryAfter)
	}
}

func TestAllow_WindowSlides(t *testing.T) {
	rdb := newClient(t)
	defer rdb.Close()
	ctx := context.Background()

	window := 200 * time.Millisecond
	l, err := New(ctx, rdb, 8, window)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client := uniqueClient(t, "slide")

	for i := 0; i < 3; i++ {
		d, err := l.Allow(ctx, client, 3, fmt.Sprintf("a-%d", i))
		if err != nil || !d.Allowed {
			t.Fatalf("init %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}
	d, _ := l.Allow(ctx, client, 3, "a-over")
	if d.Allowed {
		t.Fatalf("4th in window should be denied")
	}

	// Wait the window plus a small margin, then we should be allowed again.
	time.Sleep(window + 50*time.Millisecond)
	d, err = l.Allow(ctx, client, 3, "after-window")
	if err != nil {
		t.Fatalf("after-window: %v", err)
	}
	if !d.Allowed {
		t.Fatalf("after window expired, request should be allowed (count=%d)", d.Count)
	}
}

// TestAllow_ConcurrentAccuracy is the headline accuracy test. It blasts a
// single key from many goroutines and asserts the script never allows more
// than `limit` requests in the window. Atomicity of the EVALSHA is what
// gives us this guarantee.
func TestAllow_ConcurrentAccuracy(t *testing.T) {
	rdb := newClient(t)
	defer rdb.Close()
	ctx := context.Background()

	window := 2 * time.Second
	limit := int64(500)
	attempts := 5000

	l, err := New(ctx, rdb, 8, window)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client := uniqueClient(t, "race")

	var allowed int64
	var wg sync.WaitGroup
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func(i int) {
			defer wg.Done()
			d, err := l.Allow(ctx, client, limit, fmt.Sprintf("r-%d", i))
			if err != nil {
				t.Errorf("allow: %v", err)
				return
			}
			if d.Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}(i)
	}
	wg.Wait()

	got := atomic.LoadInt64(&allowed)
	if got != limit {
		t.Fatalf("expected exactly %d allowed under contention, got %d (over by %d)", limit, got, got-limit)
	}
}

func TestAllow_IsolationAcrossClients(t *testing.T) {
	rdb := newClient(t)
	defer rdb.Close()
	ctx := context.Background()

	l, err := New(ctx, rdb, 8, time.Second)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	a := uniqueClient(t, "a")
	b := uniqueClient(t, "b")

	for i := 0; i < 3; i++ {
		d, _ := l.Allow(ctx, a, 3, fmt.Sprintf("a-%d", i))
		if !d.Allowed {
			t.Fatalf("a request %d should be allowed", i)
		}
	}
	// a is now exhausted; b should still be free.
	d, _ := l.Allow(ctx, a, 3, "a-over")
	if d.Allowed {
		t.Fatalf("a should be exhausted")
	}
	d, _ = l.Allow(ctx, b, 3, "b-first")
	if !d.Allowed {
		t.Fatalf("b should be unaffected by a's exhaustion")
	}
}

func TestAllow_ScriptReloadAfterFlush(t *testing.T) {
	rdb := newClient(t)
	defer rdb.Close()
	ctx := context.Background()
	l, err := New(ctx, rdb, 8, time.Second)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// SCRIPT FLUSH should not break the limiter; the NOSCRIPT recovery
	// path reloads and retries.
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("script flush: %v", err)
	}
	client := uniqueClient(t, "reload")
	d, err := l.Allow(ctx, client, 5, "after-flush")
	if err != nil {
		t.Fatalf("allow after flush: %v", err)
	}
	if !d.Allowed {
		t.Fatalf("first request after flush should be allowed")
	}
}
