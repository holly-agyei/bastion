package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed sliding_window.lua
var slidingWindowScript string

// Limiter is a sliding-window rate limiter. The Lua script runs atomically
// inside Redis, so the [trim, count, add] sequence cannot interleave with
// another client's check for the same key. EVALSHA is used after the first
// SCRIPT LOAD to avoid sending the script body on every call.
type Limiter struct {
	rdb    *redis.Client
	sha    string
	shards int
	window time.Duration
}

// Decision is the result of a rate-limit check.
type Decision struct {
	Allowed    bool
	Count      int64
	RetryAfter time.Duration
}

func New(ctx context.Context, rdb *redis.Client, shards int, window time.Duration) (*Limiter, error) {
	if shards < 1 {
		shards = 1
	}
	sha, err := rdb.ScriptLoad(ctx, slidingWindowScript).Result()
	if err != nil {
		return nil, fmt.Errorf("script load: %w", err)
	}
	return &Limiter{rdb: rdb, sha: sha, shards: shards, window: window}, nil
}

// Allow checks whether the given clientID is permitted one more request under
// the supplied limit, in the configured window. It is safe for concurrent use.
func (l *Limiter) Allow(ctx context.Context, clientID string, limit int64, member string) (Decision, error) {
	key := l.shardKey(clientID)
	now := time.Now().UnixMilli()
	res, err := l.rdb.EvalSha(ctx, l.sha, []string{key},
		now, l.window.Milliseconds(), limit, member,
	).Result()
	if err != nil {
		// If the script was flushed (rare; happens after SCRIPT FLUSH or a
		// failover to a replica that never saw the SCRIPT LOAD), reload and
		// retry once.
		if isNoScriptErr(err) {
			sha, lerr := l.rdb.ScriptLoad(ctx, slidingWindowScript).Result()
			if lerr != nil {
				return Decision{}, fmt.Errorf("reload script: %w", lerr)
			}
			l.sha = sha
			res, err = l.rdb.EvalSha(ctx, l.sha, []string{key},
				now, l.window.Milliseconds(), limit, member,
			).Result()
			if err != nil {
				return Decision{}, fmt.Errorf("evalsha retry: %w", err)
			}
		} else {
			return Decision{}, fmt.Errorf("evalsha: %w", err)
		}
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 3 {
		return Decision{}, fmt.Errorf("unexpected script reply: %T %v", res, res)
	}
	allowed, _ := arr[0].(int64)
	count, _ := arr[1].(int64)
	retry, _ := arr[2].(int64)
	return Decision{
		Allowed:    allowed == 1,
		Count:      count,
		RetryAfter: time.Duration(retry) * time.Millisecond,
	}, nil
}

// shardKey maps a client id to one of N keys. This spreads load and (for a
// Redis Cluster deployment) lets each shard own a contiguous range of slots
// without hotspotting a single key. We do not use a hash tag here because we
// *want* keys to land on different slots.
func (l *Limiter) shardKey(clientID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(clientID))
	shard := h.Sum32() % uint32(l.shards)
	return "rl:" + strconv.FormatUint(uint64(shard), 10) + ":" + clientID
}

func isNoScriptErr(err error) bool {
	return err != nil && len(err.Error()) >= 8 && err.Error()[:8] == "NOSCRIPT"
}
