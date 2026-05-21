-- Sliding window rate limiter (precise, log-based).
--
-- KEYS[1] = bucket key (per client)
-- ARGV[1] = now_ms        (current epoch millis, supplied by caller)
-- ARGV[2] = window_ms     (window size in millis)
-- ARGV[3] = limit         (max requests permitted in the window)
-- ARGV[4] = member        (unique request id; avoids ZADD collisions on identical now_ms)
--
-- Returns: { allowed (0|1), count_in_window, retry_after_ms }
--
-- Algorithm:
--   1. Drop entries older than (now - window).
--   2. Count remaining entries (== requests currently inside the window).
--   3. If count < limit, ZADD the new request and PEXPIRE the key one window ahead.
--   4. Otherwise, look up the oldest entry; retry_after = window - (now - oldest).
--
-- All four steps execute inside a single Redis EVALSHA, so the entire
-- decision is atomic for a given key. Sharding is handled by hashing the
-- client id into the key on the caller side (sharded keyspace, not cluster).

local key       = KEYS[1]
local now_ms    = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit     = tonumber(ARGV[3])
local member    = ARGV[4]

local cutoff = now_ms - window_ms

-- 1. Trim entries that fell out of the window.
redis.call('ZREMRANGEBYSCORE', key, '-inf', '(' .. cutoff)

-- 2. Count what's still in the window.
local count = redis.call('ZCARD', key)

if count < limit then
  redis.call('ZADD', key, now_ms, member)
  -- Expire one window after the latest possible relevant entry so the key
  -- self-cleans for clients that go idle.
  redis.call('PEXPIRE', key, window_ms + 1000)
  return {1, count + 1, 0}
end

-- 3. Over the limit. Compute retry_after from the oldest in-window entry.
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local retry_after_ms = window_ms
if oldest[2] then
  retry_after_ms = window_ms - (now_ms - tonumber(oldest[2]))
  if retry_after_ms < 0 then retry_after_ms = 0 end
end

return {0, count, retry_after_ms}
