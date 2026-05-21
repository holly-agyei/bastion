package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr      string
	BackendTargets  []string
	BackendPoolSize int

	RedisAddr   string
	RedisShards int

	KafkaBrokers []string
	KafkaTopic   string

	WindowMs        int64
	DefaultLimit    int64
	ClientHeader    string
	ShutdownTimeout time.Duration
}

func FromEnv() (*Config, error) {
	c := &Config{
		ListenAddr:      getEnv("LISTEN_ADDR", ":8080"),
		BackendTargets:  splitCSV(getEnv("BACKEND_TARGETS", "backend:9090")),
		BackendPoolSize: getEnvInt("BACKEND_POOL_SIZE", 8),
		RedisAddr:       getEnv("REDIS_ADDR", "redis:6379"),
		RedisShards:     getEnvInt("REDIS_SHARDS", 64),
		KafkaBrokers:    splitCSV(getEnv("KAFKA_BROKERS", "kafka:9092")),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "telemetry.rejections"),
		WindowMs:        int64(getEnvInt("WINDOW_MS", 1000)),
		DefaultLimit:    int64(getEnvInt("DEFAULT_LIMIT", 100)),
		ClientHeader:    getEnv("CLIENT_HEADER", "X-Client-Id"),
		ShutdownTimeout: time.Duration(getEnvInt("SHUTDOWN_TIMEOUT_SEC", 15)) * time.Second,
	}
	if len(c.BackendTargets) == 0 {
		return nil, fmt.Errorf("BACKEND_TARGETS must not be empty")
	}
	if c.BackendPoolSize < 1 {
		return nil, fmt.Errorf("BACKEND_POOL_SIZE must be >= 1")
	}
	return c, nil
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
