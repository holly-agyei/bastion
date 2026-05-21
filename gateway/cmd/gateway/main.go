package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/holly-agyei/bastion/gateway/internal/config"
	"github.com/holly-agyei/bastion/gateway/internal/forwarder"
	"github.com/holly-agyei/bastion/gateway/internal/metrics"
	"github.com/holly-agyei/bastion/gateway/internal/ratelimit"
	"github.com/holly-agyei/bastion/gateway/internal/server"
	"github.com/holly-agyei/bastion/gateway/internal/telemetry"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.FromEnv()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		PoolSize:     256,
		MinIdleConns: 32,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	defer rdb.Close()

	bootCtx, bootCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer bootCancel()
	if err := waitForRedis(bootCtx, rdb, log); err != nil {
		log.Error("redis not reachable", "err", err)
		os.Exit(1)
	}

	limiter, err := ratelimit.New(bootCtx, rdb, cfg.RedisShards, time.Duration(cfg.WindowMs)*time.Millisecond)
	if err != nil {
		log.Error("limiter", "err", err)
		os.Exit(1)
	}

	pool, err := forwarder.NewPool(bootCtx, cfg.BackendTargets, cfg.BackendPoolSize)
	if err != nil {
		log.Error("forwarder pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	prod := telemetry.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic, log)
	defer func() { _ = prod.Close() }()

	// Mirror the autoscaler-monitor's spike rule in-process so the dashboard
	// surfaces the same alerts the Kafka consumer would fire.
	//   snapshotWindow=60s  (UI history)
	//   spikeWindow=5s      (matches autoscaler-monitor's WINDOW_SEC)
	//   threshold=1000      (matches autoscaler-monitor's THRESHOLD)
	//   cooldown=5s         (matches autoscaler-monitor's COOLDOWN_SEC for demo)
	rec := metrics.New(60, 5, 1000, 5*time.Second)

	srv := server.New(server.Options{
		Limiter:      limiter,
		Pool:         pool,
		Producer:     prod,
		Recorder:     rec,
		Limit:        cfg.DefaultLimit,
		WindowMs:     cfg.WindowMs,
		ClientHeader: cfg.ClientHeader,
		ListenAddr:   cfg.ListenAddr,
		Log:          log,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := server.Run(ctx, cfg.ListenAddr, srv.Routes(), cfg.ShutdownTimeout, log); err != nil {
		log.Error("server", "err", err)
		os.Exit(1)
	}
}

func waitForRedis(ctx context.Context, rdb *redis.Client, log *slog.Logger) error {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			log.Info("waiting for redis", "addr", rdb.Options().Addr)
		}
	}
}
