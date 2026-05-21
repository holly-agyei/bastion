package com.bastion.backend;

import com.bastion.proto.BackendServiceGrpc;
import com.bastion.proto.ProcessRequestRequest;
import com.bastion.proto.ProcessRequestResponse;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight gRPC backend. Receives ProcessRequest calls from the Go
 * gateway, simulates 2ms of "work", and returns a 200 response. The point of
 * this service is to give the gateway a realistic upstream — netty-backed
 * gRPC over HTTP/2, no proxies, no extra hops — so the headline latency
 * number reflects gateway + transport rather than any business logic.
 */
public final class BackendServer {

  private static final Logger LOG = LoggerFactory.getLogger(BackendServer.class);

  private final int port;
  private final String nodeId;
  private final long simulatedWorkNanos;
  private final AtomicLong served = new AtomicLong();
  private Server server;

  public BackendServer(int port, String nodeId, long simulatedWorkNanos) {
    this.port = port;
    this.nodeId = nodeId;
    this.simulatedWorkNanos = simulatedWorkNanos;
  }

  public void start() throws Exception {
    // A dedicated executor for the synchronous service impl. The Netty
    // event loop should never call user code directly under load.
    var serviceExecutor = new ThreadPoolExecutor(
        Runtime.getRuntime().availableProcessors() * 4,
        Runtime.getRuntime().availableProcessors() * 16,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(8192),
        new ThreadPoolExecutor.CallerRunsPolicy());

    server = NettyServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", port))
        .addService(new Impl())
        .addService(ProtoReflectionService.newInstance())
        .executor(serviceExecutor)
        .maxConcurrentCallsPerConnection(10_000)
        .permitKeepAliveWithoutCalls(true)
        .permitKeepAliveTime(30, TimeUnit.SECONDS)
        .build()
        .start();

    LOG.info("backend listening on {} node_id={} pid={}", port, nodeId,
        ManagementFactory.getRuntimeMXBean().getName());

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("shutdown signal received, draining…");
      try {
        if (server != null) {
          server.shutdown();
          if (!server.awaitTermination(15, TimeUnit.SECONDS)) {
            server.shutdownNow();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      LOG.info("served={} during this process lifetime", served.get());
    }, "backend-shutdown"));
  }

  public void awaitTermination() throws InterruptedException {
    if (server != null) server.awaitTermination();
  }

  private final class Impl extends BackendServiceGrpc.BackendServiceImplBase {
    @Override
    public void processRequest(ProcessRequestRequest req, StreamObserver<ProcessRequestResponse> obs) {
      if (simulatedWorkNanos > 0) {
        // Busy-wait for sub-millisecond precision; Thread.sleep granularity
        // on Linux is too coarse for a 2ms target under load.
        long deadline = System.nanoTime() + simulatedWorkNanos;
        while (System.nanoTime() < deadline) {
          // Spin. Cheap relative to the actual gRPC framing cost; predictable.
        }
      }
      var resp = ProcessRequestResponse.newBuilder()
          .setRequestId(req.getRequestId())
          .setStatusCode(200)
          .setPayload(ByteString.copyFromUtf8("{\"ok\":true}"))
          .setServerTsMs(System.currentTimeMillis())
          .setNodeId(nodeId)
          .build();
      obs.onNext(resp);
      obs.onCompleted();
      served.incrementAndGet();
    }

    @Override
    public StreamObserver<ProcessRequestRequest> processStream(StreamObserver<ProcessRequestResponse> obs) {
      return new StreamObserver<>() {
        @Override public void onNext(ProcessRequestRequest req) {
          obs.onNext(ProcessRequestResponse.newBuilder()
              .setRequestId(req.getRequestId())
              .setStatusCode(200)
              .setServerTsMs(System.currentTimeMillis())
              .setNodeId(nodeId)
              .build());
          served.incrementAndGet();
        }
        @Override public void onError(Throwable t) {
          LOG.warn("stream error", t);
        }
        @Override public void onCompleted() {
          obs.onCompleted();
        }
      };
    }
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "9090"));
    String nodeId = System.getenv().getOrDefault("NODE_ID",
        ManagementFactory.getRuntimeMXBean().getName());
    long workUs = Long.parseLong(System.getenv().getOrDefault("SIMULATED_WORK_US", "2000"));
    var srv = new BackendServer(port, nodeId, TimeUnit.MICROSECONDS.toNanos(workUs));
    srv.start();
    srv.awaitTermination();
  }
}
