package com.ashu.bulk_api.adapters.outbound.web;


import com.ashu.bulk_api.adapters.outbound.jpa.StatusRecord;
import com.ashu.bulk_api.adapters.outbound.jpa.StatusRepository;
import com.ashu.bulk_api.core.domain.job.BatchResult;
import com.ashu.bulk_api.core.domain.model.Signal;
import com.ashu.bulk_api.core.port.outbound.SignalBatchPort;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.PrematureCloseException;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class ExternalSignalBatchSender implements SignalBatchPort {

    // ===== Dependencies =====
    private final WebClient webClient;
    private final StatusRepository statusRepository;
    private final CircuitBreaker circuitBreaker;

    // ===== Concurrency Guard =====
    // Number of concurrent HTTP calls allowed per service instance (used by Reactor flatMap).
    private final int maxConcurrentRequests;

    // ===== External API base URL (e.g., http://localhost:8081) =====
    @Value("${bulk-api.external-api.base-url}")
    private String externalApiBaseUrl;

    public ExternalSignalBatchSender(
            WebClient webClient,
            StatusRepository statusRepository,
            CircuitBreakerRegistry circuitBreakerRegistry,
            @Value("${bulk-api.processing.rate-limit:100}") int maxConcurrentRequests
    ) {
        this.webClient = webClient;
        this.statusRepository = statusRepository;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("signalApi");
        this.maxConcurrentRequests = Math.max(1, maxConcurrentRequests);
    }

    /**
     * Process a batch of messages asynchronously using the app’s task executor.
     * - We intentionally return a CompletableFuture to match the rest of your orchestration code.
     * - Internally each message call is fully reactive (Mono), with retry + circuit breaker.
     * - We do NOT persist status here; that happens inside each call pipeline (success/error).
     */
    @Override
    @Async("bulkApiTaskExecutor")
    public CompletableFuture<BatchResult> submitBatch(List<Signal> messages) {
        int size = messages == null ? 0 : messages.size();
        if (size == 0) {
            return CompletableFuture.completedFuture(BatchResult.empty());
        }

        log.info("🚀 Starting batch of {} messages on thread={} | concurrencyCap={} ",
                size, Thread.currentThread().getName(), maxConcurrentRequests);

        return Flux.fromIterable(messages)
                .flatMap(this::postMessageReactive, maxConcurrentRequests)
                .collectList()
                .map(BatchResult::fromBooleans)
                .doOnError(ex -> log.error("❌ Batch completed with errors: {}", ex.getMessage(), ex))
                .doOnSuccess(result -> log.info("✅ Batch completed: {} success / {} failure",
                        result.successCount(), result.failureCount()))
                .toFuture();
    }

    /**
     * Core reactive pipeline for posting a single message.
     * Key points:
     *  - **Reactor flatMap handles concurrency upstream**, so this pipeline stays fully non-blocking.
     *  - **Timeout** to fail slow calls (mapped to error to trigger retry/breaker).
     *  - **Circuit Breaker** (Resilience4j) to short-circuit when downstream is unhealthy.
     *  - **Retry** (reactor.util.retry.Retry) on *retryable* exceptions only.
     *  - **Single-source persistence**: PASS on success (ceh_event_id present), FAIL on error/fallback.
     *  - No duplication between onError and fallback handlers; exactly-once status write per message.
     */
    private Mono<Boolean> postMessageReactive(Signal message) {
        // Defensive nulls
        Objects.requireNonNull(message, "message must not be null");

        // Build the reactive call
        return webClient.post()
                .uri(externalApiBaseUrl + "/create-signal")
                .bodyValue(message)
                .retrieve()
                // Expect a JSON response, e.g., {"ceh_event_id": 123456}
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                // Hard timeout; ensures slow calls fail and can be retried / trip breaker
                .timeout(Duration.ofSeconds(10))
                // Apply the circuit breaker (reactive operator, Resilience4j)
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                // Retry ONLY on specific, transient failures
                .retryWhen(buildRetrySpec())
                .defaultIfEmpty(Collections.emptyMap())
                // On success: validate ceh_event_id and persist PASS/FAIL accordingly
                .doOnSuccess(response -> handleSuccess(message, response))
                // Convert payload into a boolean outcome so orchestrator can aggregate results
                .map(response -> response != null && response.get("ceh_event_id") != null)
                // On any terminal error (after retries and/or breaker open), persist FAIL once
                .doOnError(ex -> handleException(message, ex))
                // Swallow errors for orchestration purposes (status already persisted via handleException)
                .onErrorReturn(false);
    }

    /**
     * Reactive Retry policy:
     *  - Max 3 attempts (1 original + 2 retries) using exponential backoff.
     *  - ONLY retry on *transient* failures (timeouts, connection resets, 5xx, 429).
     *  - Do NOT retry on 4xx client errors (e.g., 400/404/409), as those are not transient.
     */
    private Retry buildRetrySpec() {
        return Retry
                .backoff(3, Duration.ofSeconds(1))   // 3 attempts, backoff 1s → 2s → 4s (capped below)
                .maxBackoff(Duration.ofSeconds(5))   // cap max backoff at 5s
                .filter(this::isRetryable)           // retry only certain exceptions
                .doAfterRetry(retrySignal -> log.warn(
                        "🔁 Retry attempt #{}, cause={}, circuitBreakerState={}",
                        retrySignal.totalRetries() + 1,
                        retrySignal.failure() == null ? "unknown" : retrySignal.failure().toString(),
                        circuitBreaker.getState()
                ));
    }

    /**
     * Define which errors are retryable.
     * - Timeouts, connection problems, premature close (downstream dropped connection)
     * - HTTP 5xx and 429 (Too Many Requests)
     * - NOT retrying HTTP 4xx other than 429.
     */
    private boolean isRetryable(Throwable ex) {
        // Reactor / Netty transient network issues
        if (ex instanceof PrematureCloseException) return true;
        if (ex instanceof WebClientRequestException) return true; // includes IO issues like connect reset

        // Map IOExceptions in general as retryable
        if (ex instanceof IOException) return true;

        // HTTP status-based logic
        if (ex instanceof WebClientResponseException wcre) {
            int status = wcre.getRawStatusCode();
            if (status == 429) return true;         // Too Many Requests
            if (status >= 500 && status < 600) return true; // 5xx server errors are retryable
            return false; // 4xx (except 429) are by default permanent failures
        }

        // Timeouts from Reactor's timeout() appear as java.util.concurrent.TimeoutException
        if (ex instanceof java.util.concurrent.TimeoutException) return true;

        return false;
    }

    /** Persist PASS with ceh_event_id. */
    private void persistPass(Signal message, long cehEventId) {
        Long uabsEventId = message.getUabsEventId();
        StatusRecord rec = new StatusRecord(uabsEventId, cehEventId, "PASS", null);
        statusRepository.save(rec);
        log.debug("💾 Saved PASS status for uabsEventId={} ceh_event_id={}", uabsEventId, cehEventId);
    }

    /** Persist FAIL (ceh_event_id is null). Reason is captured for future diagnostics. */
    private void persistFail(Signal message, String reason) {
        Long uabsEventId = message.getUabsEventId();
        String sanitizedReason = (reason == null || reason.isBlank()) ? "UNKNOWN" : reason;
        StatusRecord rec = new StatusRecord(uabsEventId, null, "FAIL", sanitizedReason);
        statusRepository.save(rec);
        log.debug("💾 Saved FAIL status for uabsEventId={} reason={}", uabsEventId, sanitizedReason);
    }

    private void handleSuccess(Signal message, Map<String, Object> response) {
        Object ceh = response == null ? null : response.get("ceh_event_id");
        if (ceh != null) {
            long cehId = parseLongSafely(ceh);
            persistPass(message, cehId);
            log.info("✅ Posted uabsEventId={} | ceh_event_id={} | thread={}",
                    message.getUabsEventId(), cehId, Thread.currentThread().getName());
        } else {
            // Consider missing ceh_event_id as a logical failure (no retry here; we've already retried)
            persistFail(message, "NO_CEH_EVENT_ID");
            log.warn("⚠️ No ceh_event_id returned for uabsEventId={} | thread={}",
                    message.getUabsEventId(), Thread.currentThread().getName());
        }
    }

    private void handleException(Signal message, Throwable ex) {
        String status;

        if (ex instanceof WebClientResponseException wcre) {
            int code = wcre.getRawStatusCode();
            if (code == 429 || (code >= 500 && code < 600)) {
                status = "FAIL_TRANSIENT";
            } else {
                status = "FAIL_PERMANENT";
            }
        } else if (ex instanceof java.util.concurrent.TimeoutException) {
            status = "TIMEOUT";
        } else if (ex instanceof io.github.resilience4j.circuitbreaker.CallNotPermittedException) {
            status = "BLOCKED_BY_CIRCUIT";
        } else if (ex instanceof WebClientRequestException
                || ex instanceof PrematureCloseException
                || ex instanceof IOException) {
            status = "FAIL_TRANSIENT";
        } else if (ex instanceof InterruptedException) {
            status = "INTERRUPTED";
        } else {
            status = "FAIL_UNKNOWN";
        }

        persistFail(message, status);
        log.error("❌ Final failure for uabsEventId={} | status={} | error={} | thread={}",
                message.getUabsEventId(), status, ex.toString(), Thread.currentThread().getName());
    }



    /** Safe parse (handles numeric types or numeric strings). */
    private long parseLongSafely(Object value) {
        if (value instanceof Number n) return n.longValue();
        return Long.parseLong(String.valueOf(value));
    }

}
