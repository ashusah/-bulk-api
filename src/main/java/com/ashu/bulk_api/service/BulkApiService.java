package com.ashu.bulk_api.service;


import com.ashu.bulk_api.jpa.StatusRecord;
import com.ashu.bulk_api.jpa.StatusRepository;
import com.ashu.bulk_api.model.ApiMessage;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
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
import java.util.concurrent.Semaphore;

@Service
@Slf4j
public class BulkApiService {

    // ===== Dependencies =====
    private final WebClient webClient;
    private final StatusRepository statusRepository;
    private final CircuitBreaker circuitBreaker;

    // ===== Concurrency Guard =====
    // We use a semaphore to cap *concurrent* in-flight requests from this service instance.
    // This is not an RPS limiter — it strictly controls concurrency (e.g., up to 1000 at a time).
    private final Semaphore rateLimiter;

    // ===== External API base URL (e.g., http://localhost:8081) =====
    @Value("${bulk-api.external-api.base-url}")
    private String externalApiBaseUrl;

    public BulkApiService(
            WebClient webClient,
            StatusRepository statusRepository,
            CircuitBreakerRegistry circuitBreakerRegistry
    ) {
        this.webClient = webClient;
        this.statusRepository = statusRepository;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("signalApi");
        this.rateLimiter = new Semaphore(100); // limit to 1000 concurrent calls
    }

    /**
     * Process a batch of messages asynchronously using the app’s task executor.
     * - We intentionally return a CompletableFuture to match the rest of your orchestration code.
     * - Internally each message call is fully reactive (Mono), with retry + circuit breaker.
     * - We do NOT persist status here; that happens inside each call pipeline (success/error).
     */
    @Async("bulkApiTaskExecutor")
    public CompletableFuture<Void> processBatch(List<ApiMessage> messages) {
        int size = messages == null ? 0 : messages.size();
        log.info("🚀 Starting batch of {} messages on thread={} | availablePermits={}",
                size, Thread.currentThread().getName(), rateLimiter.availablePermits());

        // Build a list of futures (each per message) and then wait for all of them
        List<CompletableFuture<Void>> futures = messages.stream()
                .map(this::postMessageReactiveToFuture) // returns CompletableFuture<Void>
                .toList();

        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("❌ Batch completed with errors: {}", ex.getMessage(), ex);
                    } else {
                        log.info("✅ Batch completed: {} messages processed", size);
                    }
                });
    }

    /**
     * Wraps the reactive pipeline of a single message POST into a CompletableFuture<Void>
     * for orchestration compatibility. All success/failure side-effects (status persistence, logs)
     * are handled inside the reactive pipeline.
     */
    private CompletableFuture<Void> postMessageReactiveToFuture(ApiMessage message) {
        return postMessageReactive(message)
                .then()          // Convert Mono<Map<...>> to Mono<Void> after side-effects
                .toFuture();     // Expose as CompletableFuture<Void>
    }

    /**
     * Core reactive pipeline for posting a single message.
     * Key points:
     *  - **Semaphore acquire/release** to cap concurrency.
     *  - **Timeout** to fail slow calls (mapped to error to trigger retry/breaker).
     *  - **Circuit Breaker** (Resilience4j) to short-circuit when downstream is unhealthy.
     *  - **Retry** (reactor.util.retry.Retry) on *retryable* exceptions only.
     *  - **Single-source persistence**: PASS on success (ceh_event_id present), FAIL on error/fallback.
     *  - No duplication between onError and fallback handlers; exactly-once status write per message.
     */
    private Mono<Map<String, Object>> postMessageReactive(ApiMessage message) {
        // Defensive nulls
        Objects.requireNonNull(message, "message must not be null");

        // BLOCKING acquire (on calling thread). We do it before we subscribe to the WebClient Mono,
        // because WebClient will run on reactor threads and we want concurrency bounded upfront.
        try {
            rateLimiter.acquire();
            log.debug("🔒 Acquired permit for uabsEventId={} | nowAvailable={}",
                    message.getUabsEventId(), rateLimiter.availablePermits());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // Treat as a failure; record FAIL and return a failed Mono.
            log.error("⚠️ Interrupted while acquiring semaphore for uabsEventId={}", message.getUabsEventId());
            persistFail(message, "INTERRUPTED_ACQUIRE");
            return Mono.error(ie);
        }

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
                // On success: validate ceh_event_id and persist PASS/FAIL accordingly
                .doOnSuccess(response -> {
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
                })
                // On any terminal error (after retries and/or breaker open), persist FAIL once
                .doOnError(ex -> handleException(message, ex))

                // Always release the permit at terminal signal (success or error)
                .doFinally(sig -> {
                    rateLimiter.release();
                    log.debug("🔓 Released permit for uabsEventId={} | nowAvailable={}",
                            message.getUabsEventId(), rateLimiter.availablePermits());
                })
                // In case upstream returns null somehow, normalize to empty map (shouldn't happen with retrieve())
                .switchIfEmpty(Mono.fromSupplier(Collections::emptyMap));
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
    private void persistPass(ApiMessage message, long cehEventId) {
        Long uabsEventId = message.getUabsEventId();
        StatusRecord rec = new StatusRecord(uabsEventId, cehEventId, "PASS");
        statusRepository.save(rec);
        log.debug("💾 Saved PASS status for uabsEventId={} ceh_event_id={}", uabsEventId, cehEventId);
    }

    /** Persist FAIL (ceh_event_id is null). Reason is logged in status table 'status' field. */
    private void persistFail(ApiMessage message, String reason) {
        Long uabsEventId = message.getUabsEventId();
        StatusRecord rec = new StatusRecord(uabsEventId, null, "FAIL");
        // Optional: if you want to store reason, add a column or audit table. For now we just log.
        statusRepository.save(rec);
        log.debug("💾 Saved FAIL status for uabsEventId={} reason={}", uabsEventId, reason);
    }

    private void handleException(ApiMessage message, Throwable ex) {
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

    /** Persist UNKNOWN (request sent but response not confirmed). */
    private void persistUnknown(ApiMessage message, String reason) {
        Long uabsEventId = message.getUabsEventId();
        StatusRecord rec = new StatusRecord(uabsEventId, null, "UNKNOWN");
        statusRepository.save(rec);
        log.debug("💾 Saved UNKNOWN status for uabsEventId={} reason={}", uabsEventId, reason);
    }


    /**
     * Synchronous helper (rarely used in bulk flows).
     * - Blocks the calling thread until response arrives.
     * - No explicit retries/circuit breaker here to keep it simple; use reactive path for resilience.
     * - Writes status PASS/FAIL exactly once.
     */
    public boolean postMessageSync(ApiMessage message) {
        try {
            Map<String, Object> response = webClient.post()
                    .uri(externalApiBaseUrl + "/create-signal")
                    .bodyValue(message)
                    .retrieve()
                    .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                    .block(Duration.ofSeconds(10)); // hard block with timeout

            Object ceh = response == null ? null : response.get("ceh_event_id");
            if (ceh != null) {
                long cehId = parseLongSafely(ceh);
                persistPass(message, cehId);
                log.info("✅ [SYNC] Posted uabsEventId={} | ceh_event_id={}", message.getUabsEventId(), cehId);
                return true;
            } else {
                persistFail(message, "NO_CEH_EVENT_ID_SYNC");
                log.warn("⚠️ [SYNC] No ceh_event_id returned for uabsEventId={}", message.getUabsEventId());
                return false;
            }
        } catch (Exception e) {
            persistFail(message, e.getClass().getSimpleName());
            log.error("❌ [SYNC] Failed to post uabsEventId={} | error={}", message.getUabsEventId(), e.toString(), e);
            return false;
        }
    }
}
