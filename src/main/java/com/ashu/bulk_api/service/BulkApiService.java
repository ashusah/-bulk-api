package com.ashu.bulk_api.service;

import com.ashu.bulk_api.jpa.StatusRecord;
import com.ashu.bulk_api.jpa.StatusRepository;
import com.ashu.bulk_api.model.ApiMessage;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

@Service
@Slf4j
public class BulkApiService {

    private final WebClient webClient;
    private final Semaphore rateLimiter;
    private final StatusRepository statusRepository;

    @Value("${bulk-api.external-api.base-url}")
    private String externalApiBaseUrl;

    public BulkApiService(WebClient webClient, StatusRepository statusRepository) {
        this.webClient = webClient;
        this.statusRepository = statusRepository;
        this.rateLimiter = new Semaphore(1000); // 1000 concurrent requests
    }

    @Async("bulkApiTaskExecutor")
    public CompletableFuture<Void> processBatch(List<ApiMessage> messages) {
        log.info("🚀 Starting batch of {} messages on thread: {} | Available permits: {}",
                messages.size(),
                Thread.currentThread().getName(),
                rateLimiter.availablePermits());

        List<CompletableFuture<Map<String, Object>>> futures = messages.stream()
                .map(this::postMessage)
                .toList();

        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        log.error("❌ Batch failed with error: {}", ex.getMessage());
                    } else {
                        log.info("✅ Completed batch of {} messages", messages.size());
                    }
                });
    }

    @CircuitBreaker(name = "signalApi", fallbackMethod = "signalFallback")
    @Retry(name = "signalApi")
    public CompletableFuture<Map<String, Object>> postMessage(ApiMessage message) {
        try {
            rateLimiter.acquire();
            log.debug("🔒 Acquired permit for productId={} | Remaining permits={}",
                    message.getProductId(),
                    rateLimiter.availablePermits());

            return webClient.post()
                    .uri(externalApiBaseUrl + "/create-signal")
                    .bodyValue(message)
                    .retrieve()
                    // Expect JSON response with ceh_event_id
                    .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                    .timeout(Duration.ofSeconds(10))
                    .doFinally(signal -> {
                        rateLimiter.release();
                        log.debug("🔓 Released permit for UabseventID={} | Available permits={}",
                                message.getUabsEventId(),
                                rateLimiter.availablePermits());
                    })
                    .toFuture()
                    .whenComplete((response, throwable) -> {
                        if (throwable != null) {
                            log.error("❌ Error posting message for UabseventID={} on thread={} | error={}",
                                    message.getUabsEventId(),
                                    Thread.currentThread().getName(),
                                    throwable.getMessage());

                            // Save FAIL into DB
                            saveStatus(message.getUabsEventId(), null, "FAIL");
                        } else {
                            Object cehEventId = response.get("ceh_event_id");
                            if (cehEventId != null) {
                                log.info("✅ Successfully posted message for eventID={} on thread={} | ceh_event_id={}",
                                        message.getUabsEventId(),
                                        Thread.currentThread().getName(),
                                        cehEventId);

                                // Save PASS into DB
                                saveStatus(message.getUabsEventId(), Long.parseLong(cehEventId.toString()), "PASS");

                            } else {
                                log.warn("⚠️ No ceh_event_id returned for uabsEventId {} on thread={}",
                                        message.getUabsEventId(),
                                        Thread.currentThread().getName());

                                // Save FAIL into DB
                                saveStatus(message.getUabsEventId(), null, "FAIL");
                            }
                        }
                    });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }
    }

    private void saveStatus(Long uabsEventId, Long cehEventId, String status) {
        StatusRecord record = new StatusRecord(uabsEventId, cehEventId, status);
        statusRepository.save(record);
        log.info("💾 Saved status record: {}", record);
    }

    // 👇 Fallback kicks in when circuit breaker is OPEN or retries exhausted
    private CompletableFuture<Map<String, Object>> signalFallback(ApiMessage message, Throwable ex) {
        log.error("⚠️ Fallback for uabsEventId={} due to: {}", message.getUabsEventId(), ex.getMessage());

        // Save FAIL status immediately
        saveStatus(message.getUabsEventId(), null, "FAIL");

        // Return empty response so the pipeline continues
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    public boolean postMessageSync(ApiMessage message) {

        try {
            webClient.post()
                    .uri(externalApiBaseUrl + "/create-signal")
                    .bodyValue(message)
                    .retrieve()
                    .bodyToMono(Void.class) // we don't need a body
                    .block(); // synchronous blocking call

            log.info("Successfully posted message: {}", message);
            return true;

        } catch (Exception e) {
            log.error("Failed to post message: {}, error: {}", message, e.getMessage());
            return false;
        }
    }
}
