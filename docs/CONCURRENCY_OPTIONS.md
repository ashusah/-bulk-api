# Concurrency control options for `BulkApiService`

The service currently caps concurrent HTTP calls with a classic `Semaphore`, but there are two
reactive alternatives that achieve the same goal without blocking the calling thread. The snippets
below show how each approach would look inside `BulkApiService` and call out the trade-offs so you
can pick the right guardrail for your deployment.

> The examples omit unrelated details (status persistence, retries, etc.) to focus on concurrency.

## 1. Blocking semaphore guard (current implementation)

```java
private final Semaphore rateLimiter = new Semaphore(maxConcurrentRequests);

private Mono<Boolean> postMessageReactive(ApiMessage message) {
    try {
        rateLimiter.acquire(); // blocks task-executor thread until a permit is available
    } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return Mono.just(false);
    }

    return webClient.post()
            .uri(externalApiBaseUrl + "/create-signal")
            .bodyValue(message)
            .retrieve()
            .bodyToMono(Void.class)
            .onErrorReturn(false)
            .doFinally(__ -> rateLimiter.release());
}
```

| Aspect | Advantages | Disadvantages |
| --- | --- | --- |
| Simplicity | Very small code change; easy to reason about global concurrency because permits are explicit. | None in terms of ergonomics. |
| Resource usage | Guarantees hard ceiling per service instance, even if downstream code is non-reactive. | Permit acquisition blocks the calling thread, so idle task threads stack up when the downstream is slow. |
| Observability | Easy to log available permits before/after each call. | Blocking makes it harder to scale when the executor has limited threads (it can become a bottleneck). |

**Trade-off:** best when you have ample executor threads and want deterministic behavior, but it ties
up those threads whenever the downstream is saturated.

## 2. Reactor-native concurrency (`flatMap` with a `concurrency` hint)

```java
Flux.fromIterable(messages)
        .flatMap(msg -> postMessageReactive(msg)
                        .subscribeOn(Schedulers.boundedElastic()),
                /* concurrency */ 10)
        .collectList()
        .map(BatchResult::fromBooleans);
```

| Aspect | Advantages | Disadvantages |
| --- | --- | --- |
| Non-blocking | Reactor enforces the concurrency limit internally, so producer threads never block waiting for permits. | Requires the batch orchestration to be reactive end-to-end (Flux instead of `CompletableFuture`). |
| Back pressure | `flatMap(..., concurrency=N)` and `limitRate` provide fine-grained control, naturally smoothing bursts and preventing DDOS-like spikes. | Harder to instrument at the “permit” level—monitoring relies on Reactor metrics rather than simple semaphore counts. |
| Integration | Plays nicely with other reactive operators (retry, circuit breaker, timeout). | Debugging becomes more conceptual (need to reason about Flux lifecycles and schedulers). |

**Trade-off:** great when you want maximum throughput from a small number of threads, but you pay
with additional Reactor plumbing and the need to keep orchestration reactive.

## 3. Resilience4j reactive bulkhead (non-blocking permits + metrics)

```java
BulkheadConfig bulkheadConfig = BulkheadConfig.custom()
        .maxConcurrentCalls(10)
        .maxWaitDuration(Duration.ZERO) // fail fast instead of blocking
        .build();
Bulkhead bulkhead = Bulkhead.of("signalApiBulkhead", bulkheadConfig);

private Mono<Boolean> postMessageReactive(ApiMessage message) {
    return webClient.post()
            .uri(externalApiBaseUrl + "/create-signal")
            .bodyValue(message)
            .retrieve()
            .bodyToMono(Void.class)
            .transformDeferred(BulkheadOperator.of(bulkhead))
            .onErrorResume(BulkheadFullException.class, ex -> {
                log.warn("Bulkhead full; rejecting uabsEventId={} immediately", message.getUabsEventId());
                return Mono.just(false);
            });
}
```

| Aspect | Advantages | Disadvantages |
| --- | --- | --- |
| Non-blocking | Calls fail fast with `BulkheadFullException` instead of tying up threads, eliminating semaphore-induced blocking. | Requires pulling in and configuring another Resilience4j component. |
| Observability | Exposes built-in metrics (Micrometer) for concurrent calls, queue depth, and rejections—easy to alert on DDOS-like spikes. | Slightly more overhead per request because each call passes through the bulkhead instrumentation. |
| Policy flexibility | Supports a wait queue (`maxWaitDuration`), isolation per dependency, and dynamic configuration via `BulkheadRegistry`. | Complexity increases—multiple bulkheads for different downstreams must be managed centrally. |

**Trade-off:** combines non-blocking behavior with strong observability. Ideal when you already use
Resilience4j and want operator-friendly metrics, but it adds another moving part to configure and
monitor.

---

### Choosing the right guard

| Requirement | Best fit |
| --- | --- |
| “Keep code simple, OK if threads block a little.” | Semaphore |
| “We need reactive end-to-end flow and want to squeeze throughput out of a small executor.” | Reactor `flatMap` concurrency |
| “We need strong metrics/alerting and fast-fail behavior under DDOS-like bursts.” | Resilience4j bulkhead |

Each approach still works with the existing retry logic, timeout, and circuit breaker. The main
question is whether you prefer blocking simplicity (semaphore) or a fully non-blocking, instrumented
solution (Reactor/bulkhead) for protecting the downstream endpoint.
