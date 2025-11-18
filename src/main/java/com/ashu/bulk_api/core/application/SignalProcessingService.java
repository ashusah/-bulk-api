package com.ashu.bulk_api.core.application;

import com.ashu.bulk_api.core.domain.job.BatchResult;
import com.ashu.bulk_api.core.domain.job.JobResult;
import com.ashu.bulk_api.core.domain.model.Signal;
import com.ashu.bulk_api.core.port.inbound.SignalProcessingUseCase;
import com.ashu.bulk_api.core.port.outbound.SignalBatchPort;
import com.ashu.bulk_api.core.port.outbound.SignalQueryPort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SignalProcessingService implements SignalProcessingUseCase {

    private final SignalBatchPort signalBatchPort;
    private final SignalQueryPort signalQueryPort;
    private final ThreadPoolTaskExecutor taskExecutor;
    private final int batchSize;
    private final JobProgressTracker jobProgressTracker;

    public SignalProcessingService(SignalBatchPort signalBatchPort,
                                   SignalQueryPort signalQueryPort,
                                   @Qualifier("bulkApiTaskExecutor") ThreadPoolTaskExecutor taskExecutor,
                                   @Value("${bulk-api.processing.batch-size:100}") int batchSize,
                                   JobProgressTracker jobProgressTracker) {
        this.signalBatchPort = signalBatchPort;
        this.signalQueryPort = signalQueryPort;
        this.taskExecutor = taskExecutor;
        this.batchSize = Math.max(1, batchSize);
        this.jobProgressTracker = jobProgressTracker;
    }

    @Override
    public JobResult processSignals(List<Signal> signals) {
        if (signals == null || signals.isEmpty()) {
            log.info("No signals received for synchronous processing");
            return new JobResult(0, 0, "No signals to process");
        }

        log.info("🚀 Starting bulk processing for {} signals", signals.size());
        logExecutorStats("Before submitting tasks");

        List<TrackedBatch> trackedBatches = new ArrayList<>();
        AtomicInteger batchCounter = new AtomicInteger();
        for (int start = 0; start < signals.size(); start += batchSize) {
            int end = Math.min(start + batchSize, signals.size());
            List<Signal> batch = new ArrayList<>(signals.subList(start, end));
            trackedBatches.add(submitBatch(batch, batchCounter.incrementAndGet()));
        }

        JobResult result = awaitJobCompletion(trackedBatches, "Synchronous request completed");
        log.info("✅ Synchronous processing finished. success={} failure={}",
                result.getSuccessCount(), result.getFailureCount());
        return result;
    }

    @Override
    public JobResult processSignalsFromDatabase() {
        return processSignalsFromDatabase(null);
    }

    @Override
    public JobResult processSignalsFromDatabase(String jobId) {
        log.info("Querying database for all signals...");
        logExecutorStats("Before submitting tasks");
        List<TrackedBatch> trackedBatches = new ArrayList<>();
        List<Signal> buffer = new ArrayList<>(batchSize);
        AtomicInteger totalRows = new AtomicInteger();
        AtomicInteger batchCounter = new AtomicInteger();

        signalQueryPort.streamAllSignals(signal -> {
            buffer.add(signal);
            totalRows.incrementAndGet();
            if (buffer.size() == batchSize) {
                trackedBatches.add(submitBatch(new ArrayList<>(buffer), batchCounter.incrementAndGet()));
                buffer.clear();
            }
        });

        if (!buffer.isEmpty()) {
            trackedBatches.add(submitBatch(new ArrayList<>(buffer), batchCounter.incrementAndGet()));
        }

        log.info("Fetched {} signals from DB", totalRows.get());

        JobProgressTracker.JobProgress jobProgress = jobProgressTracker.start(jobId, trackedBatches.size());
        attachProgressLogging(jobProgress, trackedBatches);

        String message = totalRows.get() == 0 ? "No signals found in DB" : "DB job completed";
        JobResult result = awaitJobCompletion(trackedBatches, message);
        log.info("✅ DB processing finished. success={} failure={}",
                result.getSuccessCount(), result.getFailureCount());
        return result;
    }

    @Override
    public Optional<Signal> findLatestSignalByAgreementId(long agreementId) {
        return signalQueryPort.findLatestByAgreementId(agreementId);
    }

    private void logExecutorStats(String stage) {
        log.info("[{}] Active threads: {}, Pool size: {}, Queue size: {}",
                stage,
                taskExecutor.getActiveCount(),
                taskExecutor.getPoolSize(),
                taskExecutor.getThreadPoolExecutor().getQueue().size());
    }

    private TrackedBatch submitBatch(List<Signal> batch, int batchNumber) {
        logExecutorStats("Submitting batch #" + batchNumber + " of size " + batch.size());
        CompletableFuture<BatchResult> future = signalBatchPort.submitBatch(batch);
        return new TrackedBatch(future, batchNumber, batch.size());
    }

    private void attachProgressLogging(JobProgressTracker.JobProgress jobProgress, List<TrackedBatch> trackedBatches) {
        if (trackedBatches.isEmpty()) {
            return;
        }
        trackedBatches.forEach(batch ->
                batch.future().thenAccept(result ->
                        jobProgressTracker.onBatchCompletion(jobProgress, batch.number(), batch.size(), result)));
    }

    private JobResult awaitJobCompletion(List<TrackedBatch> trackedBatches, String message) {
        if (trackedBatches.isEmpty()) {
            return new JobResult(0, 0, message);
        }

        List<CompletableFuture<BatchResult>> futures = trackedBatches.stream()
                .map(TrackedBatch::future)
                .collect(Collectors.toList());

        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        all.join();

        List<BatchResult> results = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        int success = results.stream()
                .mapToInt(BatchResult::successCount)
                .sum();

        int failure = results.stream()
                .mapToInt(BatchResult::failureCount)
                .sum();

        return new JobResult(success, failure, message);
    }

    private record TrackedBatch(CompletableFuture<BatchResult> future, int number, int size) {}
}
