package com.ashu.bulk_api.orchestrator;

import com.ashu.bulk_api.dto.BatchResult;
import com.ashu.bulk_api.dto.JobResult;
import com.ashu.bulk_api.model.ApiMessage;
import com.ashu.bulk_api.orchestrator.JobProgressTracker.JobProgress;
import com.ashu.bulk_api.service.BulkApiService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component
@Slf4j
public class BulkApiOrchestrator {

    private final BulkApiService bulkApiService;
    private final JdbcTemplate jdbcTemplate;
    private static final String SIGNAL_SELECT = "SELECT uabs_event_id,event_record_date_time, event_type, event_status, " +
            "unauthorized_debit_balance, book_date, grv, product_id, agreement_id, signal_start_date, signal_end_date FROM signals";

    private final ThreadPoolTaskExecutor taskExecutor;
    private final int batchSize;
    private final JobProgressTracker jobProgressTracker;

    public BulkApiOrchestrator(BulkApiService bulkApiService,
                               JdbcTemplate jdbcTemplate,
                               @Qualifier("bulkApiTaskExecutor") ThreadPoolTaskExecutor taskExecutor,
                               @Value("${bulk-api.processing.batch-size:100}") int batchSize,
                               JobProgressTracker jobProgressTracker) {
        this.bulkApiService = bulkApiService;
        this.jdbcTemplate = jdbcTemplate;
        this.taskExecutor = taskExecutor;
        this.batchSize = Math.max(1, batchSize);
        this.jobProgressTracker = jobProgressTracker;
    }

    public JobResult processAllMessages(List<ApiMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            log.info("No messages received for synchronous processing");
            return new JobResult(0, 0, "No messages to process");
        }

        log.info("🚀 Starting bulk processing for {} messages", messages.size());
        logExecutorStats("Before submitting tasks");

        List<TrackedBatch> trackedBatches = new ArrayList<>();
        AtomicInteger batchCounter = new AtomicInteger();
        for (int start = 0; start < messages.size(); start += batchSize) {
            int end = Math.min(start + batchSize, messages.size());
            List<ApiMessage> batch = new ArrayList<>(messages.subList(start, end));
            trackedBatches.add(submitBatch(batch, batchCounter.incrementAndGet()));
        }

        JobResult result = awaitJobCompletion(trackedBatches, "Synchronous request completed");
        log.info("✅ Synchronous processing finished. success={} failure={}",
                result.getSuccessCount(), result.getFailureCount());
        return result;
    }

    public JobResult processAllMessagesFromDb() {
        return processAllMessagesFromDb(null);
    }

    public JobResult processAllMessagesFromDb(String jobId) {
        log.info("Querying database for all signals...");
        logExecutorStats("Before submitting tasks");
        List<TrackedBatch> trackedBatches = new ArrayList<>();
        List<ApiMessage> buffer = new ArrayList<>(batchSize);
        AtomicInteger totalRows = new AtomicInteger();
        AtomicInteger batchCounter = new AtomicInteger();

        jdbcTemplate.query(SIGNAL_SELECT, rs -> {
            buffer.add(mapRow(rs));
            totalRows.incrementAndGet();
            if (buffer.size() == batchSize) {
                trackedBatches.add(submitBatch(new ArrayList<>(buffer), batchCounter.incrementAndGet()));
                buffer.clear();
            }
        });

        if (!buffer.isEmpty()) {
            trackedBatches.add(submitBatch(new ArrayList<>(buffer), batchCounter.incrementAndGet()));
        }

        log.info("Fetched {} messages from DB", totalRows.get());

        JobProgress jobProgress = jobProgressTracker.start(jobId, trackedBatches.size());
        attachProgressLogging(jobProgress, trackedBatches);

        String message = totalRows.get() == 0 ? "No signals found in DB" : "DB job completed";
        JobResult result = awaitJobCompletion(trackedBatches, message);
        log.info("✅ DB processing finished. success={} failure={}",
                result.getSuccessCount(), result.getFailureCount());
        return result;
    }

    private void logExecutorStats(String stage) {
        log.info("[{}] Active threads: {}, Pool size: {}, Queue size: {}",
                stage,
                taskExecutor.getActiveCount(),
                taskExecutor.getPoolSize(),
                taskExecutor.getThreadPoolExecutor().getQueue().size()
        );
    }

    private TrackedBatch submitBatch(List<ApiMessage> batch, int batchNumber) {
        logExecutorStats("Submitting batch #" + batchNumber + " of size " + batch.size());
        CompletableFuture<BatchResult> future = bulkApiService.processBatch(batch);
        return new TrackedBatch(future, batchNumber, batch.size());
    }

    private void attachProgressLogging(JobProgress jobProgress, List<TrackedBatch> trackedBatches) {
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

    private ApiMessage mapRow(ResultSet rs) throws SQLException {
        return new ApiMessage(
                rs.getLong("uabs_event_id"),
                rs.getString("event_type"),
                rs.getString("event_status"),
                rs.getInt("unauthorized_debit_balance"),
                rs.getTimestamp("event_record_date_time").toLocalDateTime(),
                rs.getDate("book_date").toLocalDate(),
                rs.getInt("grv"),
                rs.getLong("product_id"),
                rs.getLong("agreement_id"),
                rs.getDate("signal_start_date").toLocalDate(),
                rs.getDate("signal_end_date").toLocalDate()
        );
    }

    private record TrackedBatch(CompletableFuture<BatchResult> future, int number, int size) {}
}

