package com.ashu.bulk_api.orchestrator;

import com.ashu.bulk_api.dto.BatchResult;
import com.ashu.bulk_api.dto.JobResult;
import com.ashu.bulk_api.model.ApiMessage;
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

    public BulkApiOrchestrator(BulkApiService bulkApiService,
                               JdbcTemplate jdbcTemplate,
                               @Qualifier("bulkApiTaskExecutor") ThreadPoolTaskExecutor taskExecutor,
                               @Value("${bulk-api.processing.batch-size:100}") int batchSize) {
        this.bulkApiService = bulkApiService;
        this.jdbcTemplate = jdbcTemplate;
        this.taskExecutor = taskExecutor;
        this.batchSize = Math.max(1, batchSize);
    }

    public JobResult processAllMessages(List<ApiMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            log.info("No messages received for synchronous processing");
            return new JobResult(0, 0, "No messages to process");
        }

        log.info("🚀 Starting bulk processing for {} messages", messages.size());
        logExecutorStats("Before submitting tasks");

        List<CompletableFuture<BatchResult>> batchFutures = new ArrayList<>();
        for (int start = 0; start < messages.size(); start += batchSize) {
            int end = Math.min(start + batchSize, messages.size());
            List<ApiMessage> batch = new ArrayList<>(messages.subList(start, end));
            batchFutures.add(submitBatch(batch));
        }

        JobResult result = awaitJobCompletion(batchFutures, "Synchronous request completed");
        log.info("✅ Synchronous processing finished. success={} failure={}",
                result.getSuccessCount(), result.getFailureCount());
        return result;
    }

    public JobResult processAllMessagesFromDb() {
        log.info("Querying database for all signals...");
        logExecutorStats("Before submitting tasks");
        List<CompletableFuture<BatchResult>> batchFutures = new ArrayList<>();
        List<ApiMessage> buffer = new ArrayList<>(batchSize);
        AtomicInteger totalRows = new AtomicInteger();

        jdbcTemplate.query(SIGNAL_SELECT, rs -> {
            buffer.add(mapRow(rs));
            totalRows.incrementAndGet();
            if (buffer.size() == batchSize) {
                batchFutures.add(submitBatch(new ArrayList<>(buffer)));
                buffer.clear();
            }
        });

        if (!buffer.isEmpty()) {
            batchFutures.add(submitBatch(new ArrayList<>(buffer)));
        }

        log.info("Fetched {} messages from DB", totalRows.get());

        String message = totalRows.get() == 0 ? "No signals found in DB" : "DB job completed";
        JobResult result = awaitJobCompletion(batchFutures, message);
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

    private CompletableFuture<BatchResult> submitBatch(List<ApiMessage> batch) {
        logExecutorStats("Submitting batch of size " + batch.size());
        return bulkApiService.processBatch(batch);
    }

    private JobResult awaitJobCompletion(List<CompletableFuture<BatchResult>> batchFutures, String message) {
        if (batchFutures.isEmpty()) {
            return new JobResult(0, 0, message);
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]));
        all.join();

        List<BatchResult> results = batchFutures.stream()
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
}

