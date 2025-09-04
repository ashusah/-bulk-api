package com.ashu.bulk_api.orchestrator;

import com.ashu.bulk_api.dto.JobResult;
import com.ashu.bulk_api.model.ApiMessage;
import com.ashu.bulk_api.service.BulkApiService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class BulkApiOrchestrator {

    private final BulkApiService bulkApiService;
    private final JdbcTemplate jdbcTemplate;
    private final ThreadPoolTaskExecutor taskExecutor;


    public BulkApiOrchestrator(BulkApiService bulkApiService, JdbcTemplate jdbcTemplate,
                               @Qualifier("bulkApiTaskExecutor") TaskExecutor bulkApiTaskExecutor, ThreadPoolTaskExecutor taskExecutor) {
        this.bulkApiService = bulkApiService;
        this.jdbcTemplate = jdbcTemplate;
        this.taskExecutor = (ThreadPoolTaskExecutor) bulkApiTaskExecutor;;
    }

    public JobResult processAllMessages(List<ApiMessage> messages) {

        log.info("🚀 Starting bulk processing for {} messages", messages.size());

        logExecutorStats("Before submitting tasks");

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (ApiMessage message : messages) {
            boolean success = bulkApiService.postMessageSync(message); // sync post
            if (success) successCount.incrementAndGet();
            else failureCount.incrementAndGet();
        }

        return new JobResult(successCount.get(), failureCount.get(), "sent");
    }

    public void processAllMessagesFromDb() {
        log.info("Querying database for all signals...");
        logExecutorStats("Before submitting tasks");

        List<ApiMessage> allMessages = jdbcTemplate.query(
                "SELECT uabs_event_id,event_record_date_time, event_type, event_status, unauthorized_debit_balance, " +
                        "book_date, grv, product_id, agreement_id, signal_start_date, signal_end_date " +
                        "FROM signals",
                (rs, rowNum) -> new ApiMessage(
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
                )
        );

        log.info("Fetched {} messages from DB", allMessages.size());

        // Process in batches of 100
        int batchSize = 100;
        for (int i = 0; i < allMessages.size(); i += batchSize) {
            int end = Math.min(i + batchSize, allMessages.size());
            List<ApiMessage> batch = allMessages.subList(i, end);
            bulkApiService.processBatch(batch);

            logExecutorStats("Submitted batch of size " + batch.size());

        }
    }

    private void logExecutorStats(String stage) {
        log.info("[{}] Active threads: {}, Pool size: {}, Queue size: {}",
                stage,
                taskExecutor.getActiveCount(),
                taskExecutor.getPoolSize(),
                taskExecutor.getThreadPoolExecutor().getQueue().size()
        );
    }
}

