package com.ashu.bulk_api.controller;

import com.ashu.bulk_api.dto.AsyncJobResponse;
import com.ashu.bulk_api.dto.JobResult;
import com.ashu.bulk_api.model.ApiMessage;
import com.ashu.bulk_api.orchestrator.BulkApiOrchestrator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/bulk")
@Slf4j
public class BulkApiController {

    private final BulkApiOrchestrator orchestrator;
    private final ThreadPoolTaskExecutor taskExecutor;

    public BulkApiController(BulkApiOrchestrator orchestrator,
                             @Qualifier("bulkApiTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        this.orchestrator = orchestrator;
        this.taskExecutor = taskExecutor;
    }

    @PostMapping("/process-db")
    public ResponseEntity<JobResult> processBulkFromDb() {
        JobResult result = orchestrator.processAllMessagesFromDb();
        return ResponseEntity.ok(result);
    }

    @PostMapping("/process-db-async")
    public ResponseEntity<AsyncJobResponse> processBulkFromDbAsync() {
        String jobId = UUID.randomUUID().toString();

        CompletableFuture<JobResult> jobFuture = CompletableFuture
                .supplyAsync(() -> orchestrator.processAllMessagesFromDb(jobId), taskExecutor);

        jobFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Async DB job {} failed", jobId, ex);
            } else {
                log.info("Async DB job {} finished: success={} failure={} message={}",
                        jobId, result.getSuccessCount(), result.getFailureCount(), result.getMessage());
            }
        });

        return ResponseEntity.accepted().body(AsyncJobResponse.accepted(jobId));
    }

    @PostMapping("/process-sync")
    public ResponseEntity<JobResult> processSync(@RequestBody List<ApiMessage> messages) {
        JobResult result = orchestrator.processAllMessages(messages); // Same orchestrator
        return ResponseEntity.ok(result);
    }

}
