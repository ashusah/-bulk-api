package com.ashu.bulk_api.controller;

import com.ashu.bulk_api.dto.AsyncJobResponse;
import com.ashu.bulk_api.dto.JobResult;
import com.ashu.bulk_api.model.ApiMessage;
import com.ashu.bulk_api.orchestrator.BulkApiOrchestrator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/bulk")
@Slf4j
public class BulkApiController {

    private static final String JOB_KEY_PREFIX = "job:";

    private final BulkApiOrchestrator orchestrator;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ThreadPoolTaskExecutor taskExecutor;
    private final Duration jobTtl;

    public BulkApiController(BulkApiOrchestrator orchestrator,
                             RedisTemplate<String, Object> redisTemplate,
                             @Qualifier("bulkApiTaskExecutor") ThreadPoolTaskExecutor taskExecutor,
                             @Value("${bulk-api.processing.job-ttl-hours:24}") long jobTtlHours) {
        this.orchestrator = orchestrator;
        this.redisTemplate = redisTemplate;
        this.taskExecutor = taskExecutor;
        this.jobTtl = Duration.ofHours(Math.max(1, jobTtlHours));
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
                .supplyAsync(orchestrator::processAllMessagesFromDb, taskExecutor);

        jobFuture.thenAccept(result -> cacheJobResult(jobId, result))
                .exceptionally(ex -> {
                    log.error("Async DB job {} failed", jobId, ex);
                    JobResult failure = new JobResult(0, 0, "FAILED: " + ex.getMessage());
                    cacheJobResult(jobId, failure);
                    return null;
                });

        return ResponseEntity.accepted().body(AsyncJobResponse.accepted(jobId));
    }

    @PostMapping("/process-sync")
    public ResponseEntity<JobResult> processSync(@RequestBody List<ApiMessage> messages) {
        JobResult result = orchestrator.processAllMessages(messages); // Same orchestrator
        return ResponseEntity.ok(result);
    }

    @GetMapping("/job/{jobId}")
    public ResponseEntity<Object> getJobStatus(@PathVariable String jobId) {
        Object result = redisTemplate.opsForValue().get(jobKey(jobId));
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }

    private void cacheJobResult(String jobId, JobResult jobResult) {
        redisTemplate.opsForValue().set(jobKey(jobId), jobResult, jobTtl);
    }

    private String jobKey(String jobId) {
        return JOB_KEY_PREFIX + jobId;
    }
}
