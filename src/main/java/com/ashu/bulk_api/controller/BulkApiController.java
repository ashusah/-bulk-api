package com.ashu.bulk_api.controller;

import com.ashu.bulk_api.dto.JobResult;
import com.ashu.bulk_api.model.ApiMessage;
import com.ashu.bulk_api.orchestrator.BulkApiOrchestrator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/bulk")
@Slf4j
public class BulkApiController {

    private final BulkApiOrchestrator orchestrator;
    private final RedisTemplate<String, Object> redisTemplate;

    public BulkApiController(BulkApiOrchestrator orchestrator, RedisTemplate<String, Object> redisTemplate) {
        this.orchestrator = orchestrator;
        this.redisTemplate = redisTemplate;
    }

    @PostMapping("/process-db")
    public ResponseEntity<String> processBulkFromDb() {
        orchestrator.processAllMessagesFromDb();
        return ResponseEntity.ok("Processing started synchronously");
    }

    @PostMapping("/process-db-async")
    public ResponseEntity<String> processBulkFromDbAsync() {
        String jobId = UUID.randomUUID().toString();

        CompletableFuture.runAsync(() -> {
            orchestrator.processAllMessagesFromDb();
            redisTemplate.opsForValue().set("job:" + jobId, "Completed", Duration.ofHours(24));
        });

        return ResponseEntity.accepted().body("Job started with ID: " + jobId);
    }

    @PostMapping("/process-sync")
    public ResponseEntity<JobResult> processSync(@RequestBody List<ApiMessage> messages) {
        JobResult result = orchestrator.processAllMessages(messages); // Same orchestrator
        return ResponseEntity.ok(result);
    }
}
