package com.ashu.bulk_api.adapters.inbound.rest;

import com.ashu.bulk_api.core.domain.job.AsyncJobResponse;
import com.ashu.bulk_api.core.domain.job.JobResult;
import com.ashu.bulk_api.core.domain.model.Signal;
import com.ashu.bulk_api.core.port.inbound.SignalProcessingUseCase;
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
public class SignalController {

    private final SignalProcessingUseCase signalProcessingUseCase;
    private final ThreadPoolTaskExecutor taskExecutor;

    public SignalController(SignalProcessingUseCase signalProcessingUseCase,
                            @Qualifier("bulkApiTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        this.signalProcessingUseCase = signalProcessingUseCase;
        this.taskExecutor = taskExecutor;
    }

    @PostMapping("/process-db")
    public ResponseEntity<JobResult> processBulkFromDb() {
        JobResult result = signalProcessingUseCase.processSignalsFromDatabase();
        return ResponseEntity.ok(result);
    }

    @PostMapping("/process-db-async")
    public ResponseEntity<AsyncJobResponse> processBulkFromDbAsync() {
        String jobId = UUID.randomUUID().toString();

        CompletableFuture<JobResult> jobFuture = CompletableFuture
                .supplyAsync(() -> signalProcessingUseCase.processSignalsFromDatabase(jobId), taskExecutor);

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
    public ResponseEntity<JobResult> processSync(@RequestBody List<Signal> messages) {
        JobResult result = signalProcessingUseCase.processSignals(messages);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/signals/{agreementId}")
    public ResponseEntity<Signal> findLatestSignal(@PathVariable("agreementId") long agreementId) {
        return signalProcessingUseCase.findLatestSignalByAgreementId(agreementId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

}
