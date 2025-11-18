package com.ashu.bulk_api.core.application;

import com.ashu.bulk_api.core.domain.job.BatchResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple in-memory tracker that logs job progress without persisting anything to Redis.
 */
@Component
@Slf4j
public class JobProgressTracker {

    public JobProgress start(String jobId, int totalBatches) {
        if (jobId == null || totalBatches <= 0) {
            return JobProgress.disabled();
        }
        JobProgress progress = new JobProgress(jobId, totalBatches);
        log.info("Job {} started with {} batches", jobId, totalBatches);
        return progress;
    }

    public void onBatchCompletion(JobProgress progress, int batchNumber, int batchSize, BatchResult result) {
        if (!progress.enabled) {
            return;
        }
        int completed = progress.completedBatches.incrementAndGet();
        int cumulativeSuccess = progress.successCount.addAndGet(result.successCount());
        int cumulativeFailure = progress.failureCount.addAndGet(result.failureCount());

        log.info("Job {} batch #{} (size {}) complete. Batch success={} failure={}. Progress: {}/{} batches done (success={}, failure={})",
                progress.jobId, batchNumber, batchSize,
                result.successCount(), result.failureCount(),
                completed, progress.totalBatches, cumulativeSuccess, cumulativeFailure);

        if (completed >= progress.totalBatches) {
            log.info("Job {} finished. Total success={} failure={}",
                    progress.jobId, cumulativeSuccess, cumulativeFailure);
        }
    }

    public static final class JobProgress {
        private final String jobId;
        private final int totalBatches;
        private final AtomicInteger completedBatches = new AtomicInteger();
        private final AtomicInteger successCount = new AtomicInteger();
        private final AtomicInteger failureCount = new AtomicInteger();
        private final boolean enabled;

        private JobProgress(String jobId, int totalBatches, boolean enabled) {
            this.jobId = jobId;
            this.totalBatches = totalBatches;
            this.enabled = enabled;
        }

        private static JobProgress disabled() {
            return new JobProgress(null, 0, false);
        }
    }
}
