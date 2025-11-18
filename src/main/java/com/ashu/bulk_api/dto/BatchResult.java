package com.ashu.bulk_api.dto;

/**
 * Lightweight summary of a batch submission. Keeps track of how many messages
 * completed successfully vs. failed so the orchestrator can aggregate across
 * multiple batches without re-scanning the database.
 */
public record BatchResult(int successCount, int failureCount) {

    public static BatchResult empty() {
        return new BatchResult(0, 0);
    }

    public BatchResult merge(BatchResult other) {
        if (other == null) {
            return this;
        }
        return new BatchResult(this.successCount + other.successCount,
                this.failureCount + other.failureCount);
    }

    public static BatchResult fromBooleans(Iterable<Boolean> results) {
        int success = 0;
        int failure = 0;
        if (results != null) {
            for (Boolean result : results) {
                if (Boolean.TRUE.equals(result)) {
                    success++;
                } else {
                    failure++;
                }
            }
        }
        return new BatchResult(success, failure);
    }
}
