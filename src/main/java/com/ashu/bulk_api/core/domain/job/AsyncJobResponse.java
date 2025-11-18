package com.ashu.bulk_api.core.domain.job;

/**
 * Lightweight response payload for asynchronous DB jobs so clients get a
 * structured job identifier instead of a plain String message.
 */
public record AsyncJobResponse(String jobId, String status) {
    public static AsyncJobResponse accepted(String jobId) {
        return new AsyncJobResponse(jobId, "Job accepted");
    }
}
