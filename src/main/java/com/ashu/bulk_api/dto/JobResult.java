package com.ashu.bulk_api.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobResult {

    private int successCount;
    private int failureCount;
    private long timestamp;
    private String message; // optional, for errors

    // Convenience constructor for message + auto timestamp
    public JobResult(int successCount, int failureCount, String message) {
        this.successCount = successCount;
        this.failureCount = failureCount;
        this.message = message;
        this.timestamp = System.currentTimeMillis();
    }
}
