package com.ashu.bulk_api.core.port.outbound;

import com.ashu.bulk_api.core.domain.job.BatchResult;
import com.ashu.bulk_api.core.domain.model.Signal;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SignalBatchPort {
    CompletableFuture<BatchResult> submitBatch(List<Signal> signals);
}
