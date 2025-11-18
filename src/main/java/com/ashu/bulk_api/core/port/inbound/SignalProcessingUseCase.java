package com.ashu.bulk_api.core.port.inbound;

import com.ashu.bulk_api.core.domain.job.JobResult;
import com.ashu.bulk_api.core.domain.model.Signal;

import java.util.List;
import java.util.Optional;

public interface SignalProcessingUseCase {
    JobResult processSignals(List<Signal> signals);

    JobResult processSignalsFromDatabase();

    JobResult processSignalsFromDatabase(String jobId);

    Optional<Signal> findLatestSignalByAgreementId(long agreementId);
}
