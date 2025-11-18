package com.ashu.bulk_api.core.port.outbound;

import com.ashu.bulk_api.core.domain.model.Signal;

import java.util.Optional;
import java.util.function.Consumer;

public interface SignalQueryPort {
    void streamAllSignals(Consumer<Signal> consumer);

    Optional<Signal> findLatestByAgreementId(long agreementId);
}
