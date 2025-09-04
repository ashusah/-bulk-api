package com.ashu.bulk_api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApiMessage {
    private long uabsEventId;
    private String eventType;
    private String eventStatus;
    private int unauthorizedDebitBalance;
    private LocalDateTime eventRecordDateTime;
    private LocalDate bookDate;
    private int grv;
    private long productId;
    private long agreementId;
    private LocalDate signalStartDate;
    private LocalDate signalEndDate;
}

