package com.ashu.bulk_api.adapters.outbound.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "status")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StatusRecord {

    @Id
    @Column(name = "uabs_event_id")
    private Long uabsEventId;

    @Column(name = "ceh_event_id")
    private Long cehEventId;

    @Column(name = "status", nullable = false, length = 32)
    private String status;
}
