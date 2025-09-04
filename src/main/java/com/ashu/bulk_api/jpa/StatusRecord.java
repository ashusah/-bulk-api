package com.ashu.bulk_api.jpa;

import jakarta.persistence.*;
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

    @Column(name = "status", nullable = false, length = 10)
    private String status;
}