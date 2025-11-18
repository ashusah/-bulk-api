package com.ashu.bulk_api.adapters.outbound.jpa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StatusRepository extends JpaRepository<StatusRecord, Long> {
}
