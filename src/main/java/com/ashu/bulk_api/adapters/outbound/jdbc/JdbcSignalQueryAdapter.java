package com.ashu.bulk_api.adapters.outbound.jdbc;

import com.ashu.bulk_api.core.domain.model.Signal;
import com.ashu.bulk_api.core.port.outbound.SignalQueryPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Component
public class JdbcSignalQueryAdapter implements SignalQueryPort {

    private static final String SIGNAL_SELECT = "SELECT uabs_event_id, event_record_date_time, event_type, event_status, " +
            "unauthorized_debit_balance, book_date, grv, product_id, agreement_id, signal_start_date, signal_end_date FROM signals";
    private static final String SIGNAL_SELECT_LATEST_BY_AGREEMENT = SIGNAL_SELECT +
            " WHERE agreement_id = ? ORDER BY event_record_date_time DESC LIMIT 1";

    private final JdbcTemplate jdbcTemplate;
    private final RowMapper<Signal> mapper = new SignalRowMapper();

    public JdbcSignalQueryAdapter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void streamAllSignals(Consumer<Signal> consumer) {
        AtomicInteger rowNum = new AtomicInteger();
        jdbcTemplate.query(SIGNAL_SELECT, rs ->
                consumer.accept(mapper.mapRow(rs, rowNum.getAndIncrement())));
    }

    @Override
    public Optional<Signal> findLatestByAgreementId(long agreementId) {
        List<Signal> result = jdbcTemplate.query(SIGNAL_SELECT_LATEST_BY_AGREEMENT, mapper, agreementId);
        return result.stream().findFirst();
    }

    private static class SignalRowMapper implements RowMapper<Signal> {
        @Override
        public Signal mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Signal(
                    rs.getLong("uabs_event_id"),
                    rs.getString("event_type"),
                    rs.getString("event_status"),
                    rs.getInt("unauthorized_debit_balance"),
                    rs.getTimestamp("event_record_date_time").toLocalDateTime(),
                    rs.getDate("book_date").toLocalDate(),
                    rs.getInt("grv"),
                    rs.getLong("product_id"),
                    rs.getLong("agreement_id"),
                    rs.getDate("signal_start_date").toLocalDate(),
                    rs.getDate("signal_end_date").toLocalDate()
            );
        }
    }
}
