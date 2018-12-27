package io.streap.spring;

import io.streap.core.OffsetStore;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Stores offset in a database table.
 */
public class JdbcOffsetStore implements OffsetStore {

    private String tableName;
    private String storeName;
    private JdbcTemplate jdbcTemplate;

    public JdbcOffsetStore(String tableName, String storeName, JdbcTemplate jdbcTemplate) {
        this.tableName = tableName;
        this.storeName = storeName;
        this.jdbcTemplate = jdbcTemplate;

    }

    @Override
    public void write(long offset) {
        jdbcTemplate.update("UPDATE ? SET offset = ? WHERE store = ?  AND offset < ?",
                tableName,
                offset,
                storeName,
                offset);
    }

    @Override
    public long read() {
        return jdbcTemplate.queryForObject("SELECT offset FROM EMPLOYEE", Long.class);
    }
}
