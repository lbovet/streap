package io.streap.spring;

import io.streap.core.OffsetStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Stores offset in a database table.
 */
public class JdbcOffsetStore implements OffsetStore {

    private static Logger log = LoggerFactory.getLogger(JdbcOffsetStore.class);

    private String tableName;
    private String storeName;
    private JdbcTemplate jdbcTemplate;

    public static void createTable(JdbcTemplate jdbcTemplate, String tableName) {
        try {
            jdbcTemplate.execute("CREATE TABLE " + tableName + " (store VARCHAR NOT NULL PRIMARY KEY, off BIGINT)");
        } catch (BadSqlGrammarException e) {
            log.warn("Could not create table '{}'. Probably because it already exists. More details with debug level.", tableName);
            log.debug("Exception while creating table", e);
        }
    }

    public JdbcOffsetStore(String tableName, String storeName, JdbcTemplate jdbcTemplate) {
        this.tableName = tableName;
        this.storeName = storeName;
        this.jdbcTemplate = jdbcTemplate;
        try {
            jdbcTemplate.update("INSERT INTO "+tableName+" (off,store) VALUES (-1, ?)", storeName);
        } catch (DataIntegrityViolationException e) {
            log.debug("Store '{}' already exists in table '{}'", storeName, tableName);
            log.debug("Exception was", e);
        }
    }

    @Override
    public void write(long offset) {
        jdbcTemplate.update("UPDATE "+tableName+" SET off = ? WHERE store = ?  AND off < ?",
                offset,
                storeName,
                offset);
    }

    @Override
    public long read() {
        return jdbcTemplate.queryForObject("SELECT off FROM "+tableName+" WHERE store = ?",
                Long.class,
                storeName);
    }
}
