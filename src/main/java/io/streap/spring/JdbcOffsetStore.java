package io.streap.spring;

import io.streap.idempotence.OffsetStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
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
            jdbcTemplate.execute("CREATE TABLE " + tableName + " (store VARCHAR NOT NULL, partition INTEGER NOT NULL, off BIGINT NOT NULL, " +
                    "CONSTRAINT PK PRIMARY KEY (store, partition))");
        } catch (BadSqlGrammarException e) {
            log.warn("Could not create table '{}'. Probably because it already exists. More details with debug level.", tableName);
            log.debug("Exception while creating table", e);
        }
    }

    public JdbcOffsetStore(JdbcTemplate jdbcTemplate, String tableName, String storeName) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
        this.storeName = storeName;
    }

    @Override
    public void write(int partition, long offset) {
        int updated = jdbcTemplate.update("UPDATE " + tableName + " SET off = ? WHERE store = ? AND partition = ? AND off < ?",
                offset,
                storeName,
                partition,
                offset);
        if (updated == 0) {
            try {
                jdbcTemplate.update("INSERT INTO " + tableName + " (off,partition,store) VALUES (?, ?, ?)", offset, partition, storeName);
            } catch (DuplicateKeyException e) {
                // already exists, ignore
            }
        }
    }

    @Override
    public long read(int partition) {
        try {
            return jdbcTemplate.queryForObject("SELECT off FROM " + tableName + " WHERE store = ? AND partition = ?",
                    Long.class,
                    storeName,
                    partition);
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }
    }
}
