package io.streap.spring;

import io.streap.test.EmbeddedDatabaseSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OffsetStoreTest extends EmbeddedDatabaseSupport {

    @Test
    public void testOffsets() {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        JdbcOffsetStore store = new JdbcOffsetStore(jdbcTemplate, "offsets", "orders");
        assertEquals(-1, store.read());
        store.write(3);
        assertEquals(3, store.read());
        store.write(2);
        assertEquals(3, store.read());
    }

    @Test
    public void testDoubleInsert() {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        new JdbcOffsetStore(jdbcTemplate, "offsets", "orders");
        new JdbcOffsetStore(jdbcTemplate, "offsets", "orders");
    }

    @Test
    public void testDoubleCreate() {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
    }
}