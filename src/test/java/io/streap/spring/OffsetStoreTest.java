package io.streap.spring;

import io.streap.test.EmbeddedDatabaseSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OffsetStoreTest extends EmbeddedDatabaseSupport {

    @Test
    public void testOffsets() {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        JdbcOffsetStore store = new JdbcOffsetStore("offsets", "orders", jdbcTemplate);
        assertEquals(-1, store.read());
        store.write(3);
        assertEquals(3, store.read());
        store.write(2);
        assertEquals(3, store.read());
    }

    @Test
    public void testDoubleInsert() {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        new JdbcOffsetStore("offsets", "orders", jdbcTemplate);
        new JdbcOffsetStore("offsets", "orders", jdbcTemplate);
    }

    @Test
    public void testDoubleCreate() {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
    }
}