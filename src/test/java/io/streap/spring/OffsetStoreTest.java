package io.streap.spring;

import io.streap.test.EmbeddedDatabaseSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OffsetStoreTest extends EmbeddedDatabaseSupport {

    @Test
    public void testOffsets() {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        JdbcOffsetStore store = new JdbcOffsetStore(jdbcTemplate, "offsets", "orders");
        assertEquals(-1, store.read(0));
        store.write(0, 3);
        assertEquals(3, store.read(0));
        store.write(0, 2);
        assertEquals(3, store.read(0));
    }

    @Test
    public void testDoubleCreate() {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
    }
}