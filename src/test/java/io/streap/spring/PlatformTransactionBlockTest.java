package io.streap.spring;

import io.streap.core.block.Block;
import io.streap.test.EmbeddedDatabaseSupport;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.*;

@Slf4j
public class PlatformTransactionBlockTest extends EmbeddedDatabaseSupport {

    @Test
    public void testTransactionWithoutBlock() {
        jdbcTemplate.execute("CREATE TABLE PERSON (ID INTEGER, NAME VARCHAR)");
        jdbcTemplate.update("INSERT INTO PERSON(ID, NAME) VALUES (?, ?)", 1, "john");

        assertEquals("john",
                jdbcTemplate.queryForObject("SELECT NAME FROM PERSON WHERE ID = ?", String.class, 1));

        transactionTemplate.execute((x)->
                jdbcTemplate.update("UPDATE PERSON SET NAME = ? WHERE ID = ?", "mary", 1));

        assertEquals("mary",
                jdbcTemplate.queryForObject("SELECT NAME FROM PERSON WHERE ID = ?", String.class, 1));

        try {
            transactionTemplate.execute((x) -> {
                jdbcTemplate.update("UPDATE PERSON SET NAME = ? WHERE ID = ?", "peter", 1);
                throw new IllegalStateException();
            });
        } catch(IllegalStateException e) {
            //expected
        }

        assertEquals("mary",
                jdbcTemplate.queryForObject("SELECT NAME FROM PERSON WHERE ID = ?", String.class, 1));
    }

    @Test
    public void testSuccessfulTransaction() throws InterruptedException {
        jdbcTemplate.execute("CREATE TABLE PERSON (ID INTEGER, NAME VARCHAR)");
        jdbcTemplate.update("INSERT INTO PERSON(ID, NAME) VALUES (?, ?)", 1, "john");

        Block block = new PlatformTransaction(transactionTemplate);
        block.wrap((x)->
                jdbcTemplate.update("UPDATE PERSON SET NAME = ? WHERE ID = ?",x, 1)).apply("mary").block();
        assertEquals("john",
                jdbcTemplate.queryForObject("SELECT NAME FROM PERSON WHERE ID = ?", String.class, 1));
        block.wrap((x)->
                jdbcTemplate.update("UPDATE PERSON SET NAME = ? WHERE ID = ?",x, 1)).apply("peter").block();

        assertFalse(block.isAborted());
        assertFalse(block.isCompleted());
        block.commit().block();
        assertFalse(block.isAborted());
        assertTrue(block.isCompleted());

        assertEquals("peter",
                jdbcTemplate.queryForObject("SELECT NAME FROM PERSON WHERE ID = ?", String.class, 1));
    }

    @Test
    public void testRollback() throws InterruptedException {
        jdbcTemplate.execute("CREATE TABLE PERSON (ID INTEGER, NAME VARCHAR)");
        jdbcTemplate.update("INSERT INTO PERSON(ID, NAME) VALUES (?, ?)", 1, "john");

        Block block = new PlatformTransaction(transactionTemplate);
        block.wrap((x)->
                jdbcTemplate.update("UPDATE PERSON SET NAME = ? WHERE ID = ?",x, 1)).apply("mary").block();
        assertEquals("john",
                jdbcTemplate.queryForObject("SELECT NAME FROM PERSON WHERE ID = ?", String.class, 1));

        assertFalse(block.isAborted());
        assertFalse(block.isCompleted());
        block.abort().block();
        assertTrue(block.isAborted());
        assertTrue(block.isCompleted());

        assertEquals("john",
                jdbcTemplate.queryForObject("SELECT NAME FROM PERSON WHERE ID = ?", String.class, 1));
    }
}
