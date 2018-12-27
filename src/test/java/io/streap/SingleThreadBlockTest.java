package io.streap;

import io.streap.core.SingleThreadBlock;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SingleThreadBlockTest {

    @Test
    public void testSequence() {
        List<String> result = new ArrayList<>();
        SingleThreadBlock block = new SingleThreadBlock();
        block.start((r) -> {
            result.add("before");
            r.run();
            result.add("after");
        }, Executors.newCachedThreadPool());

        assertEquals("HELLO", block.execute((String x) -> {
            result.add(x);
            return x.toUpperCase();
        }).apply("hello").block());

        assertEquals("FOO", block.execute((String x) -> {
            result.add(x);
            return x.toUpperCase();
        }).apply("foo").block());

        assertEquals("BAR", block.execute((String x) -> {
            result.add(x);
            return x.toUpperCase();
        }).apply("bar").block());

        assertFalse(block.isAborted());
        assertFalse(block.isCompleted());
        block.commit();
        assertFalse(block.isAborted());
        assertTrue(block.isCompleted());

        assertEquals(Arrays.asList("before", "hello", "foo", "bar", "after"), result);
    }

    @Test
    public void testParallel() throws InterruptedException {
        List<String> result = new ArrayList<>();
        SingleThreadBlock block = new SingleThreadBlock();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch end = new CountDownLatch(1);

        block.start((r) -> {
                    result.add(Thread.currentThread().getName());
                    start.countDown();
                    r.run();
                },
                Executors.newCachedThreadPool());

        ExecutorService executor = Executors.newFixedThreadPool(5);

        Function<Integer, Void> fn = (i) -> {
            result.add(Thread.currentThread().getName() + "." + i + ".in");
            try {
                Thread.sleep((long) (Math.random() * 300));
            } catch (InterruptedException e) {
            }
            result.add(Thread.currentThread().getName() + "." + i + ".out");
            if (result.size() == 9) {
                end.countDown();
            }
            return null;
        };

        start.await();
        String t = result.get(0);
        List<String> expected = new ArrayList<>();
        expected.add(t);
        Flux.range(0, 4)
                .delayElements(Duration.ofMillis(50))
                .doOnNext(i -> {
                    System.out.println("Submitted task from " + Thread.currentThread().getName());
                    executor.submit(() -> block.execute(fn).apply(i));
                    expected.add(t + "." + i + ".in");
                    expected.add(t + "." + i + ".out");
                })
                .subscribe();

        end.await();
        assertEquals(expected, result);
    }

    @Test
    public void testException() {
        SingleThreadBlock block = new SingleThreadBlock();
        block.start((r) -> r.run(), Executors.newCachedThreadPool());

        Assert.assertEquals("ouch",
            block.execute((Void) -> {
                throw new IllegalStateException("ouch");
            }).apply(null)
                    .onErrorResume(e -> Mono.just(e.getCause().getMessage()))
                    .block());
    }
}
