package io.streap.core;

import reactor.core.publisher.Mono;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Runs all operations of this block in a perimeter on a single thread.
 * This allows to serialize blocking operations needing a thread context.
 */
public class SingleThreadBlock implements Block {

    private BlockingQueue<Runnable> operations = new LinkedBlockingQueue<>();
    private volatile boolean running = true;
    private boolean aborted;

    /**
     * Starts block using a single thread taken from an executor service.
     * The perimeter is created and the block is ready to accept operations.
     * Operations can be submitted before calling start, they will be enqueued.
     *
     * @param perimeter
     * @param executorService
     */
    public void start(Consumer<Runnable> perimeter, ExecutorService executorService) {
        executorService.submit(() ->
                perimeter.accept(() -> {
                    while (running) {
                        try {
                            operations.take().run();
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                })
        );
    }

    @Override
    public <T, U> Function<T, Mono<U>> wrap(Function<T, U> fn) {
        return (T t) ->
                Mono.fromFuture(
                        CompletableFuture.supplyAsync(
                                () -> fn.apply(t),
                                (r) -> operations.add(r)
                        ));
    }

    private void terminate() {
        running = false;
        operations.add(() -> {
        });
    }

    @Override
    public void commit() {
        terminate();
    }

    @Override
    public void abort() {
        aborted = true;
        terminate();
    }

    @Override
    public boolean isAborted() {
        return aborted;
    }

    @Override
    public boolean isCompleted() {
        return !running;
    }


}
