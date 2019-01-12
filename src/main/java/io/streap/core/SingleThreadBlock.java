package io.streap.core;

import reactor.core.publisher.Mono;

import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Runs all operations of this block in a perimeter on a single thread.
 * This allows to serialize blocking operations needing a thread context.
 */
public class SingleThreadBlock implements Block {

    private static int POLL_TIMEOUT_SECONDS = 5;
    private BlockingQueue<Runnable> operations = new LinkedBlockingQueue<>();
    private volatile boolean running = true;
    private volatile CompletableFuture<Void> execution;
    private volatile boolean aborted;

    /**
     * Starts block using a single thread taken from an executor service.
     * The perimeter is created and the block is ready to accept operations.
     * Operations can be submitted before calling start, they will be enqueued.
     *
     * @param perimeter
     * @param executorService
     */
    public void start(Consumer<Runnable> perimeter, ExecutorService executorService) {
        execution = CompletableFuture.runAsync(() ->
                        perimeter.accept(() -> {
                            while (running && !isAborted()) {
                                try {
                                    Runnable task = operations.poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                                    if (task != null) {
                                        task.run();
                                    }
                                } catch (InterruptedException e) {
                                    break;
                                }
                            }
                        }),
                executorService
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

    private <R> Mono<R> terminate() {
        return Mono
                .just(false)
                .doOnNext(x -> running = x)
                .doOnNext(x -> operations.add(() -> {
                }))
                .then(Mono.fromFuture(execution).map(x -> null));
    }

    @Override
    public <R> Mono<R> commit() {
        return terminate();
    }

    @Override
    public <R> Mono<R> abort() {
        return Mono
                .just(true)
                .doOnNext(x -> aborted = x)
                .then(terminate());
    }

    @Override
    public boolean isAborted() {
        return aborted;
    }

    @Override
    public boolean isCompleted() {
        return execution.isDone();
    }


}
