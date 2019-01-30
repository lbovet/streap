package io.streap.core;

import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Delimits a unit of work.
 */
public interface Block extends Context {

    /**
     * Commits the block if not yet committed.
     */
    <R> Mono<R> commit();

    /**
     * Aborts the transaction. Rollbacks if supported by the resource.
     */
    <R> Mono<R> abort();

    /**
     * If the block was aborted.
     */
    boolean isAborted();

    /**
     * If the block has terminated normally or was aborted.
     */
    boolean isCompleted();
}
