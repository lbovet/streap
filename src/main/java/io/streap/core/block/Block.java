package io.streap.core.block;

import io.streap.core.context.Context;
import reactor.core.publisher.Mono;

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
