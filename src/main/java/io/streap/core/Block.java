package io.streap.core;

import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Delimits a unit of work.
 */
public interface Block {

    /**
     * Wraps a function so that it runs in the context of the block, whatever it means.
     * @return the wrapped function with
     */
    <U,V> Function<U,Mono<V>> wrap(Function<U,V> fn);

    /**
     * Commits the block if not yet committed.
     */
    void commit();

    /**
     * Aborts the transaction. Rollbacks if supported by the resource.
     */
    void abort();

    /**
     * If the block was aborted.
     */
    boolean isAborted();

    /**
     * If the block has terminated normally or was aborted.
     */
    boolean isCompleted();
}
