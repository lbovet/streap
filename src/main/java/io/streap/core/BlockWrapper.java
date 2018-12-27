package io.streap.core;

import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * A block wrapping another one.
 */
public class BlockWrapper implements Block {

    private Block wrappedBlock;

    public BlockWrapper(Block wrappedBlock) {
        this.wrappedBlock = wrappedBlock;
    }

    @Override
    public <U, V> Function<U, Mono<V>> execute(Function<U, V> fn) {
        return wrappedBlock.execute(fn);
    }

    @Override
    public void commit() {
        wrappedBlock.commit();
    }

    @Override
    public void abort() {
        wrappedBlock.abort();
    }

    @Override
    public boolean isAborted() {
        return wrappedBlock.isAborted();
    }

    @Override
    public boolean isCompleted() {
        return wrappedBlock.isCompleted();
    }
}
