package io.streap.block;

import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * A block containing another one.
 */
public class BlockDecorator implements Block {

    private Block nestedBlock;

    public BlockDecorator(Block nestedBlock) {
        this.nestedBlock = nestedBlock;
    }

    @Override
    public <U, V> Function<U, Mono<V>> wrap(Function<U, V> fn) {
        return nestedBlock.wrap(fn);
    }

    @Override
    public <R> Mono<R> commit() {
        return nestedBlock.commit();
    }

    @Override
    public <R> Mono<R> abort() {
        return nestedBlock.abort();
    }

    @Override
    public boolean isAborted() {
        return nestedBlock.isAborted();
    }

    @Override
    public boolean isCompleted() {
        return nestedBlock.isCompleted();
    }
}
