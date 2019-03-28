package io.streap.core.block;

import reactor.core.publisher.Mono;

import java.util.function.Function;

public class DefaultBlock implements Block {

    private boolean aborted;
    private boolean completed;

    @Override
    public <R> Mono<R> commit() {
        completed = true;
        return Mono.empty();
    }

    @Override
    public <R> Mono<R> abort() {
        aborted = true;
        return Mono.empty();
    }

    @Override
    public boolean isAborted() {
        return aborted;
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public <U, V> Function<U, Mono<V>> wrap(Function<U, V> fn) {
        return u -> Mono.just(fn.apply(u));
    }

}
