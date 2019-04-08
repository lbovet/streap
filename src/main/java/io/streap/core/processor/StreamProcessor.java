package io.streap.core.processor;

import io.streap.core.block.Block;
import io.streap.core.block.DefaultBlock;
import io.streap.core.context.Context;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public abstract class StreamProcessor<R, C extends Context, S> {

    public abstract <T extends S> Flux<T> process(BiFunction<Flux<? extends R>, C, Flux<T>> body);

    private Supplier<? extends Block> blockSupplier = null;

    protected Block createBlock() {
        Block result = null;
        if (blockSupplier != null) {
            result = blockSupplier.get();
        }
        if (result == null) {
            result = new DefaultBlock();
        }
        return result;
    }

    public <X extends Block> StreamProcessor<R, X, S> withContext(Supplier<X> blockSupplier) {
        this.blockSupplier = blockSupplier;
        return (StreamProcessor<R, X, S>) this;
    }
}
