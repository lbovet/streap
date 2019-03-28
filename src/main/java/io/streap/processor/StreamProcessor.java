package io.streap.processor;

import io.streap.block.Block;
import io.streap.block.DefaultBlock;
import io.streap.context.Context;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public abstract class StreamProcessor<R,C extends Context,S> {
    public abstract Flux<? extends StreamProcessor> process(BiFunction<Flux<R>, C, Flux<S>> body);

    private Supplier<Block> blockSupplier = null;

    protected Block createBlock() {
        return Optional.ofNullable(blockSupplier).orElse(DefaultBlock::new).get();
    }

    public StreamProcessor<R,C,S> withContext(Supplier<Block> blockSupplier) {
        this.blockSupplier = blockSupplier;
        return this;
    }
}
