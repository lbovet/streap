package io.streap.kafka;

import io.streap.core.*;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.TransactionManager;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A block for receiving and sending Kafka messages with exactly-one semantics.
 */
public class ExactlyOnceBlock<T> extends BlockWrapper implements IdempotentBlock<T> {

    private static class ProcessingBlockBuilder {
        Supplier<Block> innerBlockSupplier;
        TransactionManager transactionManager;

        private ProcessingBlockBuilder(TransactionManager transactionManager, Supplier<Block> innerBlockSupplier) {
            this.transactionManager = transactionManager;
            this.innerBlockSupplier = innerBlockSupplier;
        }

        public IdempotentBlockBuilder with(OffsetStore offsetStore) {
            return new IdempotentBlockBuilder(this, offsetStore);
        }

        public <U> Function<Flux<Flux<U>>, Flux<? extends ProcessingBlock<U>>> transformer() {
            return f -> f.map( items -> new ExactlyOnceBlock<>(transactionManager, innerBlockSupplier.get(), items));
        }
    }

    private static class IdempotentBlockBuilder {
        ProcessingBlockBuilder blockBuilder;
        OffsetStore offsetStore;

        public IdempotentBlockBuilder(ProcessingBlockBuilder blockBuilder, OffsetStore offsetStore) {
            this.blockBuilder = blockBuilder;
            this.offsetStore = offsetStore;
        }

        public <U> Function<Flux<Flux<U>>, Flux<? extends IdempotentBlock<U>>> transformer() {
            return f -> f.map( items -> {
                ExactlyOnceBlock<U> block = new ExactlyOnceBlock<>(blockBuilder.transactionManager,
                        blockBuilder.innerBlockSupplier.get(), items);
                block.setOffsetStore(offsetStore);
                return block;
            });
        }
    }

    public static ProcessingBlockBuilder createBlock(TransactionManager transactionManager, Supplier<Block> innerBlockSupplier) {
        return new ProcessingBlockBuilder(transactionManager, innerBlockSupplier);
    }

    private TransactionManager transactionManager;
    private Flux<T> items;
    private OffsetStore offsetStore;

    public ExactlyOnceBlock(TransactionManager transactionManager, Block innerBlock, Flux<T> items) {
        super(innerBlock);
        this.transactionManager = transactionManager;
        this.items = items;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    @Override
    public Flux<T> items() {
        return items;
    }

    @Override
    public void commit() {
        super.commit();
        transactionManager.commit();
    }

    @Override
    public void abort() {
        super.abort();
        transactionManager.abort();
    }

    @Override
    public <U> Function<T, Flux<U>> once(Function<T, U> fn) {
        return null;
    }
}
