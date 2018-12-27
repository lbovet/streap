package io.streap.kafka;

import io.streap.core.OffsetStore;
import io.streap.core.Block;
import io.streap.core.BlockWrapper;
import io.streap.core.ProcessingBlock;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.TransactionManager;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A block for receiving and sending Kafka messages with exactly-one semantics.
 */
public class ExactlyOnceBlock<T> extends BlockWrapper implements ProcessingBlock<T> {

    private static class BlockBuilder {
        Supplier<Block> innerBlockSupplier;
        TransactionManager transactionManager;
        OffsetStore offsetStore;

        private BlockBuilder(TransactionManager transactionManager, Supplier<Block> innerBlockSupplier) {
            this.transactionManager = transactionManager;
            this.innerBlockSupplier = innerBlockSupplier;
        }

        public void with(OffsetStore offsetStore) {
            this.offsetStore = offsetStore;
        }

        public <U> Function<Flux<Flux<U>>, Flux<? extends ProcessingBlock<U>>> transformer() {
            return f -> f.map( items -> new ExactlyOnceBlock<>(transactionManager, innerBlockSupplier.get(), items));
        }
    }

    public static BlockBuilder createBlock(TransactionManager transactionManager, Supplier<Block> innerBlockSupplier) {
        return new BlockBuilder(transactionManager, innerBlockSupplier);
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

}
