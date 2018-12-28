package io.streap.kafka;

import io.streap.core.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.TransactionManager;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A block for receiving and sending Kafka messages with exactly-one semantics.
 */
public class ExactlyOnceBlock<U, V> extends BlockDecorator implements IdempotentBlock<ReceiverRecord<U,V>> {

    private static class ProcessingBlockBuilder<T extends ReceiverRecord<U,V>,U,V> {
        Supplier<Block> innerBlockSupplier;
        TransactionManager transactionManager;

        private ProcessingBlockBuilder(TransactionManager transactionManager, Supplier<Block> innerBlockSupplier) {
            this.transactionManager = transactionManager;
            this.innerBlockSupplier = innerBlockSupplier;
        }

        public IdempotentBlockBuilder with(OffsetStore offsetStore) {
            return new IdempotentBlockBuilder(this, offsetStore);
        }

        public Function<Flux<Flux<ReceiverRecord<U,V>>>, Flux<? extends ProcessingBlock<ReceiverRecord<U,V>>>> transformer() {
            return f -> f.map( items -> new ExactlyOnceBlock<U,V>(transactionManager, innerBlockSupplier.get(), items));
        }
    }

    private static class IdempotentBlockBuilder<T extends ReceiverRecord<U,V>,U,V> {
        ProcessingBlockBuilder<T,U,V> blockBuilder;
        OffsetStore offsetStore;

        private IdempotentBlockBuilder(ProcessingBlockBuilder blockBuilder, OffsetStore offsetStore) {
            this.blockBuilder = blockBuilder;
            this.offsetStore = offsetStore;
        }

        public Function<Flux<Flux<ReceiverRecord<U,V>>>, Flux<? extends IdempotentBlock<ReceiverRecord<U,V>>>> transformer() {
            return f -> f.map( items -> {
                ExactlyOnceBlock<U,V> block = new ExactlyOnceBlock<U,V>(blockBuilder.transactionManager,
                        blockBuilder.innerBlockSupplier.get(), items);
                block.setOffsetStore(offsetStore);
                return block;
            });
        }
    }

    public static <T extends ReceiverRecord<U,V>, U, V> ProcessingBlockBuilder<T,U,V> createBlock(TransactionManager transactionManager, Supplier<Block> innerBlockSupplier) {
        return new ProcessingBlockBuilder<>(transactionManager, innerBlockSupplier);
    }

    private TransactionManager transactionManager;
    private Flux<ReceiverRecord<U,V>> items;
    private OffsetStore offsetStore;
    private Long lastOffset;

    public ExactlyOnceBlock(TransactionManager transactionManager, Block innerBlock, Flux<ReceiverRecord<U,V>> items) {
        super(innerBlock);
        this.transactionManager = transactionManager;
        this.items = items;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    @Override
    public Flux<ReceiverRecord<U,V>> items() {
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
    public Function<ReceiverRecord<U,V>, Mono<ReceiverRecord<U,V>>> wrapOnce(Consumer<ReceiverRecord<U,V>> operation) {
        return t -> {
            if (lastOffset == null) {
                lastOffset = offsetStore.read();
            }
            if (t.offset() > lastOffset) {
                return wrap((ReceiverRecord<U,V> x) -> {
                    operation.accept(x);
                    offsetStore.write(x.offset());
                    return x;
                }).apply(t);
            } else {
                return Mono.just(t);
            }
        };
    }
}
