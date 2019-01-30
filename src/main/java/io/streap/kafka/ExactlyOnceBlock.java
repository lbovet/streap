package io.streap.kafka;

import io.streap.core.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.TransactionManager;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A block for receiving and sending Kafka messages with exactly-one semantics.
 */
public class ExactlyOnceBlock<U, V> extends BlockDecorator implements IdempotentBlock<ConsumerRecord<U,V>> {

    public  static class ProcessingBlockBuilder<U,V> {
        private Supplier<Block> innerBlockSupplier;
        private TransactionManager transactionManager;

        private ProcessingBlockBuilder(TransactionManager transactionManager, Supplier<Block> innerBlockSupplier) {
            this.transactionManager = transactionManager;
            this.innerBlockSupplier = innerBlockSupplier;
        }

        public IdempotentBlockBuilder<U,V> with(OffsetStore offsetStore) {
            return new IdempotentBlockBuilder<>(this, offsetStore);
        }

        public Function<Flux<Flux<ConsumerRecord<U,V>>>, Publisher<ProcessingBlock<ConsumerRecord<U,V>>>> transformer() {
            return f -> f.map( items -> new ExactlyOnceBlock<U,V>(transactionManager, innerBlockSupplier.get(), items));
        }
    }

    public static class IdempotentBlockBuilder<U,V> {
        private ProcessingBlockBuilder<U,V> blockBuilder;
        private OffsetStore offsetStore;

        private IdempotentBlockBuilder(ProcessingBlockBuilder blockBuilder, OffsetStore offsetStore) {
            this.blockBuilder = blockBuilder;
            this.offsetStore = offsetStore;
        }

        public Function<Flux<Flux<ConsumerRecord<U,V>>>, Flux<IdempotentBlock<ConsumerRecord<U,V>>>> transformer() {
            return f -> f.map( items -> {
                ExactlyOnceBlock<U,V> block = new ExactlyOnceBlock<>(blockBuilder.transactionManager,
                        blockBuilder.innerBlockSupplier.get(), items);
                block.setOffsetStore(offsetStore);
                return block;
            });
        }
    }

    public static <U, V> ProcessingBlockBuilder<U,V> createBlock(TransactionManager transactionManager, Supplier<Block> innerBlockSupplier) {
        return new ProcessingBlockBuilder<>(transactionManager, innerBlockSupplier);
    }

    private TransactionManager transactionManager;
    private Flux<ConsumerRecord<U,V>> items;
    private OffsetStore offsetStore;
    private Long lastOffset;

    public ExactlyOnceBlock(TransactionManager transactionManager, Block innerBlock, Flux<ConsumerRecord<U,V>> items) {
        super(innerBlock);
        this.transactionManager = transactionManager;
        this.items = items;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    @Override
    public Flux<ConsumerRecord<U,V>> items() {
        return items;
    }

    @Override
    public <R> Mono<R> commit() {
        return super.commit().then(transactionManager.commit()).map(x->null);
    }

    @Override
    public <R> Mono<R> abort() {
        return super.abort().then(transactionManager.abort()).map(x->null);
    }

    @Override
    public Function<ConsumerRecord<U,V>, Mono<ConsumerRecord<U,V>>> wrapOnce(Consumer<ConsumerRecord<U,V>> operation) {
        return t -> {
            if (lastOffset == null) {
                lastOffset = offsetStore.read(t.partition());
            }
            if (t.offset() > lastOffset) {
                return wrap((ConsumerRecord<U,V> x) -> {
                    operation.accept(x);
                    offsetStore.write(t.partition(), x.offset());
                    return x;
                }).apply(t);
            } else {
                return Mono.just(t);
            }
        };
    }

    @Override
    public Function<ConsumerRecord<U,V>, Mono<ConsumerRecord<U,V>>> doOnce(Consumer<ConsumerRecord<U,V>> operation) {
        return t -> {
            if (lastOffset == null) {
                lastOffset = offsetStore.read(t.partition());
            }
            if (t.offset() > lastOffset) {
                operation.accept(t);
            }
            return Mono.just(t);
        };
    }
}
