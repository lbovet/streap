package li.chee.streap.kafka;

import li.chee.streap.core.Block;
import li.chee.streap.core.BlockWrapper;
import li.chee.streap.core.ProcessingBlock;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.TransactionManager;

import java.util.function.Supplier;

/**
 * A block for receiving and sending Kafka messages with exactly-one semantics.
 */
public class ExactlyOnceBlock<T> extends BlockWrapper implements ProcessingBlock<T> {

    private static class BlockBuilder {
        Supplier<Block> innerBlockSupplier;
        TransactionManager transactionManager;

        public BlockBuilder(KafkaSender<?, ?> sender) {
            this.transactionManager = sender.transactionManager();
        }

        public BlockBuilder containing(Supplier<Block> innerBlockSupplier) {
            this.innerBlockSupplier = innerBlockSupplier;
            return this;
        }

        //public <T,U> Function<Flux<Flux<T>>, Flux<? extends
    }

    public static BlockBuilder synchronizedWith(KafkaSender<?,?> sender) {
        return new BlockBuilder(sender);
    }

    
}
