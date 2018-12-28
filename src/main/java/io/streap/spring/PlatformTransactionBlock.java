package io.streap.spring;

import io.streap.core.Block;
import io.streap.core.SingleThreadBlock;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * A block running operations inside a Spring Transaction.
 */
public class PlatformTransactionBlock extends SingleThreadBlock {

    public static Supplier<Block> supplier(TransactionTemplate transactionTemplate) {
        return () -> new PlatformTransactionBlock(transactionTemplate);
    }

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    private TransactionStatus transactionStatus;

    public PlatformTransactionBlock(TransactionTemplate transactionTemplate) {
        this(transactionTemplate, executorService);
    }

    public PlatformTransactionBlock(TransactionTemplate transactionTemplate, ExecutorService executorService) {
        start(r -> transactionTemplate.execute((txStatus) -> {
            transactionStatus = txStatus;
            r.run();
            return null;
        }), executorService);
    }

    @Override
    public void commit() {
        super.commit();
    }

    @Override
    public boolean isAborted() {
        return transactionStatus.isRollbackOnly();
    }

    @Override
    public boolean isCompleted() {
        return transactionStatus.isCompleted();
    }

    @Override
    public void abort() {
        transactionStatus.setRollbackOnly();
        super.abort();
    }
}
