package io.streap.spring;

import io.streap.core.block.Block;
import io.streap.core.block.SingleThreadBlock;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * A block running operations inside a Spring Transaction.
 */
public class PlatformTransaction extends SingleThreadBlock {

    public static Supplier<Block> supplier(TransactionTemplate transactionTemplate) {
        return () -> new PlatformTransaction(transactionTemplate);
    }

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    private volatile TransactionStatus transactionStatus;

    public PlatformTransaction(TransactionTemplate transactionTemplate) {
        this(transactionTemplate, executorService);
    }

    public PlatformTransaction(TransactionTemplate transactionTemplate, ExecutorService executorService) {
        start(r -> transactionTemplate.execute((txStatus) -> {
            transactionStatus = txStatus;
            r.run();
            return null;
        }), executorService);
    }

    @Override
    public <R> Mono<R> commit() {
        return super.commit();
    }

    @Override
    public boolean isAborted() {
        return transactionStatus != null && transactionStatus.isRollbackOnly();
    }

    @Override
    public boolean isCompleted() {
        return transactionStatus != null && transactionStatus.isCompleted();
    }

    @Override
    public <R> Mono<R> abort() {
        return Mono.empty().doOnSuccess(x -> transactionStatus.setRollbackOnly()).then(super.abort());
    }
}
