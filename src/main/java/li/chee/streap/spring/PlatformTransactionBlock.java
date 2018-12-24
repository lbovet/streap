package li.chee.streap.spring;

import li.chee.streap.core.SingleThreadBlock;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A block running operations inside a Spring Transaction.
 */
public class PlatformTransactionBlock extends SingleThreadBlock {

    private TransactionStatus transactionStatus;

    public PlatformTransactionBlock(TransactionTemplate transactionTemplate) {
        this(transactionTemplate, Executors.newCachedThreadPool());
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
