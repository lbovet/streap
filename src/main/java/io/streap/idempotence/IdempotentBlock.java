package io.streap.idempotence;

import io.streap.block.ProcessingBlock;

/**
 * Block providing idempotence by skipping non-idempotent operations.
 */
public interface IdempotentBlock<T> extends ProcessingBlock<T>, IdempotentContext<T> {

}
