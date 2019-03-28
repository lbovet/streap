package io.streap.core.idempotence;

import io.streap.core.block.ProcessingBlock;

/**
 * Block providing idempotence by skipping non-idempotent operations.
 */
public interface IdempotentBlock<T> extends ProcessingBlock<T>, IdempotentContext<T> {

}
