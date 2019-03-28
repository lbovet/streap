package io.streap.core.block;

import reactor.core.publisher.Flux;

/**
 * A block transporting also the flux to process in its scope.
 */
public interface ProcessingBlock<T> extends Block {

    /**
     * @return the items being processed in this block
     */
    Flux<T> items();

}
