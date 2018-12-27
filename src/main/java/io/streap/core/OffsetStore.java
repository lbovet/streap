package io.streap.core;

/**
 * Stores offsets for idempotent processing.
 */
public interface OffsetStore {

    /**
     * Note: This will not overwrite a higher value.
     */
    void write(long offset);

    long read();
}
