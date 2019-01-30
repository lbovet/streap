package io.streap.core;

/**
 * Stores offsets for idempotent processing.
 */
public interface OffsetStore {

    /**
     * Note: This never overwrites a higher value.
     */
    void write(int partition, long offset);

    long read(int partition);
}
