package io.streap.core.idempotence;

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
