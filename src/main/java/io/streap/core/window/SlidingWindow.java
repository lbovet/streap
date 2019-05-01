package io.streap.core.window;

import java.util.Deque;
import java.util.LinkedList;

/**
 * Maintains a sliding window over a multiple items. Item position is a long value
 * that can represent time (but must not).
 */
public abstract class SlidingWindow<T> {

    private long size;
    private Deque<T> items = new LinkedList<>();

    public SlidingWindow(long size) {
        this.size = size;
    }

    /**
     * Add an item to the window but does not yet update the window boundaries.
     */
    public void add(T item) {
        items.addLast(item);
    }

    /**
     * Updates the window boundaries according to the last added items.
     */
    public void update() {
        while(!items.isEmpty() && position(items.peekLast()) > position(items.peekFirst()) + size) {
            items.removeFirst();
        }
    }

    protected abstract long position(T item);
}
