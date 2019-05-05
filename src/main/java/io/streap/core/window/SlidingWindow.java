package io.streap.core.window;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.ToLongFunction;

/**
 * Maintains a sliding window over multiple items. Ticks are monotonic long values
 * that can represent time (but must not).
 */
public class SlidingWindow<T> {

    private long size;
    private Deque<T> items = new LinkedList<>();
    private ToLongFunction<T> resolveTick;

    public SlidingWindow(long size, ToLongFunction<T> resolveTick) {
        this.size = size;
        this.resolveTick = resolveTick;
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
        while (!items.isEmpty() &&
                resolveTick.applyAsLong(items.peekLast()) > resolveTick.applyAsLong(items.peekFirst()) + size) {
            items.removeFirst();
        }
    }

    public T first() {
        return items.peekFirst();
    }

    public T last() {
        return items.peekLast();
    }

    public boolean isEmpty() {
        return items.isEmpty();
    }
}
