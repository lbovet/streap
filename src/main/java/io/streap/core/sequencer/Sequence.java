package io.streap.core.sequencer;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * TODO
 */
public class Sequence<T> {

    private Predicate<T> additionalSelector = x -> false;
    private SortedSet<T> items = new TreeSet<>();

    public void add(T item) {
        items.add(item);
    }

    public void selectAlso(Predicate<T> additionalSelector) {
        this.additionalSelector = additionalSelector;
    }

    public List<T> take() {
        return null;
    }
}
