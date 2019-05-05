package io.streap.core.sequencer;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * Collect items and allows for extracting them in an ordered way.
 */
public class Sequence<T> {

    private Predicate<T> additionalSelector = x -> false;
    private SortedSet<T> items;
    private ToIntFunction<T> position;
    private int next = 0;

    public Sequence(ToIntFunction<T> position) {
        this.position = position;
        items = new TreeSet<>(Comparator.comparingInt(this.position));
    }

    public void add(T item) {
        items.add(item);
    }

    /**
     * Take will also extract unordered items matching this predicate
     */
    public void selectAlso(Predicate<T> additionalSelector) {
        this.additionalSelector = additionalSelector;
    }

    /**
     * Extracts (remove and return) items that are already correctly ordered and also those matching
     * the "select also" predicate.
     */
    public List<T> take() {
        List<T> result = new ArrayList<>(items.size());
        Iterator<T> it = items.iterator();
        while(it.hasNext()) {
            T item = it.next();
            boolean selected = (position.applyAsInt(item) == next);
            if(selected) {
                next++;
            }
            if(selected || additionalSelector.test(item)) {
                result.add(item);
                it.remove();
            }
        }
        return result;
    }

    public boolean isEmpty() {
        return items.isEmpty();
    }
}
