package io.streap.core.sequencer;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * Collects items and group them in sequences for extracting them in an ordered way.
 */
public class Sequencer<T> {
    private Map<Object, Sequence<T>> sequences = new LinkedHashMap<>();
    private ToIntFunction<T> resolvePosition;
    private Function<T, Object> resolveKey;

    public Sequencer(ToIntFunction<T> resolvePosition, Function<T, Object> resolveKey) {
        this.resolvePosition = resolvePosition;
        this.resolveKey = resolveKey;
    }

    public void add(T item) {
        sequences
                .computeIfAbsent(resolveKey.apply(item), x -> new Sequence<>(resolvePosition))
                .add(item);
    }

    /**
     * Extracts (removes and returns) all already ordered items plus the ones matching the predicate.
     */
    public List<T> takeWith(Predicate<T> predicate) {
        List<T> result = new ArrayList<>();
        Iterator<Sequence<T>> it = sequences.values().iterator();
        while (it.hasNext()) {
            Sequence<T> sequence = it.next();
            sequence.selectAlso(predicate);
            result.addAll(sequence.take());
            if(sequence.isEmpty()) {
                it.remove();
            }
        }
        return result;
    }
}
