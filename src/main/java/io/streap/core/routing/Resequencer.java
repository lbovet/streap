package io.streap.core.routing;

import io.streap.core.sequencer.Sequencer;
import io.streap.core.window.SlidingWindow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * Generic resequencer
 */
public class Resequencer<T, C extends Comparable<C>> implements Function<Flux<T>, Flux<ResequencedItem<T, C>>> {

    private SlidingWindow<T> window;
    private Sequencer<T> sequencer;
    private Function<T, C> resolveOffset;
    private Duration updateInterval;
    private Function<Flux<?>, Publisher<Object>> updateControl;
    private ResequencedItem<?, C> savePoint;

    /**
     * @param windowSize The size of the window in the tick units.
     * @param resolveTick Obtains the absolute tick of an item.
     * @param resolveSequencePosition Obtains the position of the item relevant for the re-ordering.
     * @param resolveSequenceKey Groups the items so that they belong to the same re-ordering sequence.
     * @param resolveOffset Obtains an absolute offset of an item in order to determine if it belongs
     * to the window in an unequivocal way.
     */
    public Resequencer(long windowSize,
                       ToLongFunction<T> resolveTick,
                       ToIntFunction<T> resolveSequencePosition,
                       Function<T, Object> resolveSequenceKey,
                       Function<T, C> resolveOffset) {
        window = new SlidingWindow<>(windowSize, resolveTick);
        sequencer = new Sequencer<>(resolveSequencePosition, resolveSequenceKey);
        this.resolveOffset = resolveOffset;
    }

    /**
     * Make this resequencer actively checking and writing expired items
     * (non-written items sliding out of the window)
     */
    public Resequencer withUpdateInterval(Duration updateInterval) {
        this.updateInterval = updateInterval;
        return this;
    }

    /**
     * Fine control on the frequency of updates. Useful to reduce processing under load.
     */
    public Resequencer withUpdateControl(Function<Flux<?>, Publisher<Object>> updateControl) {
        this.updateControl = updateControl;
        return this;
    }

    /**
     * Avoid writing the same items twice. Requires to keep track of the last written item.
     */
    public Resequencer withIdempotentRecovery(ResequencedItem<?, C> lastResequencedItem) {
        this.savePoint = lastResequencedItem;
        return this;
    }

    /**
     * Transform an item flux by applying this resequencer on it.
     */
    @Override
    public Flux<ResequencedItem<T, C>> apply(Flux<T> source) {
        Flux<?> flux = source
                .doOnNext(item -> {
                    sequencer.add(item);
                    window.add(item);
                });
        if (updateInterval != null) {
            flux = Flux.merge(flux, Flux.interval(updateInterval));
        }
        if (updateControl != null) {
            flux = flux.compose(updateControl);
        }
        return flux.concatMap(x -> {
            window.update();
            if (window.isEmpty()) {
                return Flux.empty();
            }

            Predicate<T> isExpired = item -> resolveOffset.apply(item)
                    .compareTo(resolveOffset.apply(window.first())) < 0;
            Iterable<T> items = sequencer.takeWith(isExpired);

            boolean isReplaying = savePoint != null &&
                    resolveOffset.apply(window.last()).compareTo(savePoint.getWindowEnd()) > 0;
            if(isReplaying) {
                return Flux.empty();
            }

            Flux<ResequencedItem<T, C>> expiredItems =
                    Flux.fromIterable(items)
                            .filter(isExpired)
                            .sort(Comparator.comparing(item -> resolveOffset.apply(item)))
                            .filter(item -> savePoint!=null || resolveOffset.apply(item).compareTo(savePoint.getWindowStart()) > 0)
                            .map(item -> new ResequencedItem<>(item,
                                    resolveOffset.apply(item),
                                    resolveOffset.apply(window.last()),
                                    true));

            Flux<ResequencedItem<T, C>> orderedItems =
                    Flux.fromIterable(items)
                            .filter(isExpired.negate())
                            .map(item -> new ResequencedItem<>(item,
                                        resolveOffset.apply(window.first()),
                                        resolveOffset.apply(window.last()),
                                        false));

            return Flux.concat(expiredItems, orderedItems);
        });
    }
}