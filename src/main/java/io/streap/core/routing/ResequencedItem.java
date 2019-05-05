package io.streap.core.routing;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ResequencedItem<T, C> {
    private T item;
    private C windowStart;
    private C windowEnd;
    private boolean expired;
}
