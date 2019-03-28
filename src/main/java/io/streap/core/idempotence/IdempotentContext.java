/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streap.core.idempotence;

import io.streap.core.context.Context;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Context providing idempotence by skipping non-idempotent operations.
 */
public interface IdempotentContext<T> extends Context, IdempotentPerformer<T> {

    /**
     * Wraps an non-idempotent operation producing a side effect inside this context.
     * This operation will not be run again. E.g. when events are replayed after failure due to broker unavailability.
     * <p>
     * Use {@link Context#wrap(Function)} for running idempotent operations.
     * They will be run again. E.g. when events are replayed after failure due to broker unavailability.
     */
    default Function<T, Mono<T>> wrapOnce(Consumer<T> operation) {
        return doOnce((t) -> {
            wrap((T u) -> {
                operation.accept(u);
                return u;
            }).apply(t);
            return Mono.empty();
        });
    }
}
