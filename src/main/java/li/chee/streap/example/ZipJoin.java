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
package li.chee.streap.example;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

/**
 * @author Laurent Bovet <laurent.bovet@swisspush.org>
 */
public class ZipJoin {

    interface Storage {
        void store(Horse horse);
        void store(Jockey jockey);
        Pair get(String s);
    }

    Flux<Horse> horses;
    Flux<Jockey> jockeys;

    class Item {
        String key;
    }

    class Horse extends Item {
        String value;
    }

    class Jockey extends Item {
        String value;
    }

    class Pair extends Item {
        String key;
        Jockey jockey;
        Horse horse;
    }

    Storage storage;

    public void example() {
        Flux.merge(
            jockeys.doOnNext(storage::store),
            horses.doOnNext(storage::store))
                .map( x -> storage.get(x.key))
                .distinct()
                .subscribe(System.out::println);

    }

}
