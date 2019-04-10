package io.streap.test;

import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LatchWaiter {

    public static <T> Publisher<T> waitOn(CountDownLatch latch) {
        return Mono.<T>fromRunnable(() -> {
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw Exceptions.propagate(e);
            }
        }).publishOn(Schedulers.elastic());
    }
}
