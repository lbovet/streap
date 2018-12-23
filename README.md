# Streap

## Use cases

_( ... ) denotes storage transaction boundaries_

_{ ... } denotes receive transaction boudaries_

_< ... > denotes send transaction boudaries_


### Process without side-effect
[Reactor Kafka Exactly-Once Flow](https://projectreactor.io/docs/kafka/release/reference/#exactly-once)

    {< Receive, Process, Emit >}

### Process with side effects
Process updates the local state

    {( Receive, Store )} -> ( Poll, Process, Store) -> <( Poll, Emit )>

[Reactive pipeline with Kafka source](https://projectreactor.io/docs/kafka/release/reference/#kafka-source)

    receive()
        .compose(receiveQueue.writeEach().then(ack))
        .flatMap(b ->
             b.unwrap().flatMap(service::process)
                       .flatMap(b.execute(storage::update))
                       .compose(sendQueue.writeAll().then(b::commit)))
        .compose(send())

    orderReceiver.receive()
        .compose(createBlock(orderSender))
        .concatMap(b -> b.items()
            .map(r -> r.value)
            .flatMap(b.run(storage::createOrder))
            .flatMap(order -> service.getArticleAvailabilities(order))
                .flatMap(b.run(a -> storage.setAvailibility(order, a))
                .compose(sendAvailability())
                .all(a -> a.qty >= 0)
                .filter(Boolean.TRUE::equals)
                .compose(orderSender::send)
                .timeout(Duration.ofSeconds(30))
                .doOnComplete(b::commit)
                .doOnError(b::rollback)
            )
        .subscribe()

    receive()
        .compose(runBlock(b -> b.items()
              .map(r -> r.value)
              .flatMap(storage::createOrder)
              .flatMap(order -> service.getArticlesAvailability(order))
                  .flatMap(b.run(a -> storage.setAvailibility(order, a))
                  .compose(sendAvailability())
                  .all(a -> a.qty >= 0)
                  .filter(Boolean.TRUE::equals)
                  .compose(sendOrder())
            ).subscribe()

    writeCommit(Queue q) {
        return f -> {

            return q.read();
        }
    }

    ack(Record r) {
        r.receiverOffset().acknowledge()
    }

    Queue.writeMap( Function<T,T> mapper, Consumer<T> onCommit): Function<Flux<T>, Flux<Tx<T>>> {
        return f -> f.concatMap( r ->
                                q.push(r, r -> r.receiverOffset().acknowledge() )
                                .concatWith(timer(100)
                            return q.read()

    }
