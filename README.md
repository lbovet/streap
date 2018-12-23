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


    orderReceiver.receive()
        .compose(createBlock(orderSender))
        .concatMap(b -> b.items()
            .map(r -> r.value)
            .flatMap(b.once(storage::createOrder))
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

Block {
    Block
}