# Streap

## Use cases

- _( ... ) denotes storage transaction boundaries_
- _{ ... } denotes receive transaction boudaries_
- _< ... > denotes send transaction boudaries_


### Process without side-effect
[Reactor Kafka Exactly-Once Flow](https://projectreactor.io/docs/kafka/release/reference/#exactly-once)

    {< Receive, Process, Emit >}

### Process with side effects
Process updates the local state

    {( Receive, Store )} -> ( Poll, Process, Store) -> <( Poll, Emit )>

[Reactive pipeline with Kafka source](https://projectreactor.io/docs/kafka/release/reference/#kafka-source)

    
    orderReceiver
        .receiveExactlyOnce(orderSender.transactionManager()))
        .compose(createBlock(orderSender.transactionManager(),
                 PlatformTransactionBlock.createBlock(transactionTemplate))
                    .)
        .concatMap(b -> b.items()
            .map(r -> r.value)
            .flatMap(b.once(storage::createOrder))
            .flatMap(order -> service.getArticleAvailabilities(order))
                .flatMap(b.execute(a -> storage.setAvailability(order, a))
                .compose(availabilitySender)
                .all(a -> a.qty >= 0)
                .filter(Boolean.TRUE::equals)
                .compose(orderSender::send))
                .timeout(Duration.ofSeconds(30)))
            .doOnComplete(b::commit)
            .doOnError(b::abort))
        .subscribe()

    orderSender = 

    ExactlyOnceBlock
        .createBlock(orderSender,
            PlatformTransactionBlock.createBlock(transactionTemplate))
        .receiveFrom(orderReceiver,
        b ->
            b.items()
                    .map(r -> r.value)
                    .flatMap(b.once(storage::createOrder))
                    .flatMap(order -> service.getArticleAvailabilities(order))
                        .flatMap(b.execute(a -> storage.setAvailability(order, a))
                        .compose(availabilitySender)
                        .all(a -> a.qty >= 0)
                        .filter(Boolean.TRUE::equals)
                        .compose(orderSender::send))
                        .timeout(Duration.ofSeconds(30)))


Block:
    
    begin broker tx
        read broker offset 
        begin db tx
            do db work
            remember broker offset
        commit db tx
    commit broker tx

    interface Block {
        /**
         * Runs an non-idempotent operation producing a side effect. 
         * This operation will not be run again when replaying events after failure due to broker unavailability.
         */
        Function<T,Flux<U>> once(Function<T,U>)
        
        /**
         * Runs an idempotent operation producing a side effect. 
         * This operation will be run again when replaying events after failure due to broker unavailability.
         */
        Function<T,Flux<U>> run(Function<T,U>)
        
        /**
         * Commit the block if not yet committed.
         */
        void commit()
        
        /**
         * Abort the transaction. Rollbacks if supported by the resource.
         */
        void abort()
    }