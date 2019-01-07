# Streap
    
Write streaming pipelines using blocks.

Blocks are units of work. They synchronize transactional contexts during the processing of stream items.
    
    orderReceiver
        .receiveExactlyOnce(confirmationSender.transactionManager()))
        .compose(createBlock(confirmationSender.transactionManager(),
                 PlatformTransactionBlock.supplier(transactionTemplate)))
        .concatMap(b -> b.items()
            .map(r -> r.value)
            .flatMap(b.wrapOnce(storage::createOrder))  // don't run this again after a failure
            .flatMap(order -> service.getArticleAvailabilities(order))
                .flatMap(b.wrap(a -> storage.setAvailability(order, a)) // delegate this to be executed in the block
                .compose(sendAvailability)
                .all(a -> a.qty >= 0)
                .filter(Boolean.TRUE::equals)
                .compose(sendConfirmation)
                .timeout(Duration.ofSeconds(30)))
            .doOnComplete(b::commit)
            .doOnError(b::abort))
        .subscribe()

See [Reactor Kafka Exactly-Once Flow](https://projectreactor.io/docs/kafka/release/reference/#exactly-once)
