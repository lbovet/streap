# Streap
[![Build Status](https://travis-ci.org/lbovet/streap.svg?branch=master)](https://travis-ci.org/lbovet/streap)

<p align='right'>A <a href="https://developer.post.ch/">swisspost</a> project <a href="https://developer.post.ch/" border=0> <img align="top"  src='https://avatars.githubusercontent.com/u/92710854?s=32&v=4'></a></p>

<p align='center'><img src='https://user-images.githubusercontent.com/692124/51073824-f0001500-1676-11e9-9300-b89f090f89b5.png' /></p>

Write streaming pipelines using blocks.

Blocks are units of work. They synchronize transactional contexts during the processing of streamed items.
    
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
