### Processor API

``
KafkaStreamProcessor
  .<Long,Order>from(receiverOptions)   // ReceivingProcessor
  .withIdempotence(offsetStore)    // IdempotentReceivingProcessor
  .to(senderOptions)
  .withContext(PlatformTransactionBlock.supplier(transactionTemplate))
  .process((records, context) -> records
     .map(ConsumerRecord::value)
     .flatMap(context.doOnce(audit))
     .flatMap(context.wrap(saveName))
     .map(createConfirmation))  // Flux<Processor>
         
KafkaStreamProcessor
  .from(receiverOptions)
  .withContext(PlatformTransactionBlock.supplier(transactionTemplate))
  .process((records, context) -> records
     .map(ConsumerRecord::value)
     .flatMap(context.wrap(saveName)))
  
KafkaStreamProcessor
  .from(sourceFlux)
  .to(senderOptions)
  .using(PlatformTransactionBlock.supplier(transactionTemplate))
  .process((lines, context) -> lines
     .map(ConsumerRecord::value)
     .flatMap(context.wrap(markAsSent))
     .map(createConfirmation))  
 
       
Flux<SenderRecord<Confirmation>> saveAndConfirmRecords(Flux<ReceiverRecords<String>> records, Context context) {
    return records
                .map(ConsumerRecord::value)
                .flatMap(context.wrap(saveName))
                .map(createConfirmation)
                .map(createRecord)
}
     
StreamProcessor processor =
    KafkaStreamProcessor
      .from(receiverOptions)
      .to(senderOptions)
      .withContext(PlatformTransactionBlock.supplier(transactionTemplate));
            
processor.process(saveAndConfirmRecords).blockLast();

Flux<SenderRecord<Confirmation>> saveAndConfirmRecords(Flux<ReceiverRecords<String>> records, Context context) {
    return records
                .map(ConsumerRecord::value)                                
                .compose(saveAndConfirm(context))
                .map(createRecord)
}

Function<Flux<String>,Flux<Confirmation>> saveAndConfirm(Context context) {
    return (items) -> items
        .flatMap(context.wrap(saveName))
        .map(createConfirmation)
}
```

## Dependency Management

```
processor
  .doAfterTerminate(()-> log("Processor terminated"))
  .publish().refcount(1, Duration.ofSeconds(1))               

source1 = source1.compose(depends(processor1, processor2))
source2 = source2.compose(depends(processor1))

merge(source1, source2)
     
Function<Flux<T>, Flux<T>> depends(Flux<U>... others) {
    return (f) -> zip(others).flatMap(x -> f.last())
}     

graph(
    edge(source1, processor1),
    edge(source1, processor2),
    edge(source2, processor2)
).blockLast()

```

### Resequencer 

```

m = new LinkedHashMap()

last = output.poll(output.committed() - 1)

input.position(timestampToOffset(last.timestamp) - window - margin)

while true
    records = poll()
    now = 0
    for r in records
        g = m.putIfAbsent(r.key, new Group())
        g.add(r)
        g.pollWritableRecords(r ->
            if(r.offset > last.offset)
                write(r)
        if(g.complete()) {
            if(r.offset > last.offset)
                writeMarker(g.key)
            m.remove(g)
        now = max(now, r.timestamp)
    if now == 0
        now = date.now()
    while g=m.peek()
        if(g.timestamp < now - window)
```