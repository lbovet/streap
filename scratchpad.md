whead = 0
rhead = 0

----

send(m):

begin
    update where p = whead set message = m and whead >= rhead
    if(1)
        update whead = (whead + 1) % size
commit

----

receive():

begin
    update rhead = (rhead + 1) % size where rhead < whead
    if(1)
        select from message where pos = rhead

----

===> Interfaces: Mono -> Publisher ??

```
new KafkaStreamProcessor()
  .<Long,Order>receiving(receiverOptions)   // ReceivingProcessor
  .idempotently(offsetStore)    // IdempotentReceivingProcessor
  .sending(senderOptions)
  .using(PlatformTransactionBlock.supplier(transactionTemplate))
  .process((records, context) -> records
     .map(ConsumerRecord::value)
     .flatMap(context.doOnce(audit))
     .flatMap(context.wrap(saveName))
     .map(createConfirmation))  // Flux<Processor>
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
 
Mono<Void> graph(Edge... edges) {
    Flux.of(edges)
        .groupBy(Edge::target)
        .map(Flux::replay)      // because we traverse the group twice
        .flatMap(edges ->
            edges
                .count()
                .map(n -> edges.key().publish().refCount(n))
                .flatMap(target -> edges
                    .map(edge -> edge.newTarget(target)))
        .groupBy(Edge::source)
        .map(Flux::replay)
        .flatMap(edges ->
                   edge.newSource(edge.source().depends(edges.map(Edge::newTarget))
                   .map(edge -> edgetarget))
                .
        .flatMap(Edge::combine)
        .groupBy(Edge::index)
        .
        
        

     
KafkaStreamProcessor
  .create()
  .receiving(receiverOptions)
  .using(PlatformTransactionBlock.supplier(transactionTemplate))
  .process((records, context) -> records
     .map(ConsumerRecord::value)
     .flatMap(context.wrap(saveName)))
  
KafkaStreamSource
  .create()
  .sending(senderOptions)
  .using(PlatformTransactionBlock.supplier(transactionTemplate))
  .
  .process((lines,context) -> lines
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
    new StreamProcessor()
      .receiving(receiverOptions)
      .sending(senderOptions)
      .using(PlatformTransactionBlock.supplier(transactionTemplate));
            
processor.process(saveAndConfirmRecords);

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

---

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
            
            
             