# Processor API

    KafkaStreamProcessor
      .from(receiverOptions)
      .to(senderOptions)
      .transactional(() -> new PlatformTransaction(transactionTemplate))
      .idempotent(offsetStore)
      .using(() -> DSLContext.using(configuration))
      .parallel(2)
      .process((records, ctx, create) -> records
         .map(ConsumerRecord::value)
         .flatMap(ctx.runOnce(audit))
         .flatMap(ctx.run(saveName))
         .flatMap(ctx.lift(loadAddresses))
         .map(ResultSetUtil::stream)
         .flatMap(ctx.apply())
         .flatMap(ctx.run( x -> create.update(AUTHOR).set(AUTHOR.FIRSTNAME, x).execute()))
         .map(createConfirmation))  
             
    KafkaStreamProcessor
      .from(receiverOptions)
      .to(senderOptions)
      .transactional(() -> new PlatformTransaction(transactionTemplate))
      .process((records, ctx) -> records
         .map(ConsumerRecord::value)
         .flatMap(ctx.sync(saveName))
         .map(createConfirmation))           
             
    KafkaStreamProcessor
      .from(receiverOptions)
      .to(senderOptions)
      .process(records -> records
         .map(ConsumerRecord::value)
         .map(createConfirmation))           
          
             
    KafkaStreamProcessor
      .from(receiverOptions)
      .transactional(PlatformTransactionBlock.supplier(transactionTemplate))
      .process((records, context) -> records
         .map(ConsumerRecord::value)
         .flatMap(context.wrap(saveName)))
      
    KafkaStreamProcessor
      .from(sourceFlux)
      .to(senderOptions)
      .transactional(PlatformTransactionBlock.supplier(transactionTemplate))
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
          .transactional(PlatformTransactionBlock.supplier(transactionTemplate));
                
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

# Dependency Management

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


# Resequencer 
             
## processor

    w = new Window(duration)
    last = output.poll(output.committed() - 1)
    
    input.seek(last.window.start || 0)
        .doOnNext(record ->
            sequencer.add(record)
            window.add(record)
        .zipWith(timer)
        .doOnNext( r -> window.update() )
        .doOnNext( r -> performWrites() )
            
    performWrites()        
        sequencer.selectAlso(r.offset < window.start().offset)
        writable = sequencer.take()
        if(window.end().offset() > last.window.end())
            write(writable), window

## sequencer

    class sequencer
        sequences = new LinkedHashMap()
        add(item)
            sequences.putIfAbsent(item.key, new Sequence()).add(item)
        take(predicate)
            result = []
            it = sequences.iterator()
            while(it.HasNext())
                s = it.next()
                result.add(s.take(predicate))
                if(s.isEmpty())
                    it.remove()
            return result                    
        
    class Sequence
        expected = 0
        add(item)
            items.add(item)
        selectAlso(selector)
            selector = selector
        take()
            result = []
            it = items.iterator()
            while(it.hasNext())
                item = it.next()
                selected = predicate.apply(selector))
                if(item.value == expected)
                    expected++
                    selected = true
                if(selected)
                    result.add(it.remove())
            return result

## window

    class window(duration)
        items = Deque()
        add(item)
            items.addLast(item)
        update()
            if(!items.empty() && itemsPeekLast().timestamp > items.peekFirst().timestamp + duration)
                items.removeFirst() 
        first() 
           return items.peekFirst()                 
        last()
            end = items.peekLast()
            


