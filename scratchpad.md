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


new StreamProcessor()
  .receiving(receiverOptions)
  .sending(senderOptions)
  .using(PlatformTransactionBlock.supplier(transactionTemplate))
  .process((records, context) -> records
     .map(ConsumerRecord::value)
     .flatMap(context.wrap(saveName))
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