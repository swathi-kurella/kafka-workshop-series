package com.tw.consumer.offsetcommits;

import com.tw.consumer.BaseConsumer;
import com.tw.consumer.CommitType;

public class ConsumerOffsets extends BaseConsumer {

  public static void main(String[] args) throws InterruptedException {
    ConsumerOffsets consumer = new ConsumerOffsets();
    consumer.consume(CommitType.SYNC); //TODO: try other commit Types: ASYNC, ASYNC with callback
    //if enable.auto.commit is set to true,
    // offsets of previous poll are committed async with a default callBack once the next poll starts
  }

}
