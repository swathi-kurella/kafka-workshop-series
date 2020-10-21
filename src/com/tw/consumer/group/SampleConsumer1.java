package com.tw.consumer.group;

import com.tw.consumer.BaseConsumer;
import com.tw.consumer.CommitType;

public class SampleConsumer1 extends BaseConsumer {

  public static void main(String[] args) throws InterruptedException {
    SampleConsumer1 consumer1 = new SampleConsumer1();
    consumer1.consume(CommitType.SYNC);
  }

}
