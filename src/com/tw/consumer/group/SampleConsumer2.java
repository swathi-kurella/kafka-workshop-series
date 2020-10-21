package com.tw.consumer.group;

import com.tw.consumer.BaseConsumer;
import com.tw.consumer.CommitType;

public class SampleConsumer2 extends BaseConsumer {

  public static void main(String[] args) throws InterruptedException {
    SampleConsumer2 consumer2 = new SampleConsumer2();
    consumer2.consume(CommitType.SYNC);

  }

}
