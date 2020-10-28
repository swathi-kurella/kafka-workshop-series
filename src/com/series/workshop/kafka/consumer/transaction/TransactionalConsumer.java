package com.series.workshop.kafka.consumer.transaction;

import com.series.workshop.kafka.consumer.base.BaseConsumer;
import com.series.workshop.kafka.consumer.base.CommitType;
import com.series.workshop.kafka.consumer.base.Constants;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class TransactionalConsumer extends BaseConsumer {
  /**
    * You can consume only committed offsets by producer part of transaction by setting isolation.level to read_committed
  */
private void startConsumer() throws InterruptedException {
  //Create BaseConsumer Properties
  Properties props = getBasicConsumerProperties();
  props.put("group.id", "test");
  props.put("isolation.level", "read_committed");

  //Create Kafka BaseConsumer
  KafkaConsumer<String, String> consumer = getKafkaConsumer(props);

  consumer.subscribe(Collections.singletonList(Constants.SAMPLE_TOPIC));

  pollProcessCommit(consumer, CommitType.DEFAULT);
}

public static void main(String[] args) throws InterruptedException {
  TransactionalConsumer consumer = new TransactionalConsumer();
  consumer.startConsumer();
}

}
