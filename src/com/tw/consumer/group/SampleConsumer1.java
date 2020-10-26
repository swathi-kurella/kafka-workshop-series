package com.tw.consumer.group;

import static com.tw.consumer.base.Constants.SAMPLE_TOPIC;

import com.tw.consumer.base.BaseConsumer;
import com.tw.consumer.base.CommitType;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SampleConsumer1 extends BaseConsumer {

  private void startConsumer() throws InterruptedException {
    //Create BaseConsumer Properties
    Properties props = getBasicConsumerProperties();
    props.put("group.id", "test");

    //Create Kafka BaseConsumer
    KafkaConsumer<String, String> consumer = getKafkaConsumer(props);

    consumer.subscribe(Collections.singletonList(SAMPLE_TOPIC));

    pollProcessCommit(consumer, CommitType.DEFAULT);
  }

  public static void main(String[] args) throws InterruptedException {
    SampleConsumer1 consumer1 = new SampleConsumer1();
    consumer1.startConsumer();
  }

}
