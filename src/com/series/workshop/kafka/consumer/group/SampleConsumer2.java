package com.series.workshop.kafka.consumer.group;

import com.series.workshop.kafka.consumer.base.BaseConsumer;
import com.series.workshop.kafka.consumer.base.Constants;
import com.series.workshop.kafka.consumer.base.CommitType;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SampleConsumer2 extends BaseConsumer {

  private void startConsumer() throws InterruptedException {
    //Create BaseConsumer Properties
    Properties props = getBasicConsumerProperties();
    props.put("group.id", "test");

    //Create Kafka BaseConsumer
    KafkaConsumer<String, String> consumer = getKafkaConsumer(props);

    consumer.subscribe(Collections.singletonList(Constants.SAMPLE_TOPIC));

    pollProcessCommit(consumer, CommitType.DEFAULT);
  }

  public static void main(String[] args) throws InterruptedException {
    SampleConsumer2 consumer2 = new SampleConsumer2();
    consumer2.startConsumer();
  }

}
