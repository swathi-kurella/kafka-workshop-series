package com.tw.consumer.offsetcommits;

import static com.tw.consumer.base.Constants.SAMPLE_TOPIC;

import com.tw.consumer.base.BaseConsumer;
import com.tw.consumer.base.CommitType;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerOffsetsAutoCommit extends BaseConsumer {

  private void consumeAndCommitAuto() throws InterruptedException {
    //Create BaseConsumer Properties
    Properties props = getBasicConsumerProperties();

    props.put("enable.auto.commit", true);//Enable auto commit
    props.put("auto.commit.interval.ms", 5);//Auto commit interval

    //Create Kafka BaseConsumer
    KafkaConsumer<String, String> consumer = getKafkaConsumer(props);

    consumer.subscribe(Collections.singletonList(SAMPLE_TOPIC));

    pollProcessCommit(consumer, CommitType.AUTO);
  }

  public static void main(String[] args) throws InterruptedException {
    ConsumerOffsetsAutoCommit consumer = new ConsumerOffsetsAutoCommit();
    consumer.consumeAndCommitAuto();
  }

}
