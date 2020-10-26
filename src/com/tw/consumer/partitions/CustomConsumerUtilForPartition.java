package com.tw.consumer.partitions;

import static com.tw.consumer.base.Constants.SAMPLE_TOPIC;

import com.tw.consumer.base.BaseConsumer;
import com.tw.consumer.base.CommitType;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class CustomConsumerUtilForPartition extends BaseConsumer {

  private void consumeFromPartition() throws InterruptedException {
    //Create BaseConsumer Properties
    Properties props = getBasicConsumerProperties(); //Props without group id
    props.put("group.id", "");

    //Create Kafka BaseConsumer
    KafkaConsumer<String, String> consumer = getKafkaConsumer(props);

    //Manually assign partitions
    consumer.assign(Collections.singletonList(new TopicPartition(SAMPLE_TOPIC, 2)));

    pollProcessCommit(consumer, CommitType.DEFAULT);
  }

  public static void main(String[] args) throws InterruptedException {
    CustomConsumerUtilForPartition consumer = new CustomConsumerUtilForPartition();
    consumer.consumeFromPartition();
  }
}
