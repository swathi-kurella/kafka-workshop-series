package com.tw.consumer.partitions;

import com.tw.consumer.BaseConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class CustomConsumerUtilForPartition extends BaseConsumer {
/***
 * Read about SubscriptionType
 * private enum SubscriptionType {
 *    NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
 * }
 *     */
  private void consumeFromPartition() {
    //Create BaseConsumer Properties
    Properties props = getBasicConsumerProperties(); //Props without group id

    props.put("enable.auto.commit", false);//Requires manual Commit
    props.put("auto.offset.reset", "earliest");//TODO [earliest, latest, none] with a NEW group id and observe
    props.put("max.poll.records", 1);//TODO

    //Create Kafka BaseConsumer
    KafkaConsumer<String, String> consumer = getKafkaConsumer(props);

    //Manually assign partitions
    consumer.assign(Collections.singletonList(new TopicPartition("kafka-workshop-eg", 2)));

    while (true) {
      //Poll for records
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      printRecords(records);
      //Commit the messages processed successfully
      consumer.commitSync();//TODO Remove this statement and see
    }
  }
  public static void main(String[] args) {
    CustomConsumerUtilForPartition consumer = new CustomConsumerUtilForPartition();
    consumer.consumeFromPartition();
  }
}
