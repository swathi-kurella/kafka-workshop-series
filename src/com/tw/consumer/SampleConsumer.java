package com.tw.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SampleConsumer {

  private void consume() {
    //Create Consumer Properties
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //Create Kafka Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    //Subscribe consumer to topics
    consumer.subscribe(Collections.singletonList("kafka-workshop-eg"));
    while (true) {
      //Poll for records
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      //Print records
      for (ConsumerRecord<String, String> record : records) {
        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
      }
      //Commit the messages processed successfully
      consumer.commitSync();//TODO Remove this statement and see
    }
  }

  public static void main(String[] args) {
    SampleConsumer consumer = new SampleConsumer();
    consumer.consume();
  }

}
