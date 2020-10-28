package com.series.workshop.kafka.producer.ordering;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import com.series.workshop.kafka.producer.SampleProducer;
import com.series.workshop.kafka.producer.callback.ProducerFireAndForget;
import com.series.workshop.kafka.producer.transaction.ProducerUtil;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PartitionOrderingProducer extends SampleProducer {

  /**
   * TODO Pause the consumer group for validation during producing. Unpause the consumer group for validation after producing
   * eg: bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-workshop-eg --group validate
  **/
  private void produce(String key, String message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SAMPLE_TOPIC, key, key+"-"+message);

    //Send data using Kafka Producer
    producer.send(producerRecord);
  }

  public static void main(String[] args) {
    PartitionOrderingProducer producer = new PartitionOrderingProducer();
    producer.initProducer();

    Scanner scan = new Scanner(System.in);
    for (int i = 0; i < 10; i++) {
      String message = scan.next();
      producer.produce(getKey(i), message);
    }
  }

  private static String getKey(int i) {
    return String.valueOf(i%3); //Generate 0,1,2 keys for all messages and observe the ordering with in the partition for messages with same key
  }

}