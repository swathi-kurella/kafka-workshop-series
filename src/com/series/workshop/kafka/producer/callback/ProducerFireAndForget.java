package com.series.workshop.kafka.producer.callback;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import com.series.workshop.kafka.producer.SampleProducer;
import java.util.Scanner;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerFireAndForget extends SampleProducer {

  /**
   * TODO Pause or stop the broker container in between producing and bring it back within delivery.timeout.ms
   * eg: docker pause broker & docker unpause broker
   * TODO Keep the console consumer running
   * eg: bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-workshop-eg --group validate
  **/
  private void produceAndForget(String message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SAMPLE_TOPIC, message);

    //Send data using Kafka Producer
    producer.send(producerRecord);
  }

  public static void main(String[] args) {
    ProducerFireAndForget producer = new ProducerFireAndForget();
    producer.initProducer();

    Scanner scan = new Scanner(System.in);
    while(true) {
      String message = scan.next();
      producer.produceAndForget(message);
    }
  }

}
