package com.series.workshop.kafka.producer.transaction;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;

public class InOrderProducer extends ProducerUtil {

  KafkaProducer<String, String> producer;
  /**
   * TODO Pause or stop the broker container in between producing and bring it back within delivery.timeout.ms
   * eg: docker pause broker & docker unpause broker
   * TODO Keep the console consumer running
   * eg: bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-workshop-eg --group validate
  **/
  private void initProducer() {
    //Create Producer properties
    Properties props = getBasicProducerProperties();
    props.put("max.in.flight.requests.per.connection", 2);

    producer = new KafkaProducer<>(props);

  }

  public static void main(String[] args) {
    InOrderProducer inOrderProducer = new InOrderProducer();
    inOrderProducer.initProducer();

    Scanner scan = new Scanner(System.in);
    while (true) {
      String message = scan.next();
      //Pause or stop the broker container before sending eg: docker pause broker & docker unpause broker
      inOrderProducer.produce(inOrderProducer.producer, message);
    }
  }

}