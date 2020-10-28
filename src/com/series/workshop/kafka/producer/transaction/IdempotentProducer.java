package com.series.workshop.kafka.producer.transaction;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;

public class IdempotentProducer extends ProducerUtil {

  KafkaProducer<String, String> producer;

  /**
   * TODO Pause or stop the broker container in between producing and bring it back within delivery.timeout.ms
   * eg: docker pause broker & docker unpause broker
   * TODO Keep the console consumer running
   * eg: bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-workshop-eg --group validate
   *
   * When enable.idempotence is set to true, the below properties are implicitly configured
   *
   * props.put("acks", "all"); //should be all
   * props.put("max.in.flight.requests.per.connection", 4); //should be < 5
   * props.put("retries", Integer.MAX_VALUE); //should be > 0
   * props.put("delivery.timeout.ms", 120000);
   * Indefinitely retries till this timeout value and till it gets ack from all replica broker nodes
   *
   * References:
   * org.apache.kafka.clients.producer.ProducerConfig.maybeOverrideAcksAndRetries
   * org.apache.kafka.clients.producer.KafkaProducer.configureRetries
   * org.apache.kafka.clients.producer.KafkaProducer.configureInflightRequests
   * org.apache.kafka.clients.producer.KafkaProducer.configureAcks
  **/

  private void initProducer() {
    //Create Producer properties
    Properties props = getBasicProducerProperties();
    props.put("enable.idempotence", true);

    //Optional
    adjustDeliveryTimeout(props);

    producer = new KafkaProducer<>(props);

  }

  public static void main(String[] args) throws InterruptedException{
    IdempotentProducer idempotentProducer = new IdempotentProducer();
    idempotentProducer.initProducer();

    Scanner scan = new Scanner(System.in);
    while (true) {
      String message = scan.next();
      idempotentProducer.produceWithCallback(idempotentProducer.producer, message);
    }
  }

}

