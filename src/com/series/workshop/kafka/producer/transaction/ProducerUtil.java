package com.series.workshop.kafka.producer.transaction;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerUtil {

  Properties getBasicProducerProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return props;
  }


  void adjustDeliveryTimeout(Properties props) {
    props.put("linger.ms", 0);
    props.put("request.timeout.ms", 20000);

    props.put("delivery.timeout.ms", 20000); //should be equal to or larger than linger.ms + request.timeout.ms
  }


  void produceWithCallback(KafkaProducer<String, String> producer, String message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SAMPLE_TOPIC, message);
    System.out.println("send()-->" + new Date());
    //Send data using Kafka Producer
    producer.send(producerRecord, (metadata, exception) -> {
      if (exception == null) {
        System.out.println("onSuccess()-->" + new Date());
        System.out.println(
            "Successfully produced the message to partition: " + metadata.partition() + " and offset: " + metadata
                .offset());
      } else {
        System.out.println("onError()-->" + new Date());
        System.out.println("Problem occurred while producing the message: " + producerRecord.value());
      }
    });
  }

  void produce(KafkaProducer<String, String> producer, String message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SAMPLE_TOPIC, message);

    //Send data using Kafka Producer
    producer.send(producerRecord);
  }

  /**
   * In a typical Streaming app (read-write cycle)
   * i.e Consume from source topic -> process -> produce to destination topic
   * You can commit the consumed offsets as part of the transaction by adding offsets to the txn request
   *
   * In this example we have used a simple scanner as consumer for simplicity.
   */
  void readWriteCycle(Scanner scan, KafkaProducer<String, String> producer) throws InterruptedException {

    //Consume
    String message = scan.next();

    //process
    sleep();

    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SAMPLE_TOPIC, message);

    //Produce
    producer.send(producerRecord);
  }

  void sleep() throws InterruptedException {
    System.out.println("processing..");
    Thread.sleep(2000);
  }

}
