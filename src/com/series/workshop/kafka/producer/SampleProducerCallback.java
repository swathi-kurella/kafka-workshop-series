package com.series.workshop.kafka.producer;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SampleProducerCallback extends SampleProducer{

  private void produce(String message) {
    KafkaProducer<String, String> producer = getProducer();

    //Send data using Kafka Producer
    producer.send(new ProducerRecord<>(SAMPLE_TOPIC, message), (metadata, exception) -> {
      if(exception == null) {
        System.out.println(
            "Successfully produced the message to partition: " + metadata.partition() + " and offset: " + metadata
                .offset());
      }
    });

    //producer.flush();//TODO Read why need to flush before close
    producer.close();
  }

  public static void main(String[] args) {
    SampleProducerCallback producer = new SampleProducerCallback();
    Scanner scan = new Scanner(System.in);
    while(true) {
      String message = scan.next();
      producer.produce(message);
    }
  }

}
