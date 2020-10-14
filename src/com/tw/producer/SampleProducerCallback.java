package com.tw.producer;

import java.util.Scanner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SampleProducerCallback extends SampleProducer{

  private void produce(String message) {
    KafkaProducer<String, String> producer = getProducer();

    //Send data using Kafka Producer
    producer.send(new ProducerRecord<>("kafka-workshop-eg", message), new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
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
    String message = scan.next();
    while(!"exit".equalsIgnoreCase(message)) {
      producer.produce(message);
      message = scan.next();
    }
  }

}
