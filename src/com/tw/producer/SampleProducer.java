package com.tw.producer;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SampleProducer {

  KafkaProducer getProducer() {
    //Create Producer properties
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("acks", "1");

    //Create Kafka Producer
    return new KafkaProducer<String, String>(props);
  }

  private void produce(String message) {
    KafkaProducer<String, String> producer = getProducer();
    //Send data using Kafka Producer
    producer.send(new ProducerRecord<>("kafka-workshop-eg", message));
    //producer.flush();//TODO Read why need to flush before close
    producer.close();
  }

  public static void main(String[] args) {
    SampleProducer producer = new SampleProducer();
    Scanner scan = new Scanner(System.in);
    String message = scan.next();
    while(!"exit".equalsIgnoreCase(message)) {
      producer.produce(message);
      message = scan.next();
    }
  }

}
