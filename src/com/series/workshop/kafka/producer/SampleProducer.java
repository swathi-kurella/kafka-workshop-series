package com.series.workshop.kafka.producer;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SampleProducer {

  protected KafkaProducer<String, String> producer;

  protected void initProducer() {
    //Create Producer properties
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    //Create Kafka Producer
    producer = new KafkaProducer<>(props);
  }

  private void produce(String message) {
    //Send data using Kafka Producer
    producer.send(new ProducerRecord<>(SAMPLE_TOPIC, message));
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException{
    SampleProducer producer = new SampleProducer();
    producer.initProducer();

    Scanner scan = new Scanner(System.in);
    while(true) {
      String message = scan.next();
      producer.produce(message);
    }
  }

}
