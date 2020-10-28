package com.series.workshop.kafka.producer.callback;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import com.series.workshop.kafka.producer.SampleProducer;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerBlock extends SampleProducer {

  /**
   * TODO Pause or stop the broker container in between producing and bring it back within delivery.timeout.ms
   * eg: docker pause broker & docker unpause broker
   * TODO Keep the console consumer running
   * eg: bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-workshop-eg --group validate
  **/
  private void produceAndBlock(String message) throws ExecutionException, InterruptedException {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SAMPLE_TOPIC, message);

    System.out.println("send()-->" + new Date());

    //Send data using Kafka Producer
    RecordMetadata metadata = producer.send(producerRecord).get();

    System.out.println("onCompletion()-->" + new Date());

    //start some other task
    try {
      someOtherTask();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  public static void main(String[] args)
      throws ExecutionException, InterruptedException {
    ProducerBlock producer = new ProducerBlock();
    producer.initProducer();

    Scanner scan = new Scanner(System.in);
    while (true) {
      String message = scan.next();
      producer.produceAndBlock(message);
    }
  }

  private void someOtherTask() throws InterruptedException {
    System.out.println("[" + Thread.currentThread() + "] started Other Task.. -->" + new Date());
    Thread.sleep(3000);
    System.out.println("[" + Thread.currentThread() + "] done Other Task.. -->" + new Date());
  }

}
