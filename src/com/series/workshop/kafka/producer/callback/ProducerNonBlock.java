package com.series.workshop.kafka.producer.callback;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import com.series.workshop.kafka.producer.SampleProducer;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerNonBlock extends SampleProducer {

  /**
   * TODO Pause or stop the broker container in between producing and bring it back within delivery.timeout.ms
   * eg: docker pause broker & docker unpause broker
   * TODO Keep the console consumer running
   * eg: bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-workshop-eg --group validate
  **/
  private void produceAndNonBlock(String message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SAMPLE_TOPIC, message);

    System.out.println("[" + Thread.currentThread() + "] send()-->" + new Date());

    //Send data using Kafka Producer
    Future<RecordMetadata> future = producer.send(producerRecord);

    //Run separate thread to check for metadata
    metadataThread(future);

    //start some other task
    try {
      someOtherTask();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  private void metadataThread(Future<RecordMetadata> future) {
    Runnable r = () -> {
      System.out.println("[" + Thread.currentThread() + "] started Metadata thread-->" + new Date());
      boolean done = false;
      while (!done) {
        if (future.isDone()) {
          try {
            System.out.println("[" + Thread.currentThread() + "] onCompletion()-->" + new Date());
            done = true;
            RecordMetadata recordMetadata = future.get();
          } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
          }
        }
      }
    };

    new Thread(r).start();
  }

  public static void main(String[] args) {
    ProducerNonBlock producer = new ProducerNonBlock();
    producer.initProducer();

    Scanner scan = new Scanner(System.in);
    while (true) {
      String message = scan.next();
      producer.produceAndNonBlock(message);
    }
  }

  private void someOtherTask() throws InterruptedException {
    System.out.println("[" + Thread.currentThread() + "] started Other Task.. -->" + new Date());
    Thread.sleep(3000);
    System.out.println("[" + Thread.currentThread() + "] done Other Task.. -->" + new Date());
  }

}
