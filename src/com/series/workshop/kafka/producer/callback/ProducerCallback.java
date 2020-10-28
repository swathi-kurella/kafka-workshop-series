package com.series.workshop.kafka.producer.callback;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import com.series.workshop.kafka.producer.SampleProducer;
import java.util.Date;
import java.util.Scanner;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerCallback extends SampleProducer {

  /**
   * TODO Pause or stop the broker container in between producing and bring it back within delivery.timeout.ms
   * eg: docker pause broker & docker unpause broker
   * TODO Keep the console consumer running
   * eg: bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-workshop-eg --group validate
  **/
  private void produceWithCallback(String message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SAMPLE_TOPIC, message);

    //Send data using Kafka Producer
    producer.send(producerRecord, (metadata, exception) -> {
      if (exception == null) {
        System.out.println(
            "Successfully produced the message to partition: " + metadata.partition() + " and offset: " + metadata
                .offset());
      } else {
        System.out.println("Problem occurred while producing the message: " + producerRecord.value());
      }
    });

    //start some other task
    try {
      someOtherTask();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    ProducerCallback producer = new ProducerCallback();
    producer.initProducer();

    Scanner scan = new Scanner(System.in);
    while (true) {
      String message = scan.next();
      producer.produceWithCallback(message);
    }
  }

  private void someOtherTask() throws InterruptedException {
    System.out.println("[" + Thread.currentThread() + "] started Other Task.. -->" + new Date());
    Thread.sleep(3000);
    System.out.println("[" + Thread.currentThread() + "] done Other Task.. -->" + new Date());
  }

}
