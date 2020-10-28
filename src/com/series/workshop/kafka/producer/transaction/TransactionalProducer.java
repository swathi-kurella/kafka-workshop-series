package com.series.workshop.kafka.producer.transaction;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class TransactionalProducer extends IdempotentProducer {

  /**
   * TODO Pause or stop the broker container in between producing and bring it back within delivery.timeout.ms
   * eg: docker pause broker & docker unpause broker
   * TODO Keep the console consumer running eg:
   * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-workshop-eg --group validate
   * TODO Make sure the below broker settings are set accordingly if the cluster has only one broker
   * transaction.state.log.replication.factor=1
   * transaction.state.log.min.isr=1
   * bin/kafka-configs.sh --zookeeper localhost:2181 --alter --entity-type topics --entity-name __transaction_state --add-config min.insync.replicas=1
   *
   * When transactional.id is set i.e transactional, enable.idempotence is implicitly set to true Therefore, the below
   * properties are also implicitly configured
   *
   * props.put("acks", "all"); //should be all props.put("max.in.flight.requests.per.connection", 4); //should be < 5
   * props.put("retries", Integer.MAX_VALUE); //should be > 0 props.put("delivery.timeout.ms", 120000); Indefinitely
   * retries till this timeout value and till it gets ack from all replica broker nodes
   *
   * References: org.apache.kafka.clients.producer.ProducerConfig.maybeOverrideEnableIdempotence
   **/

  private void initProducer() {
    //Create Producer properties
    Properties props = getBasicProducerProperties();

    props.put("transactional.id", "tx1");
    //props.put("max.block.ms", 120000);

    //Optional
    adjustDeliveryTimeout(props);

    producer = new KafkaProducer<>(props);

  }

  public static void main(String[] args) throws InterruptedException {
    TransactionalProducer transactionalProducer = new TransactionalProducer();
    transactionalProducer.initProducer();

    Scanner scan = new Scanner(System.in);

    transactionalProducer.producer.initTransactions();
    transactionalProducer.producer.beginTransaction();
    System.out.println("start");
      for (int i = 0; i < 10; i++) {
        //String message = scan.next();
        transactionalProducer.readWriteCycle(transactionalProducer.producer, String.valueOf(i)); //typical streaming app
      }
    transactionalProducer.producer.commitTransaction();
  }

}

