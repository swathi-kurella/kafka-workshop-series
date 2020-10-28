package com.series.workshop.kafka.consumer.group;

import com.series.workshop.kafka.consumer.base.Constants;
import com.series.workshop.kafka.consumer.base.BaseConsumer;
import com.series.workshop.kafka.consumer.base.CommitType;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
  /**
   * TODO Make sure the below broker settings are set accordingly if the cluster has only one broker
   * offsets.topic.replication.factor=1
   * bin/kafka-configs.sh --zookeeper localhost:2181 --alter --entity-type topics --entity-name __consumer_offsets --add-config min.insync.replicas=1
  **/

public class SampleConsumer1 extends BaseConsumer {

  private void startConsumer() throws InterruptedException {
    //Create BaseConsumer Properties
    Properties props = getBasicConsumerProperties();
    props.put("group.id", "test");

    //Create Kafka BaseConsumer
    KafkaConsumer<String, String> consumer = getKafkaConsumer(props);

    consumer.subscribe(Collections.singletonList(Constants.SAMPLE_TOPIC));

    pollProcessCommit(consumer, CommitType.DEFAULT);
  }

  public static void main(String[] args) throws InterruptedException {
    SampleConsumer1 consumer1 = new SampleConsumer1();
    consumer1.startConsumer();
  }

}
