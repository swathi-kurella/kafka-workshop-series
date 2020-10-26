package com.series.workshop.kafka.consumer.offsetcommits;

import static com.series.workshop.kafka.consumer.base.Constants.SAMPLE_TOPIC;

import com.series.workshop.kafka.consumer.base.BaseConsumer;
import com.series.workshop.kafka.consumer.base.CommitType;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerOffsetsManualCommit extends BaseConsumer {

  private KafkaConsumer<String, String> getKafkaConsumer() {
    //Create BaseConsumer Properties
    Properties props = getBasicConsumerProperties();

    props.put("enable.auto.commit", false);//Disable auto commit

    //Create Kafka BaseConsumer
    KafkaConsumer<String, String> consumer = getKafkaConsumer(props);

    consumer.subscribe(Collections.singletonList(SAMPLE_TOPIC));
    return consumer;
  }

  private void consumeAndCommitManualSync() throws InterruptedException {
    KafkaConsumer<String, String> consumer = getKafkaConsumer();

    pollProcessCommit(consumer, CommitType.SYNC);
  }

  private void consumeAndCommitManualAsync() throws InterruptedException {
    KafkaConsumer<String, String> consumer = getKafkaConsumer();

    pollProcessCommit(consumer, CommitType.ASYNC);
  }

  private void consumeAndCommitManualAsyncWithCallback() throws InterruptedException {
    KafkaConsumer<String, String> consumer = getKafkaConsumer();

    pollProcessCommit(consumer, CommitType.ASYNC_CALLBACK);
  }

  public static void main(String[] args) throws InterruptedException {
    ConsumerOffsetsManualCommit consumerOffsets = new ConsumerOffsetsManualCommit();
    consumerOffsets.consumeAndCommitManualSync();
    //consumerOffsets.consumeAndCommitManualAsync();
    //consumerOffsets.consumeAndCommitManualAsyncWithCallback();
  }

}
