package com.tw.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class BaseConsumer {


  public static void main(String[] args) throws InterruptedException {
    BaseConsumer baseConsumer = new BaseConsumer();
    baseConsumer.consume(CommitType.SYNC);
  }

  protected void consume(CommitType commitType) throws InterruptedException {
    //Create BaseConsumer Properties
    Properties props = getBasicConsumerProperties();
    props.putAll(getOtherConsumerProperties());
    //Create Kafka BaseConsumer
    KafkaConsumer<String, String> consumer = getKafkaConsumer(props);
    //Subscribe consumer to topics
    //Auto assignment of partitions to consumers in group
    consumer.subscribe(Collections.singletonList("kafka-workshop-eg"));
    while (true) {
      //Poll for records
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      sleep(); //Add some processing delay (Optional)
      printRecords(records);
      switch (commitType) {
        case SYNC: commitSync(consumer); break;
        case ASYNC: commitAsync(consumer); break;
        case ASYNC_CALLBACK: commitAsyncCallback(consumer); break;
      }
    }
  }

  private void commitSync(KafkaConsumer<String, String> consumer) {
    //Manually Commit the messages processed successfully if enable.auto.commit is set to false
    //This is a blocking call
    consumer.commitSync();
  }

  private void commitAsync(KafkaConsumer<String, String> consumer) {
    //Manually Commit the messages processed successfully if enable.auto.commit is set to false
    //This is a non blocking call
    consumer.commitAsync();
  }

  private void commitAsyncCallback(KafkaConsumer<String, String> consumer) {
    //Manually Commit the messages processed successfully if enable.auto.commit is set to false
    //This is a non blocking call with callBack
    consumer.commitAsync((offsets, exception) -> {
      if(exception == null) {
        for(Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
          System.out.println(
              "Successfully committed the message to partition: " + e.getKey() + " and offset: " + e.getValue().offset());
        }
      }
    });
  }

  private void sleep() throws InterruptedException {
    System.out.println("waiting..");
    Thread.sleep(2000);
  }

  protected KafkaConsumer<String, String> getKafkaConsumer(Properties props) {
    return new KafkaConsumer<>(props);
  }



  protected Properties getBasicConsumerProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    return props;
  }

  protected Map<String, Object> getOtherConsumerProperties() {

    Map<String, Object> props = new HashMap<>();
    props.put("group.id", "test");
    props.put("enable.auto.commit", false);//Requires manual Commit
    props.put("auto.offset.reset", "latest");//TODO [earliest, latest, none] with a NEW group id and observe
    props.put("max.poll.records", 1);//TODO
    return props;
  }

  protected void printRecords(ConsumerRecords<String, String> records) {
    for (ConsumerRecord<String, String> record : records) {
      System.out.printf("topic = %s, partition = %s, offset = %d, key = %s, value = %s%n", record.topic(),
          record.partition(), record.offset(), record.key(), record.value());
    }
  }

}

