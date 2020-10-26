package com.series.workshop.kafka.consumer.base;

import java.time.Duration;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class BaseConsumer {

  protected void pollProcessCommit(KafkaConsumer<String, String> consumer, CommitType commitType)
      throws InterruptedException {
    while (true) {
      //Poll for records
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

      //Process
      sleep(); //Add some processing delay (Optional)
      printRecords(records);

      //Commit
      switch (commitType) {
        case AUTO:
          break;
        case SYNC:
          commitSync(consumer);
          break;
        case ASYNC:
          commitAsync(consumer);
          break;
        case ASYNC_CALLBACK:
          commitAsyncCallback(consumer);
          break;
        case DEFAULT:
          break; //AUTO commit is default commit type
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
      if (exception == null) {
        for (Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
          System.out.println(
              "Successfully committed the message to partition: " + e.getKey() + " and offset: " + e.getValue()
                  .offset());
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
    props.put("max.poll.records", 1);
    props.put("auto.commit.interval.ms", 5); //Will be applied only if auto commit is enables

    return props;
  }

  private void printRecords(ConsumerRecords<String, String> records) {
    for (ConsumerRecord<String, String> record : records) {
      System.out.printf("topic = %s, partition = %s, offset = %d, key = %s, value = %s%n", record.topic(),
          record.partition(), record.offset(), record.key(), record.value());
    }
  }

}

