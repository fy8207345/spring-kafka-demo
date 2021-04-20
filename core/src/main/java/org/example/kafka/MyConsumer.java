package org.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

@Slf4j
public class MyConsumer implements Runnable{

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties, new StringDeserializer(), new StringDeserializer());
        TopicPartition topicPartition = new TopicPartition("my-topic-transaction", 0);
        consumer.assign(Arrays.asList(topicPartition));
        Map<TopicPartition, Long> topicPartitionLongMap = consumer.beginningOffsets(Arrays.asList(topicPartition));
        log.info("partations: {}", topicPartitionLongMap);
        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(5));
            List<ConsumerRecord<String, String>> records = poll.records(topicPartition);
            if(!records.isEmpty()){
                long offset = records.get(records.size() - 1).offset();
                log.info("consumed offset is : {}", offset);
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        log.info("commited : {}", offsets, exception);
                    }
                });
            }
            log.info("offset : {} - {}", consumer.partitionsFor("my-topic-transaction"), poll.count());
            for(ConsumerRecord<String, String> recordd : poll){
                log.info("offset= {}, key = {}, value = {}", recordd.offset(), recordd.key(), recordd.value());
            }
        }
    }

    public static void main(String[] args) {
        Thread thread = new Thread(new MyConsumer());
        thread.start();
    }
}
