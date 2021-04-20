package org.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Slf4j
public class MyProducer {

    public static void publish(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        Stream.generate(new Supplier<Integer>() {
            int count = 0;
            @Override
            public Integer get() {
                return count++;
            }
        }).limit(100).forEach(integer -> {
            producer.send(new ProducerRecord<String, String>("my-topic", String.valueOf(integer), String.valueOf(integer)));
        });

        producer.close();
    }

    public static void transaction(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("transactional.id", "my-transactional-id");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties, new StringSerializer(), new StringSerializer());
        producer.initTransactions();
        try {
            producer.beginTransaction();
            Stream.generate(new Supplier<Integer>() {
                int count = 0;
                @Override
                public Integer get() {
                    return count++;
                }
            }).limit(10).forEach(integer -> {
                producer.send(new ProducerRecord<String, String>("my-topic-transaction", String.valueOf(integer), String.valueOf(integer)), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        log.info("onCompletion: {}", metadata, exception);
                    }
                });
            });
            producer.commitTransaction();
        }catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException exception){
            exception.printStackTrace();
        }catch (KafkaException e){
            e.printStackTrace();
            producer.abortTransaction();
        }finally {
            producer.close();
        }
    }

    public static void main(String[] args) {
        MyProducer.transaction();
    }
}
