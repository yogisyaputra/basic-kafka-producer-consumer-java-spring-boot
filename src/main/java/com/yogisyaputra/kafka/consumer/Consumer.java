package com.yogisyaputra.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * project kafka-producer-consumer-java-spring-boot
 * created Saturday 27/08/2022
 * author Yogi Syaputra @yogisyaputra
 */
public class Consumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("topic-java"));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record : records){
                System.out.printf("offest = %d, key = %s, value = %s",record.offset(),record.key(),record.value());
            }
        }
    }
}
