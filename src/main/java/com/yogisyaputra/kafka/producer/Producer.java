package com.yogisyaputra.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * project kafka-producer-consumer-java-spring-boot
 * created Friday 26/08/2022
 * author Yogi Syaputra @yogisyaputra
 */
public class Producer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0 ; i < 10 ; i++){
            ProducerRecord<String,String> record = new ProducerRecord<>("topic-java","Data ke " +i);
            producer.send(record);
        }

        producer.close();
    }
}
