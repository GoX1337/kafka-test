package org.gox.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private KafkaConsumer<Integer, String> consumer;

    public Consumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "gox_app");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumer = new KafkaConsumer<>(props);
    }

    public void listenEvents(){
        consumer.subscribe(Collections.singleton(KafkaProperties.TOPIC));
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<Integer, String> record : records) {
                logger.info("Key: " + record.key() + ", Partition: " + record.partition() + ", Offset: " + record.offset() + ", Value: " + record.value());
            }
        }
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        consumer.listenEvents();
    }
}
