package org.gox.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private KafkaProducer<Integer, String> producer;

    public Producer(String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    public void sendEvent(Integer messageKey, String messageStr){
        try {
            producer.send(new ProducerRecord<>(KafkaProperties.TOPIC, null, messageStr)).get();
            logger.info("Sent message: (" + messageKey + ", " + messageStr + ")");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static Thread createProducerThread(String clientId, int delay){
        return new Thread(() -> {
            Producer producer = new Producer(clientId);
            try {
                int i = 0;
                while(true) {
                    producer.sendEvent(0, clientId + "-" + i++ + " " + LocalDateTime.now());
                    Thread.sleep(delay);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public static void main(String[] args){
        Thread producer1 = createProducerThread("producer-1", 1000);
        Thread producer2 = createProducerThread("producer-2", 1000);
        Thread producer3 = createProducerThread("producer-3", 1000);

        producer1.start();
        producer2.start();
        producer3.start();
    }
}
