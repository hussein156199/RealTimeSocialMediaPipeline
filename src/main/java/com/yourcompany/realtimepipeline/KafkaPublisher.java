package com.yourcompany.realtimepipeline;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class KafkaPublisher {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";  // adjust if using remote server
    private static final String TOPIC = "filtered-posts";

    private static Producer<String, String> producer;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try {
            producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            System.err.println("🚨 Error initializing Kafka producer: " + e.getMessage());
            throw new RuntimeException("Failed to initialize Kafka producer", e);
        }
    }

    public static void sendMessage(String message) {

        try {
            producer.send(new ProducerRecord<>(TOPIC, message)).get(); // Synchronous send
            System.out.println("\uD83D\uDD34 Message sent to Kafka topic: " + TOPIC);
        } catch (Exception e) {
            System.err.println("🚨 Error sending message to Kafka: " + e.getMessage());
            throw new RuntimeException("Failed to send message to Kafka", e);
        }
    }

    public static void close() {
        try {
            producer.close();
            System.out.println("📴 Kafka producer closed");
        } catch (Exception e) {
            System.err.println("🚨 Error closing Kafka producer: " + e.getMessage());
        }
    }
}