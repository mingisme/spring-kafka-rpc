package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.SendTo;

import java.nio.ByteBuffer;
import java.util.*;

@SpringBootApplication
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }


    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:58896");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> replyTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:58896");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
//        factory.setReplyTemplate(replyTemplate());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(4);
        factory.getContainerProperties().setGroupId("server");
        return factory;
    }

    public String getTopic1() {
        return "kRequests";
    }

    @KafkaListener(topics = "#{__listener.topic1}", batch = "true")
//    @SendTo
    public void listen(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) throws InterruptedException {
        System.out.println("batch size: " + records.size());
        for (ConsumerRecord<String, String> record : records) {
            Headers headers = record.headers();
            System.out.println("headers:");
            headers.forEach(head -> {
                System.out.println("key: " + head.key() + ", value: " + new String(head.value()));
            });
            System.out.println("Server received: " + record);
            acknowledgment.acknowledge();
            Thread.sleep(new Random().nextInt(100));

            String replyTopic = record.headers().lastHeader(KafkaHeaders.REPLY_TOPIC) != null ? new String(record.headers().lastHeader(KafkaHeaders.REPLY_TOPIC).value()) : null;

            if (replyTopic != null) {

                byte[] replyPartitionBytes = record.headers().lastHeader(KafkaHeaders.REPLY_PARTITION).value();
                Integer replyPartition = replyPartitionBytes != null ? ByteBuffer.wrap(replyPartitionBytes).getInt() : null;

                byte[] correlationId = record.headers().lastHeader(KafkaHeaders.CORRELATION_ID).value();

                // Use retrieved headers as needed
                System.out.println("Reply Topic: " + replyTopic);
                System.out.println("Reply Partition: " + replyPartition);
                System.out.println("Correlation ID: " + correlationId);

                ProducerRecord<String, String> replyRecord = new ProducerRecord<>(replyTopic, replyPartition, record.key(), record.value());
                replyRecord.headers().add(KafkaHeaders.CORRELATION_ID, correlationId);
                replyTemplate().send(replyRecord);
            }
        }

        //return record.value().toUpperCase(Locale.ROOT);
    }
}