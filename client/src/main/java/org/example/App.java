package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.TopicPartitionOffset;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    public ApplicationRunner runner(ReplyingKafkaTemplate<String, String, String> template) {
        return args -> {
            try {
                if (!template.waitForAssignment(Duration.ofSeconds(10))) {
                    throw new IllegalStateException("Reply container did not initialize");
                }

                template.setReplyErrorChecker(record -> {
                    Header error = record.headers().lastHeader("serverSentAnError");
                    if (error != null) {
                        System.err.println(new String(error.value()));
                        return new RuntimeException(new String(error.value()));
                    }
                    else {
                        return null;
                    }
                });

//                template.setSharedReplyTopic(true); //not need

            } catch (Throwable t) {
                t.printStackTrace();
            }
        };
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
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:58896");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
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
        return factory;
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, String> repliesContainer() {
        ConcurrentMessageListenerContainer<String, String> repliesContainer =
                kafkaListenerContainerFactory().createContainer(new TopicPartitionOffset("kReplies", Integer.parseInt(System.getProperty("partition"))));
        repliesContainer.getContainerProperties().setGroupId("replyGroup");
//        Properties props = new Properties();
//        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // so the new group doesn't get old replies
//        repliesContainer.getContainerProperties().setKafkaConsumerProperties(props);
        repliesContainer.setAutoStartup(false);
        return repliesContainer;
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingTemplate(
            ConcurrentMessageListenerContainer<String, String> repliesContainer) {
        return new ReplyingKafkaTemplate<>(producerFactory(), repliesContainer);
    }

}