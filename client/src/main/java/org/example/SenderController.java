package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@RestController
public class SenderController {

    @Autowired
    ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("send")
    public String send() {
        try {

            IntStream.rangeClosed(1, 10).parallel().forEach(i -> {
                List<RequestReplyFuture<String, String, String>> futures = new ArrayList<>();
                int origin = 0;
                for (int j = 0; j < 5; j++) {
                    int v = new Random().nextInt(100);
                    int k = v % 10;
                    ProducerRecord<String, String> record = new ProducerRecord<>("kRequests", String.valueOf(k), String.valueOf(v));
                    RequestReplyFuture<String, String, String> replyFuture = replyingKafkaTemplate.sendAndReceive(record);
                    try {
                        SendResult<String, String> sendResult = replyFuture.getSendFuture().get(10, TimeUnit.SECONDS);
                        System.out.println("Sent ok: " + sendResult.getRecordMetadata());
                        futures.add(replyFuture);
                        origin += v;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                int sum = 0;
                for (RequestReplyFuture<String, String, String> f : futures) {
                    try {
                        ConsumerRecord<String, String> consumerRecord = f.get(10, TimeUnit.SECONDS);
                        System.out.println("Return value: " + consumerRecord.value());
                        sum += Integer.parseInt(consumerRecord.value());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                if (origin != sum) {
                    System.err.println("origin != sum, " + origin + ", " + sum);
                }
            });

            return "success";
        } catch (Throwable t) {
            t.printStackTrace();
            return t.getMessage();
        }
    }

    @GetMapping("oneway")
    public String oneway() {
        try {
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("kRequests", "KKK", "VVV");
            future.addCallback(System.out::println, Throwable::printStackTrace);
            return "success";
        } catch (Throwable e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }
}
