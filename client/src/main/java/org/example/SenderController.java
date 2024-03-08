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
import java.util.concurrent.CompletableFuture;
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

            //mock up requests from multiple threads
            IntStream.rangeClosed(1, 10).parallel().forEach(i -> {

                List<RequestReplyFuture<String, String, String>> futures = new ArrayList<>();
                int origin = 0;
                // send 5 requests to different servers
                for (int j = 0; j < 5; j++) {
                    int v = new Random().nextInt(100);
                    int k = v % 10;
                    ProducerRecord<String, String> record = new ProducerRecord<>("kRequests", String.valueOf(k), String.valueOf(v));
                    RequestReplyFuture<String, String, String> replyFuture = replyingKafkaTemplate.sendAndReceive(record);
                    try {
                        replyFuture.getSendFuture().addCallback(result->{
                            System.out.println("Sent ok: " + result);
                        }, Throwable::printStackTrace);

                        futures.add(replyFuture);
                        origin += v;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                CompletableFuture<ConsumerRecord<String, String>> completableFuture = CompletableFuture.supplyAsync(() -> new ConsumerRecord<>("", 0,0,"k","0"));
                for (RequestReplyFuture<String, String, String> f : futures) {
                    CompletableFuture<ConsumerRecord<String, String>> completable = f.completable();
                    completableFuture = completableFuture.thenCombineAsync(completable,(r1,r2) ->{
                        int value1 = Integer.parseInt(r1.value());
                        int value2 = Integer.parseInt(r2.value());
                        return new ConsumerRecord<>("", 0,0,"k", String.valueOf(value1 + value2));
                    });
                }

                int finalOrigin = origin;
                completableFuture.thenAccept(r->{
                    int sum = Integer.parseInt(r.value());
                    if (finalOrigin != sum) {
                        System.err.println("origin != sum, " + finalOrigin + ", " + sum);
                    }else{
                        System.out.println("task "+ i + " is finished on thread " + Thread.currentThread());
                    }
                });
            });

            System.out.println("current thread finish");
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
