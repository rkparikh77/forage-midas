package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "${general.kafka-topic}")
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void receive(Transaction transaction) {
        // For Task Two, we do nothing except receive.
        // Put a breakpoint here or log to see incoming messages.
        System.out.println("Received transaction: " + transaction);
    }
}
