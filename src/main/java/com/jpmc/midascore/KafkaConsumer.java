package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public KafkaConsumer(UserRepository userRepository,
                         TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void receive(Transaction transaction) {

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Rule 1: both users must exist
        if (sender == null || recipient == null) {
            return;
        }

        // Rule 2: sender must have enough balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // Apply transfer
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        userRepository.save(sender);
        userRepository.save(recipient);

        // Record the transaction
        TransactionRecord record =
                new TransactionRecord(sender, recipient, transaction.getAmount());

        transactionRecordRepository.save(record);
    }
}
