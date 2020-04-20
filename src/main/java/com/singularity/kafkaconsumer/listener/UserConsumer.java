package com.singularity.kafkaconsumer.listener;

import com.singularity.kafkaconsumer.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserConsumer {

    @KafkaListener(topics = "user-topic-final-data",
                    groupId = "group-consumer-user",
                    containerFactory = "userKafkaListenerFactory")
    public void userConsumer(User user){
        System.out.println(user);
    }
}
