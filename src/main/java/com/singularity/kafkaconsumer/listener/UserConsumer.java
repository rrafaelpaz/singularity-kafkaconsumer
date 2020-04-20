package com.singularity.kafkaconsumer.listener;

import com.singularity.kafkaconsumer.model.User;
import com.singularity.kafkaconsumer.model.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserConsumer {

    @Autowired
    UserRepository userRepository;

    @KafkaListener(topics = "user-topic-final-data",
                    groupId = "group-consumer-user",
                    containerFactory = "userKafkaListenerFactory")
    public void userConsumer(User user){
        userRepository.save(user);
        System.out.println(user);
    }
}
