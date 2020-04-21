package com.singularity.kafkaconsumer.listener;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.singularity.kafkaconsumer.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserConsumer {

    @Autowired
    DynamoDBMapper dynamoDBMapper;

    @KafkaListener(topics = "user-topic-final-data",
                    groupId = "group-consumer-user",
                    containerFactory = "userKafkaListenerFactory")
    public void userConsumer(User user){
        createUser(user);
        System.out.println("committed into DynamoDB");
    }

    private void createUser(User user) {
        try {
            dynamoDBMapper.save(user);
        } catch (AmazonServiceException e) {
            System.out.println( e.getMessage());
        } catch (AmazonClientException e) {
            System.out.println( e.getMessage());
        }
    }

}
