package com.singularity.kafkaconsumer.listener;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.singularity.kafkaconsumer.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class UserConsumer {

    @Autowired
    DynamoDBMapper dynamoDBMapper;

    @KafkaListener(topics = "user-topic-final-data",
                    groupId = "group-consumer-user",
                    containerFactory = "kafkaListenerContainerFactory")
    public void userConsumer(String input){
        System.out.println("entered userConsumer method");
        User user = parseJson(input);
        System.out.println("parsed input");
        createUser(user);
        System.out.println("committed into DynamoDB");
    }

    private User parseJson(String  json){
        ObjectMapper mapper = new ObjectMapper();
        User user = null;
        try {
            user = mapper.readValue(json, User.class);
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return user;
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
