package com.singularity.kafkaconsumer.config;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DynamoDBConfig {
    @Bean
    public DynamoDBMapper dynamoDBMapper() {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                //.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("AKIA27QG4B5C2C4TLLU5", "tWk9oMVkSrUU6PSxC0ULgi1vLKwcePYgJhNdLS5o")))
                .withRegion(Regions.AP_SOUTHEAST_2)
                .build();
        return new DynamoDBMapper(client, DynamoDBMapperConfig.DEFAULT);
    }
}

