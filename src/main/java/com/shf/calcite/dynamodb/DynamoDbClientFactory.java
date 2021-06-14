package com.shf.calcite.dynamodb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

/**
 * @author songhaifeng
 */
public class DynamoDbClientFactory {

    public static AmazonDynamoDB dynamoDbClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(System.getProperties().getProperty("accessKey"), System.getProperties().getProperty("secretKey"))))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(System.getProperties().getProperty("endpoint"), System.getProperties().getProperty("region")))
                .build();
    }

    public static DynamoDBMapper dynamoDbMapper() {
        return new DynamoDBMapper(dynamoDbClient());
    }


    public static DynamoDB dynamoDb() {
        return new DynamoDB(dynamoDbClient());
    }


}
