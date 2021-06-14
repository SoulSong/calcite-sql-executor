package com.shf.calcite.dynamodb.dynamic;

import com.shf.calcite.dynamodb.DynamoDbClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 21:36
 */
@Slf4j
public class DynamoDbSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        log.info("operand : {}", operand.toString());
        // mock aws config
        System.getProperties().put("accessKey", operand.get("accessKey").toString());
        System.getProperties().put("secretKey", operand.get("secretKey").toString());
        System.getProperties().put("region", operand.get("region").toString());
        System.getProperties().put("endpoint", operand.get("endpoint").toString());

        return new DynamoDbSchema(DynamoDbClientFactory.dynamoDbClient(),DynamoDbClientFactory.dynamoDb());
    }
}
