package com.shf.calcite.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.shf.calcite.dynamodb.DynamoDbClientFactory;
import com.shf.calcite.dynamodb.mapper.scanner.TableModelScanner;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;
import java.util.Optional;

/**
 * description :
 * 支持多个packageName配置，通过逗号`,`分割。
 *
 * @author songhaifeng
 * @date 2021/6/12 1:51
 */
@Slf4j
public class DynamoDbMapperSchemaFactory implements SchemaFactory {
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        log.info("operand : {}", operand.toString());
        checkOperand();
        // mock aws config
        System.getProperties().put("accessKey", operand.get("accessKey").toString());
        System.getProperties().put("secretKey", operand.get("secretKey").toString());
        System.getProperties().put("region", operand.get("region").toString());
        System.getProperties().put("endpoint", operand.get("endpoint").toString());
        DynamoDBMapper dynamoDbMapper = DynamoDbClientFactory.dynamoDbMapper();
        DynamoDB dynamoDb = DynamoDbClientFactory.dynamoDb();

        String packageNames = Optional.ofNullable(operand.get("packageNames"))
                .orElseThrow(() -> new IllegalArgumentException("packageNames not config.")).toString();
        return new DynamoDbMapperSchema(dynamoDbMapper, new TableModelScanner(packageNames, dynamoDbMapper, dynamoDb));
    }

    /**
     * TODO
     *
     * @return true表示通过
     */
    private boolean checkOperand() {
        return true;
    }
}
