package com.shf.calcite.dynamodb.dynamic;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementResult;
import com.shf.calcite.exception.NotSupportException;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/5 1:30
 */
public class DynamoDbEnumerator implements Enumerator<Object[]> {
    /**
     * 投影的字段清单
     */
    private List<String> fieldNames;
    /**
     * 用于根据字段类型快速取值
     */
    private Map<String, String> fieldNameRefAttributeType;
    private int fieldSize;
    private Iterator<Map<String, AttributeValue>> iterator;
    private Object[] current;
    private static final ExecuteStatementRequest request = new ExecuteStatementRequest();

    public DynamoDbEnumerator(String statement, AmazonDynamoDB dynamoDb, List<String> fieldNames, Map<String, String> fieldNameRefAttributeType) {
        this.fieldNames = fieldNames;
        this.fieldSize = fieldNames.size();
        this.fieldNameRefAttributeType = fieldNameRefAttributeType;
        // 获取通过Partiql查询获取的结果集
        ExecuteStatementResult result = executeStatementRequest(dynamoDb, statement);
        iterator = result.getItems().iterator();
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        if (iterator.hasNext()) {
            Map<String, AttributeValue> values = iterator.next();
            current = new Object[fieldSize];
            for (int i = 0; i < fieldSize; i++) {
                String fieldName = fieldNames.get(i);
                String fieldAttributeType = fieldNameRefAttributeType.get(fieldName);
                AttributeValue value = values.get(fieldName);
                // TODO 更多类型判断
                switch (fieldAttributeType) {
                    case "S":
                        current[i] = value.getS();
                        break;
                    case "N":
                        // 需要确保此处的赋值类型与schema中定义的sqlTypeName对应
                        if (StringUtils.isNotBlank(value.getN())) {
                            current[i] = Integer.parseInt(value.getN());
                        }
                        break;
                    default:
                        throw new NotSupportException(String.format("type [%s] not support.", fieldAttributeType));
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public void reset() {
    }

    @Override
    public void close() {
    }

    /**
     * 执行Partiql
     *
     * @param client    AmazonDynamoDB
     * @param statement statement
     * @return ExecuteStatementResult
     */
    private ExecuteStatementResult executeStatementRequest(AmazonDynamoDB client, String statement) {
        request.setStatement(statement);
        return client.executeStatement(request);
    }
}
