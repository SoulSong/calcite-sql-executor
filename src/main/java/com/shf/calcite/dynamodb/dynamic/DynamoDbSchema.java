package com.shf.calcite.dynamodb.dynamic;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shf.calcite.common.AbstractBaseSchema;
import com.shf.calcite.constant.Constant;
import com.sun.istack.internal.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 21:34
 */
@Slf4j
public class DynamoDbSchema extends AbstractBaseSchema {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Map<String, Table> tableMap = new HashMap<>();
    private AmazonDynamoDB dynamoDbClient;
    private DynamoDB dynamoDb;

    public DynamoDbSchema(AmazonDynamoDB dynamoDbClient, DynamoDB dynamoDb) {
        // 并不存储实际表信息，添加占位表作为标识
        tableMap.put(Constant.PLACE_HOLDER, null);
        this.dynamoDbClient = dynamoDbClient;
        this.dynamoDb = dynamoDb;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    /**
     * 根据查询语句中的动态表名，获取cache中对一个的查询参数.
     * 此处需要特别注意，由于是动态表名，故每次查询均会重新实例化DynamoDbTable对象。
     *
     * @param name 动态表名
     * @return {@link DynamoDbTable}
     */
    @Override
    public Table getTable(String name) {
        try {
            String relStatement = StatementCache.CACHE.getIfPresent(name);
            if (StringUtils.isEmpty(relStatement)) {
                log.error("error");
                super.getTable(name);
            }
            Statement statement = OBJECT_MAPPER.readValue(relStatement, Statement.class);
            return new DynamoDbTable(statement, dynamoDbClient, dynamoDb);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return super.getTable(name);
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Statement {
        private String tableName;
        @NotNull
        private String hashKey;
        @NotNull
        private Object hashValue;
        private String rangeKey;
        private Object rangeValue;

        String generatePartiql() {
            // TODO 添加更多的数据类型处理
            if (hashValue instanceof Number) {
            } else {
                hashValue = "'" + hashValue + "'";
            }
            if (StringUtils.isBlank(rangeKey)) {
                return String.format("select * from %s where %s=%s", tableName, hashKey, hashValue);
            } else {
                if (rangeValue instanceof Number) {
                } else {
                    rangeValue = "'" + rangeValue + "'";
                }
                return String.format("select * from %s where %s=%s and %s=%s", tableName, hashKey, hashValue, rangeKey, rangeValue);
            }
        }

        @Override
        public String toString() {
            return "Statement{" +
                    "tableName='" + tableName + '\'' +
                    ", hashKey='" + hashKey + '\'' +
                    ", hashValue=" + hashValue +
                    ", rangeKey='" + rangeKey + '\'' +
                    ", rangeValue=" + rangeValue +
                    '}';
        }
    }

}
