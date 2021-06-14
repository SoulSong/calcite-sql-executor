package com.shf.calcite.dynamodb.dynamic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shf.calcite.executor.ExecutorTemplate;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 17:40
 */
@Slf4j
public class DynamoDbDynamicTest {
    @Test
    public void querySql() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        StatementCache.CACHE.put("statement1".toUpperCase(),
                objectMapper.writeValueAsString(DynamoDbSchema.Statement.builder().tableName("t_music").hashKey("artist").hashValue("lin junjie").rangeKey("title").rangeValue("caocao").build()));

        String[] strArray = {
                "select T1.* from dynamodb_dynamic.statement1 t1"
        };
        ExecutorTemplate executorTemplate = new ExecutorTemplate("/dynamic.json", true);
        for (String sql : strArray) {
            executorTemplate.query(sql);
        }
    }

}
