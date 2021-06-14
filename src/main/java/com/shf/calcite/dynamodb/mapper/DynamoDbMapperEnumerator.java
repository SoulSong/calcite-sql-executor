package com.shf.calcite.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/13 19:28
 */
@Slf4j
public class DynamoDbMapperEnumerator implements Enumerator<Object[]> {
    private static final ObjectMapper OBJECT_MAPPER;
    private Iterator iterator;
    private Object[] current;
    /**
     * 通过 projectionFields 实现了投影下推
     */
    private List<String> projectionFields;
    private Class<?> clazz;

    static {
        OBJECT_MAPPER = new ObjectMapper();
    }

    @SneakyThrows
    public DynamoDbMapperEnumerator(DynamoDBMapper dynamoDbMapper, DynamoDBQueryExpression queryExpression,
                                    Class<?> clazz, List<String> projectionFields) {
        this.clazz = clazz;
        this.projectionFields = projectionFields;
        log.info("execute dynamoDb query : {}", OBJECT_MAPPER.writeValueAsString(queryExpression));
        PaginatedQueryList list = dynamoDbMapper.query(clazz, queryExpression);
        iterator = list.iterator();
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        if (iterator.hasNext()) {
            Object row = iterator.next();
            Object[] values = new Object[projectionFields.size()];
            Field[] declaredFields = clazz.getDeclaredFields();
            // 将所有字段的fieldName和Field进行映射，由于projection中所有字段为大写，故此处需要将key值转换为大写，从而便于后续根绝fieldName获取Field
            Map<String, Field> nameRefField = Arrays.stream(declaredFields)
                    .collect(Collectors.toMap(declaredField -> declaredField.getName().toUpperCase(), declaredField -> declaredField));
            for (int i = 0; i < projectionFields.size(); i++) {
                try {
                    // 通过反射获取对应的value值
                    Field field = nameRefField.get(projectionFields.get(i));
                    field.setAccessible(true);
                    values[i] = field.get(row);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            current = values;
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
}
