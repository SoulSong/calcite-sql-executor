package com.shf.calcite.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.shf.calcite.common.AbstractBaseSchema;
import com.shf.calcite.dynamodb.mapper.scanner.TableModelScanner;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/12 1:09
 */
@Slf4j
public class DynamoDbMapperSchema extends AbstractBaseSchema {
    private Map<String, Table> tableMap = new HashMap<>();

    public DynamoDbMapperSchema(DynamoDBMapper dynamoDbMapper, TableModelScanner tableModelScanner) {
        // 启动加载tableModel
        tableModelScanner.scanTableSpecs().forEach(tableSpec -> {
            tableMap.put(tableSpec.getTableName(), new DynamoDbMapperTable(tableSpec, dynamoDbMapper));
        });
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    /**
     * 获取当前查询表，需要特别注意，注册的表名均为大写
     *
     * @param name 待查询表名
     * @return {@link DynamoDbMapperTable}
     */
    @Override
    public Table getTable(String name) {
        return super.getTable(name.toUpperCase());
    }
}
