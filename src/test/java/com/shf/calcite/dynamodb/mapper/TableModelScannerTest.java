package com.shf.calcite.dynamodb.mapper;

import com.shf.calcite.dynamodb.AbstractDynamoDbTest;
import com.shf.calcite.dynamodb.DynamoDbClientFactory;
import com.shf.calcite.dynamodb.mapper.entity.TableSpec;
import com.shf.calcite.dynamodb.mapper.scanner.TableModelScanner;
import org.junit.Test;

import java.util.List;

public class TableModelScannerTest extends AbstractDynamoDbTest {

    /**
     * 测试验证扫描获取tableSpec的逻辑实现
     */
    @Test
    public void scanTableSpecs() {
        TableModelScanner scanner = new TableModelScanner("com.shf.calcite.dynamodb.table.entity",
                DynamoDbClientFactory.dynamoDbMapper(), DynamoDbClientFactory.dynamoDb());
        List<TableSpec> tableSpecList = scanner.scanTableSpecs();
        assert tableSpecList.size() > 0;
    }

}
