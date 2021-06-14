package com.shf.calcite.dynamodb.dynamic;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 21:07
 */
@Slf4j
public class DynamoDbTable extends AbstractTable implements ScannableTable {
    private static final Map<String, SqlTypeName> ATTRIUTETYPE_MAPTO_SQLTYPE = new HashMap<>();

    private DynamoDbSchema.Statement statement;
    private AmazonDynamoDB dynamoDbClient;
    private DynamoDB dynamoDb;

    static {
        // TODO
        ATTRIUTETYPE_MAPTO_SQLTYPE.put("S", SqlTypeName.VARCHAR);
        ATTRIUTETYPE_MAPTO_SQLTYPE.put("N", SqlTypeName.INTEGER);
    }

    public DynamoDbTable(DynamoDbSchema.Statement statement, AmazonDynamoDB dynamoDbClient, DynamoDB dynamoDb) {
        this.statement = statement;
        this.dynamoDbClient = dynamoDbClient;
        this.dynamoDb = dynamoDb;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        log.info("statement : {} ;", statement.toString());
        // 完全依赖通过script进行的表定义，非AttributeDefinition描述的字段无法被加载
        TableDescription tableDescription = dynamoDb.getTable(statement.getTableName()).describe();
        List<AttributeDefinition> attributeDefinitionList = tableDescription.getAttributeDefinitions();
        final List<String> fieldNames = Lists.newLinkedList();
        final Map<String, String> fieldNameRefAttributeType = new HashMap<>();
        attributeDefinitionList.forEach(attributeDefinition -> {
            String fieldName = attributeDefinition.getAttributeName();
            String fieldAttributeType = attributeDefinition.getAttributeType();
            fieldNames.add(fieldName);
            fieldNameRefAttributeType.put(fieldName, fieldAttributeType);
        });
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new DynamoDbEnumerator(statement.generatePartiql(), dynamoDbClient, fieldNames, fieldNameRefAttributeType);
            }
        };
    }

    /**
     * fieldName必须转换大写，否则会出现column not found
     *
     * @param typeFactory typeFactory
     * @return RelDataType
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        TableDescription tableDescription = dynamoDb.getTable(statement.getTableName()).describe();
        List<String> fieldNames = Lists.newLinkedList();
        List<RelDataType> types = Lists.newLinkedList();

        // 完全依赖通过script进行的表定义，非AttributeDefinition描述的字段无法被加载
        List<AttributeDefinition> attributeDefinitionList = tableDescription.getAttributeDefinitions();
        attributeDefinitionList.forEach(attributeDefinition -> {
            String fieldName = attributeDefinition.getAttributeName();
            String fieldAttributeType = attributeDefinition.getAttributeType();
            fieldNames.add(fieldName);
            types.add(typeFactory.createSqlType(ATTRIUTETYPE_MAPTO_SQLTYPE.getOrDefault(fieldAttributeType, SqlTypeName.VARCHAR)));
        });
        return typeFactory.createStructType(Pair.zip(fieldNames, types));
    }

}
