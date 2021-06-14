package com.shf.calcite.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Lists;
import com.shf.calcite.dynamodb.mapper.entity.TableFieldSpec;
import com.shf.calcite.dynamodb.mapper.entity.TableSpec;
import com.shf.calcite.exception.NotSupportException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections4.CollectionUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/12 1:30
 */
@Slf4j
public class DynamoDbMapperTable extends AbstractTable implements ProjectableFilterableTable {
    private TableSpec tableSpec;
    private DynamoDBMapper dynamoDbMapper;

    public DynamoDbMapperTable(TableSpec tableSpec, DynamoDBMapper dynamoDbMapper) {
        this.tableSpec = tableSpec;
        this.dynamoDbMapper = dynamoDbMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<String> names = Lists.newLinkedList();
        List<RelDataType> types = Lists.newLinkedList();
        tableSpec.getFields().forEach((fieldName, tableFieldSpec) -> {
            names.add(fieldName);
            types.add(typeFactory.createSqlType(tableFieldSpec.getSqlType()));
        });
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        log.info("query table : {} ; projects : {}", tableSpec.getTableName(), projects);
        // 解析filters信息
        if (CollectionUtils.isEmpty(filters)) {
            throw new NotSupportException("Only support query by hashKey(must) and rangKey(optional).");
        }

        // TODO 判断当前是原始数据表还是二级索引
        return handlerTable(filters, projects);
    }

    private Enumerable<Object[]> handlerTable(List<RexNode> filters, int[] projects) {
        // 所有原始字段定义
        LinkedHashMap<String, TableFieldSpec> fields = tableSpec.getFields();
        // 记录所有的原始字段名，通过List结构保障其顺序性，便于通过index获取fieldName
        List<String> fieldNames = new ArrayList<>();
        fields.forEach((fieldName, tableFieldSpec) -> {
            fieldNames.add(fieldName);
        });

        // 记录参与过滤字段名
        Set<String> filterFieldNames = new HashSet<>();
        // 记录每个参与查询过滤字段对应的表达式，key为fieldName，value为表达式
        Map<String, String> fieldExpressions = new HashMap<>();
        HashMap<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        filters.forEach(filter -> {
            handleRexNode(filter, filterFieldNames, fieldNames, fieldExpressions, expressionAttributeValues, fields);
        });
        // 如果过滤参数不包含hashKey则直接抛出异常
        if (!filterFieldNames.contains(tableSpec.getHashKeyName())) {
            throw new NotSupportException("Query filter must contains hashKey.");
        }
        filterFieldNames.remove(tableSpec.getHashKeyName());

        DynamoDBQueryExpression queryExpression = new DynamoDBQueryExpression<>();
        // 设置keyCondition
        String keyConditionExpression = fieldExpressions.get(tableSpec.getHashKeyName());
        if (filterFieldNames.contains(tableSpec.getRangeKeyName())) {
            keyConditionExpression += " and " + fieldExpressions.get(tableSpec.getRangeKeyName());
            filterFieldNames.remove(tableSpec.getRangeKeyName());
        }
        queryExpression.withKeyConditionExpression(keyConditionExpression);

        // 除去keyCondition，其他的则为filterExpression
        if (CollectionUtils.isNotEmpty(filterFieldNames)) {
            String filterExpression = filterFieldNames.stream().map(fieldExpressions::get).collect(Collectors.joining(" and "));
            queryExpression.withFilterExpression(filterExpression);
        }
        queryExpression.withExpressionAttributeValues(expressionAttributeValues);

        // 构建projection
        // 如果projects为空，则说明sql文本采用的是select * 全字段查询
        List<String> projectionFields = new ArrayList<>(fieldNames.size());
        if (projects == null) {
            projectionFields = fieldNames;
        } else {
            for (int index : projects) {
                projectionFields.add(fieldNames.get(index));
            }
        }
        final List<String> finalProjectionFields = projectionFields;
        queryExpression.withProjectionExpression(finalProjectionFields.stream().map(String::toLowerCase).collect(Collectors.joining(",")));

        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new DynamoDbMapperEnumerator(dynamoDbMapper, queryExpression, tableSpec.getClazz(), finalProjectionFields);
            }
        };
    }

    /**
     * 解析每个filter子项
     *
     * @param filter                    比对项
     * @param filterFieldNames          记录参与过滤字段名
     * @param fieldNames                当前表字段列表
     * @param fieldExpressions          存储每个field对应的比对表达式
     * @param expressionAttributeValues 存储表达式中的占位符value值
     * @param fields                    当前表字段清单
     */
    private void handleRexNode(RexNode filter, Set<String> filterFieldNames, List<String> fieldNames, Map<String, String> fieldExpressions,
                               HashMap<String, AttributeValue> expressionAttributeValues, LinkedHashMap<String, TableFieldSpec> fields) {
        // TODO 过滤比较复杂，根据场景接入逐步添加
        if (filter instanceof RexCall) {
            handleRexCall((RexCall) filter, filterFieldNames, fieldNames, fieldExpressions, expressionAttributeValues, fields);
        } else {
            throw new NotSupportException(String.format("Filter of type [%s] not support.", filter.getClass().getSimpleName()));
        }
    }

    /**
     * 根据当前filter的Operator类型，判断其逻辑：
     * · 如果是and则表明为多个条件判断的连接表达式，需要进行二次拆分；
     * · 如果是=则表明为最终等值比对项，则直接进行解析处理；
     *
     * @param filter                    比对项
     * @param filterFieldNames          记录参与过滤字段名
     * @param fieldNames                当前表字段列表
     * @param fieldExpressions          存储每个field对应的比对表达式
     * @param expressionAttributeValues 存储表达式中的占位符value值
     * @param fields                    当前表字段清单
     */
    private void handleRexCall(RexCall filter, Set<String> filterFieldNames, List<String> fieldNames, Map<String, String> fieldExpressions,
                               HashMap<String, AttributeValue> expressionAttributeValues, LinkedHashMap<String, TableFieldSpec> fields) {
        // 当前仅支持多条件同时满足的查询方式
        SqlKind sqlKind = filter.getOperator().getKind();
        switch (sqlKind) {
            case AND:
                // and 表明当前存在多个条件判断，需要再次遍历
                filter.getOperands().forEach(rexNode -> {
                    handleRexNode(rexNode, filterFieldNames, fieldNames, fieldExpressions, expressionAttributeValues, fields);
                });
                break;
            case EQUALS:
                // equals 表明处理到最终字段的等值比对逻辑
                handleEquals(filter, filterFieldNames, fieldNames, fieldExpressions, expressionAttributeValues, fields);
                break;
            default:
                throw new NotSupportException(String.format("SqlKind of type [%s] not support.", sqlKind.name()));
        }

    }

    /**
     * 处理每个等值比对项
     *
     * @param filter                    比对项
     * @param filterFieldNames          记录参与过滤字段名
     * @param fieldNames                当前表字段列表
     * @param fieldExpressions          存储每个field对应的比对表达式
     * @param expressionAttributeValues 存储表达式中的占位符value值
     * @param fields                    当前表字段清单
     */
    private void handleEquals(RexCall filter, Set<String> filterFieldNames, List<String> fieldNames, Map<String, String> fieldExpressions,
                              HashMap<String, AttributeValue> expressionAttributeValues, LinkedHashMap<String, TableFieldSpec> fields) {
        List<RexNode> rexNodes = filter.getOperands();
        // 获取当前filter字段在表字段定义中的index信息
        RexNode rexNode = rexNodes.get(0);
        RexInputRef index;
        if (rexNode instanceof RexInputRef) {
            // 第一个为字段在schema中的索引位置
            index = (RexInputRef) rexNode;
        } else if (rexNode instanceof RexCall && ((RexCall) rexNode).getOperator() instanceof SqlCastFunction) {
            // 如果是cast数据转换，则需要从Operands二次提取
            index = (RexInputRef) ((RexCall) rexNode).getOperands().get(0);
        } else {
            throw new NotSupportException(String.format("%s not support.", rexNode.toString()));
        }
        String fieldName = fieldNames.get(index.getIndex());
        String lowerCaseFieldName = fieldName.toLowerCase();
        filterFieldNames.add(fieldName);
        // 第二个为比对值
        RexLiteral rexLiteral = (RexLiteral) rexNodes.get(1);
        Comparable comparable = rexLiteral.getValue();
        String filedValue;
        if (comparable instanceof NlsString) {
            NlsString value = (NlsString) comparable;
            filedValue = value.asSql(false, false);
        } else if (comparable instanceof BigDecimal) {
            filedValue = ((BigDecimal) comparable).toPlainString();
        } else {
            throw new NotSupportException(String.format("Compare value of type [%s] not support.", comparable.getClass().getSimpleName()));
        }

        // 如果value中包含了`‘`则需要剔除
        if (filedValue.startsWith("'")) {
            filedValue = filedValue.substring(1);
        }
        if (filedValue.endsWith("'")) {
            filedValue = filedValue.substring(0, filedValue.length() - 1);
        }
        DynamoDBMapperFieldModel.DynamoDBAttributeType fieldType = fields.get(fieldName).getDynamoDBAttributeType();
        if (DynamoDBMapperFieldModel.DynamoDBAttributeType.S.equals(fieldType)) {
            expressionAttributeValues.put(":" + lowerCaseFieldName, new AttributeValue().withS(filedValue));
        } else if (DynamoDBMapperFieldModel.DynamoDBAttributeType.N.equals(fieldType)) {
            expressionAttributeValues.put(":" + lowerCaseFieldName, new AttributeValue().withN(filedValue));
        } else {
            throw new NotSupportException(String.format("type [%s] not support.", fieldType.name()));
        }
        fieldExpressions.put(fieldName, String.format("%s %s :%s", lowerCaseFieldName, filter.getOperator().getName(), lowerCaseFieldName));
    }

}
