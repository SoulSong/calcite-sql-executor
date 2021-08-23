package com.shf.calcite.dynamodb.mapper.scanner;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.shf.calcite.dynamodb.mapper.entity.TableFieldSpec;
import com.shf.calcite.dynamodb.mapper.entity.TableSpec;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections4.CollectionUtils;
import org.reflections.Reflections;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 约束 :
 * 二级索引的表注册，通过`-`实现tableName-indexName的连接操作，其区别于表名和索引名中的`_`连接符
 *
 * @author songhaifeng
 * @date 2021/6/12 0:51
 */
@Slf4j
public class TableModelScanner {

    private String packageNames;
    private DynamoDBMapper mapper;
    private DynamoDB dynamoDb;

    private static final Map<Class<?>, SqlTypeName> JAVATYPE_MAPTO_SQLTYPE = new HashMap<>();

    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.DataTypes.html
    static {
        // TODO 持续添加
        JAVATYPE_MAPTO_SQLTYPE.put(String.class, SqlTypeName.VARCHAR);
        JAVATYPE_MAPTO_SQLTYPE.put(int.class, SqlTypeName.INTEGER);
        JAVATYPE_MAPTO_SQLTYPE.put(Integer.class, SqlTypeName.INTEGER);
    }

    public TableModelScanner(String packageNames, DynamoDBMapper mapper, DynamoDB dynamoDb) {
        this.packageNames = packageNames;
        this.mapper = mapper;
        this.dynamoDb = dynamoDb;
    }

    /**
     * 所有的tableName和fieldName全部转换为大写
     *
     * @return
     */
    public List<TableSpec> scanTableSpecs() {
        log.info("start to scan tables");
        Stopwatch stopwatch = Stopwatch.createStarted();
        List<TableSpec> list = Lists.newArrayList();
        Arrays.stream(packageNames.split(",")).forEach(packageName -> {
            Reflections reflections = new Reflections(packageName);
            Set<Class<?>> classes = reflections.getTypesAnnotatedWith(DynamoDBTable.class);
            classes.forEach(clazz -> {
                // 获取所有字段的javaType
                Field[] declaredFields = clazz.getDeclaredFields();
                if (declaredFields.length == 0) {
                    return;
                }

                Map<String, Class<?>> fieldRefJavaType = Arrays.stream(declaredFields)
                        .collect(Collectors.toMap(declaredField -> declaredField.getName().toUpperCase(), Field::getType));

                String originTableName = clazz.getAnnotation(DynamoDBTable.class).tableName();
                String tableName = originTableName.toUpperCase();
                DynamoDBMapperTableModel<?> tableModel = mapper.getTableModel(clazz);

                // 根据Entity构建所有字段的完整定义
                Map<String, TableFieldSpec> allFields = tableModel.fields().stream().map(fieldModel -> {
                    String fieldName = fieldModel.name().toUpperCase();
                    Class<?> javaType = fieldRefJavaType.get(fieldName);
                    DynamoDBMapperFieldModel.DynamoDBAttributeType attributeType = fieldModel.attributeType();
                    SqlTypeName sqlTypeName = JAVATYPE_MAPTO_SQLTYPE.getOrDefault(javaType, SqlTypeName.VARCHAR);
                    return TableFieldSpec.builder().name(fieldName).javaType(javaType).sqlType(sqlTypeName).dynamoDBAttributeType(attributeType).build();
                }).collect(Collectors.toMap(TableFieldSpec::getName, field -> field));

                // 根据字段列表重新排序字段映射表
                LinkedHashMap<String, TableFieldSpec> fields = new LinkedHashMap<>(allFields.size());
                for (Field declaredField : declaredFields) {
                    // 排除一些实体中定义的常量
                    if (Modifier.isTransient(declaredField.getModifiers())) {
                        continue;
                    }
                    String fieldName = declaredField.getName().toUpperCase();
                    fields.put(fieldName, allFields.get(fieldName));
                }

                // 注册原始表
                TableSpec tableSpec = TableSpec.builder().tableName(tableName).fields(fields).clazz(clazz).build();
                DynamoDBMapperFieldModel hashKey = tableModel.hashKey();
                fields.get(hashKey.name().toUpperCase()).setHashKey(true);
                tableSpec.setHashKeyName(hashKey.name().toUpperCase());
                DynamoDBMapperFieldModel rangeKey = tableModel.rangeKey();
                if (rangeKey != null) {
                    fields.get(rangeKey.name().toUpperCase()).setRangeKey(true);
                    tableSpec.setRangeKeyName(rangeKey.name().toUpperCase());
                }
                list.add(tableSpec);

                // 注册索引
                try {
                    TableDescription tableDescription = dynamoDb.getTable(originTableName).describe();

                    // 注册本地二级索引
                    List<LocalSecondaryIndexDescription> localSecondaryIndices = tableDescription.getLocalSecondaryIndexes();
                    if (CollectionUtils.isNotEmpty(localSecondaryIndices)) {
                        localSecondaryIndices.forEach(localSecondaryIndex -> {
                            list.add(buildTableSpecByIndexDesc(clazz, tableName, localSecondaryIndex.getIndexName(), fields,
                                    localSecondaryIndex.getProjection(), localSecondaryIndex.getKeySchema()));
                        });
                    }

                    // 注册全局二级索引
                    List<GlobalSecondaryIndexDescription> globalSecondaryIndices = tableDescription.getGlobalSecondaryIndexes();
                    if (CollectionUtils.isNotEmpty(globalSecondaryIndices)) {
                        globalSecondaryIndices.forEach(globalSecondaryIndex -> {
                            list.add(buildTableSpecByIndexDesc(clazz, tableName, globalSecondaryIndex.getIndexName(), fields,
                                    globalSecondaryIndex.getProjection(), globalSecondaryIndex.getKeySchema()));
                        });
                    }
                } catch (ResourceNotFoundException exception) {
                    log.warn("table [{}] not found.", originTableName);
                }


            });
        });
        stopwatch.stop();
        log.info("scan tables used {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return list;
    }

    /**
     * 构建表定义
     *
     * @param clazz             当前构建实体类
     * @param tableName         表名
     * @param indexName         索引名称
     * @param fields            全部字段列表
     * @param projection        投影信息
     * @param keySchemaElements keySchema设定
     * @return {@link TableSpec}
     */
    private TableSpec buildTableSpecByIndexDesc(Class<?> clazz, String tableName, String indexName, LinkedHashMap<String, TableFieldSpec> fields,
                                                Projection projection, List<KeySchemaElement> keySchemaElements) {
        // 字段信息来源于projection和KeySchemaElement
        List<String> fieldNames = new ArrayList<>();
        switch (projection.getProjectionType()) {
            case "INCLUDE":
                keySchemaElements.forEach(keySchemaElement -> {
                    fieldNames.add(keySchemaElement.getAttributeName());
                });
                fieldNames.addAll(projection.getNonKeyAttributes());
                break;
            case "KEYS_ONLY":
                keySchemaElements.forEach(keySchemaElement -> {
                    fieldNames.add(keySchemaElement.getAttributeName());
                });
                break;
            case "ALL":
            default:
        }

        LinkedHashMap<String, TableFieldSpec> indexFields;
        if (CollectionUtils.isEmpty(fieldNames)) {
            indexFields = fields;
        } else {
            indexFields = fieldNames.stream()
                    .map(String::toUpperCase)
                    .map(fields::get)
                    .collect(Collectors.toMap(TableFieldSpec::getName, field -> field, (u, v) -> {
                        throw new IllegalStateException(String.format("Duplicate key %s", u));
                    }, LinkedHashMap::new));
        }

        TableSpec indexTableSpec = TableSpec.builder()
                .tableName(tableName.concat("_").concat(indexName.toUpperCase()))
                .fields(indexFields)
                .clazz(clazz)
                .build();

        keySchemaElements.forEach(keySchemaElement -> {
            if ("HASH".equalsIgnoreCase(keySchemaElement.getKeyType())) {
                indexFields.get(keySchemaElement.getAttributeName().toUpperCase()).setHashKey(true);
                indexTableSpec.setHashKeyName(keySchemaElement.getAttributeName().toUpperCase());
            } else {
                indexFields.get(keySchemaElement.getAttributeName().toUpperCase()).setRangeKey(true);
                indexTableSpec.setRangeKeyName(keySchemaElement.getAttributeName().toUpperCase());
            }
        });
        return indexTableSpec;
    }
}
