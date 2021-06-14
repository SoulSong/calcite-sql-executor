# Description
基于Apache calcite构建多数据源联合查询适配器，当前已支持：
- CSV
    - 支持投影下推
- DynamoDb
   - 支持无模式定义：动态表名，其映射至一个json文本，json中描述具体的查询目标table、hashKey等信息；
   - 支持模式定义：通过读取被@DynamoDBTable注解的Entity类，注册Table信息；
   - 支持投影下推
   - 支持过滤下推
- HTTP
   - TODO
 
# Constraint
所有的查询适配器当前仅验证了Varchar和Integer类型，其他处于Todo验证状态。

## CSV
- 数据行head需要包含SqlTypeName定义，其格式如下
```text
ID:VARCHAR,NAME1:VARCHAR,NAME2:VARCHAR
```

## DynamoDb
### 无模式
- 待查询目标表需要通过script定义，否则无法获取完整的AttributeDefinition，
其应用于[DynamoDbTable](./src/main/java/com/shf/calcite/dynamodb/dynamic/DynamoDbTable.java)。
- 有别于有模式注册，每个查询均会动态注册一个新的DynamoDbTable实例。而getTableMap仅仅返回一个占位表信息作为标识。

### 有模式
- 实体定义中字段schema定义全部小写，便于与sql文对应匹配；
- 实体定义中表名、索引连接符采用`_`；
- 通过`-`实现tableName-indexName的连接操作，其作为索引表的注册表名；
- 目前仅支持所有query filter的AND连接语义操作；
- 目前仅支持等值过滤，若需要添加其他类型，则需要更多判断，如hashKey仅支持等值；


# Test
## CSV
- [CsvTest](./src/test/java/com/shf/calcite/csv/CsvTest.java)

## DynamoDb
Load sample dynamoDb data with [AbstractDynamoDbTest](./src/test/java/com/shf/calcite/dynamodb/AbstractDynamoDbTest.java)

### 无模式
- [DynamoDbDynamicTest](./src/test/java/com/shf/calcite/dynamodb/dynamic/DynamoDbDynamicTest.java)

### 有模式
- [DynamoDbMapperTest](./src/test/java/com/shf/calcite/dynamodb/mapper/DynamoDbMapperTest.java)


##  MultipleDatasource
组合DynamoDb和CSV两个数据源的联合查询
- [MultipleDatasourceTest](./src/test/java/com/shf/calcite/multiple/MultipleDatasourceTest.java)

# Reference
- https://github.com/apache/calcite
```text
官方示例，可参考example中实现，包含UDF和Rule示例。
```
- https://github.com/gaohanghbut/yugo
```text
一个支持HTTP的示例。
```
- https://github.com/shezhiming/calcite-demo
```text
以CSV适配器实现，介绍SQL的parse和validate，以及RBO和CBO的使用。
```
- https://github.com/querifylabs/querifylabs-blog
```text
基于blog介绍的一个示例项目。
```