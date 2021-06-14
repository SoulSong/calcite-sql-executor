package com.shf.calcite.dynamodb.mapper.entity;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/6 22:38
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableFieldSpec {
    private String name;
    private Class<?> javaType;
    /**
     * 默认值为varchar
     */
    private SqlTypeName sqlType = SqlTypeName.VARCHAR;
    private DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoDBAttributeType;
    private boolean isHashKey;
    private boolean isRangeKey;


}
