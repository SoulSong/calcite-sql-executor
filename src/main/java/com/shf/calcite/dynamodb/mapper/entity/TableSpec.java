package com.shf.calcite.dynamodb.mapper.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;


/**
 * description :
 * 注册登记的tableName和所有的fieldName均为大写，使用过程中需要注意处理
 *
 * @author songhaifeng
 * @date 2021/6/6 21:06
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableSpec {
    private String tableName;
    private LinkedHashMap<String, TableFieldSpec> fields;
    private String hashKeyName;
    private String rangeKeyName;
    private Class<?> clazz;
}
