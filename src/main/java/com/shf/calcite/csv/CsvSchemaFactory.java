package com.shf.calcite.csv;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 1:46
 */
public class CsvSchemaFactory implements SchemaFactory {

    /**
     * @param parentSchema 他的父节点，一般为root
     * @param name         数据库的名字，对应于启动配置文件中的$.schemas.name定义，当前为csv.json
     * @param operand      对应于启动配置文件中的$.schemas.operand，是Map类型，用于传入自定义参数。
     */
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        // dataFile对应待读取的csv文件名列表
        return new CsvSchema(String.valueOf(operand.get("dataFiles")));
    }
}