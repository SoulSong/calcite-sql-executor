package com.shf.calcite.csv;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.shf.calcite.common.AbstractBaseSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 1:47
 */
@Slf4j
public class CsvSchema extends AbstractBaseSchema {
    private String dataFileNames;

    public CsvSchema(String dataFiles) {
        this.dataFileNames = dataFiles;
    }

    /**
     * 集中注册所有文件为目标表，并将文件名注册为目标表名
     *
     * @return key:tableName,value:{@link CsvTable}
     */
    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (String dataFileName : dataFileNames.split(",")) {
            URL url = Resources.getResource(dataFileName);
            Source source = Sources.of(url);

            List<String> names = Lists.newLinkedList();
            List<String> types = Lists.newLinkedList();

            // 获取表schema信息，第一行包含了字段名、类型信息，格式为ID:VARCHAR,NAME1:VARCHAR,NAME2:VARCHAR
            try (BufferedReader reader = new BufferedReader(new FileReader(source.file()))) {
                String line = reader.readLine();
                List<String> lines = Lists.newArrayList(line.split(","));
                lines.forEach(column -> {
                    String[] columnInfo = column.split(":");
                    names.add(columnInfo[0]);
                    types.add(columnInfo[1]);
                });
            } catch (IOException e) {
                log.error("Read csv file [{}] error.", source.path());
            }

            // 剔除后缀的文件名即为表名
            builder.put(dataFileName.split("\\.")[0], new CsvTable(source, names, types));
        }
        return builder.build();
    }

    @Override
    public Table getTable(String name) {
        log.info("query table : {}", name);
        return super.getTable(name);
    }
}