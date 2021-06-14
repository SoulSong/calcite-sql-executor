package com.shf.calcite.csv;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;

import java.util.List;
import java.util.stream.Collectors;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 1:08
 */
@Slf4j
public class CsvTable extends AbstractTable implements ProjectableFilterableTable {
    private Source source;
    // 所有字段名称
    private List<String> names;
    // 所有字段类型，原则与names一一对应
    private List<String> types;

    public CsvTable(Source source, List<String> names, List<String> types) {
        this.source = source;
        this.names = names;
        this.types = types;
    }

    /**
     * 注册目标table的字段、类型信息
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<RelDataType> relDataTypes = types.stream()
                .map(type -> typeFactory.createSqlType(SqlTypeName.get(type)))
                .collect(Collectors.toList());
        return typeFactory.createStructType(Pair.zip(names, relDataTypes));
    }

    /**
     * TODO 目前仅支持投影下推，暂不支持filter下推
     *
     * @param root     root
     * @param filters  filters
     * @param projects projects
     * @return {@link CsvEnumerator}
     */
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator(source, projects, types);
            }
        };
    }


}