package com.shf.calcite.dynamodb;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Date;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/6 22:28
 */
public class TypeConverter {

    /**
     * 根据源字段的java类型获取对应的SqlType，默认值为{@link SqlTypeName#VARCHAR}
     *
     * @param javaType 源类型
     * @return sqlTypeName
     */
    public static SqlTypeName javaTypeToSqlType(Class<?> javaType) {
        SqlTypeName sqlTypeName = SqlTypeName.VARCHAR;
        if (String.class.equals(javaType)) {
            return SqlTypeName.VARCHAR;
        } else if (int.class.equals(javaType) || Integer.class.equals(javaType)) {
            return SqlTypeName.INTEGER;
        } else if (Date.class.equals(javaType)) {
            return SqlTypeName.DATE;
        }
        // TODO 添加更多的类型判断
        return sqlTypeName;
    }

    private static boolean isJavaClass(Class<?> clz) {
        return clz != null && clz.getClassLoader() == null;
    }

}
