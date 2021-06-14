package com.shf.calcite.util.resultset;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;

/**
 * description :
 * ResultSet帮助类
 *
 * @author songhaifeng
 * @date 2021/6/13 22:46
 */
@Slf4j
public class ResultSetHelper {

    /**
     * 归集查询后的数据并注入到 List
     *
     * @param resultSet resultSet
     * @return listMap
     * @throws Exception e
     */
    public static List<Map<String, Object>> toListMap(ResultSet resultSet) throws Exception {
        List<Map<String, Object>> list = Lists.newArrayList();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();

        while (resultSet.next()) {

            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            list.add(map);
        }
        return list;
    }

    public static void prettyPrintResultSet(String sql, ResultSet resultSet) throws Exception {
        if (resultSet == null) {
            log.warn("ResultSet is null, please check.");
            return;
        }
        StringBuilder sb = new StringBuilder(500);
        sb.append("\n-------------------------  start  -------------------------  ");
        sb.append("\nsql : ").append(sql);
        String pretty = JSON.toJSONString(toListMap(resultSet),
                SerializerFeature.PrettyFormat,
                SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteDateUseDateFormat);
        sb.append("\nresult : \n").append(pretty);
        sb.append("\n-------------------------  end  -------------------------  ");
        log.info(sb.toString());
    }

}
