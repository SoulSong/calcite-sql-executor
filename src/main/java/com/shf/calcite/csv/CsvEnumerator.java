package com.shf.calcite.csv;

import com.shf.calcite.exception.NotSupportException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Source;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

/**
 * description :
 * 遍历逐行读取数据
 *
 * @author songhaifeng
 * @date 2021/6/4 1:48
 */
@Slf4j
public class CsvEnumerator implements Enumerator<Object[]> {

    private Object[] current;
    private BufferedReader br;
    private int[] projects;
    private List<String> types;

    public CsvEnumerator(Source source, int[] projects, List<String> types) {
        try {
            this.br = new BufferedReader(source.reader());
            this.br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.projects = projects;
        this.types = types;
    }

    @Override
    public Object[] current() {
        return current;
    }

    /**
     * 最终的数据行读取位置
     *
     * @return true表示有数据，false表示读取完毕，么有更多数据
     */
    @Override
    public boolean moveNext() {
        try {
            String line = br.readLine();
            if (line == null) {
                return false;
            }
            String[] values = line.split(",");
            // 投影下推
            if (projects != null) {
                int size = projects.length;
                current = new Object[size];
                for (int i = 0; i < size; i++) {
                    int indexOfFields = projects[i];
                    String type = types.get(indexOfFields);
                    // TODO 更多类型支持
                    switch (type) {
                        case "VARCHAR":
                            current[i] = values[indexOfFields];
                            break;
                        case "INTEGER":
                            // 需要确保此处的赋值类型与schema中定义的sqlTypeName对应
                            if (StringUtils.isNotBlank(values[indexOfFields])) {
                                current[i] = Integer.parseInt(values[indexOfFields]);
                            }
                            break;
                        default:
                            throw new NotSupportException(String.format("type [%s] not support.", type));
                    }

                }
            } else {
                current = values;
            }
        } catch (IOException e) {
            log.error("Read error.");
            return false;
        }
        return true;
    }

    @Override
    public void reset() {
        log.error("error invoke.");
    }

    /**
     * InputStream流在这里关闭
     */
    @Override
    public void close() {
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}