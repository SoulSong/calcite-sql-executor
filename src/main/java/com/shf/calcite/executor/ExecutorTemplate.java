package com.shf.calcite.executor;

import com.shf.calcite.util.resultset.ResultSetHelper;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/13 22:39
 */
@Slf4j
public class ExecutorTemplate extends BaseExecutor {
    private String configFilePath;
    private boolean needPrintResultSet;

    public ExecutorTemplate(String configFilePath, boolean needPrintResultSet) {
        this.configFilePath = configFilePath;
        this.needPrintResultSet = needPrintResultSet;
    }

    public ResultSet query(String sql) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = createStatement(getConnection(configFilePath));
            resultSet = statement.executeQuery(sql);
            if (needPrintResultSet) {
                ResultSetHelper.prettyPrintResultSet(sql, resultSet);
            }
            return resultSet;
        } catch (Exception e) {
            log.error("Query sql [{}] fail,error message : {}.", sql, e.getMessage(),e);
            return null;
        } finally {
            close(connection, statement, resultSet);
        }
    }
}
