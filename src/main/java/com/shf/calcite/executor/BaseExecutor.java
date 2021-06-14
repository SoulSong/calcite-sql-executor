package com.shf.calcite.executor;

import com.shf.calcite.util.calcite.CalciteHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/14 0:18
 */
public abstract class BaseExecutor implements Executor {
    @Override
    public Connection getConnection(String configFilePath) {
        return CalciteHelper.getConnect(configFilePath);
    }

    @Override
    public Statement createStatement(Connection connection) throws SQLException {
        return connection.createStatement();
    }

    @Override
    public ResultSet executeQuery(Statement statement, String sql) throws SQLException {
        return statement.executeQuery(sql);
    }


}
