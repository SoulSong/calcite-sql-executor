package com.shf.calcite.executor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/13 22:39
 */
public interface Executor {

    Connection getConnection(String configFileName);

    Statement createStatement(Connection connection) throws SQLException;

    ResultSet executeQuery(Statement statement, String sql) throws SQLException;

    default void close(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
