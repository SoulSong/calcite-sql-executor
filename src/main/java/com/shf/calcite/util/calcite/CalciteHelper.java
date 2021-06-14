package com.shf.calcite.util.calcite;

import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 21:51
 */
public class CalciteHelper {
    /**
     * 根据给定的 *.json 文件获取 Connection
     *
     * @param configFileName
     * @return
     */
    public static Connection getConnect(String configFileName) {
        Connection connection = null;
        try {
            URL url = CalciteHelper.class.getResource(configFileName);
            String str = URLDecoder.decode(url.toString(), "UTF-8");
            Properties info = new Properties();
            info.put("model", str.replace("file:", ""));
            info.setProperty("caseSensitive", "false");
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            // connection.unwrap(CalciteConnection.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

}