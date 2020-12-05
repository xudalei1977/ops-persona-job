package com.leiyu.ops.etl.util;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * @Auther: hmm
 * @Date: 2019/4/19 17:56
 * @Description:JDBC操作
 */
public class JDBCUtils {

    private static Connection conn = null;

    public static Connection getConnection(){
        PropertiesUtil propertiesUtil = new PropertiesUtil("jdbc.properties");
        String driver = propertiesUtil.readResourceProperty("driver");
        String url = propertiesUtil.readResourceProperty("url");
        String username = propertiesUtil.readResourceProperty("username");
        String password = propertiesUtil.readResourceProperty("password");

        try {

            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);

        } catch (ClassNotFoundException e) {

            e.printStackTrace();
        } catch (SQLException e) {

            e.printStackTrace();
            close();
        }
        return conn;
    }

    public static void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
