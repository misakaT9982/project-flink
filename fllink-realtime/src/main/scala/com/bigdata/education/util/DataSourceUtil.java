package com.bigdata.education.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Ciel
 * project-flink: com.bigdata.education.util
 * 2020-08-25 09:25:03
 */
public class DataSourceUtil {
    public static DataSource dataSource = null;

    static {
        try {
            Properties pros = new Properties();
            pros.setProperty("url", ConfigurationManager.getProperty("jdbc.url"));
            pros.setProperty("username", ConfigurationManager.getProperty("jdbc.user"));
            pros.setProperty("password", ConfigurationManager.getProperty("jdbc.password"));
            pros.setProperty("initialSize", "5"); //初始化大小
            pros.setProperty("maxActive", "20"); //最大连接
            pros.setProperty("minIdle", "5");  //最小连接
            pros.setProperty("maxWait", "60000"); //等待时长
            pros.setProperty("timeBetweenEvictionRunsMillis", "2000");//配置多久进行一次检测,检测需要关闭的连接 单位毫秒
            pros.setProperty("minEvictableIdleTimeMillis", "600000");//配置连接在连接池中最小生存时间 单位毫秒
            pros.setProperty("maxEvictableIdleTimeMillis", "900000"); //配置连接在连接池中最大生存时间 单位毫秒
            pros.setProperty("validationQuery", "select 1");
            pros.setProperty("testWhileIdle", "true");
            pros.setProperty("testOnBorrow", "false");
            pros.setProperty("testOnReturn", "false");
            pros.setProperty("keepAlive", "true");
            pros.setProperty("phyMaxUseCount", "100000");
//            pros.setProperty("driverClassName", "com.mysql.jdbc.Driver");
            dataSource = DruidDataSourceFactory.createDataSource(pros);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //提供获取连接的方法
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    // 提供关闭资源的方法【connection是归还到连接池】
    // 提供关闭资源的方法 【方法重载】3 dql
    public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement,
                                     Connection connection) {
        // 关闭结果集
        // ctrl+alt+m 将java语句抽取成方法
        closeResultSet(resultSet);
        // 关闭语句执行者
        closePrepareStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closePrepareStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
