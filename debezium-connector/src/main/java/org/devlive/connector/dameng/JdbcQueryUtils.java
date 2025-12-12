package org.devlive.connector.dameng;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcQueryUtils {
    
    private static HikariDataSource dataSource;
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcQueryUtils.class);

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:dm://192.168.1.69:5236/THELING?useSSL=false&serverTimezone=UTC");
        config.setUsername("THELING");
        config.setPassword("OnPCz12u^@~*ecfmZvR");
        config.setDriverClassName("dm.jdbc.driver.DmDriver");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setIdleTimeout(60000);
        config.setMaxLifetime(1800000);
        config.setConnectionTimeout(30000);
        dataSource = new HikariDataSource(config);
    }

    // 获取连接
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    // 关闭数据源
    public static void closeDataSource() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    // ------------------ 查询方法 ------------------

    /**
     * 查询多条记录
     * @param sql SQL语句
     * @param params 参数列表
     * @return List<Map<String,Object>> 每条记录是一个Map，列名为key
     */
    public static List<Map<String, Object>> queryList(String sql, Object... params) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            setParameters(ps, params);

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(metaData.getColumnLabel(i), rs.getObject(i));
                    }
                    resultList.add(row);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("查询数据库失败", e);
        }
        return resultList;
    }

    /**
     * 查询单条记录
     * @param sql SQL语句
     * @param params 参数列表
     * @return Map<String,Object> 如果没有记录返回null
     */
    public static Map<String, Object> queryOne(String sql, Object... params) {
        List<Map<String, Object>> list = queryList(sql, params);
        return list.isEmpty() ? null : list.get(0);
    }

    // ------------------ 私有辅助方法 ------------------

    private static void setParameters(PreparedStatement ps, Object... params) throws SQLException {
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        }
    }
    
    /**
     * 关闭连接池
     */
    public static void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOGGER.info("Database connection pool closed");
        }
    }
}
