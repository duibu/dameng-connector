package org.devlive.connector.startup;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson2.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class JdbcQueryUtils {
    
    private static final DruidDataSource dataSource;
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcQueryUtils.class);

    static {
        DruidDataSource ds = new DruidDataSource();
//        HikariConfig config = new HikariConfig();
//        config.setJdbcUrl("jdbc:dm://192.168.1.69:5236/THELING?useSSL=false&serverTimezone=UTC");
//        config.setUsername("THELING");
//        config.setPassword("OnPCz12u^@~*ecfmZvR");
//        config.setDriverClassName("dm.jdbc.driver.DmDriver");
//        config.setMaximumPoolSize(10);
//        config.setMinimumIdle(2);
//        config.setIdleTimeout(6000000);
//        config.setMaxLifetime(6000000);
//        config.setConnectionTimeout(30000);
//        config.setConnectionTestQuery("SELECT 1");
        ds.setUrl("jdbc:dm://192.168.1.69:5236/THELING?useSSL=false&serverTimezone=UTC");
        ds.setUsername("THELING");
        ds.setPassword("OnPCz12u^@~*ecfmZvR");
        ds.setDriverClassName("dm.jdbc.driver.DmDriver");
        // 连接池规模
        ds.setInitialSize(2);
        ds.setMinIdle(2);
        ds.setMaxActive(10);

        // 等待超时
        ds.setMaxWait(30000);

        // 空闲连接回收（非常关键）
        ds.setTimeBetweenEvictionRunsMillis(60_000); // 1 分钟检测一次
        ds.setMinEvictableIdleTimeMillis(10 * 60_000); // 空闲 10 分钟回收
        ds.setMaxEvictableIdleTimeMillis(20 * 60_000); // 最长存活 20 分钟（软 maxLifetime）

        // 连接校验（必须）
        ds.setValidationQuery("SELECT 1");
        ds.setTestWhileIdle(true);
        ds.setTestOnBorrow(false);
        ds.setTestOnReturn(false);

        // 连接泄露防护（可选）
        ds.setRemoveAbandoned(true);
        ds.setRemoveAbandonedTimeout(180);
        ds.setLogAbandoned(true);
        ds.setKeepAlive(true);
        ds.setKeepAliveBetweenTimeMillis(300000);

        // 关闭 PS 缓存（DM + Kafka 场景更稳）
//        ds.setPoolPreparedStatements(false);

        dataSource = ds;
        
        LOGGER.info("HikariCP 连接池初始化完成");
    }

    // 获取连接
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    // 关闭数据源
    public static void closeDataSource() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOGGER.info("Database connection pool closed");
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
                        String columnName = metaData.getColumnLabel(i);
                        int columnType = metaData.getColumnType(i);

                        Object value;

                        // 根据类型特殊处理
                        if (columnType == Types.CLOB) {
                            // CLOB 类型读取为字符串
                            value = readClob(rs, columnName);
                        } else if (columnType == Types.BLOB) {
                            // BLOB 类型读取为字符串
                            value = readBlobAsString(rs, columnName);
                        } else {
                            // 普通类型
                            value = rs.getObject(i);
                        }

                        row.put(columnName, value);
                    }
                    resultList.add(row);
                }
            }

        } catch (SQLException e) {
            LOGGER.error("查询数据库失败: {}", sql, e);
            throw new RuntimeException("查询数据库失败", e);
        }
        return resultList;
    }

    /**
     * 从 ResultSet 中读取 CLOB 字段
     * @param rs ResultSet
     * @param columnName 列名
     * @return CLOB内容字符串
     */
    private static String readClob(ResultSet rs, String columnName) throws SQLException {
        try {
            // 方式1：直接 getString（达梦支持，最简单）
            String value = rs.getString(columnName);
            if (value != null) {
                return value;
            }

            // 方式2：使用 Clob 对象
            Clob clob = rs.getClob(columnName);
            if (clob == null) {
                return null;
            }

            // 优先使用 getSubString（性能好）
            try {
                long length = clob.length();
                if (length == 0) {
                    return "";
                }
                return clob.getSubString(1, (int) length);
            } catch (SQLException e) {
                // 如果 getSubString 失败，降级使用 Reader
                LOGGER.warn("使用getSubString读取CLOB失败，降级使用Reader: {}", columnName);
                return readClobWithReader(clob);
            }

        } catch (SQLException e) {
            LOGGER.error("读取CLOB字段失败: {}", columnName, e);
            throw e;
        }
    }

    /**
     * 使用 Reader 读取 CLOB（用于大数据或兜底）
     * @param clob Clob对象
     * @return CLOB内容字符串
     */
    private static String readClobWithReader(Clob clob) throws SQLException {
        if (clob == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        try (Reader reader = clob.getCharacterStream()) {
            char[] buffer = new char[4096];
            int len;
            while ((len = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, len);
            }
            return sb.toString();
        } catch (Exception e) {
            throw new SQLException("使用Reader读取CLOB失败", e);
        }
    }


    /**
     * 从 ResultSet 中读取 BLOB 字段并转换为字符串
     * @param rs ResultSet
     * @param columnName 列名
     * @return BLOB内容字符串（UTF-8编码）
     */
    private static String readBlobAsString(ResultSet rs, String columnName) throws SQLException {
        try {
            // 方式1：直接获取字节数组
            byte[] bytes = rs.getBytes(columnName);
            if (bytes != null && bytes.length > 0) {
                return new String(bytes, StandardCharsets.UTF_8);
            }

            // 方式2：使用 Blob 对象
            Blob blob = rs.getBlob(columnName);
            if (blob == null) {
                return null;
            }

            try {
                long length = blob.length();
                if (length == 0) {
                    return "";
                }

                // BLOB 转字节数组再转字符串
                bytes = blob.getBytes(1, (int) length);
                return new String(bytes, StandardCharsets.UTF_8);

            } catch (SQLException e) {
                // 如果失败，使用流式读取
                LOGGER.warn("使用getBytes读取BLOB失败，降级使用InputStream: {}", columnName);
                return readBlobWithStream(blob);
            }

        } catch (Exception e) {
            LOGGER.error("读取BLOB字段失败: {}", columnName, e);
            throw new SQLException("读取BLOB字段失败", e);
        }
    }

    /**
     * 使用 InputStream 读取 BLOB（用于大数据或兜底）
     * @param blob Blob对象
     * @return BLOB内容字符串（UTF-8编码）
     */
    private static String readBlobWithStream(Blob blob) throws SQLException {
        if (blob == null) {
            return null;
        }

        try (InputStream is = blob.getBinaryStream();
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int len;
            while ((len = is.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }

            return baos.toString(StandardCharsets.UTF_8);

        } catch (Exception e) {
            throw new SQLException("使用InputStream读取BLOB失败", e);
        }
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

    /**
     * 查询单个值
     * @param sql SQL语句
     * @param params 参数列表
     * @return Object 单个值，如果没有记录返回null
     */
    public static Object queryScalar(String sql, Object... params) {
        Map<String, Object> row = queryOne(sql, params);
        if (row != null && !row.isEmpty()) {
            return row.values().iterator().next();
        }
        return null;
    }

    // ------------------ 更新方法（INSERT/UPDATE/DELETE）------------------

    /**
     * 执行单条更新（INSERT/UPDATE/DELETE）
     * @param sql SQL语句
     * @param params 参数列表
     * @return 影响的行数
     */
    public static int executeUpdate(String sql, Object... params) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
        ) {
            setParameters(ps, params);
            int affected = ps.executeUpdate();

            LOGGER.debug("执行更新: {} - 影响行数: {}", sql, affected);
            return affected;

        } catch (SQLException e) {
            LOGGER.error("执行更新失败: {}", sql, e);
            throw new RuntimeException("执行更新失败", e);
        }
    }

    /**
     * 执行批量更新
     * @param sql SQL语句
     * @param paramsList 参数列表的列表
     * @return 每条语句影响的行数数组
     */
    public static int[] executeBatch(String sql, List<Object[]> paramsList) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (Object[] params : paramsList) {
                setParameters(ps, params);
                ps.addBatch();
            }

            int[] results = ps.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);

            LOGGER.debug("批量执行: {} - 批次大小: {}", sql, paramsList.size());
            return results;

        } catch (SQLException e) {
            LOGGER.error("批量执行失败: {}", sql, e);
            throw new RuntimeException("批量执行失败", e);
        }
    }

    /**
     * 执行插入并返回自增主键
     * @param sql SQL语句
     * @param params 参数列表
     * @return 生成的主键值
     */
    public static Long executeInsertAndGetKey(String sql, Object... params) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            setParameters(ps, params);
            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }

        } catch (SQLException e) {
            LOGGER.error("执行插入失败: {}", sql, e);
            throw new RuntimeException("执行插入失败", e);
        }
        return null;
    }

    /**
     * 在事务中执行多个操作
     * @param operations 操作列表
     * @return 是否成功
     */
    public static boolean executeTransaction(List<SqlOperation> operations) {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            for (SqlOperation op : operations) {
                try (PreparedStatement ps = conn.prepareStatement(op.sql)) {
                    setParameters(ps, op.params);
                    ps.executeUpdate();
                }
            }

            conn.commit();
            conn.setAutoCommit(true);
            LOGGER.debug("事务执行成功，操作数: {}", operations.size());
            return true;

        } catch (SQLException e) {
            LOGGER.error("事务执行失败", e);
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.setAutoCommit(true);
                } catch (SQLException ex) {
                    LOGGER.error("事务回滚失败", ex);
                }
            }
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.error("关闭连接失败", e);
                }
            }
        }
    }

    // ------------------ 工具方法 ------------------

    /**
     * 检查记录是否存在
     * @param sql SQL语句（应该返回 COUNT(*) 或类似）
     * @param params 参数列表
     * @return 是否存在
     */
    public static boolean exists(String sql, Object... params) {
        Object count = queryScalar(sql, params);
        if (count instanceof Number) {
            return ((Number) count).intValue() > 0;
        }
        return false;
    }

    /**
     * 获取表的列信息
     * @param schema 模式名
     * @param tableName 表名
     * @return 列信息列表
     */
    public static List<Map<String, Object>> getTableColumns(String schema, String tableName) {
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE " +
                "FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?";
        return queryList(sql, tableName.toUpperCase());
    }

    /**
     * 获取连接池状态
     * @return 连接池状态信息
     */
    public static Map<String, Object> getPoolStatus() {
        Map<String, Object> status = new HashMap<>();
        if (dataSource != null) {
//            status.put("totalConnections", dataSource.getHikariPoolMXBean().getTotalConnections());
//            status.put("activeConnections", dataSource.getHikariPoolMXBean().getActiveConnections());
//            status.put("idleConnections", dataSource.getHikariPoolMXBean().getIdleConnections());
//            status.put("threadsAwaitingConnection", dataSource.getHikariPoolMXBean().getThreadsAwaitingConnection());
        }
        return status;
    }

    // ------------------ 私有辅助方法 ------------------

    private static void setParameters(PreparedStatement ps, Object... params) throws SQLException {
        // 首先获取参数的元数据，判断哪些是CLOB
        ParameterMetaData pmd = ps.getParameterMetaData();

        for (int i = 0; i < params.length; i++) {
            int paramIndex = i + 1;
            Object param = params[i];

            try {
                // 获取参数类型
                int sqlType = pmd.getParameterType(paramIndex);

                if (param == null) {
                    // 根据实际类型设置null
                    ps.setNull(paramIndex, sqlType);

                } else if (sqlType == java.sql.Types.CLOB) {
                    // ===== CLOB字段特殊处理 =====
                    String clobContent = param.toString();

                    if (clobContent.isEmpty()) {
                        ps.setClob(paramIndex, new StringReader(""));
                    } else {
                        // 使用流式传输
                        Reader reader = new StringReader(clobContent);
                        ps.setCharacterStream(paramIndex, reader, clobContent.length());
                    }

                } else if (sqlType == java.sql.Types.BLOB) {
                    // BLOB字段处理
                    byte[] blobContent = (byte[]) param;
                    ps.setBlob(paramIndex, new ByteArrayInputStream(blobContent));

                } else if (param instanceof String) {
                    ps.setString(paramIndex, (String) param);

                } else if (param instanceof Integer) {
                    ps.setInt(paramIndex, (Integer) param);

                } else if (param instanceof Long) {
                    ps.setLong(paramIndex, (Long) param);

                } else if (param instanceof Timestamp) {
                    ps.setTimestamp(paramIndex, (Timestamp) param);

                } else if (param instanceof Date) {
                    ps.setDate(paramIndex, (Date) param);

                } else {
                    ps.setObject(paramIndex, param);
                }

            } catch (SQLException e) {
                LOGGER.error("设置参数{}失败: 类型={}, 值={}",
                        paramIndex,
                        param != null ? param.getClass().getSimpleName() : "null",
                        param);
                throw e;
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

    // ------------------ 内部类 ------------------

    /**
     * SQL 操作对象（用于事务）
     */
    public static class SqlOperation {
        public String sql;
        public Object[] params;

        public SqlOperation(String sql, Object... params) {
            this.sql = sql;
            this.params = params;
        }
    }

    // ------------------ 测试方法 ------------------

    public static void main(String[] args) {
        try {
            // 测试查询
            List<Map<String, Object>> list = queryList("SELECT * FROM test.test WHERE id > ?", "0");
            System.out.println("查询结果: " + list.size() + " 条");

            // 测试插入
//            int inserted = executeUpdate(
//                    "INSERT INTO test.test (ID, NAME, AGE) VALUES (?, ?, ?)",
//                    4, "张三", 25
//            );
//            System.out.println("插入成功: " + inserted + " 条");

            // 测试更新
            int updated = executeUpdate(
                    "UPDATE \"THELING\".\"sys_user\" SET \"USER_TYPE\" = ?, \"USER_NAME\" = ?, \"LOGIN_NAME\" = ?, \"PASSWORD\" = ?, \"DEP_ID\" = ?, \"ADMIN_CODE\" = ?, \"ADMIN_AREA\" = ?, \"DEP_CODE\" = ?, \"DEP_NAME\" = ?, \"STATUS\" = ?, \"EMAIL\" = ?, \"QQ\" = ?, \"MOBILE\" = ?, \"MOBILD\" = ?, \"LAST_LOGIN_ID\" = ?, \"LAST_LOGIN_TIME\" = ?, \"CRE_USER_NAME\" = ?, \"CRE_TIME\" = ?, \"STREET_LAKE\" = ?, \"IS_CPC\" = ?, \"CPC_ID\" = ?, \"CPC_NAME\" = ?, \"CPC_USER_TYPE\" = ?, \"SPELL_NAME\" = ?, \"OPEN_ID\" = ?, \"HEAD_IMG_URL\" = ?, \"NICK_NAME\" = ?, \"WORK_AUTHORITY\" = ?, \"DIS_ID\" = ?, \"DIS_CODE\" = ?, \"DIS_NAME\" = ?, \"TUCAO_ID\" = ?, \"UNION_ID\" = ?, \"TUCAO_USER_ID\" = ?, \"PAS_HISTORY\" = ?, \"auth_group\" = ?, \"app_list\" = ?, \"role_ids\" = ?, \"app_id\" = ?, \"role_keys\" = ?, \"session_key\" = ?, \"exec_status\" = ?, \"redis_cache\" = ?, \"deleted\" = ?, \"cmcy_user_id\" = ?, \"wx_mp_push\" = ? WHERE \"USER_ID\" = ?",
                    "operation.user","jyfwC","jyfwC","dc4c27d780f863f58f4a22c339a737ba","0","110000","全国","0","全国",1,null,"123456..","15081613713",null,null,"2025-12-18 21:53:36","系统管理员",null,null,0,null,null,null,"jyfwC",null,"https://dev.theling.team/qwd/headImg/nantou3.png",null,"zhong_shen",null,null,null,"49404a08fb2c41558f113a8996904c3d",null,null,null,"hybs-submitter;zsk-edit;zsk-sc-look",";null;jyfw;cycm_jg_pc_110116;jyfw_wapp;jyfw_cloud_operate;jyyd_web_app",";156095dd58fcc23ae0b4336d0d5efc21;63ee50f50add4281a65ac354eb7e70ef;a09bfe73ba3247a6bff8281c1d910edf;ac06234749916d649a7930f4a131387d;d07c8931cd104a5f8896b7495d9197c7;f1900fcdb306400cbcfca5bb85e90a11;f1900fcdb306400cbcfca5bb85e90a12;f1900fcdb306400cbcfca5bb85e90ab5;0d3e6491feaac6ecd47b89788efd9689;",1,null,null,2,"1","1",null,"0","ad1707e30d802937919889ccb1ad6e07"
            );
            System.out.println("更新成功: " + updated + " 条");

            // 测试删除
//            int deleted = executeUpdate(
//                    "DELETE FROM test.test WHERE ID = ?",
//                    "4"
//            );
//            System.out.println("删除成功: " + deleted + " 条");
//
//            // 测试事务
//            List<SqlOperation> ops = Arrays.asList(
//                    new SqlOperation("INSERT INTO test.test (ID, NAME) VALUES (?, ?)", 4, "李四"),
//                    new SqlOperation("UPDATE test.test SET NAME = ? WHERE ID = ?", "李四(修改)", 4)
//            );
//            boolean success = executeTransaction(ops);
//            System.out.println("事务执行: " + (success ? "成功" : "失败"));

            // 查看连接池状态
            System.out.println("连接池状态: " + getPoolStatus());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeDataSource();
        }
    }
}
