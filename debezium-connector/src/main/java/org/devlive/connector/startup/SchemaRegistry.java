package org.devlive.connector.startup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CDC 表结构注册中心
 *
 * 功能：
 * 1. 从 history.dat 加载所有表结构
 * 2. 提供快速的表结构查询（线程安全）
 * 3. 支持运行时动态添加新表
 * 4. 高性能、低延迟
 *
 * 使用场景：
 * - CDC 数据消费时快速获取表结构
 * - 根据表结构生成 SQL 语句
 * - 动态映射数据库列
 */
public class SchemaRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistry.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // 表结构缓存：key = "schema.table"，线程安全
    private final ConcurrentHashMap<String, TableSchema> tables = new ConcurrentHashMap<>();
    
    private volatile static boolean IS_LOADING = false;
    
    private volatile String historyFile;

    // 统计信息
    private volatile int loadedCount = 0;
    private volatile int dynamicCount = 0;

    /**
     * 表结构
     */
    public static class TableSchema {
        private final String schema;
        private final String tableName;
        private final String fullName;
        private final List<String> primaryKeys;
        private final Map<String, ColumnInfo> columns; // 列名 -> 列信息
        private final List<ColumnInfo> columnList;     // 保持顺序

        public TableSchema(String schema, String tableName, List<String> primaryKeys,
                           List<ColumnInfo> columns) {
            this.schema = schema;
            this.tableName = tableName;
            this.fullName = schema + "." + tableName;
            this.primaryKeys = primaryKeys;
            this.columnList = Collections.unmodifiableList(new ArrayList<>(columns));

            // 构建列名索引
            Map<String, ColumnInfo> colMap = new HashMap<>();
            for (ColumnInfo col : columns) {
                colMap.put(col.getName(), col);
            }
            this.columns = Collections.unmodifiableMap(colMap);
        }

        public String getSchema() { return schema; }
        public String getTableName() { return tableName; }
        public String getFullName() { return fullName; }
        public List<String> getPrimaryKeys() { return primaryKeys; }
        public List<ColumnInfo> getColumns() { return columnList; }
        public ColumnInfo getColumn(String name) { return columns.get(name); }
        public boolean hasColumn(String name) { return columns.containsKey(name); }
        public int getColumnCount() { return columnList.size(); }
    }

    /**
     * 列信息
     */
    public static class ColumnInfo {
        private final String name;
        private final int jdbcType;
        private final String typeName;
        private final int length;
        private final int position;
        private final boolean nullable;
        private final boolean isPrimaryKey;

        public ColumnInfo(String name, int jdbcType, String typeName, int length,
                          int position, boolean nullable, boolean isPrimaryKey) {
            this.name = name;
            this.jdbcType = jdbcType;
            this.typeName = typeName;
            this.length = length;
            this.position = position;
            this.nullable = nullable;
            this.isPrimaryKey = isPrimaryKey;
        }

        public String getName() { return name; }
        public int getJdbcType() { return jdbcType; }
        public String getTypeName() { return typeName; }
        public int getLength() { return length; }
        public int getPosition() { return position; }
        public boolean isNullable() { return nullable; }
        public boolean isPrimaryKey() { return isPrimaryKey; }
    }

    /**
     * 从 history.dat 加载表结构
     */
    public void loadFromHistory(String historyFile) {
        LOGGER.info("[SchemaRegistry] 开始加载: {}", historyFile);
        this.historyFile = historyFile;
        long startTime = System.currentTimeMillis();
        int lineCount = 0;
        int successCount = 0;
        int errorCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(historyFile))) {
            String line;

            while ((line = reader.readLine()) != null) {
                lineCount++;

                if (line.trim().isEmpty()) {
                    continue;
                }

                try {
                    JsonNode root = JSON_MAPPER.readTree(line);
                    JsonNode tableChanges = root.get("tableChanges");

                    if (tableChanges != null && tableChanges.isArray()) {
                        for (JsonNode change : tableChanges) {
                            TableSchema table = parseTable(change);
                            if (table != null) {
                                tables.put(table.getFullName(), table);
                                successCount++;
                            }
                        }
                    }

                } catch (Exception e) {
                    errorCount++;
                    LOGGER.debug("[SchemaRegistry] 行 {} 解析失败: {}", lineCount, e.getMessage());
                }
            }
            IS_LOADING = true;
        } catch (Exception e) {
            LOGGER.error("[SchemaRegistry] 加载失败", e);
//            throw new RuntimeException("加载 history.dat 失败", e);
            IS_LOADING = false;
        }

        loadedCount = successCount;
        long duration = System.currentTimeMillis() - startTime;

        LOGGER.info("[SchemaRegistry] 加载完成: 行数={}, 表数={}, 失败={}, 耗时={}ms",
                lineCount, successCount, errorCount, duration);
    }

    /**
     * 解析表结构
     */
    private TableSchema parseTable(JsonNode change) {
        try {
            // 1. 解析表标识
            String id = change.get("id").asText();
            String[] parts = id.replace("\"", "").split("\\.");

            String schema = parts.length >= 2 ? parts[parts.length - 2] : "UNKNOWN";
            String tableName = parts[parts.length - 1];

            // 2. 解析 table 节点
            JsonNode tableNode = change.get("table");
            if (tableNode == null) {
                return null;
            }

            // 3. 解析主键
            List<String> primaryKeys = new ArrayList<>();
            JsonNode pkNode = tableNode.get("primaryKeyColumnNames");
            if (pkNode != null && pkNode.isArray()) {
                for (JsonNode pk : pkNode) {
                    primaryKeys.add(pk.asText());
                }
            }

            // 4. 解析列
            List<ColumnInfo> columns = new ArrayList<>();
            JsonNode colsNode = tableNode.get("columns");
            if (colsNode != null && colsNode.isArray()) {
                Set<String> pkSet = new HashSet<>(primaryKeys);

                for (JsonNode colNode : colsNode) {
                    String name = colNode.get("name").asText();
                    int jdbcType = colNode.get("jdbcType").asInt();
                    String typeName = colNode.get("typeName").asText();

                    JsonNode lengthNode = colNode.get("length");
                    int length = lengthNode != null && !lengthNode.isNull() ? lengthNode.asInt() : 0;

                    JsonNode posNode = colNode.get("position");
                    int position = posNode != null ? posNode.asInt() : 0;

                    JsonNode optNode = colNode.get("optional");
                    boolean nullable = optNode != null && optNode.asBoolean();

                    boolean isPk = pkSet.contains(name);

                    columns.add(new ColumnInfo(name, jdbcType, typeName, length,
                            position, nullable, isPk));
                }
            }

            return new TableSchema(schema, tableName, primaryKeys, columns);

        } catch (Exception e) {
            LOGGER.debug("[SchemaRegistry] 解析表失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 获取表结构（线程安全）
     *
     * @param schema 数据库名
     * @param tableName 表名
     * @return 表结构，如果不存在返回 null
     */
    public TableSchema getTable(String schema, String tableName) {
        if (!IS_LOADING) {
            this.loadFromHistory(this.historyFile);
            if (!IS_LOADING) {
                throw new RuntimeException("加载 history.dat 失败");
            }
        }
        String key = schema + "." + tableName;
        return tables.get(key);
    }

    /**
     * 检查表是否存在
     */
    public boolean hasTable(String schema, String tableName) {
        String key = schema + "." + tableName;
        return tables.containsKey(key);
    }

    /**
     * 动态添加表结构（用于运行时发现新表）
     */
    public void addTable(TableSchema table) {
        tables.put(table.getFullName(), table);
        dynamicCount++;
        LOGGER.info("[SchemaRegistry] 动态添加表: {}", table.getFullName());
    }

    /**
     * 获取所有表名
     */
    public Set<String> getAllTableNames() {
        return new HashSet<>(tables.keySet());
    }

    /**
     * 按 schema 分组
     */
    public Map<String, List<String>> groupBySchema() {
        Map<String, List<String>> result = new TreeMap<>();

        for (TableSchema table : tables.values()) {
            result.computeIfAbsent(table.getSchema(), k -> new ArrayList<>())
                    .add(table.getTableName());
        }

        return result;
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("表数: %d (加载: %d, 动态: %d)",
                tables.size(), loadedCount, dynamicCount);
    }

    /**
     * 打印表结构（调试用）
     */
    public void printTable(String schema, String tableName) {
        TableSchema table = getTable(schema, tableName);
        if (table == null) {
            System.out.println("表不存在: " + schema + "." + tableName);
            return;
        }

        System.out.println("========================================");
        System.out.println("表: " + table.getFullName());
        System.out.println("========================================");
        System.out.println("主键: " + table.getPrimaryKeys());
        System.out.println("列数: " + table.getColumnCount());
        System.out.println();

        System.out.printf("%-30s %-15s %-10s %-10s %-10s%n",
                "列名", "类型", "JDBC", "长度", "可空");
        System.out.println("-".repeat(75));

        for (ColumnInfo col : table.getColumns()) {
            System.out.printf("%-30s %-15s %-10d %-10d %-10s%n",
                    col.getName(),
                    col.getTypeName(),
                    col.getJdbcType(),
                    col.getLength(),
                    col.isNullable() ? "YES" : "NO"
            );
        }

        System.out.println("========================================");
    }

    /**
     * 打印所有表
     */
    public void printAllTables() {
        System.out.println("========================================");
        System.out.println("所有表 (" + tables.size() + " 个)");
        System.out.println("========================================");

        Map<String, List<String>> grouped = groupBySchema();
        for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
            System.out.println("Schema: " + entry.getKey());
            for (String tableName : entry.getValue()) {
                TableSchema table = getTable(entry.getKey(), tableName);
                System.out.printf("  - %-40s (%d 列, 主键: %s)%n",
                        tableName, table.getColumnCount(), table.getPrimaryKeys());
            }
        }

        System.out.println("========================================");
    }

    // ==================== 测试 ====================

    public static void main(String[] args) {
        String historyFile = args.length > 0 ? args[0] : "history.dat";

        // 1. 创建注册中心
        SchemaRegistry registry = new SchemaRegistry();

        // 2. 加载表结构
        registry.loadFromHistory(historyFile);

        // 3. 打印统计
        System.out.println("\n统计信息: " + registry.getStats());
        System.out.println();

        // 4. 打印所有表
        registry.printAllTables();

        // 5. 查询特定表
        System.out.println("\n查询示例:");
        if (!registry.getAllTableNames().isEmpty()) {
            String firstTable = registry.getAllTableNames().iterator().next();
            String[] parts = firstTable.split("\\.");
            if (parts.length == 2) {
                registry.printTable(parts[0], parts[1]);
            }
        }
    }
}
