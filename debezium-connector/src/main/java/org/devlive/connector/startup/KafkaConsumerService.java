package org.devlive.connector.startup;

import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaConsumerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private volatile boolean running = false;
    private final SchemaRegistry schemaRegistry;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final SQLCache sqlCache = new SQLCache();
    private volatile boolean encypted = false;

    public KafkaConsumerService(Properties props, String topic, boolean encrypted, String historyFile) {
        this.encypted = encrypted;
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(props);
        // 初始化 SchemaRegistry
        this.schemaRegistry = new SchemaRegistry();
        LOGGER.info("[Kafka] 初始化 SchemaRegistry，加载表结构...");
        schemaRegistry.loadFromHistory(historyFile);
        LOGGER.info("[Kafka] {}", schemaRegistry.getStats());
    }

    public void start() {
        running = true;
        consumer.subscribe(Pattern.compile(topic));

        LOGGER.info("Kafka consumer subscribed to topic: {}", topic);
        
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                // 手动提交 offset
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            if (running) {
                LOGGER.error("Consumer wakeup exception", e);
            }
        } catch (Exception e) {
            LOGGER.error("Error polling records", e);
            try {
                Thread.sleep(5000); // 错误后等待重试
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        } finally {
            consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            LOGGER.debug("Consumed record: key={}, value={}, partition={}, offset={}",
                    record.key(), record.value(), record.partition(), record.offset());

            // 处理业务逻辑
            handleBusinessLogic(record);

        } catch (Exception e) {
            LOGGER.error("Error processing record: {}", record, e);
            // 可以将失败的记录发送到 DLQ (Dead Letter Queue)
        }
    }

    private void handleBusinessLogic(ConsumerRecord<String, String> record) {
        try {
            // 1. 解析 CDC JSON 数据
            JsonNode root = jsonMapper.readTree(record.value());
            JsonNode payload = root.get("payload");

            if (payload == null) {
                LOGGER.debug("[CDC] payload 为空，跳过");
                return;
            }

            // 2. 提取元数据
            JsonNode source = payload.get("source");
            if (source == null) {
                LOGGER.warn("[CDC] source 为空，跳过");
                return;
            }

            // 兼容不同的 source 字段名
            String schema = extractSchema(source);
            String tableName = source.get("table").asText();
            String op = payload.get("op").asText();

            // 3. 查询表结构（高性能 O(1) 查询）
            SchemaRegistry.TableSchema table = schemaRegistry.getTable("THELING", tableName);

            if (table == null) {
                LOGGER.warn("[CDC] 未找到表结构: {}.{}", schema, tableName);
                return;
            }

            // 4. 根据操作类型处理
            switch (op) {
                case "c": // create
                case "r": // read (snapshot)
                    executeInsert(table, payload);
                    break;
                case "u": // update
                    executeUpdate(table, payload);
                    break;
                case "d": // delete
                    executeDelete(table, payload);
                    break;
                default:
                    LOGGER.warn("[CDC] 未知操作类型: {}", op);
            }

        } catch (Exception e) {
            LOGGER.error("[CDC] 处理业务逻辑失败", e);
        }
    }

    /**
     * 提取 schema 名称（兼容多种字段名）
     */
    private String extractSchema(JsonNode source) {
        if (source.has("schema")) {
            return source.get("schema").asText();
        } else if (source.has("db")) {
            return source.get("db").asText();
        } else if (source.has("schemaName")) {
            return source.get("schemaName").asText();
        } else {
            return "UNKNOWN";
        }
    }

    /**
     * 执行插入
     */
    private void executeInsert(SchemaRegistry.TableSchema table, JsonNode payload) {
        JsonNode after = payload.get("after");
        if (after == null || after.isNull()) {
            return;
        }

        try {
            // 生成 SQL（带缓存）
            String sql = sqlCache.getInsertSQL(table);

            // 提取参数
            Object[] params = new Object[table.getColumnCount()];
            int i = 0;
            for (SchemaRegistry.ColumnInfo col : table.getColumns()) {
                params[i++] = extractValue(after.get(col.getName()), col);
            }

            // 执行
            JdbcQueryUtils.executeUpdate(sql, params);
            LOGGER.debug("[CDC] 插入成功: {}", table.getFullName());

        } catch (Exception e) {
            // 主键冲突，尝试更新
            if (isDuplicateKeyError(e)) {
                LOGGER.debug("[CDC] 记录已存在，转为更新: {}", table.getFullName());
                executeUpdate(table, payload);
            } else {
                LOGGER.error("[CDC] 插入失败: {}", table.getFullName(), e);
            }
        }
    }

    /**
     * 执行更新
     */
    private void executeUpdate(SchemaRegistry.TableSchema table, JsonNode payload) {
        JsonNode after = payload.get("after");
        JsonNode before = payload.get("before");

        if (after == null || after.isNull()) {
            return;
        }

        try {
            // 生成 SQL（带缓存）
            String sql = sqlCache.getUpdateSQL(table);

            // 提取参数：SET 子句 + WHERE 子句
            List<Object> paramList = new ArrayList<>();

            // SET 子句（非主键列）
            for (SchemaRegistry.ColumnInfo col : table.getColumns()) {
                if (!col.isPrimaryKey()) {
                    paramList.add(extractValue(after.get(col.getName()), col));
                }
            }

            // WHERE 子句（主键列）
            for (String pkName : table.getPrimaryKeys()) {
                SchemaRegistry.ColumnInfo col = table.getColumn(pkName);
                JsonNode pkValue = before != null && !before.isNull()
                        ? before.get(pkName)
                        : after.get(pkName);
                paramList.add(extractValue(pkValue, col));
            }

            // 执行
            int affected = JdbcQueryUtils.executeUpdate(sql, paramList.toArray());

            if (affected == 0) {
                LOGGER.debug("[CDC] 更新未找到记录，转为插入: {}", table.getFullName());
                executeInsert(table, payload);
            } else {
                LOGGER.debug("[CDC] 更新成功: {}", table.getFullName());
            }

        } catch (Exception e) {
            LOGGER.error("[CDC] 更新失败: {}", table.getFullName(), e);
        }
    }

    /**
     * 执行删除
     */
    private void executeDelete(SchemaRegistry.TableSchema table, JsonNode payload) {
        JsonNode before = payload.get("before");
        if (before == null || before.isNull()) {
            return;
        }

        try {
            // 生成 SQL（带缓存）
            String sql = sqlCache.getDeleteSQL(table);

            // 提取主键参数
            List<Object> paramList = new ArrayList<>();
            for (String pkName : table.getPrimaryKeys()) {
                SchemaRegistry.ColumnInfo col = table.getColumn(pkName);
                paramList.add(extractValue(before.get(pkName), col));
            }

            // 执行
            JdbcQueryUtils.executeUpdate(sql, paramList.toArray());
            LOGGER.debug("[CDC] 删除成功: {}", table.getFullName());

        } catch (Exception e) {
            LOGGER.error("[CDC] 删除失败: {}", table.getFullName(), e);
        }
    }

    /**
     * 从 JsonNode 提取值并转换类型
     */
    private Object extractValue(JsonNode value, SchemaRegistry.ColumnInfo col) {
        if (value == null || value.isNull()) {
            return null;
        }

        try {
            switch (col.getJdbcType()) {
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    return value.asInt();

                case Types.BIGINT:
                    return value.asLong();

                case Types.FLOAT:
                case Types.REAL:
                    return (float) value.asDouble();

                case Types.DOUBLE:
                    return value.asDouble();

                case Types.DECIMAL:
                case Types.NUMERIC:
                    return new java.math.BigDecimal(value.asText());

                case Types.BOOLEAN:
                case Types.BIT:
                    if (value.isBoolean()) {
                        return value.asBoolean();
                    } else if (value.isNumber()) {
                        return value.asInt() != 0;
                    } else {
                        return Boolean.parseBoolean(value.asText());
                    }

                case Types.DATE:
                    if (value.isNumber()) {
                        return new java.sql.Date(value.asLong());
                    } else {
                        return value.asText();
                    }

                case Types.TIMESTAMP:
                    if (value.isNumber()) {
                        return new java.sql.Timestamp(value.asLong());
                    } else {
                        return value.asText();
                    }

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                default:
                    if (encypted) {
                        return CaesarEncryptUtils.encryptCaesarSalt(value.asText(), "");
                    } else {
                        return value.asText();
                    }
            }
        } catch (Exception e) {
            LOGGER.warn("[CDC] 提取值失败: column={}, type={}, value={}",
                    col.getName(), col.getJdbcType(), value, e);
            return value.asText();
        }
    }

    /**
     * 判断是否是主键冲突错误（达梦数据库）
     */
    private boolean isDuplicateKeyError(Exception e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }

        // 达梦数据库的主键冲突错误
        return message.contains("唯一约束") ||
                message.contains("UNIQUE") ||
                message.contains("duplicate") ||
                message.contains("PRIMARY KEY");
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }

    /**
     * SQL 缓存（线程安全）
     */
    private static class SQLCache {

        private final Map<String, String> insertCache = new HashMap<>();
        private final Map<String, String> updateCache = new HashMap<>();
        private final Map<String, String> deleteCache = new HashMap<>();

        /**
         * 获取 INSERT SQL（带缓存）
         */
        public synchronized String getInsertSQL(SchemaRegistry.TableSchema table) {
            return insertCache.computeIfAbsent(table.getFullName(), k -> {
                StringBuilder sql = new StringBuilder("INSERT INTO ");
                sql.append("\"").append(table.getSchema()).append("\".\"")
                        .append(table.getTableName()).append("\" (");

                String columns = table.getColumns().stream()
                        .map(col -> "\"" + col.getName() + "\"")
                        .collect(Collectors.joining(", "));

                String placeholders = table.getColumns().stream()
                        .map(c -> "?")
                        .collect(Collectors.joining(", "));

                sql.append(columns).append(") VALUES (").append(placeholders).append(")");

                LOGGER.debug("[SQL] 生成 INSERT: {}", sql);
                return sql.toString();
            });
        }

        /**
         * 获取 UPDATE SQL（带缓存）
         */
        public synchronized String getUpdateSQL(SchemaRegistry.TableSchema table) {
            return updateCache.computeIfAbsent(table.getFullName(), k -> {
                StringBuilder sql = new StringBuilder("UPDATE ");
                sql.append("\"").append(table.getSchema()).append("\".\"")
                        .append(table.getTableName()).append("\" SET ");

                String setClause = table.getColumns().stream()
                        .filter(col -> !col.isPrimaryKey())
                        .map(col -> "\"" + col.getName() + "\" = ?")
                        .collect(Collectors.joining(", "));

                String whereClause = table.getPrimaryKeys().stream()
                        .map(pk -> "\"" + pk + "\" = ?")
                        .collect(Collectors.joining(" AND "));

                sql.append(setClause).append(" WHERE ").append(whereClause);

                LOGGER.debug("[SQL] 生成 UPDATE: {}", sql);
                return sql.toString();
            });
        }

        /**
         * 获取 DELETE SQL（带缓存）
         */
        public synchronized String getDeleteSQL(SchemaRegistry.TableSchema table) {
            return deleteCache.computeIfAbsent(table.getFullName(), k -> {
                StringBuilder sql = new StringBuilder("DELETE FROM ");
                sql.append("\"").append(table.getSchema()).append("\".\"")
                        .append(table.getTableName()).append("\" WHERE ");

                String whereClause = table.getPrimaryKeys().stream()
                        .map(pk -> "\"" + pk + "\" = ?")
                        .collect(Collectors.joining(" AND "));

                sql.append(whereClause);

                LOGGER.debug("[SQL] 生成 DELETE: {}", sql);
                return sql.toString();
            });
        }
    }


}
