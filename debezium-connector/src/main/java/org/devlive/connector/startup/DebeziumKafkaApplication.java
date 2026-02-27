package org.devlive.connector.startup;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class DebeziumKafkaApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumKafkaApplication.class);

    private final DebeziumEngine<ChangeEvent<String, String>> engine;
    private final ExecutorService engineExecutor;
    private final KafkaConsumerService consumerService;
    private final ExecutorService consumerExecutor;
    private final CountDownLatch shutdownLatch;
    private volatile boolean running = true;
    private ChangeEventProcessor eventProcessor;

    public DebeziumKafkaApplication(Properties debeziumProps, Properties kafkaProps, String topic) {
        this.eventProcessor = new ChangeEventProcessor(Boolean.parseBoolean(kafkaProps.getProperty("database.encrypt")));
        this.shutdownLatch = new CountDownLatch(1);

        // 使用有界线程池，避免资源泄漏
        this.engineExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "debezium-engine");
                t.setDaemon(false);
                t.setUncaughtExceptionHandler((thread, throwable) -> {
                    LOGGER.error("Uncaught exception in {}", thread.getName(), throwable);
                    restart();
                });
                return t;
            }
        });

        this.consumerExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "kafka-consumer");
                t.setDaemon(false);
                t.setUncaughtExceptionHandler((thread, throwable) -> {
                    LOGGER.error("Uncaught exception in {}", thread.getName(), throwable);
                    restart();
                });
                return t;
            }
        });

        // 初始化 Kafka Consumer Service
        this.consumerService = new KafkaConsumerService(kafkaProps, topic, Boolean.parseBoolean(kafkaProps.getProperty("database.encrypt")), debeziumProps.getProperty("database.history.file.filename"));

        // 初始化 Debezium Engine - 使用 JSON 格式
        this.engine = DebeziumEngine.create(Json.class)
                .using(debeziumProps)
                .notifying(this::handleChangeEvent)
                .using(this::handleEngineCompletion)
                .build();
    }

    /**
     * 处理 Debezium CDC 变更事件
     * 处理数据，变换schema，解密数据
     */
    private void handleChangeEvent(ChangeEvent<String, String> record) {
//        try {
//            String key = record.key();
//            String value = record.value();
//            System.out.println("key: " + key);
//            System.out.println("value: " + value);
//
//            LOGGER.debug("Received change event: {}", value);
//
//            JSONObject recordKey = JSONObject.parseObject(key);
//            JSONObject recordKeySchema = recordKey.getJSONObject("schema");
//            JSONObject recordKeyStructJson = new JSONObject();
//            if (recordKeySchema != null && recordKeySchema.containsKey("fields")) {
//                for (Object item : recordKeySchema.getJSONArray("fields")) {
//                    JSONObject field = (JSONObject) item;
//                    recordKeyStructJson.put(field.getString("field"), field.get("type"));
//                }
//            }
//            String messageKey = key;
//            boolean keyChanged = false;
//            JSONObject recordKeyPayload = recordKey.getJSONObject("payload");
//            for (String s : recordKeyPayload.keySet()) {
//                if (isEncrypt && "string".equalsIgnoreCase(recordKeyStructJson.getString(s)) && StringUtils.isNotBlank(recordKeyPayload.getString(s))) {
//                    recordKeyPayload.put(s, CaesarEncryptUtils.decryptCaesarSalt(recordKeyPayload.getString(s), ""));
//                    keyChanged = true;
//                }
//            }
//            if (keyChanged) {
//                messageKey = recordKey.toJSONString(JSONWriter.Feature.WriteMapNullValue);
//            }
//            // 数据表的主键
//            String primaryKey = null;
//
//            JSONObject schema = recordKey.getJSONObject("schema");
//            JSONArray fields = schema.getJSONArray("fields");
//            if (fields != null && !fields.isEmpty()) {
//                primaryKey = fields.getJSONObject(0).getString("field");
//            }
//            String databaseByTableName = "";
//            boolean canSend = true;
//            String message = value;
//            if (value != null) {
//                boolean isChange = false;
//                JSONObject recordValue = JSONObject.parseObject(value);
//                JSONObject recordValuePayload = recordValue.getJSONObject("payload");
//                JSONObject recordValueSchema = recordValue.getJSONObject("schema");
//                JSONObject recordValueStructJson = new JSONObject();
//                if (recordValueSchema != null && recordValueSchema.containsKey("fields")) {
//                    for (Object item : recordValueSchema.getJSONArray("fields")) {
//                        JSONObject field = (JSONObject) item;
//                        if ("struct".equalsIgnoreCase(field.getString("type"))
//                                && "after".equalsIgnoreCase(field.getString("field"))) {
//                            field.getJSONArray("fields").forEach(inner -> {
//                                JSONObject json = (JSONObject) inner;
//                                recordValueStructJson.put(json.getString("field"), json.get("type"));
//                            });
//                            break; // 找到后退出
//                        }
//                    }
//                }
//
//                if (StringUtils.isBlank(primaryKey)) {
//                    LOGGER.warn("Primary key is null, skipping record");
//                    return;
//                }
//
//                if (recordValuePayload == null) {
//                    LOGGER.warn("Payload is null, skipping record");
//                    return;
//                }
//
//                JSONObject after = recordValuePayload.getJSONObject("after");
//                JSONObject before = recordValuePayload.getJSONObject("before");
//                String laId = "";
//                if (after != null) {
//                    laId = after.getString(primaryKey);
//                }
//                if (StringUtils.isBlank(laId) && before != null) {
//                    laId = before.getString(primaryKey);
//                }
//                
//                JSONObject source = recordValuePayload.getJSONObject("source");
//                if (after == null || source == null) {
//                    LOGGER.debug("No 'after' or 'source' data, skipping");
//                    return;
//                }
//                if (before != null) {
//                    for (Map.Entry<String, Object> entry : before.entrySet()) {
//                        String fieldKey = entry.getKey();
//                        Object fieldValue = entry.getValue();
//                        if (isEncrypt && fieldKey != null && fieldValue != null && !StringUtils.isNumeric(fieldValue.toString()) && "string".equalsIgnoreCase(recordValueStructJson.getString(fieldKey)) && !fieldKey.toLowerCase().contains("time")) {
//                            before.put(fieldKey, CaesarEncryptUtils.decryptCaesarSalt(fieldValue.toString(), ""));
//                            isChange = true;
//                        }
//                    }
//                }
//
//                // 处理 CLOB 字段
//                for (Map.Entry<String, Object> entry : after.entrySet()) {
//                    String fieldKey = entry.getKey();
//                    Object fieldValue = entry.getValue();
//
//                    if (primaryKey.equalsIgnoreCase(fieldKey) && fieldValue == null) {
//                        continue;
//                    }
//
//                    if ("OUT_CLOB".equalsIgnoreCase(String.valueOf(fieldValue)) || String.valueOf(fieldValue).toUpperCase().startsWith("OUT_CLOB")) {
//
//                        if (laId == null || laId.isEmpty()) {
//                            LOGGER.warn("primaryKey is null or empty, cannot query CLOB");
//                            canSend = false;
//                            continue;
//                        }
//
//                        // 查询实际的 CLOB 值
//                        String sql = String.format(
//                                "SELECT %s FROM %s.%s WHERE %s = ?",
//                                fieldKey,
//                                source.getString("schema"),
//                                source.getString("table"),
//                                primaryKey
//                        );
//
//                        try {
//                            Map<String, Object> result = JdbcQueryUtils.queryOne(sql, laId);
//                            if (result != null && result.containsKey(fieldKey)) {
//                                fieldValue = result.get(fieldKey);
//                                after.put(fieldKey, result.get(fieldKey));
//                            }
//                            isChange = true;
//                        } catch (Exception e) {
//                            LOGGER.error("Failed to query CLOB field: {}", fieldKey, e);
//                            canSend = false;
//                        }
//                    }
//
//                    // 处理 ID 字段
//                    if ("id".equalsIgnoreCase(fieldKey) && fieldValue == null) {
//                        isChange = true;
//                        if (laId != null && !laId.isEmpty()) {
//                            String idSql = String.format(
//                                    "SELECT id FROM %s.%s WHERE %s = ?",
//                                    source.getString("schema"),
//                                    source.getString("table"),
//                                    primaryKey
//                            );
//
//                            try {
//                                Map<String, Object> result = JdbcQueryUtils.queryOne(idSql, laId);
//                                if (result != null && result.get(fieldKey) != null) {
//                                    after.put(fieldKey, result.get(fieldKey).toString().trim());
//                                }
//                            } catch (Exception e) {
//                                LOGGER.error("Failed to query ID field", e);
//                                canSend = false;
//                            }
//                        }
//                    }
//
//                    // 处理 ID 字段
//                    if (fieldValue != null && StringUtils.isBlank(fieldValue.toString())) {
//                        isChange = true;
//                        if (laId != null && !laId.isEmpty()) {
//                            String idSql = String.format(
//                                    "SELECT %s FROM %s.%s WHERE %s = ?",
//                                    fieldKey,
//                                    source.getString("schema"),
//                                    source.getString("table"),
//                                    primaryKey
//                            );
//
//                            try {
//                                Map<String, Object> result = JdbcQueryUtils.queryOne(idSql, laId);
//                                if (result != null && result.get(fieldKey) != null) {
//                                    after.put(fieldKey, result.get(fieldKey).toString().trim());
//                                }
//                            } catch (Exception e) {
//                                LOGGER.error("Failed to query ID field", e);
//                                canSend = false;
//                            }
//                        }
//                    }
//                    if (isEncrypt && fieldKey != null && fieldValue != null && !StringUtils.isNumeric(fieldValue.toString()) && "string".equalsIgnoreCase(recordValueStructJson.getString(fieldKey)) && !fieldKey.toLowerCase().contains("time")) {
//                        after.put(fieldKey, CaesarEncryptUtils.decryptCaesarSalt(fieldValue.toString(), ""));
//                    }
//                }
//
//                String sourceSchemaName = source.getString("table");
//                databaseByTableName = DataTableConfig.getDatabaseByTableName(sourceSchemaName.toLowerCase());
//                if (StringUtils.isNotBlank(databaseByTableName)) {
//                    isChange = true;
//                    source.put("schema", databaseByTableName);
//                    source.put("db", databaseByTableName);
//                }
//                
//                message = isChange ? recordValue.toJSONString(JSONWriter.Feature.WriteMapNullValue) : value;
//            }
//            System.out.println(messageKey);
//            System.out.println(message);
//
//            if (canSend && StringUtils.isNotBlank(databaseByTableName)) {
//                String topic = "data-sync-dm-to-mysql." + databaseByTableName;
//                KafkaProducerManager.getInstance().sendMessage(topic, messageKey, message);
//            }
//
//        } catch (Exception e) {
//            LOGGER.error("Error processing change event", e);
//            // 不抛出异常，避免导致 engine 停止
//        }
        String key = record.key();
        String value = record.value();

        ChangeEventProcessor.EventProcessResult result = eventProcessor.process(key, value);

        if (result != null) {
            System.out.println( result.getTopic());
            System.out.println( result.getKey());
            System.out.println( result.getValue());
            KafkaProducerManager.getInstance().sendMessage(result.getTopic(), result.getKey(), result.getValue());
        }
    }

    /**
     * 处理 Debezium Engine 完成回调
     */
    private void handleEngineCompletion(boolean success, String message, Throwable error) {
        if (!success) {
            LOGGER.error("Debezium engine completed with error: {}", message, error);
            restart();
        } else {
            LOGGER.info("Debezium engine completed successfully: {}", message);
        }
    }

    /**
     * 启动应用
     */
    public void start() {
        LOGGER.info("Starting Debezium Kafka Application...");

        // 注册 shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown hook triggered");
            shutdown();
        }));

        // 启动 Debezium Engine
        engineExecutor.execute(engine);

        // 启动 Kafka Consumer
        consumerExecutor.execute(() -> {
            try {
                LOGGER.info("Starting Kafka consumer");
                consumerService.start();
            } catch (Exception e) {
                LOGGER.error("Kafka consumer failed", e);
                restart();
            }
        });

        LOGGER.info("Application started successfully");

        // 主线程等待
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Main thread interrupted");
        }
    }

    /**
     * 优雅关闭应用
     */
    public void shutdown() {
        if (!running) return;

        running = false;
        LOGGER.info("Shutting down application...");

        try {
            // 停止 engine
            engine.close();
            engineExecutor.shutdown();
            if (!engineExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                engineExecutor.shutdownNow();
            }

            // 关闭 producer
            KafkaProducerManager.getInstance().close();

            // 停止 consumer
            consumerService.stop();
            consumerExecutor.shutdown();
            if (!consumerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                consumerExecutor.shutdownNow();
            }

            // 关闭数据库连接池
            JdbcQueryUtils.close();

            LOGGER.info("Application shutdown complete");
        } catch (Exception e) {
            LOGGER.error("Error during shutdown", e);
        } finally {
            shutdownLatch.countDown();
        }
    }

    /**
     * 重启应用（交给外部进程管理工具处理）
     */
    private void restart() {
        LOGGER.warn("Attempting to restart application...");
        // 直接退出，让 systemd/docker/supervisor 等工具重启
        System.exit(1);
    }

    /**
     * 主入口
     */
    public static void main(String[] args) {
        try {
            // 加载配置
            Properties debeziumProps = loadDebeziumProperties();
            Properties kafkaProps = loadKafkaProperties();
            String topic = debeziumProps.getProperty("kafka.topic.name", "data-sync-dm-theling");

            // 启动应用
            DebeziumKafkaApplication app = new DebeziumKafkaApplication(
                    debeziumProps,
                    kafkaProps,
                    topic
            );
            app.start();

        } catch (Exception e) {
            LOGGER.error("Failed to start application", e);
            System.exit(1);
        }
    }

    /**
     * 从 classpath 或文件系统加载 Debezium 配置
     */
    private static Properties loadDebeziumProperties() throws IOException {
        Properties props = new Properties();
        String configFile = System.getProperty("debezium.config", "debezium.properties");

        // 先尝试从 classpath 加载
        try (InputStream is = DebeziumKafkaApplication.class.getClassLoader().getResourceAsStream(configFile)) {
            if (is != null) {
                props.load(is);
                LOGGER.info("Loaded debezium config from classpath: {}", configFile);
                return props;
            }
        }

        // 如果 classpath 中没有，尝试从文件系统加载
        try (InputStream is = new FileInputStream(configFile)) {
            props.load(is);
            LOGGER.info("Loaded debezium config from file system: {}", configFile);
            return props;
        }
    }

    /**
     * 从 classpath 或文件系统加载 Kafka 配置
     */
    private static Properties loadKafkaProperties() throws IOException {
        Properties props = new Properties();
        String configFile = System.getProperty("kafka.config", "kafka.properties");

        // 先尝试从 classpath 加载
        try (InputStream is = DebeziumKafkaApplication.class.getClassLoader().getResourceAsStream(configFile)) {
            if (is != null) {
                props.load(is);
                LOGGER.info("Loaded kafka config from classpath: {}", configFile);
                return props;
            }
        }

        // 如果 classpath 中没有，尝试从文件系统加载
        try (InputStream is = new FileInputStream(configFile)) {
            props.load(is);
            LOGGER.info("Loaded kafka config from file system: {}", configFile);
            return props;
        }
    }
}
