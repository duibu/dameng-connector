package org.devlive.connector;

import com.alibaba.fastjson2.JSONObject;
import org.devlive.connector.dameng.DamengConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.devlive.connector.dameng.JdbcQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DamengDebeziumConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengDebeziumConnectorTest.class);
    private static DebeziumEngine<ChangeEvent<String, String>> engine;

    public static void main(String[] args) {

        String brokers = "192.168.1.95:9092";
        String topic = "data-sync-dm-theling";

//        Properties kafkakProps = new Properties();
//        kafkakProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        kafkakProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkakProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//
//        // Kafka 4.1 依旧兼容这些属性
//        kafkakProps.put(ProducerConfig.ACKS_CONFIG, "all");
//        kafkakProps.put(ProducerConfig.RETRIES_CONFIG, 3);

//        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkakProps);
        KakfaCdcProducer kakfaCdcProducer = new KakfaCdcProducer(brokers);
        KafkaCdcConsumer kafkaCdcConsumer = new KafkaCdcConsumer("my-consumer-group", brokers);
        Properties props = new Properties();
        props.setProperty("name", "dameng-engine-localhost");
        props.setProperty("connector.class", DamengConnector.class.getName());
        props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
        props.setProperty("offset.storage.file.filename", "offset.txt");
        props.setProperty("offset.flush.interval.ms", "60000");
        props.setProperty("database.hostname", "192.168.1.69");
        props.setProperty("database.port", "5236");
        props.setProperty("database.user", "THELING");
        props.setProperty("database.dbname", "DAMENG");
        props.setProperty("database.password", "OnPCz12u^@~*ecfmZvR");
        props.setProperty("database.server.id", "85701");
        props.setProperty("column.propagate.source.type", "true");
        
        props.setProperty("snapshot.mode", "schema_only");

        props.setProperty("table.include.list", "THELING.BUSI_SCOPE_ITEMS_LABEL");
        props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.file.filename", "history.txt");
        String connectorName = "my-dameng-connector-" + getCurrentDateString();
        props.setProperty("database.server.name", connectorName);
//        props.setProperty("bootstrap.servers", "192.168.1.95:9092");
        props.setProperty("database.history.kafka.bootstrap.servers", "192.168.1.95:9092");
//        props.setProperty("database.history.kafka.topic", "dm-cdc-test");
//        props.setProperty("topic.prefix", "dm-cdc-test");

//        props.setProperty("key.converter.schemas.enable", "true");
//        props.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
//        props.setProperty("value.converter.schemas.enable", "true");
//        props.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.setProperty("database.serverTimezone", "UTC");
        props.setProperty("database.connection.adapter", "LogMiner");

        props.setProperty("debezium.log.mining.strategy", "online_catalog");
        props.setProperty("debezium.log.mining.continuous.mine", "true");
        props.setProperty("debezium.log.level", "DEBUG");

        props.setProperty("debezium.source.log.mining.batch.size", "1000");
        props.setProperty("debezium.source.poll.interval.ms", "1000");

        // 事务处理配置
//        props.setProperty("debezium.source.transaction.recover.policy", "skip"); // 或尝试 "skip"
//        props.setProperty("debezium.source.max.queue.size", "8192"); // 增加队列大小
//        props.setProperty("debezium.source.max.batch.size", "2048"); // 增加批处理大小

        // 暂时不能使用 "fast" 会导致部分字段解析失败
//        props.setProperty("internal.log.mining.dml.parser", "legacy");

        // 自动提交未提交事务的时间（以毫秒为单位）。
        props.setProperty("debezium.source.transaction.auto.commit.timeout.ms", "2000");

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                            LOGGER.info("Record: {}", record);
                            boolean isChange = false;
                            boolean canSend = true;
                            JSONObject recordValue = JSONObject.parseObject(record.value());
                            JSONObject payload = recordValue.getJSONObject("payload");
                            JSONObject after = payload.getJSONObject("after");
                            JSONObject source = payload.getJSONObject("source");
                            if (after != null) {
                                for (Map.Entry<String, Object> stringObjectEntry : after.entrySet()) {
                                    if ("OUT_CLOB".equalsIgnoreCase(stringObjectEntry.getValue() + "")) {
                                        isChange = true;
                                        if (after.getString("id") == null || "".equals(after.getString("id"))) {
                                            canSend = false;
                                        }
                                        String sql = "SELECT " + stringObjectEntry.getKey() + " FROM " + source.getString("schema") + "." + source.getString("table") + " WHERE LA_ID = '" + after.getString("LA_ID") + "'";
                                        Map<String, Object> stringObjectMap = JdbcQueryUtils.queryOne(sql);
                                        assert stringObjectMap != null;
                                        after.put(stringObjectEntry.getKey(), stringObjectMap.get(stringObjectEntry.getKey()));
                                    }
                                    if ("id".equalsIgnoreCase(stringObjectEntry.getKey())) {
                                        String idSql = "SELECT id FROM " + source.getString("schema") + "." + source.getString("table") + " WHERE LA_ID = '" + after.getString("LA_ID") + "'";
                                        Map<String, Object> stringObjectMap = JdbcQueryUtils.queryOne(idSql);
                                        assert stringObjectMap != null;
                                        Object value = stringObjectMap.get(stringObjectEntry.getKey());
                                        if (value != null) {
                                            after.put(stringObjectEntry.getKey(), value.toString().trim());
                                        }
                                    }
                                }
                            }
                            String message = record.value();
                            if (isChange) {
                                message = recordValue.toString();
                            }
                            if (canSend) {
                                kakfaCdcProducer.sendMessage(topic, record.key(), message);
                            }
                        }
                )
                .using((success, message, error) -> {
                    if (!success && error != null) {
                        LOGGER.error("Process status [ false ] with error message [ {} ], full stack trace: ", message, error);
                    }
                    closeEngine(engine);
                })
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        consumerExecutor.execute(() ->{
            kafkaCdcConsumer.subscribe(topic);
        });
    }

    private static void closeEngine(DebeziumEngine<ChangeEvent<String, String>> engine) {
        try {
            engine.close();
        } catch (IOException ignored) {
        }
    }

    private static String getCurrentDateString() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMdd");
        return dateFormat.format(new Date());
    }
}
