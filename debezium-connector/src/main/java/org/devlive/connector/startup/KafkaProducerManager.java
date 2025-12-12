package org.devlive.connector.startup;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaProducerManager {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerManager.class);
    private static volatile KafkaProducerManager instance;
    private static final String CONFIG_FILE_NAME = "kafka-producer.properties";

    private final KafkaProducer<String, String> producer;
    private final ExecutorService callbackExecutor;

    private KafkaProducerManager(Properties props) {
        this.producer = new KafkaProducer<>(props);
        this.callbackExecutor = Executors.newFixedThreadPool(4);
    }

    public static KafkaProducerManager getInstance() {
        if (instance == null) {
            synchronized (KafkaProducerManager.class) {
                if (instance == null) {
                    Properties props = new Properties();
                    try (InputStream input = loadConfigStream()) {
                        if (input == null) {
                            throw new RuntimeException("Failed to find configuration file: " + CONFIG_FILE_NAME);
                        }
                        props.load(input);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to load producer properties", e);
                    }
                    instance = new KafkaProducerManager(props);
                }
            }
        }
        return instance;
    }
    
    /**
     * 核心加载逻辑：优先文件系统，然后是 Classpath
     * @return 找到的配置文件的输入流，如果未找到则返回 null
     */
    private static InputStream loadConfigStream() {
        // 1. 尝试从文件系统加载 (原逻辑)
        try {
            // 注意：这里使用 try-with-resources 可以确保流被关闭，但在返回前要小心
            // 简单起见，我们先获取流，由上层 try-with-resources 关闭
            // 尝试直接通过文件路径加载（通常是当前工作目录）
            InputStream fileStream = new FileInputStream(CONFIG_FILE_NAME);
            System.out.println("INFO: Loaded producer properties from File System.");
            return fileStream;
        } catch (IOException fileNotFound) {
            // 文件系统加载失败，继续尝试 Classpath
            System.out.println("WARN: File System load failed. Trying Classpath...");
        }

        // 2. 尝试从 Classpath 加载
        // 使用当前类的 ClassLoader 来获取资源
        InputStream classPathStream = KafkaProducerManager.class.getClassLoader().getResourceAsStream(CONFIG_FILE_NAME);

        if (classPathStream != null) {
            System.out.println("INFO: Loaded producer properties from Classpath.");
            return classPathStream;
        }

        // 3. 都没有找到
        System.err.println("ERROR: Configuration file " + CONFIG_FILE_NAME + " not found in File System or Classpath.");
        return null;
    }

    public void sendMessage(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Failed to send message: topic={}, key={}", topic, key, exception);
                // 实现重试或 DLQ 逻辑
            } else {
                LOGGER.debug("Message sent successfully: topic={}, partition={}, offset={}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    public void close() {
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(30));
            callbackExecutor.shutdown();
            if (!callbackExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                callbackExecutor.shutdownNow();
            }
        } catch (Exception e) {
            LOGGER.error("Error closing producer", e);
        }
    }
    
}
