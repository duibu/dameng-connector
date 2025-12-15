package org.devlive.connector.startup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private volatile boolean running = false;

    public KafkaConsumerService(Properties props, String topic) {
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        running = true;
        consumer.subscribe(Collections.singletonList(topic));

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
        // 实现你的业务逻辑
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }
    
}
