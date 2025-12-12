package org.devlive.connector;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaCdcConsumer {
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    public KafkaCdcConsumer(String groupId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 从最早的消息开始消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // 自动提交偏移量
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // 每次拉取最大记录数

        this.consumer = new KafkaConsumer<>(props);
        System.out.println("✓ Kafka 消费者已初始化 - Group ID: " + groupId);
    }

    /**
     * 订阅多个 topic 并消费消息
     * @param topics topic 列表
     */
    public void subscribe(String... topics) {
        consumer.subscribe(Arrays.asList(topics));
        System.out.println("✓ 已订阅 topics: " + Arrays.toString(topics));

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("✓ 收到消息 - Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("✗ 消费消息时出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            close();
        }
    }

    /**
     * 停止消费
     */
    public void stop() {
        running = false;
    }

    /**
     * 关闭消费者
     */
    public void close() {
        if (consumer != null) {
            consumer.close();
            System.out.println("✓ 消费者已关闭");
        }
    }
}
