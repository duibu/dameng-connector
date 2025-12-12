package org.devlive.connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KakfaCdcProducer {

    private KafkaProducer<String, String> producer;

    public KakfaCdcProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 等待所有副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试次数
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 批量发送延迟
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // 压缩类型

        this.producer = new KafkaProducer<>(props);
        System.out.println("✓ Kafka 生产者已初始化");
    }

    /**
     * 发送消息到指定 topic
     * @param topic 目标 topic
     * @param key 消息键
     * @param value 消息值
     */
    public void sendMessage(String topic, String key, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // 同步发送
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();

            System.out.printf("✓ 消息已发送 - Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                    metadata.topic(), metadata.partition(), metadata.offset(), key, value);

        } catch (InterruptedException | ExecutionException e) {
            System.err.println("✗ 发送消息失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 异步发送消息
     * @param topic 目标 topic
     * @param key 消息键
     * @param value 消息值
     */
    public void sendMessageAsync(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("✗ 异步发送失败: " + exception.getMessage());
            } else {
                System.out.printf("✓ 异步消息已发送 - Topic: %s, Partition: %d, Offset: %d%n",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * 关闭生产者
     */
    public void close() {
        if (producer != null) {
            producer.close();
            System.out.println("✓ 生产者已关闭");
        }
    }
    
}
