package com.pain.kafka.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    /**
     * 消费者客户端连接 Kafka 集群所需的 broker 地址清单。这里并非需要所有的 broker 地址，因为生产者会从给定的 broker 里查找到其他 broker 的信息
     */
    public static final String BROKER_LIST = "node01:9092";

    public static final String TOPIC = "topic-demo-1020";

    public static final String GROUP_ID = "topic-demo-1020-group";

    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        // 指定 key 和 value 序列化操作的序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设定 KafkaConsumer 对应的客户端 id
        // 如果客户端不设置，则 KafkaConsumer 会自动生成一个非空字符串
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.basic");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return props;
    }

    public static void main(String[] args) {
        Properties config = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // 订阅某些主题的特定分区
        // consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0)));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord consumerRecord : consumerRecords) {
                    logger.info("topic: {}, partition: {}, offset: {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                    logger.info("key: {}, value: {}", consumerRecord.key(), consumerRecord.value());
                    System.out.println(String.format("key: %s, value: %s", consumerRecord.key(), consumerRecord.value()));
                }
            }
        } catch (Exception ex) {
            logger.error("occur exception: ", ex);
        } finally {
            consumer.close();
        }
    }
}
