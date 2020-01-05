package com.pain.kafka.basic;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    /**
     * 生产者客户端连接 Kafka 集群所需的 broker 地址清单。这里并非需要所有的 broker 地址，因为生产者会从给定的 broker 里查找到其他 broker 的信息
     */
    public static final String BROKER_LIST = "node01:9092";

    public static final String TOPIC = "topic-demo-1020";

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        // 指定 key 和 value 序列化操作的序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, HashPartitioner.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PrefixProducerInterceptor.class.getName());

        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 设定 KafkaProducer 对应的客户端 id
        // 如果客户端不设置，则 KafkaProducer 会自动生成一个非空字符串
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.basic");
        return props;
    }

    private static KafkaProducer<String, String> producer;

    /**
     * 发后即忘
     * @param producer
     */
    public static void sendAndForget(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "Hello kafka");
        producer.send(record);
        producer.close();
    }

    /**
     * 同步的发送方式
     * @param producer
     */
    public static void syncSend(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "Hello sync kafka");
        try {
            // RecordMetadata 对象里包含了消息的一些元数据信息，比如当前消息的主题、分区号、分区中的偏移量（offset）、时间戳等
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println(String.format("topic: %s, partition: %s, offset: %d", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void asyncSend(KafkaProducer<String, String> producer) {

        for (int i = 0; i < 10; ++i) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.format("Hello async kafka, count: %s", i));

            // 在返回响应时调用该函数来实现异步的发送确认
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.println(String.format("topic: %s, partition: %s, offset: %d", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()));
                    }
                }
            });
        }

        producer.close();
    }

    public static void main(String[] args) {
        Properties config = initConfig();

        // KafkaProducer 是线程安全的，可以在多个线程中共享单个 KafkaProducer 实例
        producer = new KafkaProducer<>(config);
//        sendAndForget(producer);
//        syncSend(producer);
        asyncSend(producer);
    }

}
