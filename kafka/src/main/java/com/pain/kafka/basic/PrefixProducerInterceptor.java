package com.pain.kafka.basic;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class PrefixProducerInterceptor implements ProducerInterceptor<String, String> {

    private volatile long success = 0;
    private volatile long failure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = "prefix-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), value, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            ++success;
        } else {
            ++failure;
        }
    }

    @Override
    public void close() {
        double ratio = (success * 1.0) / (success + failure);
        System.out.println(String.format("send success percent: %.2f%%", ratio * 100));
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
