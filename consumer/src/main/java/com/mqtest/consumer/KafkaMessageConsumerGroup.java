package com.mqtest.consumer;

import com.mqtest.common.MessageConsumerGroup;
import com.mqtest.common.Scenario;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

/** 하나의 consumer group 안에서 {@code consumers} 개의 스레드가 partition 을 나눠 소비한다. */
public final class KafkaMessageConsumerGroup implements MessageConsumerGroup {

    private final ConcurrentMessageListenerContainer<String, byte[]> container;

    public KafkaMessageConsumerGroup(Scenario.KafkaOptions options, String topic, String groupId, int consumers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        ContainerProperties containerProps = new ContainerProperties(topic);
        this.container = new ConcurrentMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<>(props), containerProps);
        this.container.setConcurrency(consumers);
    }

    @Override
    public void start(Consumer<byte[]> handler) {
        container.getContainerProperties().setMessageListener(
                (MessageListener<String, byte[]>) record -> handler.accept(record.value()));
        container.start();
    }

    @Override
    public void close() {
        container.stop();
    }
}
