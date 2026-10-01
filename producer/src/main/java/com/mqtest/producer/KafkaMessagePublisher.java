package com.mqtest.producer;

import com.mqtest.common.MessagePublisher;
import com.mqtest.common.Scenario;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public final class KafkaMessagePublisher implements MessagePublisher {

    private final DefaultKafkaProducerFactory<String, byte[]> factory;
    private final KafkaTemplate<String, byte[]> template;
    private final String topic;

    public KafkaMessagePublisher(Scenario.KafkaOptions options, String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, options.acks());
        props.put(ProducerConfig.LINGER_MS_CONFIG, options.lingerMs());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, options.batchSizeBytes());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, options.compression());
        // acks=all 에서 재시도 중복을 막아 유실/중복 측정을 오염시키지 않는다. acks=0/1 은 멱등성 불가.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "all".equals(options.acks()) || "-1".equals(options.acks()));
        this.factory = new DefaultKafkaProducerFactory<>(props);
        this.template = new KafkaTemplate<>(factory);
        this.topic = topic;
    }

    @Override
    public void publish(byte[] payload) {
        template.send(topic, payload);
    }

    @Override
    public void flush() {
        template.flush();
    }

    @Override
    public void close() {
        template.flush();
        factory.destroy();
    }
}
