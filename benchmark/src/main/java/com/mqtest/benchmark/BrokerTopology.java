package com.mqtest.benchmark;

import com.mqtest.common.Scenario;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

/** Run 마다 topic / queue 를 새로 만들고 끝나면 삭제해 Run 간 간섭을 없앤다. */
final class BrokerTopology {

    private BrokerTopology() {
    }

    private static final long KAFKA_MIN_RETENTION_BYTES = 256L * 1024 * 1024;
    private static final long KAFKA_SEGMENT_BYTES = 128L * 1024 * 1024;

    static void createKafkaTopic(Scenario.KafkaOptions o, String topic) throws ExecutionException, InterruptedException {
        try (AdminClient admin = AdminClient.create(kafkaAdmin(o))) {
            // 디스크 보호: Kafka 는 소비한 메시지도 보존 기간 동안 로그에 남긴다. 시간 기반 실험은 수십 GB 를 쓸 수 있어
            // (Docker VM 디스크 여유가 작다) 토픽 전체 kafka.retentionBytes(기본 약 4GB)를 넘는 오래된 세그먼트는 지운다. retention.bytes 는 partition 당 값이다.
            // 주의: consumer 가 이 보존량보다 더 뒤처지면(lag > retentionBytes) 소비 전에 지워진 메시지가 생긴다.
            // 그런 과부하 실험은 messageCount 상한으로 총량을 제한한다.
            long perPartition = Math.max(KAFKA_MIN_RETENTION_BYTES, o.retentionBytes() / o.partitions());
            NewTopic newTopic = new NewTopic(topic, o.partitions(), o.replicationFactor().shortValue()).configs(Map.of(
                    "retention.bytes", Long.toString(perPartition),
                    "segment.bytes", Long.toString(KAFKA_SEGMENT_BYTES),
                    // 토픽 삭제 후 파일이 실제로 지워지기까지 기본 60초가 걸려, 연속 Run 에서 이전 Run 의 데이터가 디스크에 겹쳐 남는다.
                    "file.delete.delay.ms", "1000"));
            admin.createTopics(List.of(newTopic)).all().get();
        }
    }

    static void deleteKafkaTopic(Scenario.KafkaOptions o, String topic) {
        try (AdminClient admin = AdminClient.create(kafkaAdmin(o))) {
            admin.deleteTopics(List.of(topic)).all().get();
        } catch (Exception e) {
            // 정리 실패는 결과에 영향이 없으므로 무시한다.
        }
    }

    static void declareRabbitQueue(Scenario.RabbitOptions o, String queue) {
        CachingConnectionFactory cf = rabbitFactory(o);
        try {
            QueueBuilder builder = QueueBuilder.durable(queue);
            if (o.queueType().equals("quorum")) {
                builder.quorum();
            }
            Queue q = builder.build();
            new RabbitAdmin(cf).declareQueue(q);
        } finally {
            cf.destroy();
        }
    }

    static void deleteRabbitQueue(Scenario.RabbitOptions o, String queue) {
        CachingConnectionFactory cf = rabbitFactory(o);
        try {
            new RabbitAdmin(cf).deleteQueue(queue);
        } catch (Exception e) {
            // 무시
        } finally {
            cf.destroy();
        }
    }

    private static Map<String, Object> kafkaAdmin(Scenario.KafkaOptions o) {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, o.bootstrapServers());
        return props;
    }

    private static CachingConnectionFactory rabbitFactory(Scenario.RabbitOptions o) {
        CachingConnectionFactory cf = new CachingConnectionFactory(o.host(), o.port());
        cf.setUsername(o.username());
        cf.setPassword(o.password());
        return cf;
    }
}
