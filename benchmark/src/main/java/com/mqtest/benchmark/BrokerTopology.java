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

    static void createKafkaTopic(Scenario.KafkaOptions o, String topic) throws ExecutionException, InterruptedException {
        try (AdminClient admin = AdminClient.create(kafkaAdmin(o))) {
            NewTopic newTopic = new NewTopic(topic, o.partitions(), o.replicationFactor().shortValue());
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
