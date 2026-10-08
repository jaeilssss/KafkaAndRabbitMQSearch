package com.mqtest.producer;

import com.mqtest.common.MessagePublisher;
import com.mqtest.common.Scenario;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/** 기본 exchange("") 로 queue 이름을 routing key 로 직접 발행한다. */
public final class RabbitMessagePublisher implements MessagePublisher {

    private static final long CONFIRM_TIMEOUT_MS = 10_000;

    private final CachingConnectionFactory connectionFactory;
    private final RabbitTemplate template;
    private final String queue;
    private final boolean confirms;
    private final int confirmBatchSize;
    // confirmBatchSize > 1 일 때만 쓴다. producer 1개 = 스레드 1개이므로 동기화하지 않는다.
    private Connection batchConnection;
    private Channel batchChannel;
    private int unconfirmed;

    public RabbitMessagePublisher(Scenario.RabbitOptions options, String queue) {
        this.connectionFactory = new CachingConnectionFactory(options.host(), options.port());
        connectionFactory.setUsername(options.username());
        connectionFactory.setPassword(options.password());
        if (options.publisherConfirms()) {
            connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.SIMPLE);
        }
        this.template = new RabbitTemplate(connectionFactory);
        this.queue = queue;
        this.confirms = options.publisherConfirms();
        this.confirmBatchSize = options.confirmBatchSize();
    }

    @Override
    public void publish(byte[] payload) {
        var message = MessageBuilder.withBody(payload)
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .build();
        if (confirms && confirmBatchSize > 1) {
            publishBatched(payload);
        } else if (confirms) {
            // 메시지 단위로 broker confirm 을 기다린다(가장 보수적인 내구성 설정, 비용이 크다).
            template.invoke(t -> {
                t.send("", queue, message);
                t.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
                return null;
            });
        } else {
            template.send("", queue, message);
        }
    }

    /**
     * 같은 channel 로 confirmBatchSize 건을 보낸 뒤 한 번에 confirm 을 기다린다(RabbitMQ 튜토리얼의 batch confirm).
     * 메시지마다 왕복을 기다리는 방식보다 훨씬 처리량이 높고, 실제 고성능 클라이언트의 일반적인 사용법에 가깝다.
     */
    private void publishBatched(byte[] payload) {
        try {
            if (batchChannel == null) {
                batchConnection = connectionFactory.createConnection();
                batchChannel = batchConnection.createChannel(false); // confirm type 설정으로 confirm 모드가 켜진다
            }
            batchChannel.basicPublish("", queue, new AMQP.BasicProperties.Builder().deliveryMode(2).build(), payload);
            if (++unconfirmed >= confirmBatchSize) {
                awaitConfirms();
            }
        } catch (IOException e) {
            throw new IllegalStateException("rabbitmq publish failed", e);
        }
    }

    private void awaitConfirms() {
        try {
            batchChannel.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
            unconfirmed = 0;
        } catch (IOException | InterruptedException | java.util.concurrent.TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("rabbitmq confirm failed", e);
        }
    }

    @Override
    public void flush() {
        // batch confirm 모드에서는 마지막 배치의 confirm 을 기다려 발행 건수와 확인 건수를 맞춘다.
        if (batchChannel != null && unconfirmed > 0) {
            awaitConfirms();
        }
    }

    @Override
    public void close() {
        try {
            if (batchChannel != null) {
                batchChannel.close();
            }
        } catch (Exception e) {
            // 정리 실패는 결과에 영향이 없다.
        }
        if (batchConnection != null) {
            batchConnection.close();
        }
        connectionFactory.destroy();
    }
}
