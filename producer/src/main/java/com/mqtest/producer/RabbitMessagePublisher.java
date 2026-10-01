package com.mqtest.producer;

import com.mqtest.common.MessagePublisher;
import com.mqtest.common.Scenario;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/** 기본 exchange("") 로 queue 이름을 routing key 로 직접 발행한다. */
public final class RabbitMessagePublisher implements MessagePublisher {

    private static final long CONFIRM_TIMEOUT_MS = 10_000;

    private final CachingConnectionFactory connectionFactory;
    private final RabbitTemplate template;
    private final String queue;
    private final boolean confirms;

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
    }

    @Override
    public void publish(byte[] payload) {
        var message = MessageBuilder.withBody(payload)
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .build();
        if (confirms) {
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

    @Override
    public void flush() {
        // RabbitTemplate 은 client 측 배치 버퍼가 없다.
    }

    @Override
    public void close() {
        connectionFactory.destroy();
    }
}
