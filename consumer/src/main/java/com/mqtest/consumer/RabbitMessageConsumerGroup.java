package com.mqtest.consumer;

import com.mqtest.common.MessageConsumerGroup;
import com.mqtest.common.Scenario;
import java.util.function.Consumer;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

/** 하나의 queue 를 {@code consumers} 개의 consumer 가 경쟁 소비한다(push, prefetch 적용). */
public final class RabbitMessageConsumerGroup implements MessageConsumerGroup {

    private final CachingConnectionFactory connectionFactory;
    private final SimpleMessageListenerContainer container;

    public RabbitMessageConsumerGroup(Scenario.RabbitOptions options, String queue, int consumers) {
        this.connectionFactory = new CachingConnectionFactory(options.host(), options.port());
        connectionFactory.setUsername(options.username());
        connectionFactory.setPassword(options.password());
        this.container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(queue);
        container.setConcurrentConsumers(consumers);
        container.setPrefetchCount(options.prefetch());
    }

    @Override
    public void start(Consumer<byte[]> handler) {
        container.setMessageListener(message -> handler.accept(message.getBody()));
        container.start();
    }

    @Override
    public void close() {
        container.stop();
        connectionFactory.destroy();
    }
}
