package com.mqtest.common;

import java.util.function.Consumer;

/** 브로커 중립 소비 추상화. 시나리오의 consumers 수만큼의 병렬 소비자를 관리한다. */
public interface MessageConsumerGroup extends AutoCloseable {

    void start(Consumer<byte[]> handler);

    @Override
    void close();
}
