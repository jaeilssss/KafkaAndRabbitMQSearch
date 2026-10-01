package com.mqtest.common;

/** 브로커 중립 발행 추상화. producer 1개(=연결/클라이언트 1개)에 구현체 1개. */
public interface MessagePublisher extends AutoCloseable {

    void publish(byte[] payload);

    /** 버퍼에 남은 메시지를 모두 브로커로 전송한다. */
    void flush();

    @Override
    void close();
}
