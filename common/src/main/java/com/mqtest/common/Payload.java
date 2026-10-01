package com.mqtest.common;

import java.nio.ByteBuffer;

/** 메시지 레이아웃: [sendNanos:8][sequence:8][padding...]. 두 브로커에서 동일하게 사용한다. */
public final class Payload {

    private Payload() {
    }

    public static byte[] encode(long sequence, long sendNanos, int size) {
        byte[] bytes = new byte[size];
        ByteBuffer.wrap(bytes).putLong(sendNanos).putLong(sequence);
        return bytes;
    }

    public static long sendNanos(byte[] payload) {
        return ByteBuffer.wrap(payload).getLong(0);
    }

    public static long sequence(byte[] payload) {
        return ByteBuffer.wrap(payload).getLong(8);
    }
}
