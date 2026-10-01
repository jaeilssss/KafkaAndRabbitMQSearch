package com.mqtest.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PayloadTest {

    @Test
    void roundTripsHeaderAndKeepsRequestedSize() {
        byte[] bytes = Payload.encode(42, 123_456_789L, 100);

        assertThat(bytes).hasSize(100);
        assertThat(Payload.sequence(bytes)).isEqualTo(42);
        assertThat(Payload.sendNanos(bytes)).isEqualTo(123_456_789L);
    }
}
