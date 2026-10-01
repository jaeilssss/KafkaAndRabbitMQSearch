package com.mqtest.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PacedLoopTest {

    /** 첫 발행이 200ms 막히는 가짜 publisher. publish 가 반환되는 시각을 "수신 시각"으로 보고 latency 를 기록한다. */
    private static final class StallingPublisher implements MessagePublisher {
        final List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>());
        private boolean first = true;

        @Override
        public void publish(byte[] payload) {
            if (first) {
                first = false;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            latenciesNanos.add(System.nanoTime() - Payload.sendNanos(payload));
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    @Test
    void latencyIsMeasuredFromScheduledTimeSoStallsAreNotHidden() {
        StallingPublisher publisher = new StallingPublisher();
        LatencyStats lag = new LatencyStats();
        long start = System.nanoTime();
        RatePacer pacer = RatePacer.split(100, 1, start).get(0); // 10ms 간격

        PacedLoop.run(publisher, pacer, 64, new AtomicLong(), Long.MAX_VALUE,
                start + TimeUnit.MILLISECONDS.toNanos(500),
                (scheduled, actual) -> lag.recordNanos(actual - scheduled));

        assertThat(publisher.latenciesNanos.size()).isGreaterThan(10);
        // 첫 메시지는 막힌 시간(≥200ms)을 포함한다.
        assertThat(publisher.latenciesNanos.get(0)).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(200));
        // 뒤따르는 메시지는 밀려서 늦게 나갔지만 계획 시각 기준이므로 큰 latency 를 가진다(실제 발행 시각 기준이면 ~0).
        for (int i = 1; i <= 10; i++) {
            assertThat(publisher.latenciesNanos.get(i)).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(100));
        }
        assertThat(lag.snapshot().p99Ms()).isGreaterThanOrEqualTo(150.0);
    }

    @Test
    void stopsAtMessageCountAndPublishesAtScheduledRate() {
        List<Long> sentAt = Collections.synchronizedList(new ArrayList<>());
        MessagePublisher publisher = new MessagePublisher() {
            public void publish(byte[] payload) {
                sentAt.add(System.nanoTime());
            }

            public void flush() {
            }

            public void close() {
            }
        };
        long start = System.nanoTime();
        RatePacer pacer = RatePacer.split(500, 1, start).get(0);

        long sent = PacedLoop.run(publisher, pacer, 64, new AtomicLong(), 50,
                start + TimeUnit.SECONDS.toNanos(10), (s, a) -> { });

        assertThat(sent).isEqualTo(50);
        double elapsedMs = (sentAt.get(49) - start) / 1e6;
        assertThat(elapsedMs).isBetween(90.0, 130.0); // 49 간격 x 2ms ≈ 98ms
    }
}
