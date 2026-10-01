package com.mqtest.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * rate 제어 발행 루프. payload 의 send 시각에는 <b>계획 시각</b>을 넣는다. 발행이 밀려도(스톨, GC 등)
 * latency 가 계획 시각 기준으로 계산되어 지연이 숨겨지지 않는다(coordinated omission 방지).
 */
public final class PacedLoop {

    /** 한 메시지를 실제로 발행한 직후 호출된다. lag = actual - scheduled. */
    @FunctionalInterface
    public interface SendListener {
        void onSent(long scheduledNanos, long actualNanos);
    }

    private static final long SPIN_THRESHOLD_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

    private PacedLoop() {
    }

    /**
     * @param sequence    producer 들이 공유하는 메시지 번호(= 총 발행 수 상한 관리)
     * @param messageCount 총 발행 상한
     * @return 이 루프가 발행한 메시지 수
     */
    public static long run(MessagePublisher publisher, RatePacer pacer, int messageSize, AtomicLong sequence,
                           long messageCount, long deadlineNanos, SendListener listener) {
        long sent = 0;
        for (long n = 0; ; n++) {
            long scheduled = pacer.scheduledAt(n);
            if (scheduled >= deadlineNanos) {
                break;
            }
            long seq = sequence.getAndIncrement();
            if (seq >= messageCount) {
                break;
            }
            sleepUntil(scheduled);
            long actual = System.nanoTime();
            publisher.publish(Payload.encode(seq, scheduled, messageSize));
            listener.onSent(scheduled, actual);
            sent++;
        }
        return sent;
    }

    public static void sleepUntilPublic(long targetNanos) {
        sleepUntil(targetNanos);
    }

    static void sleepUntil(long targetNanos) {
        long remaining;
        while ((remaining = targetNanos - System.nanoTime()) > 0) {
            if (remaining > SPIN_THRESHOLD_NANOS) {
                LockSupport.parkNanos(remaining - SPIN_THRESHOLD_NANOS);
            } else {
                Thread.onSpinWait();
            }
        }
    }
}
